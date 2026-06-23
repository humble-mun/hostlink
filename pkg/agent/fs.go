package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-logr/logr"

	"github.com/humble-mun/hostlink/pkg/agentapi"
	hostlinkv1 "github.com/humble-mun/hostlink/pkg/api/hostlink/v1"
)

// fsChunkSize bounds each fs.read/fs.write body chunk. Keeping it well under the
// gRPC max message size means a transfer of any length streams without buffering
// the whole file or raising message limits.
const fsChunkSize = 64 * 1024

// fsUploadIdleTimeout bounds how long an in-flight upload waits for the next body
// chunk before aborting. It bounds the resource use of an upload whose controller
// side gave up (e.g. the HTTP client disconnected) without sending a terminal
// chunk, while staying generous enough not to trip a merely slow transfer.
const fsUploadIdleTimeout = 2 * time.Minute

var (
	// errFsNotConfigured is returned by resolve when no working directory is set
	// (the --data-dir flag is empty), disabling the fs API.
	errFsNotConfigured = errors.New("agent working directory is not configured")
	// errPathEscapes is returned when a request path would resolve outside the
	// working directory.
	errPathEscapes = errors.New("path escapes the working directory")
)

// resolveDataDir validates the configured working directory and returns its
// absolute path. An empty value disables the fs API and is not an error.
func resolveDataDir(configured string) (dir string, err error) {
	if configured == "" {
		return
	}
	if dir, err = filepath.Abs(configured); err != nil {
		err = fmt.Errorf("agent: resolve data dir %q: %w", configured, err)
		return
	}
	var info os.FileInfo
	if info, err = os.Stat(dir); err != nil {
		err = fmt.Errorf("agent: stat data dir %q: %w", dir, err)
		return
	}
	if !info.IsDir() {
		err = fmt.Errorf("agent: data dir %q is not a directory", dir)
	}
	return
}

// resolve maps a request-relative path to an absolute path inside the working
// directory, rejecting anything that would escape it. Cleaning the path rooted at
// "/" collapses any leading ".." before it is joined to the root, and the prefix
// check is a second guard against escape.
func (a *agent) resolve(rel string) (full string, err error) {
	if a.dataDir == "" {
		err = errFsNotConfigured
		return
	}
	clean := filepath.Clean("/" + filepath.ToSlash(rel))
	full = filepath.Join(a.dataDir, filepath.FromSlash(clean))
	if full != a.dataDir && !strings.HasPrefix(full, a.dataDir+string(os.PathSeparator)) {
		err = errPathEscapes
	}
	return
}

// fsErrorCode maps an fs error to the HTTP-mirrored code and message the REST
// layer returns. It recognizes the path guard errors and the os not-exist /
// already-exist sentinels; anything else is an internal error.
func fsErrorCode(err error) (code uint32, msg string) {
	switch {
	case errors.Is(err, errFsNotConfigured):
		code = http.StatusNotImplemented
	case errors.Is(err, errPathEscapes):
		code = http.StatusBadRequest
	case errors.Is(err, os.ErrNotExist):
		code = http.StatusNotFound
	case errors.Is(err, os.ErrExist):
		code = http.StatusConflict
	default:
		code = http.StatusInternalServerError
	}
	msg = err.Error()
	return
}

// fsEntryOf projects a FileInfo to the wire FsEntry shape.
func fsEntryOf(info os.FileInfo) agentapi.FsEntry {
	return agentapi.FsEntry{
		Name:    info.Name(),
		Dir:     info.IsDir(),
		Size:    info.Size(),
		ModTime: info.ModTime(),
	}
}

// statPath serves MethodFsStat: it returns the FsEntry metadata for a single path.
func (a *agent) statPath(req *hostlinkv1.AgentRequest) (payload []byte, code uint32, errMsg string) {
	var fpr agentapi.FsPathRequest
	var err error
	if err = json.Unmarshal(req.GetPayload(), &fpr); err != nil {
		code, errMsg = http.StatusBadRequest, "invalid fs.stat payload: "+err.Error()
		return
	}
	var full string
	if full, err = a.resolve(fpr.Path); err != nil {
		code, errMsg = fsErrorCode(err)
		return
	}
	var info os.FileInfo
	if info, err = os.Stat(full); err != nil {
		code, errMsg = fsErrorCode(err)
		return
	}
	if payload, err = json.Marshal(fsEntryOf(info)); err != nil {
		a.logger.Error(err, "marshal fs.stat result failed", "path", fpr.Path)
		code, errMsg = http.StatusInternalServerError, err.Error()
		return
	}
	code = http.StatusOK
	return
}

// listDir serves MethodFsList: the immediate (non-recursive) entries of a directory.
func (a *agent) listDir(req *hostlinkv1.AgentRequest) (payload []byte, code uint32, errMsg string) {
	var fpr agentapi.FsPathRequest
	var err error
	if err = json.Unmarshal(req.GetPayload(), &fpr); err != nil {
		code, errMsg = http.StatusBadRequest, "invalid fs.list payload: "+err.Error()
		return
	}
	var full string
	if full, err = a.resolve(fpr.Path); err != nil {
		code, errMsg = fsErrorCode(err)
		return
	}
	var dirEntries []os.DirEntry
	if dirEntries, err = os.ReadDir(full); err != nil {
		code, errMsg = fsErrorCode(err)
		return
	}
	result := agentapi.FsListResult{Entries: make([]agentapi.FsEntry, 0, len(dirEntries))}
	for _, de := range dirEntries {
		var info os.FileInfo
		if info, err = de.Info(); err != nil {
			// The entry vanished between ReadDir and Info; skip it rather than
			// failing the whole listing.
			continue
		}
		result.Entries = append(result.Entries, fsEntryOf(info))
	}
	if payload, err = json.Marshal(result); err != nil {
		a.logger.Error(err, "marshal fs.list result failed", "path", fpr.Path)
		code, errMsg = http.StatusInternalServerError, err.Error()
		return
	}
	code = http.StatusOK
	return
}

// mkdir serves MethodFsMkdir: it creates a directory and any missing parents. An
// existing target is reported as a conflict so the create is not silently
// idempotent.
func (a *agent) mkdir(req *hostlinkv1.AgentRequest) (code uint32, errMsg string) {
	var fpr agentapi.FsPathRequest
	var err error
	if err = json.Unmarshal(req.GetPayload(), &fpr); err != nil {
		code, errMsg = http.StatusBadRequest, "invalid fs.mkdir payload: "+err.Error()
		return
	}
	var full string
	if full, err = a.resolve(fpr.Path); err != nil {
		code, errMsg = fsErrorCode(err)
		return
	}
	_, err = os.Stat(full)
	switch {
	case err == nil:
		code, errMsg = http.StatusConflict, "path already exists"
		return
	case errors.Is(err, os.ErrNotExist):
		// Expected: the target does not exist yet, proceed to create it.
	default:
		code, errMsg = fsErrorCode(err)
		return
	}
	if err = os.MkdirAll(full, 0o755); err != nil {
		code, errMsg = fsErrorCode(err)
		return
	}
	code = http.StatusCreated
	return
}

// remove serves MethodFsRemove: it deletes a path, recursively for directories.
// Removing the working-directory root itself is refused.
func (a *agent) remove(req *hostlinkv1.AgentRequest) (code uint32, errMsg string) {
	var fpr agentapi.FsPathRequest
	var err error
	if err = json.Unmarshal(req.GetPayload(), &fpr); err != nil {
		code, errMsg = http.StatusBadRequest, "invalid fs.remove payload: "+err.Error()
		return
	}
	var full string
	if full, err = a.resolve(fpr.Path); err != nil {
		code, errMsg = fsErrorCode(err)
		return
	}
	if full == a.dataDir {
		code, errMsg = http.StatusBadRequest, "cannot remove the working directory root"
		return
	}
	if _, err = os.Stat(full); err != nil {
		code, errMsg = fsErrorCode(err)
		return
	}
	if err = os.RemoveAll(full); err != nil {
		code, errMsg = fsErrorCode(err)
		return
	}
	code = http.StatusNoContent
	return
}

// handleRead serves the streaming MethodFsRead: each file chunk is sent as an
// AgentProgress event correlated by request_id, then a terminal AgentResult
// reports the outcome. When the file cannot be opened no body frames are emitted,
// so the controller sees the error in the terminal frame before writing a status.
func (a *agent) handleRead(ctx context.Context, req *hostlinkv1.AgentRequest, logger logr.Logger) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	requestID := req.GetRequestId()
	emit := func(data []byte) (err error) {
		if err = a.send(&hostlinkv1.AgentEvent{
			AgentId: a.nodeName,
			Kind: &hostlinkv1.AgentEvent_Progress{Progress: &hostlinkv1.AgentProgress{
				RequestId: requestID,
				Payload:   data,
			}},
		}); err != nil {
			logger.Error(err, "send fs.read chunk failed", "requestID", requestID)
			cancel()
		}
		return
	}

	code, errMsg := a.readFile(ctx, req, emit)
	result := &hostlinkv1.AgentResult{RequestId: requestID, Code: code, Error: errMsg, Final: true}
	if err := a.send(&hostlinkv1.AgentEvent{
		AgentId: a.nodeName,
		Kind:    &hostlinkv1.AgentEvent_Result{Result: result},
	}); err != nil {
		logger.Error(err, "send fs.read result failed", "requestID", requestID)
	}
}

// readFile opens the requested file and streams it through emit in fsChunkSize
// pieces. emit fully consumes each slice before returning (the send marshals it
// synchronously), so the read buffer is safely reused. A pre-data error (missing
// path or a directory) returns the code with no frames emitted.
func (a *agent) readFile(ctx context.Context, req *hostlinkv1.AgentRequest, emit func([]byte) error) (code uint32, errMsg string) {
	var fpr agentapi.FsPathRequest
	var err error
	if err = json.Unmarshal(req.GetPayload(), &fpr); err != nil {
		code, errMsg = http.StatusBadRequest, "invalid fs.read payload: "+err.Error()
		return
	}
	var full string
	if full, err = a.resolve(fpr.Path); err != nil {
		code, errMsg = fsErrorCode(err)
		return
	}
	var f *os.File
	if f, err = os.Open(full); err != nil {
		code, errMsg = fsErrorCode(err)
		return
	}
	defer func() {
		if e := f.Close(); e != nil {
			a.logger.Error(e, "close fs.read file failed", "path", fpr.Path)
		}
	}()
	var info os.FileInfo
	if info, err = f.Stat(); err != nil {
		code, errMsg = fsErrorCode(err)
		return
	}
	if info.IsDir() {
		code, errMsg = http.StatusBadRequest, "path is a directory"
		return
	}

	buf := make([]byte, fsChunkSize)
	for {
		if err = ctx.Err(); err != nil {
			code, errMsg = http.StatusInternalServerError, err.Error()
			return
		}
		var n int
		n, err = f.Read(buf)
		if n > 0 {
			if e := emit(buf[:n]); e != nil {
				code, errMsg = http.StatusInternalServerError, e.Error()
				return
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			a.logger.Error(err, "read fs.read file failed", "path", fpr.Path)
			code, errMsg = http.StatusInternalServerError, err.Error()
			return
		}
	}
	code = http.StatusOK
	return
}

// uploadChunk is one body chunk delivered to an upload handler; last marks the
// final chunk of the request body.
type uploadChunk struct {
	data []byte
	last bool
}

// inboundStream is an in-flight upload's chunk channel. The handler owns its
// lifecycle: routeChunk only sends (racing done so it never stalls the receive
// loop), and the handler closes done when it finishes.
type inboundStream struct {
	ch   chan uploadChunk
	done chan struct{}
}

// registerInbound records the chunk channel for an upload request. It runs
// synchronously in the receive loop so a chunk that immediately follows the
// opening request always finds its channel.
func (a *agent) registerInbound(requestID string) (reg *inboundStream) {
	reg = &inboundStream{ch: make(chan uploadChunk, 8), done: make(chan struct{})}
	a.inboundMu.Lock()
	a.inbound[requestID] = reg
	a.inboundMu.Unlock()
	return
}

// routeChunk forwards a body chunk to its upload handler. A chunk for an unknown
// request_id (the handler already finished or errored) is dropped. The send races
// the handler's done signal so a handler that stopped reading cannot stall the
// shared receive loop.
func (a *agent) routeChunk(chunk *hostlinkv1.AgentRequestChunk) {
	a.inboundMu.Lock()
	reg, ok := a.inbound[chunk.GetRequestId()]
	a.inboundMu.Unlock()
	if !ok {
		return
	}
	select {
	case reg.ch <- uploadChunk{data: chunk.GetData(), last: chunk.GetLast()}:
	case <-reg.done:
	}
}

// deregisterInbound removes the upload's registration and unblocks any waiting
// routeChunk. Called once by the handler when it finishes.
func (a *agent) deregisterInbound(requestID string, reg *inboundStream) {
	a.inboundMu.Lock()
	if a.inbound[requestID] == reg {
		delete(a.inbound, requestID)
	}
	a.inboundMu.Unlock()
	close(reg.done)
}

// handleWrite serves the streaming MethodFsWrite: it consumes the body chunks for
// reg and sends back the terminal AgentResult. deregisterInbound runs on exit so
// any further chunks for this request are dropped instead of blocking.
func (a *agent) handleWrite(ctx context.Context, req *hostlinkv1.AgentRequest, reg *inboundStream, logger logr.Logger) {
	defer a.deregisterInbound(req.GetRequestId(), reg)

	code, errMsg := a.writeFile(ctx, req, reg)
	result := &hostlinkv1.AgentResult{RequestId: req.GetRequestId(), Code: code, Error: errMsg, Final: true}
	if err := a.send(&hostlinkv1.AgentEvent{
		AgentId: a.nodeName,
		Kind:    &hostlinkv1.AgentEvent_Result{Result: result},
	}); err != nil {
		logger.Error(err, "send fs.write result failed", "requestID", req.GetRequestId())
	}
}

// writeFile creates (or overwrites) the target file and writes the body chunks as
// they arrive. With Exclusive set, an existing target yields a conflict; missing
// parent directories are created so an upload to a/b/c.txt needs no prior mkdir.
// A partial write left by an error is removed.
func (a *agent) writeFile(ctx context.Context, req *hostlinkv1.AgentRequest, reg *inboundStream) (code uint32, errMsg string) {
	var fwr agentapi.FsWriteRequest
	var err error
	if err = json.Unmarshal(req.GetPayload(), &fwr); err != nil {
		code, errMsg = http.StatusBadRequest, "invalid fs.write payload: "+err.Error()
		return
	}
	var full string
	if full, err = a.resolve(fwr.Path); err != nil {
		code, errMsg = fsErrorCode(err)
		return
	}
	if err = os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		code, errMsg = fsErrorCode(err)
		return
	}

	flags := os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	if fwr.Exclusive {
		flags = os.O_CREATE | os.O_WRONLY | os.O_EXCL
	}
	var f *os.File
	if f, err = os.OpenFile(full, flags, 0o644); err != nil {
		code, errMsg = fsErrorCode(err)
		return
	}
	var committed bool
	defer func() {
		if e := f.Close(); e != nil {
			a.logger.Error(e, "close fs.write file failed", "path", fwr.Path)
		}
		if !committed {
			if e := os.Remove(full); e != nil && !errors.Is(e, os.ErrNotExist) {
				a.logger.Error(e, "remove partial fs.write file failed", "path", fwr.Path)
			}
		}
	}()

	committed, code, errMsg = a.consumeUpload(ctx, f, fwr.Path, reg)
	return
}

// consumeUpload writes the body chunks for reg to f until the terminal chunk
// (committed), the context ends, or the idle timeout fires. It reports whether
// the file was fully written so the caller can discard a partial write.
func (a *agent) consumeUpload(ctx context.Context, f *os.File, logPath string, reg *inboundStream) (committed bool, code uint32, errMsg string) {
	idle := time.NewTimer(fsUploadIdleTimeout)
	defer idle.Stop()
	for {
		select {
		case <-ctx.Done():
			code, errMsg = http.StatusInternalServerError, ctx.Err().Error()
			return
		case <-idle.C:
			code, errMsg = http.StatusRequestTimeout, "upload stalled: no data within idle timeout"
			return
		case chunk := <-reg.ch:
			resetIdleTimer(idle)
			if len(chunk.data) > 0 {
				if _, err := f.Write(chunk.data); err != nil {
					a.logger.Error(err, "write fs.write file failed", "path", logPath)
					code, errMsg = fsErrorCode(err)
					return
				}
			}
			if chunk.last {
				if err := f.Sync(); err != nil {
					a.logger.Error(err, "sync fs.write file failed", "path", logPath)
					code, errMsg = fsErrorCode(err)
					return
				}
				committed, code = true, http.StatusCreated
				return
			}
		}
	}
}

// resetIdleTimer drains a possibly-fired timer and restarts it for the next chunk.
func resetIdleTimer(t *time.Timer) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(fsUploadIdleTimeout)
}
