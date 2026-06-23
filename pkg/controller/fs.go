package controller

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"path"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/go-logr/logr"

	"github.com/humble-mun/hostlink/pkg/agentapi"
	hostlinkv1 "github.com/humble-mun/hostlink/pkg/api/hostlink/v1"
)

// fsGet handles GET /api/v1/agents/:agentId/fs. It stats the path first: a
// directory is listed; a file is returned as JSON metadata when the client
// accepts JSON, otherwise streamed as a download.
func (svc *service) fsGet(c *gin.Context) {
	agentID := c.Param("agentId")
	reqPath := c.Query("path")
	logger := svc.logger.WithName("fsGet").WithValues("agentID", agentID, "path", reqPath)

	payload, err := json.Marshal(agentapi.FsPathRequest{Path: reqPath})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "encode fs request failed"})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), dispatchTimeout)
	defer cancel()

	var stat *hostlinkv1.AgentResult
	if stat, err = svc.dispatch(ctx, agentID, agentapi.MethodFsStat, payload); err != nil {
		mapDispatchError(c, logger, err)
		return
	}
	if stat.GetCode() != http.StatusOK {
		respondAgentResult(c, stat)
		return
	}

	var entry agentapi.FsEntry
	if err = json.Unmarshal(stat.GetPayload(), &entry); err != nil {
		logger.Error(err, "decode fs.stat result failed")
		c.JSON(http.StatusBadGateway, gin.H{"error": "invalid stat result from agent"})
		return
	}

	if entry.Dir {
		var list *hostlinkv1.AgentResult
		if list, err = svc.dispatch(ctx, agentID, agentapi.MethodFsList, payload); err != nil {
			mapDispatchError(c, logger, err)
			return
		}
		respondAgentResult(c, list)
		return
	}

	if acceptsJSON(c) {
		respondAgentResult(c, stat)
		return
	}

	svc.streamDownload(c, agentID, reqPath, logger)
}

// streamDownload streams a file's bytes from the agent to the HTTP client. The
// agent reports a pre-data error (missing/since-deleted file) in the first frame
// so a JSON error status can still be set before any body is written; once bytes
// flow the response is committed as 200.
func (svc *service) streamDownload(c *gin.Context, agentID, reqPath string, logger logr.Logger) {
	payload, err := json.Marshal(agentapi.FsPathRequest{Path: reqPath})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "encode fs request failed"})
		return
	}

	frames, done, cancel, err := svc.dispatchStream(c.Request.Context(), agentID, agentapi.MethodFsRead, payload)
	if err != nil {
		mapDispatchError(c, logger, err)
		return
	}
	defer cancel()

	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		logger.Error(nil, "response writer does not support streaming")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "streaming unsupported"})
		return
	}

	var headerWritten bool
	writeHeader := func() {
		name := path.Base(reqPath)
		ctype := mime.TypeByExtension(path.Ext(name))
		if ctype == "" {
			ctype = "application/octet-stream"
		}
		h := c.Writer.Header()
		h.Set("Content-Type", ctype)
		h.Set("Content-Disposition", "attachment; filename="+strconv.Quote(name))
		h.Set("X-Accel-Buffering", "no")
		c.Writer.WriteHeader(http.StatusOK)
		headerWritten = true
	}

	// emit handles one frame and reports whether the stream is finished.
	emit := func(frame streamFrame) (finished bool) {
		if frame.Final {
			if !headerWritten && frame.Code != 0 && frame.Code != http.StatusOK {
				c.JSON(int(frame.Code), gin.H{"error": frame.Error})
				return true
			}
			if !headerWritten {
				writeHeader()
			}
			flusher.Flush()
			return true
		}
		if !headerWritten {
			writeHeader()
		}
		if _, werr := c.Writer.Write(frame.Payload); werr != nil {
			logger.Error(werr, "write download chunk failed; client gone")
			return true
		}
		flusher.Flush()
		return false
	}

	consumeStream(c.Request.Context(), frames, done, emit, func() {
		if !headerWritten {
			c.JSON(http.StatusBadGateway, gin.H{"error": "agent disconnected"})
		}
	})
}

// fsPost handles POST /api/v1/agents/:agentId/fs. With ?dir=true it creates a
// directory; otherwise it uploads one or more files from a multipart form,
// creating each exclusively (an existing target is reported as a conflict).
func (svc *service) fsPost(c *gin.Context) {
	agentID := c.Param("agentId")
	reqPath := c.Query("path")
	logger := svc.logger.WithName("fsPost").WithValues("agentID", agentID, "path", reqPath)

	dir, err := parseBoolQuery(c, "dir")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid dir query parameter"})
		return
	}
	if dir {
		svc.fsMkdir(c, agentID, reqPath, logger)
		return
	}
	svc.fsUpload(c, agentID, reqPath, true, logger)
}

// fsPut handles PUT /api/v1/agents/:agentId/fs: it overwrites a single file at
// the path with the request body (raw bytes, or the first file part of a
// multipart form).
func (svc *service) fsPut(c *gin.Context) {
	agentID := c.Param("agentId")
	reqPath := c.Query("path")
	logger := svc.logger.WithName("fsPut").WithValues("agentID", agentID, "path", reqPath)

	if reqPath == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "path is required"})
		return
	}

	body := c.Request.Body
	var bodyReader io.Reader = body
	if strings.HasPrefix(c.ContentType(), "multipart/form-data") {
		mr, err := c.Request.MultipartReader()
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid multipart form"})
			return
		}
		part, perr := mr.NextPart()
		if perr != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "multipart form has no file part"})
			return
		}
		defer func() {
			if e := part.Close(); e != nil {
				logger.Error(e, "close multipart part failed")
			}
		}()
		bodyReader = part
	}

	openPayload, err := json.Marshal(agentapi.FsWriteRequest{Path: reqPath, Exclusive: false})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "encode fs request failed"})
		return
	}

	result, err := svc.dispatchUpload(c.Request.Context(), agentID, agentapi.MethodFsWrite, openPayload, bodyReader)
	if err != nil {
		mapDispatchError(c, logger, err)
		return
	}
	respondAgentResult(c, result)
}

// fsDelete handles DELETE /api/v1/agents/:agentId/fs: it removes the path,
// recursively for directories.
func (svc *service) fsDelete(c *gin.Context) {
	agentID := c.Param("agentId")
	reqPath := c.Query("path")
	logger := svc.logger.WithName("fsDelete").WithValues("agentID", agentID, "path", reqPath)
	svc.dispatchPathRequest(c, agentID, reqPath, agentapi.MethodFsRemove, logger)
}

// fsMkdir dispatches an fs.mkdir for the path and returns the agent's result.
func (svc *service) fsMkdir(c *gin.Context, agentID, reqPath string, logger logr.Logger) {
	svc.dispatchPathRequest(c, agentID, reqPath, agentapi.MethodFsMkdir, logger)
}

// dispatchPathRequest drives a single-shot path-addressed fs method (mkdir,
// remove) to the agent and writes its result. It is the shared body of the
// handlers that take only a path and return the agent's outcome verbatim.
func (svc *service) dispatchPathRequest(c *gin.Context, agentID, reqPath, method string, logger logr.Logger) {
	payload, err := json.Marshal(agentapi.FsPathRequest{Path: reqPath})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "encode fs request failed"})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), dispatchTimeout)
	defer cancel()

	var result *hostlinkv1.AgentResult
	if result, err = svc.dispatch(ctx, agentID, method, payload); err != nil {
		mapDispatchError(c, logger, err)
		return
	}
	respondAgentResult(c, result)
}

// uploadOutcome is one file's result in a multi-file upload response.
type uploadOutcome struct {
	Name  string `json:"name"`
	Error string `json:"error,omitempty"`
}

// fsUpload streams each file part of a multipart form to the agent under dirPath,
// using the part filename. With exclusive set, a file that already exists is
// reported per-file as a conflict rather than overwriting. The response
// aggregates the written files and any per-file errors.
func (svc *service) fsUpload(c *gin.Context, agentID, dirPath string, exclusive bool, logger logr.Logger) {
	mr, err := c.Request.MultipartReader()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "expected a multipart form"})
		return
	}

	written := make([]uploadOutcome, 0)
	failed := make([]uploadOutcome, 0)
	for {
		part, perr := mr.NextPart()
		if errors.Is(perr, io.EOF) {
			break
		}
		if perr != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "read multipart form failed"})
			return
		}
		if part.FileName() == "" {
			// A non-file form field; skip it.
			continue
		}

		name := path.Base(strings.ReplaceAll(part.FileName(), "\\", "/"))
		target := joinFsPath(dirPath, name)
		var openPayload []byte
		if openPayload, err = json.Marshal(agentapi.FsWriteRequest{Path: target, Exclusive: exclusive}); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "encode fs request failed"})
			return
		}

		var result *hostlinkv1.AgentResult
		if result, err = svc.dispatchUpload(c.Request.Context(), agentID, agentapi.MethodFsWrite, openPayload, part); err != nil {
			// A dispatch-level failure (agent gone, relay broken) is not per-file;
			// abort the whole request.
			mapDispatchError(c, logger, err)
			return
		}
		code := result.GetCode()
		if code == http.StatusCreated || code == http.StatusOK {
			written = append(written, uploadOutcome{Name: name})
		} else {
			failed = append(failed, uploadOutcome{Name: name, Error: agentResultMessage(result)})
		}
	}

	status := http.StatusCreated
	switch {
	case len(failed) > 0 && len(written) == 0:
		status = http.StatusConflict
	case len(failed) > 0:
		status = http.StatusMultiStatus
	}
	c.JSON(status, gin.H{"written": written, "errors": failed})
}

// joinFsPath builds the request path for a file uploaded under dir, using only
// the base name so a part filename cannot inject directory components.
func joinFsPath(dir, name string) string {
	if dir == "" {
		return name
	}
	return strings.TrimRight(dir, "/") + "/" + name
}

// acceptsJSON reports whether the client's Accept header asks for JSON.
func acceptsJSON(c *gin.Context) bool {
	return strings.Contains(c.GetHeader("Accept"), "application/json")
}

// agentResultMessage returns a human-readable message for a non-success result,
// falling back to the HTTP status text when the agent set no error string.
func agentResultMessage(result *hostlinkv1.AgentResult) string {
	if msg := result.GetError(); msg != "" {
		return msg
	}
	return http.StatusText(int(result.GetCode()))
}

// mapDispatchError writes the HTTP error for a failed dispatch, mirroring the
// mapping used across the agent-request handlers.
func mapDispatchError(c *gin.Context, logger logr.Logger, err error) {
	logger.Error(err, "dispatch to agent failed")
	switch {
	case errors.Is(err, errAgentNotConnected):
		c.JSON(http.StatusNotFound, gin.H{"error": "agent not connected"})
	case errors.Is(err, context.DeadlineExceeded):
		c.JSON(http.StatusGatewayTimeout, gin.H{"error": "agent did not respond in time"})
	default:
		c.JSON(http.StatusBadGateway, gin.H{"error": "agent request failed"})
	}
}
