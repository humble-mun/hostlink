package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/strslice"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"
	"github.com/go-logr/logr"

	"github.com/humble-mun/hostlink/pkg/agentapi"
	hostlinkv1 "github.com/humble-mun/hostlink/pkg/api/hostlink/v1"
)

// maxLogLineBytes bounds the accumulation for a single log line: a longer line
// is emitted in pieces so one pathological line can neither grow the agent's
// memory nor exceed the gRPC message size.
const maxLogLineBytes = 64 * 1024

// dockerErrorCode maps a Docker daemon error to the HTTP status the REST layer
// returns: no-such-object becomes 404, a conflicting state (e.g. removing a
// running container, a taken name) becomes 409, a rejected parameter becomes
// 400, and anything else is a 500.
func dockerErrorCode(err error) (code uint32) {
	switch {
	case cerrdefs.IsNotFound(err):
		code = http.StatusNotFound
	case cerrdefs.IsConflict(err):
		code = http.StatusConflict
	case cerrdefs.IsInvalidArgument(err):
		code = http.StatusBadRequest
	default:
		code = http.StatusInternalServerError
	}
	return
}

// listContainers lists the Docker containers (all of them when the request sets
// All, running only otherwise) and marshals them to the agentapi.ContainerSummary
// JSON shape the REST layer returns unchanged.
func (a *agent) listContainers(ctx context.Context, req *hostlinkv1.AgentRequest) (payload []byte, code uint32, errMsg string) {
	var lr agentapi.ContainerListRequest
	if len(req.GetPayload()) > 0 {
		if err := json.Unmarshal(req.GetPayload(), &lr); err != nil {
			code = http.StatusBadRequest
			errMsg = "invalid containers.list payload: " + err.Error()
			return
		}
	}

	var summaries []container.Summary
	var err error
	if summaries, err = a.docker.ContainerList(ctx, container.ListOptions{All: lr.All}); err != nil {
		a.logger.Error(err, "list docker containers failed")
		code = dockerErrorCode(err)
		errMsg = err.Error()
		return
	}

	containers := make([]agentapi.ContainerSummary, 0, len(summaries))
	for _, s := range summaries {
		summary := agentapi.ContainerSummary{
			ID:      s.ID,
			Names:   s.Names,
			Image:   s.Image,
			ImageID: s.ImageID,
			Command: s.Command,
			Created: s.Created,
			Labels:  s.Labels,
			State:   string(s.State),
			Status:  s.Status,
		}
		for _, p := range s.Ports {
			summary.Ports = append(summary.Ports, agentapi.ContainerPort{
				IP:          p.IP,
				PrivatePort: p.PrivatePort,
				PublicPort:  p.PublicPort,
				Protocol:    p.Type,
			})
		}
		containers = append(containers, summary)
	}

	if payload, err = json.Marshal(containers); err != nil {
		a.logger.Error(err, "marshal docker containers failed")
		payload = nil
		code = http.StatusInternalServerError
		errMsg = err.Error()
		return
	}
	code = http.StatusOK
	return
}

// createContainer creates and starts a container (docker run semantics). The
// image must already be present on the host (pull it via images.pull first).
// When the create succeeds but the start fails, the container is left in place
// and the error names its ID so the caller can inspect or remove it.
func (a *agent) createContainer(ctx context.Context, req *hostlinkv1.AgentRequest) (payload []byte, code uint32, errMsg string) {
	var cr agentapi.ContainerCreateRequest
	if err := json.Unmarshal(req.GetPayload(), &cr); err != nil {
		code = http.StatusBadRequest
		errMsg = "invalid containers.create payload: " + err.Error()
		return
	}
	if cr.Image == "" {
		code = http.StatusBadRequest
		errMsg = "containers.create: image must not be empty"
		return
	}

	var exposed nat.PortSet
	var bindings nat.PortMap
	for _, p := range cr.Ports {
		proto := p.Protocol
		if proto == "" {
			proto = "tcp"
		}
		var port nat.Port
		var err error
		if port, err = nat.NewPort(proto, p.ContainerPort); err != nil {
			code = http.StatusBadRequest
			errMsg = "containers.create: invalid port " + p.ContainerPort + ": " + err.Error()
			return
		}
		if exposed == nil {
			exposed = nat.PortSet{}
			bindings = nat.PortMap{}
		}
		exposed[port] = struct{}{}
		if p.HostIP != "" || p.HostPort != "" {
			bindings[port] = append(bindings[port], nat.PortBinding{HostIP: p.HostIP, HostPort: p.HostPort})
		}
	}

	restartPolicy := container.RestartPolicy{
		Name:              container.RestartPolicyMode(cr.RestartPolicy),
		MaximumRetryCount: cr.MaxRetryCount,
	}
	if cr.RestartPolicy != "" {
		if err := container.ValidateRestartPolicy(restartPolicy); err != nil {
			code = http.StatusBadRequest
			errMsg = "containers.create: " + err.Error()
			return
		}
	}

	cfg := &container.Config{
		Image:        cr.Image,
		Cmd:          strslice.StrSlice(cr.Cmd),
		Entrypoint:   strslice.StrSlice(cr.Entrypoint),
		Env:          cr.Env,
		WorkingDir:   cr.WorkingDir,
		Labels:       cr.Labels,
		ExposedPorts: exposed,
	}
	hostCfg := &container.HostConfig{
		Binds:         cr.Binds,
		PortBindings:  bindings,
		NetworkMode:   container.NetworkMode(cr.NetworkMode),
		PidMode:       container.PidMode(cr.PidMode),
		RestartPolicy: restartPolicy,
		AutoRemove:    cr.AutoRemove,
	}

	var created container.CreateResponse
	var err error
	if created, err = a.docker.ContainerCreate(ctx, cfg, hostCfg, nil, nil, cr.Name); err != nil {
		a.logger.Error(err, "create docker container failed", "image", cr.Image, "name", cr.Name)
		code = dockerErrorCode(err)
		errMsg = err.Error()
		return
	}

	if err = a.docker.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		a.logger.Error(err, "start created docker container failed", "containerID", created.ID)
		code = dockerErrorCode(err)
		errMsg = "container " + created.ID + " created but start failed: " + err.Error()
		return
	}

	if payload, err = json.Marshal(agentapi.ContainerCreateResult{ID: created.ID, Warnings: created.Warnings}); err != nil {
		a.logger.Error(err, "marshal containers.create result failed")
		code = http.StatusInternalServerError
		errMsg = err.Error()
		return
	}
	code = http.StatusCreated
	return
}

// inspectContainer reports a single container's details, projected from the
// Docker inspect response to the agentapi.ContainerDetail JSON shape.
func (a *agent) inspectContainer(ctx context.Context, req *hostlinkv1.AgentRequest) (payload []byte, code uint32, errMsg string) {
	var ir agentapi.ContainerIDRequest
	if err := json.Unmarshal(req.GetPayload(), &ir); err != nil {
		code = http.StatusBadRequest
		errMsg = "invalid containers.inspect payload: " + err.Error()
		return
	}
	if ir.ID == "" {
		code = http.StatusBadRequest
		errMsg = "containers.inspect: id must not be empty"
		return
	}

	var resp container.InspectResponse
	var err error
	if resp, err = a.docker.ContainerInspect(ctx, ir.ID); err != nil {
		a.logger.Error(err, "inspect docker container failed", "containerID", ir.ID)
		code = dockerErrorCode(err)
		errMsg = err.Error()
		return
	}

	detail := agentapi.ContainerDetail{
		ID:           resp.ID,
		Name:         resp.Name,
		ImageID:      resp.Image,
		Created:      resp.Created,
		RestartCount: resp.RestartCount,
	}
	if resp.State != nil {
		detail.State = agentapi.ContainerState{
			Status:     string(resp.State.Status),
			Running:    resp.State.Running,
			Paused:     resp.State.Paused,
			Restarting: resp.State.Restarting,
			OOMKilled:  resp.State.OOMKilled,
			Dead:       resp.State.Dead,
			Pid:        resp.State.Pid,
			ExitCode:   resp.State.ExitCode,
			Error:      resp.State.Error,
			StartedAt:  resp.State.StartedAt,
			FinishedAt: resp.State.FinishedAt,
		}
	}
	if resp.Config != nil {
		detail.Image = resp.Config.Image
		detail.Cmd = resp.Config.Cmd
		detail.Entrypoint = resp.Config.Entrypoint
		detail.Env = resp.Config.Env
		detail.WorkingDir = resp.Config.WorkingDir
		detail.Labels = resp.Config.Labels
	}
	if resp.HostConfig != nil {
		detail.Binds = resp.HostConfig.Binds
		detail.RestartPolicy = string(resp.HostConfig.RestartPolicy.Name)
		detail.AutoRemove = resp.HostConfig.AutoRemove
	}
	if resp.NetworkSettings != nil {
		detail.Ports = projectPortMap(resp.NetworkSettings.Ports)
	}

	if payload, err = json.Marshal(detail); err != nil {
		a.logger.Error(err, "marshal containers.inspect result failed")
		code = http.StatusInternalServerError
		errMsg = err.Error()
		return
	}
	code = http.StatusOK
	return
}

// projectPortMap flattens a Docker inspect port map to the ContainerPort shape:
// one entry per host binding, and one unbound entry for an exposed-only port.
func projectPortMap(pm nat.PortMap) (ports []agentapi.ContainerPort) {
	for port, hostBindings := range pm {
		private := uint16(port.Int()) //nolint:gosec // nat.Port holds a valid port number
		if len(hostBindings) == 0 {
			ports = append(ports, agentapi.ContainerPort{PrivatePort: private, Protocol: port.Proto()})
			continue
		}
		for _, b := range hostBindings {
			cp := agentapi.ContainerPort{IP: b.HostIP, PrivatePort: private, Protocol: port.Proto()}
			if public, err := nat.ParsePort(b.HostPort); err == nil {
				cp.PublicPort = uint16(public) //nolint:gosec // ParsePort bounds the value
			}
			ports = append(ports, cp)
		}
	}
	return
}

// handleLogs runs a streaming containers.logs: each demuxed log line is sent as
// an AgentProgress event carrying a LogFrame, then a terminal AgentResult
// reports the outcome. emit send failures cancel ctx so a vanished consumer
// stops the underlying docker logs stream — essential for a followed stream,
// which otherwise never ends.
func (a *agent) handleLogs(ctx context.Context, req *hostlinkv1.AgentRequest, logger logr.Logger) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	requestID := req.GetRequestId()
	emit := func(f *agentapi.LogFrame) {
		payload, err := json.Marshal(f)
		if err != nil {
			logger.Error(err, "marshal log frame failed", "requestID", requestID)
			return
		}
		if err := a.send(&hostlinkv1.AgentEvent{
			AgentId: a.nodeName,
			Kind: &hostlinkv1.AgentEvent_Progress{Progress: &hostlinkv1.AgentProgress{
				RequestId: requestID,
				Payload:   payload,
			}},
		}); err != nil {
			logger.Error(err, "send log frame failed", "requestID", requestID)
			cancel()
		}
	}

	code, errMsg := a.containerLogs(ctx, req, emit)
	if err := a.send(&hostlinkv1.AgentEvent{
		AgentId: a.nodeName,
		Kind: &hostlinkv1.AgentEvent_Result{Result: &hostlinkv1.AgentResult{
			RequestId: requestID, Code: code, Error: errMsg, Final: true,
		}},
	}); err != nil {
		logger.Error(err, "send logs result failed", "requestID", requestID)
	}
}

// containerLogs opens the docker logs stream for the requested container and
// drives every line through emit until the stream ends (backlog drained, or —
// with Follow — the consumer cancels ctx). A cancelled ctx is the normal end
// of a followed stream and reports success.
func (a *agent) containerLogs(ctx context.Context, req *hostlinkv1.AgentRequest, emit func(*agentapi.LogFrame)) (code uint32, errMsg string) {
	var lr agentapi.ContainerLogsRequest
	if err := json.Unmarshal(req.GetPayload(), &lr); err != nil {
		code = http.StatusBadRequest
		errMsg = "invalid containers.logs payload: " + err.Error()
		return
	}
	if lr.ID == "" {
		code = http.StatusBadRequest
		errMsg = "containers.logs: id must not be empty"
		return
	}

	// The container's TTY mode decides the wire format of the logs stream (raw
	// bytes vs stdcopy multiplexing), so it has to be inspected first.
	var inspect container.InspectResponse
	var err error
	if inspect, err = a.docker.ContainerInspect(ctx, lr.ID); err != nil {
		a.logger.Error(err, "inspect docker container for logs failed", "containerID", lr.ID)
		code = dockerErrorCode(err)
		errMsg = err.Error()
		return
	}
	tty := inspect.Config != nil && inspect.Config.Tty

	var rc io.ReadCloser
	if rc, err = a.docker.ContainerLogs(ctx, lr.ID, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     lr.Follow,
		Tail:       lr.Tail,
		Since:      lr.Since,
		Timestamps: lr.Timestamps,
	}); err != nil {
		a.logger.Error(err, "open docker container logs failed", "containerID", lr.ID)
		code = dockerErrorCode(err)
		errMsg = err.Error()
		return
	}
	defer func() {
		if e := rc.Close(); e != nil {
			a.logger.Error(e, "close container logs stream failed", "containerID", lr.ID)
		}
	}()

	stdout := &logLineWriter{stream: "stdout", emit: emit}
	stderr := &logLineWriter{stream: "stderr", emit: emit}
	if tty {
		_, err = io.Copy(stdout, rc)
	} else {
		_, err = stdcopy.StdCopy(stdout, stderr, rc)
	}
	stdout.flush()
	stderr.flush()
	if err != nil && !errors.Is(err, io.EOF) {
		if ctx.Err() != nil {
			code = http.StatusOK
			return
		}
		a.logger.Error(err, "read container logs stream failed", "containerID", lr.ID)
		code = http.StatusBadGateway
		errMsg = err.Error()
		return
	}
	code = http.StatusOK
	return
}

// logLineWriter adapts the byte stream of one log channel (stdout or stderr)
// into per-line LogFrame emissions: bytes are buffered until a newline, a line
// exceeding maxLogLineBytes is emitted in pieces, and flush emits the final
// unterminated tail when the stream ends.
type logLineWriter struct {
	stream string
	emit   func(*agentapi.LogFrame)
	buf    []byte
}

func (w *logLineWriter) Write(p []byte) (n int, err error) {
	n = len(p)
	w.buf = append(w.buf, p...)
	for {
		i := bytes.IndexByte(w.buf, '\n')
		if i < 0 {
			break
		}
		w.emitLine(w.buf[:i])
		w.buf = w.buf[i+1:]
	}
	if len(w.buf) >= maxLogLineBytes {
		w.emitLine(w.buf)
		w.buf = nil
	}
	return
}

func (w *logLineWriter) emitLine(line []byte) {
	line = bytes.TrimSuffix(line, []byte("\r"))
	w.emit(&agentapi.LogFrame{Stream: w.stream, Line: string(line)})
}

// flush emits the final unterminated line, if any.
func (w *logLineWriter) flush() {
	if len(w.buf) > 0 {
		w.emitLine(w.buf)
		w.buf = nil
	}
}

// startContainer starts a stopped container.
func (a *agent) startContainer(ctx context.Context, req *hostlinkv1.AgentRequest) (code uint32, errMsg string) {
	var ir agentapi.ContainerIDRequest
	if err := json.Unmarshal(req.GetPayload(), &ir); err != nil {
		code = http.StatusBadRequest
		errMsg = "invalid containers.start payload: " + err.Error()
		return
	}
	if ir.ID == "" {
		code = http.StatusBadRequest
		errMsg = "containers.start: id must not be empty"
		return
	}

	if err := a.docker.ContainerStart(ctx, ir.ID, container.StartOptions{}); err != nil {
		a.logger.Error(err, "start docker container failed", "containerID", ir.ID)
		code = dockerErrorCode(err)
		errMsg = err.Error()
		return
	}
	code = http.StatusNoContent
	return
}

// stopContainer stops a running container, waiting up to the requested grace
// period (the daemon default when unset) before the daemon kills it.
func (a *agent) stopContainer(ctx context.Context, req *hostlinkv1.AgentRequest) (code uint32, errMsg string) {
	var sr agentapi.ContainerStopRequest
	if err := json.Unmarshal(req.GetPayload(), &sr); err != nil {
		code = http.StatusBadRequest
		errMsg = "invalid containers.stop payload: " + err.Error()
		return
	}
	if sr.ID == "" {
		code = http.StatusBadRequest
		errMsg = "containers.stop: id must not be empty"
		return
	}

	if err := a.docker.ContainerStop(ctx, sr.ID, container.StopOptions{Timeout: sr.Timeout}); err != nil {
		a.logger.Error(err, "stop docker container failed", "containerID", sr.ID)
		code = dockerErrorCode(err)
		errMsg = err.Error()
		return
	}
	code = http.StatusNoContent
	return
}

// restartContainer restarts a container with the same grace-period semantics as
// stopContainer.
func (a *agent) restartContainer(ctx context.Context, req *hostlinkv1.AgentRequest) (code uint32, errMsg string) {
	var sr agentapi.ContainerStopRequest
	if err := json.Unmarshal(req.GetPayload(), &sr); err != nil {
		code = http.StatusBadRequest
		errMsg = "invalid containers.restart payload: " + err.Error()
		return
	}
	if sr.ID == "" {
		code = http.StatusBadRequest
		errMsg = "containers.restart: id must not be empty"
		return
	}

	if err := a.docker.ContainerRestart(ctx, sr.ID, container.StopOptions{Timeout: sr.Timeout}); err != nil {
		a.logger.Error(err, "restart docker container failed", "containerID", sr.ID)
		code = dockerErrorCode(err)
		errMsg = err.Error()
		return
	}
	code = http.StatusNoContent
	return
}

// removeContainer removes a container (docker rm). Removing a running container
// requires Force and otherwise yields 409.
func (a *agent) removeContainer(ctx context.Context, req *hostlinkv1.AgentRequest) (code uint32, errMsg string) {
	var rr agentapi.ContainerRemoveRequest
	if err := json.Unmarshal(req.GetPayload(), &rr); err != nil {
		code = http.StatusBadRequest
		errMsg = "invalid containers.remove payload: " + err.Error()
		return
	}
	if rr.ID == "" {
		code = http.StatusBadRequest
		errMsg = "containers.remove: id must not be empty"
		return
	}

	opts := container.RemoveOptions{Force: rr.Force, RemoveVolumes: rr.RemoveVolumes}
	if err := a.docker.ContainerRemove(ctx, rr.ID, opts); err != nil {
		a.logger.Error(err, "remove docker container failed", "containerID", rr.ID)
		code = dockerErrorCode(err)
		errMsg = err.Error()
		return
	}
	code = http.StatusNoContent
	return
}
