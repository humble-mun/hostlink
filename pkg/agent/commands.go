package agent

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"

	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/registry"
	"github.com/docker/docker/pkg/jsonmessage"

	"github.com/humble-mun/hostlink/pkg/agentapi"
	hostlinkv1 "github.com/humble-mun/hostlink/pkg/api/hostlink/v1"
)

// handleRequest dispatches a controller request by method and packages the
// outcome as an AgentResult event. code mirrors an HTTP status so the REST layer
// can map it back directly. It serves single-shot methods; the streaming
// images.pull method is handled separately by handleAndReply.
func (a *agent) handleRequest(ctx context.Context, req *hostlinkv1.AgentRequest) (event *hostlinkv1.AgentEvent) {
	result := &hostlinkv1.AgentResult{RequestId: req.GetRequestId(), Final: true}
	switch req.GetMethod() {
	case agentapi.MethodImagesList:
		result.Payload, result.Code, result.Error = a.listImages(ctx)
	case agentapi.MethodImagesRemove:
		result.Payload, result.Code, result.Error = a.removeImages(ctx, req)
	default:
		result.Code = http.StatusNotImplemented
		result.Error = "unknown method: " + req.GetMethod()
	}
	event = &hostlinkv1.AgentEvent{
		AgentId: a.nodeName,
		Kind:    &hostlinkv1.AgentEvent_Result{Result: result},
	}
	return
}

// pullImage pulls req.Image, streaming each line of the Docker pull JSON stream
// back as an AgentProgress event (a PullProgress body) via emit, then returns
// the terminal code/error. A nil error and http.StatusOK indicate success; the
// terminal AgentResult payload is empty.
func (a *agent) pullImage(ctx context.Context, req *hostlinkv1.AgentRequest, emit func(*agentapi.PullProgress)) (code uint32, errMsg string) {
	var pr agentapi.PullRequest
	if err := json.Unmarshal(req.GetPayload(), &pr); err != nil {
		return http.StatusBadRequest, "invalid images.pull payload: " + err.Error()
	}
	if pr.Image == "" {
		return http.StatusBadRequest, "images.pull: image must not be empty"
	}

	var opts image.PullOptions
	if pr.Auth != nil {
		encoded, err := encodeRegistryAuth(pr.Auth)
		if err != nil {
			return http.StatusBadRequest, "images.pull: encode registry auth: " + err.Error()
		}
		opts.RegistryAuth = encoded
	}

	rc, err := a.docker.ImagePull(ctx, pr.Image, opts)
	if err != nil {
		a.logger.Error(err, "pull docker image failed", "image", pr.Image)
		return http.StatusInternalServerError, err.Error()
	}
	defer func() {
		if e := rc.Close(); e != nil {
			a.logger.Error(e, "close image pull stream failed", "image", pr.Image)
		}
	}()

	dec := json.NewDecoder(rc)
	for {
		var msg jsonmessage.JSONMessage
		if err = dec.Decode(&msg); err != nil {
			if err == io.EOF {
				break
			}
			a.logger.Error(err, "decode image pull stream failed", "image", pr.Image)
			return http.StatusBadGateway, err.Error()
		}
		if msg.Error != nil {
			// A mid-stream error (e.g. auth failure, manifest not found) is the
			// terminal outcome of the pull.
			emit(&agentapi.PullProgress{ID: msg.ID, Status: msg.Status, Error: msg.Error.Message})
			return http.StatusBadGateway, msg.Error.Message
		}
		progress := &agentapi.PullProgress{ID: msg.ID, Status: msg.Status}
		if msg.Progress != nil {
			progress.Current = msg.Progress.Current
			progress.Total = msg.Progress.Total
			progress.Progress = msg.Progress.String()
		}
		emit(progress)
	}
	return http.StatusOK, ""
}

// encodeRegistryAuth encodes registry credentials into the base64url JSON the
// Docker client expects in image.PullOptions.RegistryAuth.
func encodeRegistryAuth(auth *agentapi.RegistryAuth) (string, error) {
	cfg := registry.AuthConfig{
		Username:      auth.Username,
		Password:      auth.Password,
		ServerAddress: auth.ServerAddress,
		IdentityToken: auth.IdentityToken,
	}
	// The AuthConfig is marshaled to the base64 token Docker requires; the
	// Password field is intentionally serialized.
	buf, err := json.Marshal(cfg) //nolint:gosec // registry auth requires the password in the token
	if err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(buf), nil
}

// listImages lists the local Docker images and marshals them to the
// agentapi.ImageSummary JSON shape the REST layer returns unchanged.
func (a *agent) listImages(ctx context.Context) (payload []byte, code uint32, errMsg string) {
	var summaries []image.Summary
	var err error
	if summaries, err = a.docker.ImageList(ctx, image.ListOptions{}); err != nil {
		a.logger.Error(err, "list docker images failed")
		code = http.StatusInternalServerError
		errMsg = err.Error()
		return
	}

	images := make([]agentapi.ImageSummary, 0, len(summaries))
	for _, s := range summaries {
		images = append(images, agentapi.ImageSummary{
			ID:          s.ID,
			RepoTags:    s.RepoTags,
			RepoDigests: s.RepoDigests,
			Created:     s.Created,
			Size:        s.Size,
			Labels:      s.Labels,
		})
	}

	if payload, err = json.Marshal(images); err != nil {
		a.logger.Error(err, "marshal docker images failed")
		payload = nil
		code = http.StatusInternalServerError
		errMsg = err.Error()
		return
	}
	code = http.StatusOK
	return
}

// removeImages removes each reference in the RemoveRequest, accumulating the
// daemon's untagged/deleted layer records and recording a per-reference error
// for any reference that fails. It returns http.StatusOK whenever the request
// was well-formed; individual failures are reported in the result payload so a
// batch delete is not aborted by one bad reference.
func (a *agent) removeImages(ctx context.Context, req *hostlinkv1.AgentRequest) (payload []byte, code uint32, errMsg string) {
	var rr agentapi.RemoveRequest
	if err := json.Unmarshal(req.GetPayload(), &rr); err != nil {
		return nil, http.StatusBadRequest, "invalid images.remove payload: " + err.Error()
	}
	if len(rr.Refs) == 0 {
		return nil, http.StatusBadRequest, "images.remove: at least one reference is required"
	}

	opts := image.RemoveOptions{Force: rr.Force, PruneChildren: !rr.NoPrune}
	result := agentapi.RemoveResult{Deleted: []agentapi.DeletedImage{}}
	for _, ref := range rr.Refs {
		responses, err := a.docker.ImageRemove(ctx, ref, opts)
		if err != nil {
			a.logger.Error(err, "remove docker image failed", "ref", ref)
			result.Errors = append(result.Errors, agentapi.RemoveError{Ref: ref, Error: err.Error()})
			continue
		}
		for _, r := range responses {
			result.Deleted = append(result.Deleted, agentapi.DeletedImage{Untagged: r.Untagged, Deleted: r.Deleted})
		}
	}

	payload, err := json.Marshal(result)
	if err != nil {
		a.logger.Error(err, "marshal images.remove result failed")
		return nil, http.StatusInternalServerError, err.Error()
	}
	return payload, http.StatusOK, ""
}
