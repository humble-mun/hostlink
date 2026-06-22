package agent

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/docker/docker/api/types/image"

	"github.com/humble-mun/hostlink/pkg/agentapi"
	hostlinkv1 "github.com/humble-mun/hostlink/pkg/api/hostlink/v1"
)

// handleRequest dispatches a controller request by method and packages the
// outcome as an AgentResult event. code mirrors an HTTP status so the REST layer
// can map it back directly.
func (a *agent) handleRequest(ctx context.Context, req *hostlinkv1.AgentRequest) (event *hostlinkv1.AgentEvent) {
	result := &hostlinkv1.AgentResult{RequestId: req.GetRequestId()}
	switch req.GetMethod() {
	case agentapi.MethodImagesList:
		result.Payload, result.Code, result.Error = a.listImages(ctx)
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
