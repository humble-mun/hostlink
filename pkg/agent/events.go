package agent

import (
	"context"
	"time"

	"github.com/docker/docker/api/types/events"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/client"
	"github.com/go-logr/logr"

	hostlinkv1 "github.com/humble-mun/hostlink/pkg/api/hostlink/v1"
)

type dockerEventSource interface {
	Events(context.Context, events.ListOptions) (<-chan events.Message, <-chan error)
}

type dockerEventRetrySettings struct {
	base         time.Duration
	max          time.Duration
	healthyAfter time.Duration
}

var dockerEventRetry = dockerEventRetrySettings{
	base:         time.Second,
	max:          30 * time.Second,
	healthyAfter: time.Minute,
}

func (a *agent) watchDockerEvents(ctx context.Context) {
	logger := a.logger.WithName("events")
	if a.docker == nil {
		logger.Info("docker event watcher disabled: client unavailable")
		return
	}

	runDockerEventsLoop(ctx, logger, a.docker, a.sendDockerEvent)
}

func (a *agent) sendDockerEvent(eventType, containerID string) error {
	return a.send(&hostlinkv1.AgentEvent{
		AgentId: a.nodeName,
		Kind: &hostlinkv1.AgentEvent_Event{Event: &hostlinkv1.DockerEvent{
			Type:        eventType,
			ContainerId: containerID,
		}},
	})
}

func runDockerEventsLoop(ctx context.Context, logger logr.Logger, src dockerEventSource, send func(eventType, containerID string) error) {
	retry := dockerEventRetry
	delay := retry.base
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		subscriptionStarted := time.Now()
		messages, errs := src.Events(ctx, events.ListOptions{Filters: filters.NewArgs(
			filters.Arg("type", "container"),
			filters.Arg("event", "start"),
			filters.Arg("event", "die"),
			filters.Arg("event", "stop"),
		)})
		receivedMessage := false

	subscription:
		for {
			select {
			case <-ctx.Done():
				return
			case message, ok := <-messages:
				if !ok {
					logger.V(1).Info("docker event message stream closed")
					break subscription
				}
				receivedMessage = true
				if message.Actor.ID == "" {
					logger.V(1).Info("docker event missing container ID", "action", message.Action)
					continue
				}
				if err := send(string(message.Action), message.Actor.ID); err != nil {
					logger.V(1).Info("send docker event failed", "error", err, "action", message.Action, "containerID", message.Actor.ID)
				}
			case err, ok := <-errs:
				if !ok {
					logger.V(1).Info("docker event error stream closed")
				} else {
					logger.V(1).Info("docker event stream ended", "error", err)
				}
				break subscription
			}
		}

		if receivedMessage || time.Since(subscriptionStarted) >= retry.healthyAfter {
			delay = retry.base
		}
		logger.V(1).Info("retrying docker event stream", "delay", delay)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		case <-timer.C:
		}
		if delay < retry.max {
			delay *= 2
			if delay > retry.max {
				delay = retry.max
			}
		}
	}
}

var _ dockerEventSource = (*client.Client)(nil)
