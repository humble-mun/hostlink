package agent

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/api/types/events"
	"github.com/go-logr/logr"
)

const dockerEventsTestTimeout = 100 * time.Millisecond

type dockerEventSubscription struct {
	messages chan events.Message
	errs     chan error
}

func newDockerEventSubscription() dockerEventSubscription {
	return dockerEventSubscription{
		messages: make(chan events.Message),
		errs:     make(chan error, 1),
	}
}

type fakeDockerEventSource struct {
	mu            sync.Mutex
	subscriptions []dockerEventSubscription
	calls         chan events.ListOptions
}

func (s *fakeDockerEventSource) Events(_ context.Context, options events.ListOptions) (<-chan events.Message, <-chan error) {
	s.calls <- options

	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.subscriptions) == 0 {
		return make(chan events.Message), make(chan error)
	}

	subscription := s.subscriptions[0]
	s.subscriptions = s.subscriptions[1:]
	return subscription.messages, subscription.errs
}

func awaitDockerEventLoop[T any](t *testing.T, ch <-chan T) T {
	t.Helper()

	timer := time.NewTimer(dockerEventsTestTimeout)
	defer timer.Stop()
	select {
	case value := <-ch:
		return value
	case <-timer.C:
		t.Fatal("timed out waiting for docker event loop")
		var zero T
		return zero
	}
}

func requireDockerEventFilters(t *testing.T, options events.ListOptions) {
	t.Helper()

	if !options.Filters.Contains("type") || !options.Filters.ExactMatch("type", "container") {
		t.Errorf("event type filter = %v, want container", options.Filters.Get("type"))
	}
	if !options.Filters.Contains("event") {
		t.Fatal("event action filter is absent")
	}

	for _, action := range []string{"start", "die", "stop"} {
		if !options.Filters.ExactMatch("event", action) {
			t.Errorf("event action filters = %v, want %q", options.Filters.Get("event"), action)
		}
	}
	if got := options.Filters.Get("event"); len(got) != 3 || !slices.Contains(got, "start") || !slices.Contains(got, "die") || !slices.Contains(got, "stop") {
		t.Errorf("event action filters = %v, want start, die, stop", got)
	}
}

func runDockerEventsLoopForTest(t *testing.T, source *fakeDockerEventSource, send func(string, string) error) (context.CancelFunc, <-chan struct{}) {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		runDockerEventsLoop(ctx, logr.Discard(), source, send)
		close(done)
	}()
	return cancel, done
}

func TestRunDockerEventsLoopForwardsMessagesWithContainerFilters(t *testing.T) {
	// Given
	subscription := newDockerEventSubscription()
	source := &fakeDockerEventSource{
		subscriptions: []dockerEventSubscription{subscription},
		calls:         make(chan events.ListOptions, 1),
	}
	sent := make(chan struct {
		eventType   string
		containerID string
	}, 1)
	cancel, done := runDockerEventsLoopForTest(t, source, func(eventType, containerID string) error {
		sent <- struct {
			eventType   string
			containerID string
		}{eventType: eventType, containerID: containerID}
		return nil
	})
	t.Cleanup(func() {
		cancel()
		awaitDockerEventLoop(t, done)
	})

	// When
	options := awaitDockerEventLoop(t, source.calls)
	subscription.messages <- events.Message{Action: events.ActionStart, Actor: events.Actor{ID: "container-123"}}

	// Then
	requireDockerEventFilters(t, options)
	got := awaitDockerEventLoop(t, sent)
	if got.eventType != "start" || got.containerID != "container-123" {
		t.Errorf("sent event = (%q, %q), want (start, container-123)", got.eventType, got.containerID)
	}
}

func TestRunDockerEventsLoopResubscribesAfterSourceError(t *testing.T) {
	// Given
	originalRetry := dockerEventRetry
	dockerEventRetry = dockerEventRetrySettings{base: time.Millisecond, max: time.Millisecond, healthyAfter: time.Hour}
	t.Cleanup(func() { dockerEventRetry = originalRetry })
	first := newDockerEventSubscription()
	second := newDockerEventSubscription()
	source := &fakeDockerEventSource{
		subscriptions: []dockerEventSubscription{first, second},
		calls:         make(chan events.ListOptions, 2),
	}
	cancel, done := runDockerEventsLoopForTest(t, source, func(string, string) error { return nil })
	t.Cleanup(func() {
		cancel()
		awaitDockerEventLoop(t, done)
	})
	awaitDockerEventLoop(t, source.calls)

	// When
	first.errs <- errors.New("docker event stream ended")

	// Then
	secondOptions := awaitDockerEventLoop(t, source.calls)
	requireDockerEventFilters(t, secondOptions)
}

func TestRunDockerEventsLoopExitsWhenContextCancelled(t *testing.T) {
	// Given
	subscription := newDockerEventSubscription()
	source := &fakeDockerEventSource{
		subscriptions: []dockerEventSubscription{subscription},
		calls:         make(chan events.ListOptions, 1),
	}
	cancel, done := runDockerEventsLoopForTest(t, source, func(string, string) error { return nil })
	awaitDockerEventLoop(t, source.calls)

	// When
	cancel()

	// Then
	awaitDockerEventLoop(t, done)
}

func TestRunDockerEventsLoopContinuesAfterSendError(t *testing.T) {
	// Given
	subscription := newDockerEventSubscription()
	source := &fakeDockerEventSource{
		subscriptions: []dockerEventSubscription{subscription},
		calls:         make(chan events.ListOptions, 1),
	}
	sent := make(chan string, 1)
	first := true
	cancel, done := runDockerEventsLoopForTest(t, source, func(eventType, _ string) error {
		if first {
			first = false
			return errors.New("control stream unavailable")
		}
		sent <- eventType
		return nil
	})
	t.Cleanup(func() {
		cancel()
		awaitDockerEventLoop(t, done)
	})
	awaitDockerEventLoop(t, source.calls)

	// When
	subscription.messages <- events.Message{Action: events.ActionStop, Actor: events.Actor{ID: "container-123"}}
	subscription.messages <- events.Message{Action: events.ActionDie, Actor: events.Actor{ID: "container-123"}}

	// Then
	if got := awaitDockerEventLoop(t, sent); got != "die" {
		t.Errorf("sent event type = %q, want die", got)
	}
}
