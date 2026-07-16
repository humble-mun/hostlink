package controller

import (
	"context"
	"errors"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
)

func TestPortStoreAllocateSequential(t *testing.T) {
	// Given
	store := testPortStore()
	ctx := context.Background()

	// When
	first, err := store.allocate(ctx, 1025, 1027, testPortMapping("agent-a"))
	if err != nil {
		t.Fatalf("allocate first port: %v", err)
	}
	second, err := store.allocate(ctx, 1025, 1027, testPortMapping("agent-b"))
	if err != nil {
		t.Fatalf("allocate second port: %v", err)
	}
	third, err := store.allocate(ctx, 1025, 1027, testPortMapping("agent-c"))
	if err != nil {
		t.Fatalf("allocate third port: %v", err)
	}
	_, err = store.allocate(ctx, 1025, 1027, testPortMapping("agent-d"))

	// Then
	if got, want := []uint32{first, second, third}, []uint32{1025, 1026, 1027}; !equalPorts(got, want) {
		t.Fatalf("allocated ports = %v, want %v", got, want)
	}
	if !errors.Is(err, errPortRangeExhausted) {
		t.Fatalf("allocation error = %v, want %v", err, errPortRangeExhausted)
	}
}

func TestPortStoreAllocateSpecific(t *testing.T) {
	// Given
	store := testPortStore()
	ctx := context.Background()

	// When
	port, err := store.allocate(ctx, 2048, 2048, testPortMapping("agent-a"))
	if err != nil {
		t.Fatalf("allocate specific port: %v", err)
	}
	_, err = store.allocate(ctx, 2048, 2048, testPortMapping("agent-b"))

	// Then
	if port != 2048 {
		t.Fatalf("allocated port = %d, want 2048", port)
	}
	if !errors.Is(err, errPortRangeExhausted) {
		t.Fatalf("allocation error = %v, want %v", err, errPortRangeExhausted)
	}
}

func TestPortStoreConcurrentAllocate(t *testing.T) {
	// Given
	store := testPortStore()
	ctx := context.Background()
	start := make(chan struct{})
	results := make(chan portAllocationResult, 50)
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			<-start
			port, err := store.allocate(ctx, 3000, 3009, testPortMapping("agent-a"))
			results <- portAllocationResult{port: port, err: err}
		})
	}

	// When
	close(start)
	wg.Wait()
	close(results)

	// Then
	ports := make(map[uint32]struct{})
	var successes, exhausted int
	for result := range results {
		if result.err == nil {
			successes++
			ports[result.port] = struct{}{}
			continue
		}
		if errors.Is(result.err, errPortRangeExhausted) {
			exhausted++
			continue
		}
		t.Fatalf("unexpected allocation error: %v", result.err)
	}
	if successes != 10 || len(ports) != 10 || exhausted != 40 {
		t.Fatalf("successes=%d unique=%d exhausted=%d, want 10, 10, 40", successes, len(ports), exhausted)
	}
}

func TestPortStoreReleaseRealloc(t *testing.T) {
	// Given
	store := testPortStore()
	ctx := context.Background()
	port, err := store.allocate(ctx, 4000, 4000, testPortMapping("agent-a"))
	if err != nil {
		t.Fatalf("allocate port: %v", err)
	}

	// When
	if err := store.release(ctx, port); err != nil {
		t.Fatalf("release allocated port: %v", err)
	}
	reallocated, err := store.allocate(ctx, 4000, 4000, testPortMapping("agent-b"))
	if err != nil {
		t.Fatalf("reallocate port: %v", err)
	}
	err = store.release(ctx, 4999)

	// Then
	if reallocated != port {
		t.Fatalf("reallocated port = %d, want %d", reallocated, port)
	}
	if err != nil {
		t.Fatalf("release unallocated port: %v", err)
	}
}

func TestPortStoreLookup(t *testing.T) {
	// Given
	store := testPortStore()
	ctx := context.Background()
	mapping := testPortMapping("agent-a")
	port, err := store.allocate(ctx, 5000, 5001, mapping)
	if err != nil {
		t.Fatalf("allocate port: %v", err)
	}

	// When
	got, err := store.lookup(ctx, port)
	desired, desiredErr := store.desired(ctx)
	_, missingErr := store.lookup(ctx, 5002)

	// Then
	if err != nil {
		t.Fatalf("lookup allocated port: %v", err)
	}
	if got != mapping {
		t.Fatalf("lookup mapping = %#v, want %#v", got, mapping)
	}
	if desiredErr != nil {
		t.Fatalf("get desired ports: %v", desiredErr)
	}
	if got := desired[port]; got != mapping || len(desired) != 1 {
		t.Fatalf("desired ports = %#v, want only %d: %#v", desired, port, mapping)
	}
	if !errors.Is(missingErr, errPortNotFound) {
		t.Fatalf("missing lookup error = %v, want %v", missingErr, errPortNotFound)
	}
}

func TestPortStoreReleaseByAgent(t *testing.T) {
	// Given
	store := testPortStore()
	ctx := context.Background()
	for _, mapping := range []portMapping{testPortMapping("agent-a"), testPortMapping("agent-b"), testPortMapping("agent-a")} {
		if _, err := store.allocate(ctx, 6000, 6002, mapping); err != nil {
			t.Fatalf("allocate port: %v", err)
		}
	}

	// When
	released, err := store.releaseByAgent(ctx, "agent-a")
	desired, desiredErr := store.desired(ctx)

	// Then
	if err != nil {
		t.Fatalf("release agent ports: %v", err)
	}
	if desiredErr != nil {
		t.Fatalf("get desired ports: %v", desiredErr)
	}
	slices.Sort(released)
	if got, want := released, []uint32{6000, 6002}; !equalPorts(got, want) {
		t.Fatalf("released ports = %v, want %v", got, want)
	}
	if got, want := desired, map[uint32]portMapping{6001: testPortMapping("agent-b")}; !maps.Equal(got, want) {
		t.Fatalf("desired ports = %#v, want %#v", got, want)
	}
}

func TestPortStoreWatch(t *testing.T) {
	// Given
	store := testPortStore()
	ctx := context.Background()
	changes, stop := store.watch()
	defer stop()

	// When
	if _, err := store.allocate(ctx, 7000, 7001, testPortMapping("agent-a")); err != nil {
		t.Fatalf("allocate port: %v", err)
	}

	// Then
	waitForPortSignal(t, changes)
	if err := store.release(ctx, 7000); err != nil {
		t.Fatalf("release port: %v", err)
	}
	waitForPortSignal(t, changes)

	// When
	if _, err := store.allocate(ctx, 7000, 7001, testPortMapping("agent-a")); err != nil {
		t.Fatalf("allocate first coalesced port: %v", err)
	}
	if _, err := store.allocate(ctx, 7000, 7001, testPortMapping("agent-b")); err != nil {
		t.Fatalf("allocate second coalesced port: %v", err)
	}

	// Then
	waitForPortSignal(t, changes)
	stop()
	if err := store.release(ctx, 7000); err != nil {
		t.Fatalf("release stopped-watch port: %v", err)
	}
	select {
	case <-changes:
		t.Fatal("stopped watch received a signal")
	case <-time.After(25 * time.Millisecond):
	}
}

func TestParsePortRange(t *testing.T) {
	tests := []struct {
		input   string
		want    portRange
		wantErr bool
	}{
		{input: "1025-2025", want: portRange{from: 1025, to: 2025}},
		{input: "1025", want: portRange{from: 1025, to: 1025}},
		{input: "0-5", wantErr: true},
		{input: "70000", wantErr: true},
		{input: "9-3", wantErr: true},
		{input: "abc", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			// When
			got, err := parsePortRange(test.input)

			// Then
			if test.wantErr {
				if err == nil {
					t.Fatal("parsePortRange returned nil error")
				}
				return
			}
			if err != nil {
				t.Fatalf("parsePortRange: %v", err)
			}
			if got != test.want {
				t.Fatalf("port range = %#v, want %#v", got, test.want)
			}
		})
	}
}

type portAllocationResult struct {
	port uint32
	err  error
}

func testPortStore() portStore { return newPortStore(logr.Discard(), nil) }

func testPortMapping(agentID string) portMapping {
	return portMapping{AgentID: agentID, Target: "172.30.1.5:8080"}
}

func waitForPortSignal(t *testing.T, changes <-chan struct{}) {
	t.Helper()
	select {
	case <-changes:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for port-store watch signal")
	}
}

func equalPorts(got, want []uint32) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
