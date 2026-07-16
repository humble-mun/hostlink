package controller

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"

	hostlinkv1 "github.com/humble-mun/hostlink/pkg/api/hostlink/v1"
	"github.com/humble-mun/hostlink/pkg/tunnel"
)

// forwardSession is the agent side of a Forward RPC matched to a waiting public
// connection handler. The consumer reads first before receiving from stream and
// must close done exactly once when it has finished using the stream.
type forwardSession struct {
	stream tunnel.FrameStream
	first  *hostlinkv1.Frame
	done   chan struct{}
}

// sessionTable matches one expected Forward session ID to the agent stream that
// arrives for it.
type sessionTable struct {
	mu      sync.Mutex
	waiters map[string]chan *forwardSession
}

func newSessionTable() *sessionTable {
	return &sessionTable{waiters: make(map[string]chan *forwardSession)}
}

// expect reserves sessionID for a single Forward stream. Its cancel function is
// safe to call multiple times and removes only this waiter.
func (t *sessionTable) expect(sessionID string) (<-chan *forwardSession, func()) {
	waiter := make(chan *forwardSession, 1)
	t.mu.Lock()
	t.waiters[sessionID] = waiter
	t.mu.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			t.mu.Lock()
			if t.waiters[sessionID] == waiter {
				delete(t.waiters, sessionID)
			}
			t.mu.Unlock()
		})
	}
	return waiter, cancel
}

// deliver hands a Forward stream to its waiting consumer. A delivered session is
// removed immediately, so a second stream for the same session ID is rejected.
func (t *sessionTable) deliver(sessionID string, session *forwardSession) bool {
	t.mu.Lock()
	waiter, ok := t.waiters[sessionID]
	if ok {
		delete(t.waiters, sessionID)
	}
	t.mu.Unlock()
	if !ok {
		return false
	}

	select {
	case waiter <- session:
		return true
	default:
		return false
	}
}

func newSessionID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate forward session ID: %w", err)
	}
	return hex.EncodeToString(value[:]), nil
}
