package tunnel

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	hostlinkv1 "github.com/humble-mun/hostlink/pkg/api/hostlink/v1"
)

// FrameStream is the send/recv surface of a gRPC bidi Frame stream (both
// grpc.BidiStreamingServer[Frame,Frame] and grpc.BidiStreamingClient sides satisfy it).
type FrameStream interface {
	Send(*hostlinkv1.Frame) error
	Recv() (*hostlinkv1.Frame, error)
}

const chunkSize = 32 * 1024

// HalfCloser is what SpliceConn needs from the local socket: *net.TCPConn implements it.
type HalfCloser interface {
	net.Conn
	CloseWrite() error
}

var errReset = errors.New("tunnel reset")

// SpliceConn bridges a local TCP conn and a Frame stream until both directions finish.
func SpliceConn(conn HalfCloser, stream FrameStream) error {
	var sendMu sync.Mutex
	send := func(frame *hostlinkv1.Frame) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return stream.Send(frame)
	}

	closer := connCloser{conn: conn}
	var errOnce sync.Once
	var firstErr error
	fail := func(err error) {
		errOnce.Do(func() {
			firstErr = errors.Join(err, closer.close(true))
		})
	}
	reset := func(err error) {
		if sendErr := send(&hostlinkv1.Frame{Type: hostlinkv1.Frame_RESET}); sendErr != nil {
			err = errors.Join(err, fmt.Errorf("send reset: %w", sendErr))
		}
		fail(err)
	}

	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		buffer := make([]byte, chunkSize)
		for {
			n, readErr := conn.Read(buffer)
			if n > 0 {
				data := append([]byte(nil), buffer[:n]...)
				if err := send(&hostlinkv1.Frame{Type: hostlinkv1.Frame_DATA, Data: data}); err != nil {
					fail(fmt.Errorf("send frame data: %w", err))
					return
				}
			}
			if readErr == nil {
				continue
			}
			if errors.Is(readErr, io.EOF) {
				if err := send(&hostlinkv1.Frame{Type: hostlinkv1.Frame_HALF_CLOSE}); err != nil {
					fail(fmt.Errorf("send half-close: %w", err))
				}
				return
			}
			reset(fmt.Errorf("read local connection: %w", readErr))
			return
		}
	}()
	go func() {
		defer workers.Done()
		for {
			frame, recvErr := stream.Recv()
			if recvErr != nil {
				reset(fmt.Errorf("receive frame: %w", recvErr))
				return
			}
			switch frame.GetType() {
			case hostlinkv1.Frame_DATA:
				if err := writeFrameData(conn, frame.GetData()); err != nil {
					reset(fmt.Errorf("write local connection: %w", err))
					return
				}
			case hostlinkv1.Frame_HALF_CLOSE:
				if err := conn.CloseWrite(); err != nil {
					reset(fmt.Errorf("close local write: %w", err))
				}
				return
			case hostlinkv1.Frame_RESET:
				fail(errReset)
				return
			case hostlinkv1.Frame_OPEN, hostlinkv1.Frame_READY:
				continue
			default:
				reset(fmt.Errorf("unknown frame type %d", frame.GetType()))
				return
			}
		}
	}()
	workers.Wait()
	if firstErr != nil {
		return firstErr
	}
	return closer.close(false)
}

// SpliceStream bridges two Frame streams (relay hop). Forwards DATA/HALF_CLOSE/RESET
// in both directions. Returns when both directions have finished or either RESETs.
func SpliceStream(a, b FrameStream) error {
	var aSendMu sync.Mutex
	var bSendMu sync.Mutex
	aSend := lockedSender(a, &aSendMu)
	bSend := lockedSender(b, &bSendMu)
	results := make(chan error, 2)
	go func() {
		results <- spliceStreamDirection(a, bSend)
	}()
	go func() {
		results <- spliceStreamDirection(b, aSend)
	}()
	return errors.Join(<-results, <-results)
}

type connCloser struct {
	conn HalfCloser
	once sync.Once
	err  error
}

func (c *connCloser) close(abort bool) error {
	c.once.Do(func() {
		if abort {
			if tcpConn, ok := c.conn.(*net.TCPConn); ok {
				if err := tcpConn.SetLinger(0); err != nil {
					c.err = fmt.Errorf("set TCP linger: %w", err)
				}
			}
		}
		if err := c.conn.Close(); err != nil {
			c.err = errors.Join(c.err, fmt.Errorf("close local connection: %w", err))
		}
	})
	return c.err
}

func lockedSender(stream FrameStream, mutex *sync.Mutex) func(*hostlinkv1.Frame) error {
	return func(frame *hostlinkv1.Frame) error {
		mutex.Lock()
		defer mutex.Unlock()
		return stream.Send(frame)
	}
}

func spliceStreamDirection(source FrameStream, send func(*hostlinkv1.Frame) error) error {
	halfClosed := false
	for {
		frame, recvErr := source.Recv()
		if recvErr != nil {
			if halfClosed && errors.Is(recvErr, io.EOF) {
				return nil
			}
			return sendReset(send, fmt.Errorf("receive frame: %w", recvErr))
		}
		if halfClosed && frame.GetType() != hostlinkv1.Frame_RESET {
			return sendReset(send, fmt.Errorf("frame type %d after half-close", frame.GetType()))
		}
		switch frame.GetType() {
		case hostlinkv1.Frame_DATA:
			if err := send(&hostlinkv1.Frame{Type: hostlinkv1.Frame_DATA, Data: frame.GetData()}); err != nil {
				return fmt.Errorf("forward frame data: %w", err)
			}
		case hostlinkv1.Frame_HALF_CLOSE:
			if err := send(&hostlinkv1.Frame{Type: hostlinkv1.Frame_HALF_CLOSE}); err != nil {
				return fmt.Errorf("forward half-close: %w", err)
			}
			halfClosed = true
		case hostlinkv1.Frame_RESET:
			if err := send(&hostlinkv1.Frame{Type: hostlinkv1.Frame_RESET}); err != nil {
				return errors.Join(errReset, fmt.Errorf("forward reset: %w", err))
			}
			return errReset
		case hostlinkv1.Frame_OPEN, hostlinkv1.Frame_READY:
			continue
		default:
			return sendReset(send, fmt.Errorf("unknown frame type %d", frame.GetType()))
		}
	}
}

func sendReset(send func(*hostlinkv1.Frame) error, cause error) error {
	if err := send(&hostlinkv1.Frame{Type: hostlinkv1.Frame_RESET}); err != nil {
		return errors.Join(cause, fmt.Errorf("send reset: %w", err))
	}
	return cause
}

func writeFrameData(conn net.Conn, data []byte) error {
	for len(data) > 0 {
		n, err := conn.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
