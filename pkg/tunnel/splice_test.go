package tunnel

import (
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"time"

	hostlinkv1 "github.com/humble-mun/hostlink/pkg/api/hostlink/v1"
)

const testTimeout = 3 * time.Second

type pipeStream struct {
	in     <-chan *hostlinkv1.Frame
	out    chan<- *hostlinkv1.Frame
	closed chan struct{}
}

func (s *pipeStream) Send(frame *hostlinkv1.Frame) error {
	select {
	case <-s.closed:
		return net.ErrClosed
	case s.out <- frame:
		return nil
	}
}

func (s *pipeStream) Recv() (*hostlinkv1.Frame, error) {
	select {
	case <-s.closed:
		return nil, io.EOF
	case frame := <-s.in:
		return frame, nil
	}
}

func newStreamPair() (*pipeStream, *pipeStream) {
	leftToRight := make(chan *hostlinkv1.Frame, 4)
	rightToLeft := make(chan *hostlinkv1.Frame, 4)
	return &pipeStream{in: rightToLeft, out: leftToRight, closed: make(chan struct{})},
		&pipeStream{in: leftToRight, out: rightToLeft, closed: make(chan struct{})}
}

func newTCPPair(t *testing.T) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	accepted := make(chan struct {
		conn *net.TCPConn
		err  error
	}, 1)
	go func() {
		conn, acceptErr := listener.AcceptTCP()
		accepted <- struct {
			conn *net.TCPConn
			err  error
		}{conn: conn, err: acceptErr}
	}()
	client, err := net.DialTCP("tcp", nil, listener.Addr().(*net.TCPAddr))
	if err != nil {
		if closeErr := listener.Close(); closeErr != nil {
			t.Errorf("close listener: %v", closeErr)
		}
		t.Fatalf("dial: %v", err)
	}
	result := <-accepted
	if err := listener.Close(); err != nil {
		t.Errorf("close listener: %v", err)
	}
	if result.err != nil {
		t.Fatalf("accept: %v", result.err)
	}
	t.Cleanup(func() {
		closeTCP(t, client)
		closeTCP(t, result.conn)
	})
	return client, result.conn
}

func closeTCP(t *testing.T, conn *net.TCPConn) {
	t.Helper()
	if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Errorf("close TCP connection: %v", err)
	}
}

func startEcho(conn *net.TCPConn) <-chan error {
	result := make(chan error, 1)
	go func() {
		_, err := io.Copy(conn, conn)
		if err == nil {
			err = conn.CloseWrite()
		}
		result <- err
	}()
	return result
}

func startSplice(conn HalfCloser, stream FrameStream) <-chan error {
	result := make(chan error, 1)
	go func() {
		result <- SpliceConn(conn, stream)
	}()
	return result
}

func startRelay(left, right FrameStream) <-chan error {
	result := make(chan error, 1)
	go func() {
		result <- SpliceStream(left, right)
	}()
	return result
}

func awaitResult(t *testing.T, result <-chan error) error {
	t.Helper()
	timer := time.NewTimer(testTimeout)
	defer timer.Stop()
	select {
	case err := <-result:
		return err
	case <-timer.C:
		t.Fatal("timed out waiting for splice result")
		return nil
	}
}

func expectConnectionClosed(t *testing.T, conn *net.TCPConn) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(testTimeout)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	_, err := conn.Read(make([]byte, 1))
	if err == nil {
		t.Fatal("read succeeded after reset")
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("connection was not closed after reset: %v", err)
	}
}

type spliceBridge struct {
	client      *net.TCPConn
	remote      *pipeStream
	leftResult  <-chan error
	rightResult <-chan error
	echoResult  <-chan error
}

func newSpliceBridge(t *testing.T) *spliceBridge {
	t.Helper()
	client, left := newTCPPair(t)
	right, echo := newTCPPair(t)
	leftStream, rightStream := newStreamPair()
	return &spliceBridge{
		client:      client,
		remote:      rightStream,
		leftResult:  startSplice(left, leftStream),
		rightResult: startSplice(right, rightStream),
		echoResult:  startEcho(echo),
	}
}

func (b *spliceBridge) echo(t *testing.T, chunks [][]byte) []byte {
	t.Helper()
	if err := b.client.SetDeadline(time.Now().Add(testTimeout)); err != nil {
		t.Fatalf("set client deadline: %v", err)
	}
	for _, chunk := range chunks {
		if err := writeAll(b.client, chunk); err != nil {
			t.Fatalf("write client payload: %v", err)
		}
	}
	if err := b.client.CloseWrite(); err != nil {
		t.Fatalf("client close write: %v", err)
	}
	data, err := io.ReadAll(b.client)
	if err != nil {
		t.Fatalf("read echoed payload: %v", err)
	}
	return data
}

func (b *spliceBridge) await(t *testing.T) {
	t.Helper()
	if err := awaitResult(t, b.leftResult); err != nil {
		t.Fatalf("left splice: %v", err)
	}
	if err := awaitResult(t, b.rightResult); err != nil {
		t.Fatalf("right splice: %v", err)
	}
	if err := awaitResult(t, b.echoResult); err != nil {
		t.Fatalf("echo server: %v", err)
	}
}

func writeAll(conn net.Conn, data []byte) error {
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

func TestSpliceConnEcho(t *testing.T) {
	// Given
	bridge := newSpliceBridge(t)
	chunks := [][]byte{
		{0x01},
		bytes.Repeat([]byte{0x2A}, chunkSize-1),
		patternedBytes(100*1024 + 7),
	}
	want := bytes.Join(chunks, nil)

	// When
	got := bridge.echo(t, chunks)

	// Then
	if !bytes.Equal(got, want) {
		t.Fatal("echoed payload differs")
	}
	bridge.await(t)
}

func TestSpliceConnClientHalfClose(t *testing.T) {
	// Given
	bridge := newSpliceBridge(t)
	want := []byte("full echo after client CloseWrite")

	// When
	got := bridge.echo(t, [][]byte{want})

	// Then
	if !bytes.Equal(got, want) {
		t.Fatal("client did not receive the complete echo after CloseWrite")
	}
	bridge.await(t)
}

func TestSpliceConnReset(t *testing.T) {
	// Given
	client, local := newTCPPair(t)
	stream, peer := newStreamPair()
	spliceResult := startSplice(local, stream)

	// When
	if err := peer.Send(&hostlinkv1.Frame{Type: hostlinkv1.Frame_RESET}); err != nil {
		t.Fatalf("send reset: %v", err)
	}

	// Then
	expectConnectionClosed(t, client)
	if err := awaitResult(t, spliceResult); err == nil {
		t.Fatal("splice returned nil after reset")
	}
}

func TestSpliceConnIgnoresOpenReady(t *testing.T) {
	// Given
	bridge := newSpliceBridge(t)
	if err := bridge.remote.Send(&hostlinkv1.Frame{SessionId: "pairing", Type: hostlinkv1.Frame_OPEN}); err != nil {
		t.Fatalf("send open: %v", err)
	}
	if err := bridge.remote.Send(&hostlinkv1.Frame{Type: hostlinkv1.Frame_READY}); err != nil {
		t.Fatalf("send ready: %v", err)
	}
	want := patternedBytes(chunkSize + 3)

	// When
	got := bridge.echo(t, [][]byte{want})

	// Then
	if !bytes.Equal(got, want) {
		t.Fatal("OPEN or READY corrupted the byte stream")
	}
	bridge.await(t)
}

func TestSpliceStreamRelay(t *testing.T) {
	// Given
	client, left := newTCPPair(t)
	right, echo := newTCPPair(t)
	leftStream, firstRelay := newStreamPair()
	secondRelay, rightStream := newStreamPair()
	leftResult := startSplice(left, leftStream)
	rightResult := startSplice(right, rightStream)
	relayResult := startRelay(firstRelay, secondRelay)
	echoResult := startEcho(echo)
	want := patternedBytes(100*1024 + 11)
	if err := client.SetDeadline(time.Now().Add(testTimeout)); err != nil {
		t.Fatalf("set client deadline: %v", err)
	}

	// When
	if err := writeAll(client, want); err != nil {
		t.Fatalf("write client payload: %v", err)
	}
	if err := client.CloseWrite(); err != nil {
		t.Fatalf("client close write: %v", err)
	}
	got, err := io.ReadAll(client)
	if err != nil {
		t.Fatalf("read echoed payload: %v", err)
	}

	// Then
	if !bytes.Equal(got, want) {
		t.Fatal("relay echoed payload differs")
	}
	if err := awaitResult(t, leftResult); err != nil {
		t.Fatalf("left splice: %v", err)
	}
	if err := awaitResult(t, rightResult); err != nil {
		t.Fatalf("right splice: %v", err)
	}
	if err := awaitResult(t, echoResult); err != nil {
		t.Fatalf("echo server: %v", err)
	}
	close(firstRelay.closed)
	close(secondRelay.closed)
	if err := awaitResult(t, relayResult); err != nil {
		t.Fatalf("relay: %v", err)
	}
}

func TestSpliceStreamReset(t *testing.T) {
	// Given
	leftClient, left := newTCPPair(t)
	rightClient, right := newTCPPair(t)
	leftStream, firstRelay := newStreamPair()
	secondRelay, rightStream := newStreamPair()
	leftResult := startSplice(left, leftStream)
	rightResult := startSplice(right, rightStream)
	relayResult := startRelay(firstRelay, secondRelay)

	// When
	if err := leftStream.Send(&hostlinkv1.Frame{Type: hostlinkv1.Frame_RESET}); err != nil {
		t.Fatalf("inject reset: %v", err)
	}

	// Then
	expectConnectionClosed(t, leftClient)
	expectConnectionClosed(t, rightClient)
	if err := awaitResult(t, leftResult); err == nil {
		t.Fatal("left splice returned nil after reset")
	}
	if err := awaitResult(t, rightResult); err == nil {
		t.Fatal("right splice returned nil after reset")
	}
	if err := awaitResult(t, relayResult); err == nil {
		t.Fatal("relay returned nil after reset")
	}
}

func patternedBytes(size int) []byte {
	data := make([]byte, size)
	for index := range data {
		data[index] = byte(index)
	}
	return data
}
