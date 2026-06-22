package controller

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"

	"github.com/go-logr/logr"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	hostlinkv1 "github.com/humble-mun/hostlink/pkg/api/hostlink/v1"
)

// errAgentNotConnected means the agent is not held by this replica and no live
// mapping points elsewhere, so the request cannot be served. The REST layer maps
// it to 404.
var errAgentNotConnected = errors.New("agent not connected")

// peerServer is the ControllerPeer service: it receives a relayed request from a
// sibling pod and drives it over the agent's Control stream held locally. It is
// bound to a dedicated in-cluster listener, never the agent-facing one, so an
// agent cannot reach this plane and target other agents.
type peerServer struct {
	hostlinkv1.UnimplementedControllerPeerServer
	logger   logr.Logger
	registry *registry
}

// Dispatch resolves the agent locally and runs the request. A mapping that has
// gone stale (the agent is no longer here) is rejected with FAILED_PRECONDITION
// so the caller re-resolves and retries.
func (s *peerServer) Dispatch(ctx context.Context, req *hostlinkv1.DispatchRequest) (result *hostlinkv1.AgentResult, err error) {
	agentID := req.GetAgentId()
	conn, ok := s.registry.get(agentID)
	if !ok {
		err = status.Errorf(codes.FailedPrecondition, "agent %q not held by this controller", agentID)
		return
	}
	if result, err = conn.dispatch(ctx, req.GetRequest().GetMethod(), req.GetRequest().GetPayload()); err != nil {
		s.logger.Error(err, "relayed dispatch to agent failed", "agentID", agentID)
		if errors.Is(err, errAgentDisconnected) {
			err = status.Errorf(codes.FailedPrecondition, "agent %q disconnected during dispatch", agentID)
			return
		}
		err = status.Errorf(codes.Internal, "dispatch to agent %q failed", agentID)
		return
	}
	return
}

// DispatchStream resolves the agent locally and runs a streaming request,
// forwarding each AgentResult frame (progress frames followed by the terminal
// frame) to the caller. A stale mapping is rejected with FAILED_PRECONDITION so
// the caller re-resolves and retries.
func (s *peerServer) DispatchStream(req *hostlinkv1.DispatchRequest, stream grpc.ServerStreamingServer[hostlinkv1.AgentResult]) (err error) {
	agentID := req.GetAgentId()
	conn, ok := s.registry.get(agentID)
	if !ok {
		return status.Errorf(codes.FailedPrecondition, "agent %q not held by this controller", agentID)
	}
	ctx := stream.Context()
	frames, cancel, derr := conn.dispatchStream(ctx, req.GetRequest().GetMethod(), req.GetRequest().GetPayload())
	if derr != nil {
		s.logger.Error(derr, "relayed stream dispatch to agent failed", "agentID", agentID)
		if errors.Is(derr, errAgentDisconnected) {
			return status.Errorf(codes.FailedPrecondition, "agent %q disconnected during dispatch", agentID)
		}
		return status.Errorf(codes.Internal, "dispatch to agent %q failed", agentID)
	}
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case frame, ok := <-frames:
			if !ok {
				return status.Errorf(codes.FailedPrecondition, "agent %q disconnected during dispatch", agentID)
			}
			if err = stream.Send(&hostlinkv1.AgentResult{
				RequestId: req.GetRequest().GetRequestId(),
				Payload:   frame.Payload,
				Code:      frame.Code,
				Error:     frame.Error,
				Final:     frame.Final,
			}); err != nil {
				return fmt.Errorf("forward agent %q result frame: %w", agentID, err)
			}
			if frame.Final {
				return nil
			}
		}
	}
}

// peerClients dials sibling controllers' ControllerPeer listeners and caches one
// connection per peer address. grpc.ClientConn is safe for concurrent use, so a
// single connection per sibling is reused across relays.
type peerClients struct {
	logger     logr.Logger
	creds      credentials.TransportCredentials
	serverName string

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

func newPeerClients(logger logr.Logger, creds credentials.TransportCredentials, serverName string) *peerClients {
	return &peerClients{logger: logger, creds: creds, serverName: serverName, conns: make(map[string]*grpc.ClientConn)}
}

// dispatch relays a request to the controller at addr. A FAILED_PRECONDITION
// from the sibling (its mapping was stale) is normalized to errAgentNotConnected
// so the REST layer reports a 404 rather than a relay failure.
func (p *peerClients) dispatch(ctx context.Context, addr, agentID, method string, payload []byte) (result *hostlinkv1.AgentResult, err error) {
	var conn *grpc.ClientConn
	if conn, err = p.conn(addr); err != nil {
		return
	}
	client := hostlinkv1.NewControllerPeerClient(conn)
	if result, err = client.Dispatch(ctx, &hostlinkv1.DispatchRequest{
		AgentId: agentID,
		Request: &hostlinkv1.AgentRequest{Method: method, Payload: payload},
	}); err != nil {
		if status.Code(err) == codes.FailedPrecondition {
			err = errAgentNotConnected
		}
		return
	}
	return
}

// dispatchStream relays a streaming request to the controller at addr, returning
// a channel of frames (progress frames followed by the terminal frame) closed
// when the relay ends. A FAILED_PRECONDITION from the sibling (its mapping was
// stale) is normalized to errAgentNotConnected so the REST layer reports a 404.
// The caller must cancel ctx to release the relay goroutine.
func (p *peerClients) dispatchStream(ctx context.Context, addr, agentID, method string, payload []byte) (frames <-chan streamFrame, err error) {
	var conn *grpc.ClientConn
	if conn, err = p.conn(addr); err != nil {
		return
	}
	client := hostlinkv1.NewControllerPeerClient(conn)
	var relay grpc.ServerStreamingClient[hostlinkv1.AgentResult]
	if relay, err = client.DispatchStream(ctx, &hostlinkv1.DispatchRequest{
		AgentId: agentID,
		Request: &hostlinkv1.AgentRequest{Method: method, Payload: payload},
	}); err != nil {
		if status.Code(err) == codes.FailedPrecondition {
			err = errAgentNotConnected
		}
		return
	}
	ch := make(chan streamFrame, 64)
	frames = ch
	go func() {
		defer close(ch)
		for {
			result, rerr := relay.Recv()
			if rerr != nil {
				if !errors.Is(rerr, io.EOF) {
					p.logger.Error(rerr, "peer stream relay recv failed", "agentID", agentID, "addr", addr)
				}
				return
			}
			frame := streamFrame{
				Payload: result.GetPayload(),
				Code:    result.GetCode(),
				Error:   result.GetError(),
				Final:   result.GetFinal(),
			}
			select {
			case ch <- frame:
			case <-ctx.Done():
				return
			}
			if frame.Final {
				return
			}
		}
	}()
	return
}

// conn returns a cached connection to addr, dialing one on first use. grpc.NewClient
// is lazy, so this never blocks on the network.
func (p *peerClients) conn(addr string) (conn *grpc.ClientConn, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if conn = p.conns[addr]; conn != nil {
		return
	}

	dialOpts := []grpc.DialOption{grpc.WithTransportCredentials(p.creds)}
	if p.serverName != "" {
		dialOpts = append(dialOpts, grpc.WithAuthority(p.serverName))
	}
	if conn, err = grpc.NewClient(addr, dialOpts...); err != nil {
		err = fmt.Errorf("dial peer %q: %w", addr, err)
		return
	}
	p.conns[addr] = conn
	return
}

// close tears down all cached sibling connections.
func (p *peerClients) close() (err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for addr, conn := range p.conns {
		if e := conn.Close(); e != nil {
			p.logger.Error(e, "close peer connection failed", "addr", addr)
			err = errors.Join(err, e)
		}
		delete(p.conns, addr)
	}
	return
}

// peerServerCredentials builds the mTLS credentials the ControllerPeer listener
// presents and uses to require-and-verify sibling client certificates. There is
// no insecure fallback.
func peerServerCredentials() (creds credentials.TransportCredentials, err error) {
	var cert tls.Certificate
	var pool *x509.CertPool
	if cert, pool, err = peerCertAndCAPool(); err != nil {
		return
	}
	creds = credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	})
	return
}

// peerClientCredentials builds the mTLS credentials used to dial siblings: it
// presents the same controller certificate and verifies the sibling against the
// peer CA.
func peerClientCredentials() (creds credentials.TransportCredentials, err error) {
	var cert tls.Certificate
	var pool *x509.CertPool
	if cert, pool, err = peerCertAndCAPool(); err != nil {
		return
	}
	creds = credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ServerName:   viper.GetString(flagPeerTLSServerName),
		MinVersion:   tls.VersionTLS13,
	})
	return
}

// peerCertAndCAPool loads the shared controller certificate/key pair and the CA
// bundle used on the ControllerPeer plane, where the controller acts as both
// server and client.
func peerCertAndCAPool() (cert tls.Certificate, pool *x509.CertPool, err error) {
	certPath := viper.GetString(flagPeerTLSCertPath)
	keyPath := viper.GetString(flagPeerTLSKeyPath)
	caPath := viper.GetString(flagPeerTLSCAPath)
	if certPath == "" || keyPath == "" || caPath == "" {
		err = fmt.Errorf("peer plane mTLS requires %s, %s, and %s to be set", flagPeerTLSCertPath, flagPeerTLSKeyPath, flagPeerTLSCAPath)
		return
	}

	if cert, err = tls.LoadX509KeyPair(certPath, keyPath); err != nil {
		err = fmt.Errorf("load peer certificate/key pair: %w", err)
		return
	}

	var caPEM []byte
	if caPEM, err = os.ReadFile(caPath); err != nil {
		err = fmt.Errorf("read peer CA bundle %q: %w", caPath, err)
		return
	}
	pool = x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		err = fmt.Errorf("no certificates found in peer CA bundle %q", caPath)
		return
	}
	return
}

// startPeerServer binds the ControllerPeer mTLS listener and serves it in the
// background. The returned done channel is closed once the serve goroutine has
// fully exited, so a caller that triggers GracefulStop can wait for a clean
// shutdown. Bind errors surface synchronously so a misconfiguration fails fast.
func startPeerServer(logger logr.Logger, bindAddr string, reg *registry) (srv *grpc.Server, done <-chan struct{}, err error) {
	var creds credentials.TransportCredentials
	if creds, err = peerServerCredentials(); err != nil {
		return
	}

	var lis net.Listener
	if lis, err = net.Listen("tcp", bindAddr); err != nil {
		err = fmt.Errorf("bind peer listener on %q: %w", bindAddr, err)
		return
	}

	srv = grpc.NewServer(grpc.Creds(creds))
	hostlinkv1.RegisterControllerPeerServer(srv, &peerServer{logger: logger, registry: reg})

	stopped := make(chan struct{})
	done = stopped
	go func() {
		defer close(stopped)
		logger.Info("controller peer listener started", "addr", bindAddr)
		if e := srv.Serve(lis); e != nil {
			logger.Error(e, "controller peer listener stopped")
		}
	}()
	return
}
