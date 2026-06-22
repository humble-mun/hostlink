package agent

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/docker/docker/client"
	"github.com/go-logr/logr"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/humble-mun/hostlink/pkg/agentapi"
	hostlinkv1 "github.com/humble-mun/hostlink/pkg/api/hostlink/v1"
)

const (
	flagControllerEndpoint = "controller-endpoint"
	flagTLSCertPath        = "agent-tls-cert-path"
	flagTLSKeyPath         = "agent-tls-key-path"
	flagTLSCAPath          = "controller-tls-ca-path"
	flagTLSServerName      = "controller-tls-server-name"
)

// RegisterFlags registers the agent's controller-dial endpoint and mTLS flags.
func RegisterFlags(pfs *pflag.FlagSet) {
	pfs.String(flagControllerEndpoint, "", "address of the hostlink-controller gRPC endpoint to dial, as host:port")
	pfs.String(flagTLSCertPath, "", "The path to the client certificate the agent presents to the controller for mTLS.")
	pfs.String(flagTLSKeyPath, "", "The path to the private key matching the client certificate.")
	pfs.String(flagTLSCAPath, "", "The path to the CA bundle used to verify the controller's certificate.")
	pfs.String(flagTLSServerName, "", "The server name to verify against the controller's certificate; if empty, gRPC verifies against the dial endpoint's host, so set it explicitly when the certificate SAN differs from the dial address.")
}

// Agent is the hostlink agent: a manager.Runnable that maintains the Control
// stream to the controller and is closed on shutdown.
type Agent interface {
	manager.Runnable
	io.Closer
}

// New constructs an Agent that dials the controller at the configured endpoint
// over mTLS and serves controller-pushed Docker requests over the Control stream.
func New(logger logr.Logger, nodeName string) (ag Agent, err error) {
	endpoint := viper.GetString(flagControllerEndpoint)
	if endpoint == "" {
		return nil, fmt.Errorf("agent: %s must not be empty", flagControllerEndpoint)
	}

	creds, err := clientTransportCredentials()
	if err != nil {
		return nil, err
	}

	var docker *client.Client
	// FromEnv is lazy and does not dial the daemon here, so a daemon that is down
	// at startup does not fail construction; it surfaces when a request runs.
	if docker, err = client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation()); err != nil {
		return nil, fmt.Errorf("agent: create docker client: %w", err)
	}
	defer func() {
		if err != nil {
			if e := docker.Close(); e != nil {
				logger.Error(e, "failed to close docker client")
			}
		}
	}()

	dialOpts := []grpc.DialOption{grpc.WithTransportCredentials(creds)}
	// grpc.NewClient derives the TLS verification name (the :authority) from the
	// dial target by default, so dialing an IP makes x509 verify against that IP and
	// ignores tls.Config.ServerName (grpc overwrites it in ClientHandshake). Setting
	// the authority explicitly is the supported way to verify against a name that
	// differs from the dial address.
	if serverName := viper.GetString(flagTLSServerName); serverName != "" {
		dialOpts = append(dialOpts, grpc.WithAuthority(serverName))
	}

	var conn *grpc.ClientConn
	if conn, err = grpc.NewClient(endpoint, dialOpts...); err != nil {
		return nil, fmt.Errorf("agent: create gRPC client for %q: %w", endpoint, err)
	}

	ag = &agent{
		logger:   logger.WithName("agent"),
		nodeName: nodeName,
		conn:     conn,
		client:   hostlinkv1.NewAgentLinkClient(conn),
		docker:   docker,
	}
	return ag, nil
}

type agent struct {
	logger   logr.Logger
	nodeName string
	conn     *grpc.ClientConn
	client   hostlinkv1.AgentLinkClient
	docker   *client.Client

	stream grpc.BidiStreamingClient[hostlinkv1.AgentEvent, hostlinkv1.Command]
	sendMu sync.Mutex
}

func (a *agent) Close() (err error) {
	if a.docker != nil {
		if e := a.docker.Close(); e != nil {
			a.logger.Error(e, "failed to close docker client")
			err = errors.Join(err, e)
		}
	}
	if a.conn != nil {
		if e := a.conn.Close(); e != nil {
			a.logger.Error(e, "failed to close grpc client connection")
			err = errors.Join(err, e)
		}
	}
	return
}

// Start opens the Control stream to the controller, sends a Hello, then runs two
// concurrent activities until the context is cancelled or the stream fails: a
// heartbeat ticker that refreshes the agent->pod mapping, and a receive loop that
// handles controller-pushed commands (e.g. images.list) and sends back results.
func (a *agent) Start(ctx context.Context) error {
	logger := a.logger.WithName("control")

	stream, err := a.client.Control(ctx)
	if err != nil {
		return fmt.Errorf("agent: open control stream: %w", err)
	}
	a.stream = stream

	if err := a.send(&hostlinkv1.AgentEvent{
		AgentId: a.nodeName,
		Kind:    &hostlinkv1.AgentEvent_Hello{Hello: &hostlinkv1.Hello{}},
	}); err != nil {
		return fmt.Errorf("agent: send hello: %w", err)
	}
	logger.Info("control stream opened, hello sent")

	recvErrCh := make(chan error, 1)
	go func() { recvErrCh <- a.receiveCommands(ctx, logger) }()

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("control stream stopping", "reason", ctx.Err())
			if err := stream.CloseSend(); err != nil {
				return fmt.Errorf("agent: close control stream: %w", err)
			}
			return nil
		case err := <-recvErrCh:
			if err != nil {
				return fmt.Errorf("agent: receive commands: %w", err)
			}
			return nil
		case <-ticker.C:
			if err := a.send(&hostlinkv1.AgentEvent{
				AgentId: a.nodeName,
				Kind:    &hostlinkv1.AgentEvent_Heartbeat{Heartbeat: &hostlinkv1.Heartbeat{}},
			}); err != nil {
				return fmt.Errorf("agent: send heartbeat: %w", err)
			}
			logger.Info("heartbeat sent")
		}
	}
}

// send serializes writes to the Control stream: the heartbeat loop and command
// handlers both produce AgentEvents, and a gRPC stream allows only one
// concurrent Send.
func (a *agent) send(event *hostlinkv1.AgentEvent) (err error) {
	a.sendMu.Lock()
	defer a.sendMu.Unlock()
	if err = a.stream.Send(event); err != nil {
		err = fmt.Errorf("send event: %w", err)
	}
	return
}

// receiveCommands reads controller-pushed commands until the stream closes,
// handling each request on its own goroutine so a slow handler does not stall
// the receive loop. It returns nil on a clean close (EOF or context cancel).
func (a *agent) receiveCommands(ctx context.Context, logger logr.Logger) (err error) {
	for {
		var cmd *hostlinkv1.Command
		if cmd, err = a.stream.Recv(); err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
				return
			}
			if ctx.Err() != nil {
				err = nil
				return
			}
			return
		}

		switch c := cmd.GetCmd().(type) {
		case *hostlinkv1.Command_Request:
			go a.handleAndReply(ctx, c.Request, logger)
		default:
			logger.Info("received command with unsupported kind")
		}
	}
}

// handleAndReply executes a request and sends its result back over the Control
// stream. Single-shot methods produce one AgentResult; the streaming images.pull
// method emits zero or more AgentProgress events before its terminal AgentResult.
// A send failure is logged: the controller-side dispatcher unblocks via its own
// timeout, so there is nothing to return here.
func (a *agent) handleAndReply(ctx context.Context, req *hostlinkv1.AgentRequest, logger logr.Logger) {
	if req.GetMethod() == agentapi.MethodImagesPull {
		a.handlePull(ctx, req, logger)
		return
	}
	event := a.handleRequest(ctx, req)
	if err := a.send(event); err != nil {
		logger.Error(err, "send agent result failed",
			"requestID", req.GetRequestId(), "method", req.GetMethod())
	}
}

// handlePull runs a streaming images.pull: each PullProgress is sent as an
// AgentProgress event correlated by request_id, then a terminal AgentResult
// (Final=true) reports the outcome. emit send failures abort the pull by
// cancelling ctx, so a disconnected upstream stops the underlying docker pull.
func (a *agent) handlePull(ctx context.Context, req *hostlinkv1.AgentRequest, logger logr.Logger) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	requestID := req.GetRequestId()
	emit := func(p *agentapi.PullProgress) {
		payload, err := json.Marshal(p)
		if err != nil {
			logger.Error(err, "marshal pull progress failed", "requestID", requestID)
			return
		}
		if err := a.send(&hostlinkv1.AgentEvent{
			AgentId: a.nodeName,
			Kind: &hostlinkv1.AgentEvent_Progress{Progress: &hostlinkv1.AgentProgress{
				RequestId: requestID,
				Payload:   payload,
			}},
		}); err != nil {
			logger.Error(err, "send pull progress failed", "requestID", requestID)
			cancel()
		}
	}

	code, errMsg := a.pullImage(ctx, req, emit)
	result := &hostlinkv1.AgentResult{RequestId: requestID, Code: code, Error: errMsg, Final: true}
	if err := a.send(&hostlinkv1.AgentEvent{
		AgentId: a.nodeName,
		Kind:    &hostlinkv1.AgentEvent_Result{Result: result},
	}); err != nil {
		logger.Error(err, "send pull result failed", "requestID", requestID, "method", req.GetMethod())
	}
}

// clientTransportCredentials builds the mTLS transport credentials the agent
// uses to dial the controller. It loads the client certificate/key pair the
// agent presents for its own identity, and the CA bundle used to verify the
// controller's certificate. The connection is mutually authenticated; there is
// no insecure fallback.
func clientTransportCredentials() (credentials.TransportCredentials, error) {
	certPath := viper.GetString(flagTLSCertPath)
	keyPath := viper.GetString(flagTLSKeyPath)
	caPath := viper.GetString(flagTLSCAPath)
	if certPath == "" || keyPath == "" || caPath == "" {
		return nil, fmt.Errorf("agent: mTLS requires %s, %s, and %s to be set", flagTLSCertPath, flagTLSKeyPath, flagTLSCAPath)
	}

	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("agent: load client certificate/key pair: %w", err)
	}

	caPEM, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("agent: read CA bundle %q: %w", caPath, err)
	}
	rootCAs := x509.NewCertPool()
	if !rootCAs.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("agent: no certificates found in CA bundle %q", caPath)
	}

	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      rootCAs,
		ServerName:   viper.GetString(flagTLSServerName),
		MinVersion:   tls.VersionTLS13,
	}), nil
}
