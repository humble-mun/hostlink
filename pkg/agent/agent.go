package agent

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/docker/docker/client"
	"github.com/go-logr/logr"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
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
	flagDataDir            = "data-dir"
)

const (
	// agentKeepaliveTime and agentKeepaliveTimeout configure the client-side
	// HTTP/2 keepalive pings so a controller that vanishes (crash, redeploy, LB
	// drop) is detected within roughly Time+Timeout rather than only at the next
	// 15s heartbeat. The controller's KeepaliveEnforcementPolicy must permit this
	// ping interval, otherwise it answers with GOAWAY too_many_pings.
	agentKeepaliveTime    = 30 * time.Second
	agentKeepaliveTimeout = 20 * time.Second

	// reconnectBaseDelay and reconnectMaxDelay bound the exponential backoff the
	// supervise loop waits between control-stream reconnect attempts.
	reconnectBaseDelay = 1 * time.Second
	reconnectMaxDelay  = 30 * time.Second
)

// RegisterFlags registers the agent's controller-dial endpoint and mTLS flags.
func RegisterFlags(pfs *pflag.FlagSet) {
	pfs.String(flagControllerEndpoint, "", "address of the hostlink controller gRPC endpoint to dial, as host:port")
	pfs.String(flagTLSCertPath, "", "The path to the client certificate the agent presents to the controller for mTLS.")
	pfs.String(flagTLSKeyPath, "", "The path to the private key matching the client certificate.")
	pfs.String(flagTLSCAPath, "", "The path to the CA bundle used to verify the controller's certificate.")
	pfs.String(flagTLSServerName, "", "The server name to verify against the controller's certificate; if empty, gRPC verifies against the dial endpoint's host, so set it explicitly when the certificate SAN differs from the dial address.")
	pfs.String(flagDataDir, "", "The working directory whose contents the controller can browse, download, upload, and delete through the fs API. When empty the fs API is disabled and fs requests are rejected.")
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

	var dataDir string
	if dataDir, err = resolveDataDir(viper.GetString(flagDataDir)); err != nil {
		return nil, err
	}

	var scrapeTargets []scrapeTarget
	if scrapeTargets, err = resolveScrapeTargets(); err != nil {
		return nil, err
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

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		// Keepalive pings let the agent notice a dead controller promptly and keep
		// the connection alive through any intervening L4 LB; PermitWithoutStream
		// covers the gaps between control-stream sessions during a reconnect.
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                agentKeepaliveTime,
			Timeout:             agentKeepaliveTimeout,
			PermitWithoutStream: true,
		}),
	}
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
		logger:        logger.WithName("agent"),
		nodeName:      nodeName,
		dataDir:       dataDir,
		scrapeTargets: scrapeTargets,
		conn:          conn,
		client:        hostlinkv1.NewAgentLinkClient(conn),
		docker:        docker,
		inbound:       make(map[string]*inboundStream),
		cancels:       make(map[string]context.CancelFunc),
	}
	return ag, nil
}

type agent struct {
	logger   logr.Logger
	nodeName string
	dataDir  string
	conn     *grpc.ClientConn
	client   hostlinkv1.AgentLinkClient
	docker   *client.Client

	// scrapeTargets are the resolved upstream exporters pulled on a metrics.scrape
	// request, each carrying its own request URL and HTTP client. An empty list
	// disables the feature.
	scrapeTargets []scrapeTarget

	stream grpc.BidiStreamingClient[hostlinkv1.AgentEvent, hostlinkv1.Command]
	sendMu sync.Mutex

	// inbound holds the chunk channels for in-flight streaming uploads (fs.write),
	// keyed by request_id. An entry is registered synchronously in the receive
	// loop when the opening AgentRequest arrives, so a body chunk that follows it
	// is never dropped for a missing channel.
	inboundMu sync.Mutex
	inbound   map[string]*inboundStream

	// cancels holds the per-request context cancel of each in-flight streaming
	// handler, keyed by request_id, so a controller request.cancel can stop it.
	// An entry is registered synchronously in the receive loop (before the next
	// command is read), so a cancel that follows the opening request can never
	// miss it.
	cancelMu sync.Mutex
	cancels  map[string]context.CancelFunc
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

// Start supervises the Control stream: it runs one session at a time and, when a
// session ends because the controller went away (not because ctx was cancelled),
// reconnects after an exponential backoff. It returns only when ctx is cancelled,
// so a controller redeploy is ridden out in-process rather than by a full restart.
func (a *agent) Start(ctx context.Context) error {
	logger := a.logger.WithName("control")
	var bo backoff
	for {
		connected, err := a.runSession(ctx, logger)
		if ctx.Err() != nil {
			logger.Info("control supervisor stopping", "reason", ctx.Err())
			return nil
		}
		// A session that actually connected resets the backoff so a long-healthy
		// link reconnects promptly; one that never connected keeps backing off.
		if connected {
			bo.reset()
		}
		delay := bo.next()
		if err != nil {
			logger.Error(err, "control session ended; reconnecting", "delay", delay)
		} else {
			logger.Info("control session ended; reconnecting", "delay", delay)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
	}
}

// runSession opens one Control stream, sends a Hello, and runs the heartbeat
// ticker and command receive loop until ctx is cancelled or the stream fails. It
// reports whether the session connected (Hello sent) so the supervisor can reset
// its backoff. A session-scoped context tears down in-flight command handlers and
// the stream when the session ends, so they never bleed into the next session.
func (a *agent) runSession(ctx context.Context, logger logr.Logger) (connected bool, err error) {
	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var stream grpc.BidiStreamingClient[hostlinkv1.AgentEvent, hostlinkv1.Command]
	if stream, err = a.client.Control(sessionCtx); err != nil {
		err = fmt.Errorf("agent: open control stream: %w", err)
		return
	}
	a.setStream(stream)

	if err = a.send(&hostlinkv1.AgentEvent{
		AgentId: a.nodeName,
		Kind:    &hostlinkv1.AgentEvent_Hello{Hello: &hostlinkv1.Hello{}},
	}); err != nil {
		err = fmt.Errorf("agent: send hello: %w", err)
		return
	}
	connected = true
	logger.Info("control stream opened, hello sent")

	recvErrCh := make(chan error, 1)
	go func() { recvErrCh <- a.receiveCommands(sessionCtx, logger) }()
	go a.watchDockerEvents(sessionCtx)

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-sessionCtx.Done():
			logger.Info("control stream stopping", "reason", sessionCtx.Err())
			if cerr := stream.CloseSend(); cerr != nil {
				logger.Error(cerr, "close control stream failed")
			}
			return
		case rerr := <-recvErrCh:
			if rerr != nil {
				err = fmt.Errorf("agent: receive commands: %w", rerr)
			}
			return
		case <-ticker.C:
			if err = a.send(&hostlinkv1.AgentEvent{
				AgentId: a.nodeName,
				Kind:    &hostlinkv1.AgentEvent_Heartbeat{Heartbeat: &hostlinkv1.Heartbeat{}},
			}); err != nil {
				err = fmt.Errorf("agent: send heartbeat: %w", err)
				return
			}
			logger.V(4).Info("heartbeat sent")
		}
	}
}

// setStream publishes the active session's stream under sendMu so a concurrent
// send never races the reconnect that swaps it.
func (a *agent) setStream(stream grpc.BidiStreamingClient[hostlinkv1.AgentEvent, hostlinkv1.Command]) {
	a.sendMu.Lock()
	a.stream = stream
	a.sendMu.Unlock()
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

// backoff is the exponential reconnect delay between control-stream sessions,
// growing from reconnectBaseDelay to reconnectMaxDelay and reset after a session
// that connected.
type backoff struct {
	cur time.Duration
}

// next advances the backoff and returns the next delay with jitter applied.
func (b *backoff) next() (delay time.Duration) {
	if b.cur < reconnectBaseDelay {
		b.cur = reconnectBaseDelay
	} else {
		b.cur *= 2
		if b.cur > reconnectMaxDelay {
			b.cur = reconnectMaxDelay
		}
	}
	// Subtract up to 20% so a fleet of agents reconnecting after one controller
	// redeploy spreads out instead of stampeding in lockstep.
	delay = b.cur - time.Duration(rand.Int63n(int64(b.cur)/5+1)) //nolint:gosec // reconnect jitter is not security-sensitive
	return
}

// reset returns the backoff to its base delay.
func (b *backoff) reset() {
	b.cur = 0
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
		case *hostlinkv1.Command_OpenForward:
			a.handleOpenForward(ctx, c.OpenForward)
		case *hostlinkv1.Command_Request:
			a.startRequest(ctx, c.Request, logger)
		case *hostlinkv1.Command_Chunk:
			a.routeChunk(c.Chunk)
		default:
			logger.Info("received command with unsupported kind")
		}
	}
}

// startRequest dispatches an opening AgentRequest. Streaming methods run on their
// own goroutine but must have their inbound plumbing (upload chunk channels,
// cancel registrations) set up synchronously here (before the next command is
// read), so an early body chunk or cancel is never raced against a missing
// entry; single-shot methods are simply handled off-loop.
func (a *agent) startRequest(ctx context.Context, req *hostlinkv1.AgentRequest, logger logr.Logger) {
	switch req.GetMethod() {
	case agentapi.MethodRequestCancel:
		a.cancelInflight(req, logger)
	case agentapi.MethodImagesPull:
		a.startCancellable(ctx, req, func(ctx context.Context) { a.handlePull(ctx, req, logger) })
	case agentapi.MethodFsRead:
		a.startCancellable(ctx, req, func(ctx context.Context) { a.handleRead(ctx, req, logger) })
	case agentapi.MethodMetricsScrape:
		a.startCancellable(ctx, req, func(ctx context.Context) { a.handleScrape(ctx, req, logger) })
	case agentapi.MethodContainersLogs:
		a.startCancellable(ctx, req, func(ctx context.Context) { a.handleLogs(ctx, req, logger) })
	case agentapi.MethodFsWrite:
		reg := a.registerInbound(req.GetRequestId())
		go a.handleWrite(ctx, req, reg, logger)
	default:
		go a.handleAndReply(ctx, req, logger)
	}
}

// startCancellable runs a streaming handler on its own goroutine under a
// per-request cancel. The registration happens synchronously (before the
// receive loop reads the next command) so a request.cancel arriving right
// after the opening request always finds it.
func (a *agent) startCancellable(ctx context.Context, req *hostlinkv1.AgentRequest, run func(context.Context)) {
	reqCtx, cancel := context.WithCancel(ctx)
	requestID := req.GetRequestId()
	a.cancelMu.Lock()
	a.cancels[requestID] = cancel
	a.cancelMu.Unlock()
	go func() {
		defer func() {
			a.cancelMu.Lock()
			delete(a.cancels, requestID)
			a.cancelMu.Unlock()
			cancel()
		}()
		run(reqCtx)
	}()
}

// cancelInflight handles a request.cancel command: it cancels the named
// in-flight request's context and sends no reply. An unknown request_id means
// the request already finished and is ignored. It runs synchronously in the
// receive loop; it only flips a context, so it cannot stall the loop.
func (a *agent) cancelInflight(req *hostlinkv1.AgentRequest, logger logr.Logger) {
	var cr agentapi.CancelRequest
	if err := json.Unmarshal(req.GetPayload(), &cr); err != nil {
		logger.Error(err, "invalid request.cancel payload")
		return
	}
	a.cancelMu.Lock()
	cancel, ok := a.cancels[cr.RequestID]
	a.cancelMu.Unlock()
	if ok {
		cancel()
		logger.Info("cancelled in-flight request", "requestID", cr.RequestID)
	}
}

// handleAndReply executes a single-shot request and sends its one AgentResult
// back over the Control stream. Streaming methods are dispatched separately by
// startRequest. A send failure is logged: the controller-side dispatcher
// unblocks via its own timeout, so there is nothing to return here.
func (a *agent) handleAndReply(ctx context.Context, req *hostlinkv1.AgentRequest, logger logr.Logger) {
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
