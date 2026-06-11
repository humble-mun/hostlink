package agent

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/docker/docker/client"
	"github.com/go-logr/logr"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	hostlinkv1 "github.com/humble-mun/hostlink/pkg/api/hostlink/v1"
)

const (
	flagControllerEndpoint = "controller-endpoint"
	flagTLSCertPath        = "agent-tls-cert-path"
	flagTLSKeyPath         = "agent-tls-key-path"
	flagTLSCAPath          = "controller-tls-ca-path"
	flagTLSServerName      = "controller-tls-server-name"
)

func RegisterFlags(pfs *pflag.FlagSet) {
	pfs.String(flagControllerEndpoint, "", "address of the hostlink-controller gRPC endpoint to dial, as host:port")
	pfs.String(flagTLSCertPath, "", "The path to the client certificate the agent presents to the controller for mTLS.")
	pfs.String(flagTLSKeyPath, "", "The path to the private key matching the client certificate.")
	pfs.String(flagTLSCAPath, "", "The path to the CA bundle used to verify the controller's certificate.")
	pfs.String(flagTLSServerName, "", "The server name to verify against the controller's certificate; if empty, gRPC verifies against the dial endpoint's host, so set it explicitly when the certificate SAN differs from the dial address.")
}

type Agent interface {
	manager.Runnable
	io.Closer
}

func New(logger logr.Logger, nodeName string) (ag Agent, err error) {
	endpoint := viper.GetString(flagControllerEndpoint)
	if endpoint == "" {
		return nil, fmt.Errorf("agent: %s must not be empty", flagControllerEndpoint)
	}

	creds, err := clientTransportCredentials()
	if err != nil {
		return nil, err
	}

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
	}
	return ag, nil
}

type agent struct {
	logger   logr.Logger
	nodeName string
	conn     *grpc.ClientConn
	client   hostlinkv1.AgentLinkClient
	docker   *client.Client
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

// Start opens the Control stream to the controller and runs the helloworld-level
// handshake: it sends a Hello, then emits periodic heartbeats until the context
// is cancelled. This proves end-to-end connectivity; command handling and Docker
// event reporting are implemented later.
func (a *agent) Start(ctx context.Context) error {
	logger := a.logger.WithName("control")

	stream, err := a.client.Control(ctx)
	if err != nil {
		return fmt.Errorf("agent: open control stream: %w", err)
	}

	if err := stream.Send(&hostlinkv1.AgentEvent{
		AgentId: a.nodeName,
		Kind:    &hostlinkv1.AgentEvent_Hello{Hello: &hostlinkv1.Hello{}},
	}); err != nil {
		return fmt.Errorf("agent: send hello: %w", err)
	}
	logger.Info("control stream opened, hello sent")

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
		case <-ticker.C:
			if err := stream.Send(&hostlinkv1.AgentEvent{
				AgentId: a.nodeName,
				Kind:    &hostlinkv1.AgentEvent_Heartbeat{Heartbeat: &hostlinkv1.Heartbeat{}},
			}); err != nil {
				return fmt.Errorf("agent: send heartbeat: %w", err)
			}
			logger.Info("heartbeat sent")
		}
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
