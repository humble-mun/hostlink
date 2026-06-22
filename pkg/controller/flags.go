package controller

import (
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/humble-mun/chassis/pkg/server"
)

const (
	flagGRPCBindAddress = "grpc-bind-address"
	flagGRPCTLSCertPath = "grpc-tls-cert-path"
	flagGRPCTLSKeyPath  = "grpc-tls-key-path"
	flagGRPCTLSCAPath   = "grpc-tls-ca-path"
	flagRedisURL        = "redis-url"

	flagPeerBindAddress      = "peer-bind-address"
	flagPeerAdvertiseAddress = "peer-advertise-address"
	flagPeerTLSCertPath      = "peer-tls-cert-path"
	flagPeerTLSKeyPath       = "peer-tls-key-path"
	flagPeerTLSCAPath        = "peer-tls-ca-path"
	flagPeerTLSServerName    = "peer-tls-server-name"
)

// RegisterFlags registers the controller's mTLS gRPC listener flags. The gRPC
// listener is separate from the chassis default listener: the default listener
// stays plaintext for in-cluster probe and metrics traffic, while this listener
// terminates mutual TLS for agent connections exposed through the ingress.
func RegisterFlags(pfs *pflag.FlagSet) {
	pfs.String(flagGRPCBindAddress, "", "The address to bind the mTLS gRPC listener for agent connections, as host:port.")
	pfs.String(flagGRPCTLSCertPath, "", "The path to the server certificate the controller presents to agents for mTLS.")
	pfs.String(flagGRPCTLSKeyPath, "", "The path to the private key matching the server certificate.")
	pfs.String(flagGRPCTLSCAPath, "", "The path to the CA bundle used to verify agent client certificates.")
	pfs.String(flagRedisURL, "", "The redis connection URL (redis://[user:pass@]host:port/db) backing the cross-pod agent registry. When empty the registry is in-memory only and a request for an agent held by another replica returns 404.")

	pfs.String(flagPeerBindAddress, "", "The address to bind the in-cluster mTLS ControllerPeer listener for cross-pod request relay, as host:port. When empty the peer plane is disabled and cross-pod relay is not attempted.")
	pfs.String(flagPeerAdvertiseAddress, "", "The address siblings dial to reach this pod's ControllerPeer listener, written as the redis mapping value (e.g. $(POD_NAME).<release>-peer.<ns>.svc:8444). Required when --redis-url is set.")
	pfs.String(flagPeerTLSCertPath, "", "The path to the certificate the controller presents on the ControllerPeer plane, as both server and client.")
	pfs.String(flagPeerTLSKeyPath, "", "The path to the private key matching the ControllerPeer certificate.")
	pfs.String(flagPeerTLSCAPath, "", "The path to the CA bundle used to verify sibling controller certificates on the ControllerPeer plane.")
	pfs.String(flagPeerTLSServerName, "", "The server name to verify against a sibling controller's certificate when relaying; if empty, verification uses the dialed peer address host.")
}

// ListenerOptions builds the chassis ListenerOptions for the controller's mTLS
// gRPC listener from the registered flags. The returned options configure the
// bind address, the server certificate/key pair, and the client CA used to
// require and verify agent client certificates.
func ListenerOptions() []server.ListenerOption {
	return []server.ListenerOption{
		server.WithAddr(func() string { return viper.GetString(flagGRPCBindAddress) }),
		server.WithTLSCert(func() string { return viper.GetString(flagGRPCTLSCertPath) }, func() string { return viper.GetString(flagGRPCTLSKeyPath) }),
		server.WithMTLS(func() string { return viper.GetString(flagGRPCTLSCAPath) }),
	}
}
