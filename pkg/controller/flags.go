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
