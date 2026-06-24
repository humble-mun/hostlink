package controller

import (
	"errors"
	"fmt"
	"io"

	"github.com/gin-gonic/gin"
	"github.com/go-logr/logr"
	redisv9 "github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	hostlinkv1 "github.com/humble-mun/hostlink/pkg/api/hostlink/v1"
)

// Service is the controller's lifecycle surface: it registers the HTTP routes and
// releases its resources (peer plane, redis) on Close.
type Service interface {
	io.Closer
	RegisterRoute(*gin.Engine)
}

// RegisterGRPCService registers the AgentLink server on srv and constructs the
// controller Service, bringing up the redis-backed registry and the ControllerPeer
// relay plane when configured (otherwise it runs in single-replica in-memory mode).
func RegisterGRPCService(logger logr.Logger, nodeName string, srv *grpc.Server) (svc Service, err error) {
	logger = logger.WithName("controller")
	selfAddr := viper.GetString(flagPeerAdvertiseAddress)
	redisURL := viper.GetString(flagRedisURL)
	peerBind := viper.GetString(flagPeerBindAddress)

	// redis (the agent->pod directory) and the peer listener (the relay transport)
	// are the two halves of one cross-pod switch: one without the other can neither
	// resolve nor serve relays, so reject a half-configured controller up front
	// rather than silently 404 under multiple replicas.
	var crossPod bool
	if redisURL != "" || peerBind != "" {
		crossPod = true
		if redisURL == "" || peerBind == "" {
			err = fmt.Errorf("controller: cross-pod mode requires both %s and %s to be set", flagRedisURL, flagPeerBindAddress)
			return
		}
		if selfAddr == "" {
			err = fmt.Errorf("controller: %s is required in cross-pod mode", flagPeerAdvertiseAddress)
			return
		}
	}

	var redis redisv9.UniversalClient
	if crossPod {
		if redis, err = newRedisClient(logger.WithName("redis"), redisURL); err != nil {
			err = fmt.Errorf("controller: %w", err)
			return
		}
	}

	reg := newRegistry(logger.WithName("registry"), redis, selfAddr)
	hostlinkv1.RegisterAgentLinkServer(srv, &impl{logger: logger.WithName("service"), nodeName: nodeName, registry: reg})

	s := &service{logger: logger, nodeName: nodeName, registry: reg, selfAddr: selfAddr}
	svc = s
	if err = s.startPeerPlane(logger.WithName("peer"), reg); err != nil {
		err = fmt.Errorf("controller: %w", err)
		return
	}

	if crossPod {
		logger.Info("registry mode: redis-backed (cross-pod relay enabled)", "peerAdvertise", selfAddr, "peerBind", peerBind)
	} else {
		logger.Info("registry mode: in-memory (single-replica; cross-pod relay disabled)")
	}
	return
}

type service struct {
	logger   logr.Logger
	nodeName string
	registry *registry
	selfAddr string

	peers      *peerClients
	peerServer *grpc.Server
	peerDone   <-chan struct{}
}

// startPeerPlane brings up the cross-pod relay when --peer-bind-address is set:
// it binds the in-cluster ControllerPeer listener and prepares the sibling client
// pool. With the flag empty the peer plane stays off and a request for an agent
// held elsewhere returns 404.
func (svc *service) startPeerPlane(logger logr.Logger, reg *registry) (err error) {
	bindAddr := viper.GetString(flagPeerBindAddress)
	if bindAddr == "" {
		return
	}

	var clientCreds credentials.TransportCredentials
	if clientCreds, err = peerClientCredentials(logger.WithName("client")); err != nil {
		return
	}
	svc.peers = newPeerClients(logger.WithName("client"), clientCreds, viper.GetString(flagPeerTLSServerName))

	if svc.peerServer, svc.peerDone, err = startPeerServer(logger.WithName("server"), bindAddr, reg); err != nil {
		return
	}
	return
}

func (svc *service) RegisterRoute(mux *gin.Engine) {
	group := mux.Group("/api/v1")
	group.GET("/agents", svc.listAgents)
	group.GET("/metrics", svc.agentMetrics)
	group.GET("/agents/:agentId/images", svc.listAgentImages)
	group.POST("/agents/:agentId/images", svc.pullAgentImage)
	group.DELETE("/agents/:agentId/images", svc.removeAgentImages)
	group.DELETE("/agents/:agentId/images/:imageId", svc.removeAgentImages)
	group.GET("/agents/:agentId/files", svc.fsGet)
	group.POST("/agents/:agentId/files", svc.fsPost)
	group.PUT("/agents/:agentId/files", svc.fsPut)
	group.DELETE("/agents/:agentId/files", svc.fsDelete)
}

func (svc *service) Close() (err error) {
	// GracefulStop drains in-flight relays and makes Serve return; the peerDone
	// wait below then confirms the serve goroutine fully exited. Trigger it first
	// so no relay handler is running while dependencies are torn down.
	if svc.peerServer != nil {
		svc.peerServer.GracefulStop()
	}
	if svc.peers != nil {
		if e := svc.peers.close(); e != nil {
			err = errors.Join(err, e)
		}
	}
	if svc.registry != nil {
		if e := svc.registry.close(); e != nil {
			err = errors.Join(err, e)
		}
	}
	// The serve goroutine's post-Serve teardown touches neither peers nor
	// registry, so wait for its clean exit last instead of gating those closes.
	if svc.peerServer != nil {
		<-svc.peerDone
	}
	return
}
