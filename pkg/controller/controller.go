package controller

import (
	"context"
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
	forwardRangeRaw := viper.GetString(flagForwardPortRange)
	var forwardRange portRange
	if forwardRangeRaw != "" {
		if forwardRange, err = parsePortRange(forwardRangeRaw); err != nil {
			err = fmt.Errorf("controller: parse %s: %w", flagForwardPortRange, err)
			return
		}
	}

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
	sessions := newSessionTable()
	var store portStore
	if forwardRangeRaw != "" {
		store = newPortStore(logger.WithName("ports"), redis)
	}

	s := &service{logger: logger, nodeName: nodeName, registry: reg, selfAddr: selfAddr, sessions: sessions, store: store}
	defer func() {
		if err == nil {
			return
		}
		if closeErr := s.Close(); closeErr != nil {
			logger.Error(closeErr, "close controller after setup failure")
		}
	}()
	hostlinkv1.RegisterAgentLinkServer(srv, &impl{logger: logger.WithName("service"), nodeName: nodeName, registry: reg, sessions: sessions, store: store})

	if err = s.startPeerPlane(logger.WithName("peer"), reg); err != nil {
		err = fmt.Errorf("controller: %w", err)
		return
	}
	s.startForwardPlane(logger, redis, forwardRange)
	svc = s

	if crossPod {
		logger.Info("registry mode: redis-backed (cross-pod relay enabled)", "peerAdvertise", selfAddr, "peerBind", peerBind)
	} else {
		logger.Info("registry mode: in-memory (single-replica; cross-pod relay disabled)")
	}
	return
}

type service struct {
	logger    logr.Logger
	nodeName  string
	registry  *registry
	selfAddr  string
	sessions  *sessionTable
	store     portStore
	listeners *listenerManager
	bindings  *bindingTracker
	fwdCancel context.CancelFunc
	rangeFrom uint32
	rangeTo   uint32

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

	if svc.peerServer, svc.peerDone, err = startPeerServer(logger.WithName("server"), bindAddr, reg, svc.sessions); err != nil {
		return
	}
	return
}

func (svc *service) startForwardPlane(logger logr.Logger, redis redisv9.UniversalClient, forwardRange portRange) {
	if svc.store == nil {
		return
	}

	fwd := newForwarder(logger, svc.registry, svc.sessions, svc.store, svc.peers, svc.selfAddr)
	svc.listeners = newListenerManager(logger.WithName("listeners"), fwd.handleConn)
	svc.bindings = newBindingTracker(logger.WithName("bindings"), redis, svc.selfAddr, svc.listeners.boundPorts)
	fwdCtx, fwdCancel := context.WithCancel(context.Background())
	svc.fwdCancel = fwdCancel
	svc.rangeFrom = forwardRange.from
	svc.rangeTo = forwardRange.to
	go runPortReconciler(fwdCtx, logger.WithName("ports"), svc.store, svc.listeners, svc.bindings)
}

func (svc *service) RegisterRoute(mux *gin.Engine) {
	group := mux.Group("/api/v1")
	group.GET("/agents", svc.listAgents)
	group.GET("/metrics", svc.agentMetrics)
	group.GET("/forwards", svc.listAllForwards)
	group.DELETE("/forwards/:port", svc.deleteForward)
	group.POST("/agents/:agentId/forwards", svc.createForward)
	group.GET("/agents/:agentId/forwards", svc.listAgentForwards)
	group.GET("/agents/:agentId/images", svc.listAgentImages)
	group.POST("/agents/:agentId/images", svc.pullAgentImage)
	group.DELETE("/agents/:agentId/images", svc.removeAgentImages)
	group.DELETE("/agents/:agentId/images/:imageId", svc.removeAgentImages)
	group.GET("/agents/:agentId/containers", svc.listAgentContainers)
	group.POST("/agents/:agentId/containers", svc.createAgentContainer)
	group.GET("/agents/:agentId/containers/:containerId", svc.inspectAgentContainer)
	group.GET("/agents/:agentId/containers/:containerId/logs", svc.logsAgentContainer)
	group.POST("/agents/:agentId/containers/:containerId/start", svc.startAgentContainer)
	group.POST("/agents/:agentId/containers/:containerId/stop", svc.stopAgentContainer)
	group.POST("/agents/:agentId/containers/:containerId/restart", svc.restartAgentContainer)
	group.DELETE("/agents/:agentId/containers/:containerId", svc.removeAgentContainer)
	group.GET("/agents/:agentId/files", svc.fsGet)
	group.POST("/agents/:agentId/files", svc.fsPost)
	group.PUT("/agents/:agentId/files", svc.fsPut)
	group.DELETE("/agents/:agentId/files", svc.fsDelete)
}

func (svc *service) Close() (err error) {
	if svc.fwdCancel != nil {
		svc.fwdCancel()
	}
	if svc.listeners != nil {
		if e := svc.listeners.close(); e != nil {
			err = errors.Join(err, e)
		}
	}
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
	if svc.store != nil {
		if e := svc.store.close(); e != nil {
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
