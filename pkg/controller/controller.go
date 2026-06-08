package controller

import (
	"context"
	"io"

	"github.com/gin-gonic/gin"
	"github.com/go-logr/logr"
	"google.golang.org/grpc"

	hostlinkv1 "github.com/humble-mun/hostlink/pkg/api/hostlink/v1"
)

type Service interface {
	io.Closer
	RegisterScrapeHook(context.Context)
	RegisterRoute(*gin.Engine)
}

func RegisterGRPCService(logger logr.Logger, nodeName string, srv *grpc.Server) (svc Service, err error) {
	svc = &service{logger: logger.WithName("controller"), nodeName: nodeName}
	hostlinkv1.RegisterAgentLinkServer(srv, &impl{logger: logger.WithName("service"), nodeName: nodeName})
	return
}

type service struct {
	logger   logr.Logger
	nodeName string
}

func (svc service) RegisterScrapeHook(ctx context.Context) {}

func (svc service) RegisterRoute(mux *gin.Engine) {}

func (svc service) Close() error {
	return nil
}
