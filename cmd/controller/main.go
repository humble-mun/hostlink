package main

import (
	"context"

	"github.com/go-logr/logr"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/humble-mun/chassis/pkg/app"
	"github.com/humble-mun/chassis/pkg/metrics"
	"github.com/humble-mun/chassis/pkg/server"
	"github.com/humble-mun/chassis/pkg/version"

	"github.com/humble-mun/hostlink/pkg/controller"
)

func newRootCommand() *cobra.Command {
	var init func() error
	cmd := &cobra.Command{
		Use:   version.Name,
		Short: "hostlink-controller is the cloud-side control plane that manages external hosts and their containers.",
		Long: "hostlink-controller is the cloud-side component of hostlink. It runs in Kubernetes (with multiple " +
			"replicas for HA) and acts as the gRPC server that NAT-side hostlink-agents dial out to over a " +
			"persistent connection. It dispatches Docker and exposure commands to agents, aggregates their " +
			"metrics into a single Prometheus endpoint, and drives reverse TCP tunnels for dynamic port forwarding.",
		FParseErrWhitelist: cobra.FParseErrWhitelist{
			UnknownFlags: true,
		},
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd: true,
		},
		SilenceUsage: true,
		RunE: func(_ *cobra.Command, _ []string) (err error) {
			srv := grpc.NewServer()
			var rootLogger, logger logr.Logger
			var httpGin *server.HTTPServer
			var ctx context.Context
			var nodeName string
			if rootLogger, logger, httpGin, ctx, nodeName, err = app.BaseContext(
				app.WithInit(init),
				app.WithGRPCServer(srv),
				app.WithTCPListener(controller.ListenerOptions()...),
			); err != nil {
				return
			}
			logger = logger.WithValues("nodeName", nodeName)

			var svc controller.Service
			if svc, err = controller.RegisterGRPCService(rootLogger, nodeName, srv); err != nil {
				logger.Error(err, "register controller grpc service failed")
				return
			}
			defer func() {
				if e := svc.Close(); e != nil {
					logger.Error(e, "close controller grpc service failed")
				}
			}()

			httpGin.RegisterRoute(svc.RegisterRoute)
			metrics.RegisterScrapeHook(svc.RegisterScrapeHook)

			logger.Info("controller started")
			defer logger.Info("controller finished")
			if err = httpGin.Start(ctx); err != nil {
				logger.Error(err, "start http server failed")
				return
			}
			<-ctx.Done()
			return
		},
	}

	init = app.PrepareFlags(version.Name, cmd, controller.RegisterFlags)
	return cmd
}

func main() {
	_ = newRootCommand().Execute()
}
