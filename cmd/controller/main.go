package main

import (
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/humble-mun/chassis/pkg/app"
	"github.com/humble-mun/chassis/pkg/metrics"
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
			var base app.Base
			if base, err = app.BaseContext(
				app.WithInit(init),
				app.WithGRPCServer(srv),
				app.WithTCPListener(controller.ListenerOptions()...),
			); err != nil {
				return
			}

			var svc controller.Service
			if svc, err = controller.RegisterGRPCService(base.RootLogger, base.NodeName, srv); err != nil {
				base.Logger.Error(err, "register controller grpc service failed")
				return
			}
			defer func() {
				if e := svc.Close(); e != nil {
					base.Logger.Error(e, "close controller grpc service failed")
				}
			}()

			base.HTTPGin.RegisterRoute(svc.RegisterRoute)
			metrics.RegisterScrapeHook(svc.RegisterScrapeHook)

			base.Logger.Info("controller started")
			defer base.Logger.Info("controller finished")
			if err = base.HTTPGin.Start(base.Ctx); err != nil {
				base.Logger.Error(err, "start http server failed")
				return
			}
			<-base.Ctx.Done()
			return
		},
	}

	init = app.PrepareFlags(version.Name, cmd, controller.RegisterFlags)
	return cmd
}

func main() {
	_ = newRootCommand().Execute()
}
