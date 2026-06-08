package main

import (
	"context"
	"sync"

	"github.com/go-logr/logr"
	"github.com/spf13/cobra"

	"github.com/humble-mun/chassis/pkg/app"
	"github.com/humble-mun/chassis/pkg/server"
	"github.com/humble-mun/chassis/pkg/version"

	"github.com/humble-mun/hostlink/pkg/agent"
)

func newRootCommand() *cobra.Command {
	var init func() error
	cmd := &cobra.Command{
		Use:   version.Name,
		Short: "hostlink-agent runs on an external host to execute commands, report metrics, and carry tunnels.",
		Long: "hostlink-agent is the host-side component of hostlink. It runs as a systemd service on a Linux " +
			"host outside the cloud (behind NAT), dials out to the hostlink-controller over a persistent gRPC " +
			"connection, manages local Docker containers, reports node metrics, and carries reverse TCP " +
			"tunnels for dynamic port forwarding.",
		FParseErrWhitelist: cobra.FParseErrWhitelist{
			UnknownFlags: true,
		},
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd: true,
		},
		SilenceUsage: true,
		RunE: func(_ *cobra.Command, _ []string) (err error) {
			var rootLogger, logger logr.Logger
			var httpGin *server.HTTPServer
			var ctx context.Context
			var nodeName string
			if rootLogger, logger, httpGin, ctx, nodeName, err = app.BaseContext(app.WithInit(init)); err != nil {
				return
			}
			logger = logger.WithValues("nodeName", nodeName)

			var ag agent.Agent
			if ag, err = agent.New(rootLogger, nodeName); err != nil {
				return
			}
			defer func() {
				if e := ag.Close(); e != nil {
					logger.Error(e, "close agent failed")
				}
			}()

			logger.Info("agent started")
			defer logger.Info("agent finished")
			wg := new(sync.WaitGroup)
			wg.Add(2)
			go func() {
				defer wg.Done()
				if err = httpGin.Start(ctx); err != nil {
					logger.Error(err, "start http server failed")
					return
				}
			}()
			go func() {
				defer wg.Done()
				if err = ag.Start(ctx); err != nil {
					logger.Error(err, "start agent failed")
					return
				}
			}()
			wg.Wait()
			return
		},
	}

	init = app.PrepareFlags(version.Name, cmd, agent.RegisterFlags)
	return cmd
}

func main() {
	_ = newRootCommand().Execute()
}
