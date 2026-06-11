package main

import (
	"context"
	"errors"
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
			var rootCtx context.Context
			var nodeName string
			if rootLogger, logger, httpGin, rootCtx, nodeName, err = app.BaseContext(app.WithInit(init)); err != nil {
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
			// Run the HTTP server and the agent under a shared sub-context. If either
			// one returns (clean exit or error), its deferred cancel() tears down the
			// other so the whole process exits. A live process with a dead agent is
			// effectively unavailable, so we'd rather exit and let systemd restart us.
			ctx, cancel := context.WithCancel(rootCtx)
			defer cancel()

			wg := new(sync.WaitGroup)
			wg.Add(2)
			var httpErr, agentErr error
			go func() {
				defer wg.Done()
				defer cancel()
				if httpErr = httpGin.Start(ctx); httpErr != nil {
					logger.Error(httpErr, "start http server failed")
				}
			}()
			go func() {
				defer wg.Done()
				defer cancel()
				if agentErr = ag.Start(ctx); agentErr != nil {
					logger.Error(agentErr, "start agent failed")
				}
			}()
			wg.Wait()

			// Join both errors so neither is lost; errors.Join drops nils and returns
			// nil if both are nil. Done after wg.Wait() so there is no data race.
			err = errors.Join(agentErr, httpErr)
			return
		},
	}

	init = app.PrepareFlags(version.Name, cmd, agent.RegisterFlags)
	return cmd
}

func main() {
	_ = newRootCommand().Execute()
}
