package main

import (
	"context"
	"errors"
	"sync"

	"github.com/spf13/cobra"

	"github.com/humble-mun/chassis/pkg/app"
	"github.com/humble-mun/chassis/pkg/version"

	"github.com/humble-mun/hostlink/pkg/agent"
)

func newRootCommand() *cobra.Command {
	var init func() error
	cmd := &cobra.Command{
		Use:   version.Name,
		Short: "The hostlink agent runs on an external host to execute commands, report metrics, and carry tunnels.",
		Long: "The hostlink agent is the host-side component of hostlink. It runs as a systemd service on a Linux " +
			"host outside the cloud (behind NAT), dials out to the hostlink controller over a persistent gRPC " +
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
			var base app.Base
			if base, err = app.BaseContext(app.WithInit(init)); err != nil {
				return
			}

			var ag agent.Agent
			if ag, err = agent.New(base.RootLogger, base.NodeName); err != nil {
				return
			}
			defer func() {
				if e := ag.Close(); e != nil {
					base.Logger.Error(e, "close agent failed")
				}
			}()

			base.Logger.Info("agent started")
			defer base.Logger.Info("agent finished")
			// Run the HTTP server and the agent under a shared sub-context. If either
			// one returns (clean exit or error), its deferred cancel() tears down the
			// other so the whole process exits. A live process with a dead agent is
			// effectively unavailable, so we'd rather exit and let systemd restart us.
			ctx, cancel := context.WithCancel(base.Ctx)
			defer cancel()

			wg := new(sync.WaitGroup)
			wg.Add(2)
			var httpErr, agentErr error
			go func() {
				defer wg.Done()
				defer cancel()
				if httpErr = base.HTTPGin.Start(ctx); httpErr != nil {
					base.Logger.Error(httpErr, "start http server failed")
				}
			}()
			go func() {
				defer wg.Done()
				defer cancel()
				if agentErr = ag.Start(ctx); agentErr != nil {
					base.Logger.Error(agentErr, "start agent failed")
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
