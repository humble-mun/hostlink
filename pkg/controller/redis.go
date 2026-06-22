package controller

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-logr/logr"
	redisv9 "github.com/redis/go-redis/v9"
)

// redisLogger adapts go-redis's internal logging onto logr so the library's own
// diagnostics flow through the same klog-controlled sink as the controller.
type redisLogger struct {
	logger logr.Logger
}

func (rl redisLogger) Printf(_ context.Context, format string, v ...interface{}) {
	rl.logger.Info(fmt.Sprintf(format, v...))
}

// redisv9.SetLogger mutates a package-global in go-redis, so install the bridge
// at most once regardless of how many times a client is constructed.
var setRedisLogger sync.Once

// newRedisClient parses url into a redis client and routes go-redis's own
// logging through logger. It is the single place that knows how to assemble the
// redis client for the controller.
func newRedisClient(logger logr.Logger, url string) (client redisv9.UniversalClient, err error) {
	var opts *redisv9.Options
	if opts, err = redisv9.ParseURL(url); err != nil {
		err = fmt.Errorf("parse redis url: %w", err)
		return
	}
	setRedisLogger.Do(func() {
		redisv9.SetLogger(redisLogger{logger: logger})
	})
	client = redisv9.NewClient(opts)
	return
}
