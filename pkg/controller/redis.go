package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
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

// errUnsupportedRedisTopology is returned when the redis url carries a
// "+topology" marker that is neither sentinel nor cluster.
var errUnsupportedRedisTopology = errors.New("unsupported redis topology in url scheme")

// newRedisClient parses url into a redis client and routes go-redis's own
// logging through logger. It is the single place that knows how to assemble the
// redis client for the controller.
//
// The deployment topology is selected by the URL scheme so no extra config knob
// is needed and the connection string stays self-describing:
//
//	redis://host:6379/0                              standalone (also rediss://)
//	redis+sentinel://sentinel:26379?master_name=mymaster&addr=sentinel2:26379
//	                                                sentinel failover
//	redis+cluster://node1:6379?addr=node2:6379&addr=node3:6379
//	                                                cluster
//
// The "+sentinel"/"+cluster" marker is stripped before delegating to go-redis,
// whose parsers only accept the bare redis/rediss schemes. Extra nodes are
// supplied through the "addr" query parameter (repeatable), matching go-redis's
// own ParseClusterURL/ParseFailoverURL conventions.
func newRedisClient(logger logr.Logger, url string) (redisv9.UniversalClient, error) {
	setRedisLogger.Do(func() {
		redisv9.SetLogger(redisLogger{logger: logger})
	})

	switch scheme, rest, ok := splitRedisScheme(url); {
	case ok && scheme == "sentinel":
		opts, err := redisv9.ParseFailoverURL(rest)
		if err != nil {
			return nil, fmt.Errorf("parse redis sentinel url: %w", err)
		}
		return redisv9.NewFailoverClient(opts), nil
	case ok && scheme == "cluster":
		opts, err := redisv9.ParseClusterURL(rest)
		if err != nil {
			return nil, fmt.Errorf("parse redis cluster url: %w", err)
		}
		return redisv9.NewClusterClient(opts), nil
	case ok:
		return nil, fmt.Errorf("%w: %q", errUnsupportedRedisTopology, scheme)
	default:
		opts, err := redisv9.ParseURL(url)
		if err != nil {
			return nil, fmt.Errorf("parse redis url: %w", err)
		}
		return redisv9.NewClient(opts), nil
	}
}

// splitRedisScheme detects a "+topology" marker on the redis/rediss scheme. For
// "redis+sentinel://host" it returns ("sentinel", "redis://host", true); for a
// plain "redis://host" it reports ok=false so the caller treats it as
// standalone. The marker is removed so the remainder parses under go-redis's
// bare redis/rediss schemes.
func splitRedisScheme(url string) (topology, rest string, ok bool) {
	const sep = "://"
	i := strings.Index(url, sep)
	if i < 0 {
		return "", "", false
	}
	scheme := url[:i]
	plus := strings.IndexByte(scheme, '+')
	if plus < 0 {
		return "", "", false
	}
	return scheme[plus+1:], scheme[:plus] + url[i:], true
}
