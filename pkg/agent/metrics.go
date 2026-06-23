package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/spf13/viper"

	"github.com/humble-mun/hostlink/pkg/agentapi"
	hostlinkv1 "github.com/humble-mun/hostlink/pkg/api/hostlink/v1"
)

// flagScrapeTargets is the config key holding the upstream exporters the agent
// pulls on a metrics.scrape request. It is a structured list rather than a flag
// (a flag set cannot model a list of objects); the agent reads it from its YAML
// config via viper.
const flagScrapeTargets = "scrape-targets"

const (
	// scrapeTargetTimeout bounds each upstream exporter fetch (connect through the
	// last body byte). It is kept below the controller's per-agent fan-out deadline
	// so the agent streams what it has rather than being cut off mid-exporter.
	scrapeTargetTimeout = 4 * time.Second
	// scrapeChunkSize bounds each exposition body chunk streamed to the controller.
	// Keeping it well under the gRPC max message size means an exposition of any
	// length streams without buffering the whole body or raising message limits.
	scrapeChunkSize = 64 * 1024
	// scrapeMaxBodySize caps the total exposition the agent streams from one
	// exporter, guarding against a runaway target without capping realistic
	// node_exporter / dcgm-exporter payloads.
	scrapeMaxBodySize = 16 << 20
	// defaultScrapePath is the request path used for a unix-socket target when none
	// is given; TCP targets carry their path in the url.
	defaultScrapePath = "/metrics"
)

// scrapeTargetConfig is the YAML shape of one configured exporter. URL carries
// the whole endpoint: an http(s) URL for a TCP exporter (e.g.
// http://127.0.0.1:9100, path defaults to /metrics when omitted), or a unix://
// URL whose path is the socket (e.g. unix:///var/run/node_exporter/server.sock,
// HTTP path defaults to /metrics).
//
// Path exists as a separate field because a unix:// URL cannot express both the
// socket path and the HTTP request path in one URL: the whole URL path is the
// socket file, with no delimiter to tell where it ends and the request path
// begins (is unix:///run/x.sock/metrics a socket .../x.sock serving /metrics, or
// a socket file literally named .../x.sock/metrics?). So for a unix:// target the
// socket comes from the URL and the HTTP request path comes from Path. For an
// http(s) target the request path is already unambiguous in the URL, so Path is
// only an optional override there.
type scrapeTargetConfig struct {
	Name string `mapstructure:"name"`
	URL  string `mapstructure:"url"`
	Path string `mapstructure:"path"`
}

// scrapeTarget is a resolved exporter the agent pulls: the request URL plus the
// HTTP client to use. TCP targets share one client; a unix-socket target gets a
// client whose transport dials that socket (the request URL's host is a fixed
// placeholder the dialer ignores). Name becomes the `exporter` label the
// controller injects.
type scrapeTarget struct {
	name   string
	reqURL string
	client *http.Client
}

// resolveScrapeTargets reads and validates the configured scrape targets. An
// empty list is not an error: it simply leaves the metrics.scrape feature off.
// TCP targets share a single client; unix-socket targets each get their own.
func resolveScrapeTargets() (targets []scrapeTarget, err error) {
	var configs []scrapeTargetConfig
	if err = viper.UnmarshalKey(flagScrapeTargets, &configs); err != nil {
		err = fmt.Errorf("agent: parse %s: %w", flagScrapeTargets, err)
		return
	}
	if len(configs) == 0 {
		return
	}

	tcpClient := &http.Client{}
	targets = make([]scrapeTarget, 0, len(configs))
	for i := range configs {
		var t scrapeTarget
		if t, err = resolveScrapeTarget(&configs[i], tcpClient); err != nil {
			return
		}
		targets = append(targets, t)
	}
	return
}

// resolveScrapeTarget validates one configured exporter and resolves it to a
// request URL plus the client to fetch it with. The scheme selects the transport:
// http/https is a normal TCP fetch; unix uses a socket-dialing client.
func resolveScrapeTarget(cfg *scrapeTargetConfig, tcpClient *http.Client) (t scrapeTarget, err error) {
	if cfg.Name == "" {
		err = fmt.Errorf("agent: %s: a target name must not be empty", flagScrapeTargets)
		return
	}
	t.name = cfg.Name

	if cfg.Path != "" && !strings.HasPrefix(cfg.Path, "/") {
		err = fmt.Errorf("agent: %s %q: path must start with '/'", flagScrapeTargets, cfg.Name)
		return
	}
	if cfg.URL == "" {
		err = fmt.Errorf("agent: %s %q: url is required", flagScrapeTargets, cfg.Name)
		return
	}
	var u *url.URL
	if u, err = url.Parse(cfg.URL); err != nil {
		err = fmt.Errorf("agent: %s %q: invalid url: %w", flagScrapeTargets, cfg.Name, err)
		return
	}

	switch u.Scheme {
	case "http", "https":
		if u.Host == "" {
			err = fmt.Errorf("agent: %s %q: %s url must include a host, as %s://host:port[/path]", flagScrapeTargets, cfg.Name, u.Scheme, u.Scheme)
			return
		}
		switch {
		case cfg.Path != "":
			u.Path, u.RawPath = cfg.Path, ""
		case u.Path == "":
			u.Path = defaultScrapePath
		}
		t.reqURL = u.String()
		t.client = tcpClient
	case "unix":
		if u.Host != "" {
			err = fmt.Errorf("agent: %s %q: unix url host must be empty, as unix:///path/to.sock (three slashes)", flagScrapeTargets, cfg.Name)
			return
		}
		if u.Path == "" {
			err = fmt.Errorf("agent: %s %q: unix url must include the socket path, as unix:///path/to.sock", flagScrapeTargets, cfg.Name)
			return
		}
		path := cfg.Path
		if path == "" {
			path = defaultScrapePath
		}
		// The request host is a fixed placeholder; the unix dialer ignores it.
		t.reqURL = "http://unix" + path
		t.client = unixSocketClient(u.Path)
	default:
		err = fmt.Errorf("agent: %s %q: url scheme must be http, https, or unix", flagScrapeTargets, cfg.Name)
	}
	return
}

// unixSocketClient returns an HTTP client whose transport dials the given unix
// domain socket regardless of the request URL host.
func unixSocketClient(socket string) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", socket)
			},
		},
	}
}

// handleScrape runs a streaming metrics.scrape: it streams each configured
// exporter's exposition as MetricsFrame AgentProgress frames, then sends a
// terminal AgentResult. With no targets configured the stream carries no frames
// and the agent_up the controller synthesizes is the only signal. A frame send
// failure cancels ctx, which stops the in-flight exporter read.
func (a *agent) handleScrape(ctx context.Context, req *hostlinkv1.AgentRequest, logger logr.Logger) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	requestID := req.GetRequestId()
	emit := func(frame *agentapi.MetricsFrame) {
		payload, err := json.Marshal(frame)
		if err != nil {
			logger.Error(err, "marshal metrics frame failed", "requestID", requestID)
			return
		}
		if err = a.send(&hostlinkv1.AgentEvent{
			AgentId: a.nodeName,
			Kind: &hostlinkv1.AgentEvent_Progress{Progress: &hostlinkv1.AgentProgress{
				RequestId: requestID,
				Payload:   payload,
			}},
		}); err != nil {
			logger.Error(err, "send metrics frame failed", "requestID", requestID)
			cancel()
		}
	}

	for i := range a.scrapeTargets {
		if ctx.Err() != nil {
			break
		}
		a.streamTarget(ctx, a.scrapeTargets[i], emit)
	}

	result := &hostlinkv1.AgentResult{RequestId: requestID, Code: http.StatusOK, Final: true}
	if err := a.send(&hostlinkv1.AgentEvent{
		AgentId: a.nodeName,
		Kind:    &hostlinkv1.AgentEvent_Result{Result: result},
	}); err != nil {
		logger.Error(err, "send metrics result failed", "requestID", requestID, "method", req.GetMethod())
	}
}

// streamTarget fetches one exporter with a bounded per-target timeout and streams
// its body in chunks via emit, terminated by a Done frame. A transport error, a
// non-200 status, an oversize body, or a read error is reported in the target's
// Done frame so the controller marks just that exporter down, not the whole agent.
func (a *agent) streamTarget(ctx context.Context, target scrapeTarget, emit func(*agentapi.MetricsFrame)) {
	reqCtx, cancel := context.WithTimeout(ctx, scrapeTargetTimeout)
	defer cancel()

	var req *http.Request
	var err error
	if req, err = http.NewRequestWithContext(reqCtx, http.MethodGet, target.reqURL, nil); err != nil {
		a.logger.Error(err, "build scrape request failed", "exporter", target.name)
		emit(&agentapi.MetricsFrame{Exporter: target.name, Done: true, Error: err.Error()})
		return
	}

	var resp *http.Response
	if resp, err = target.client.Do(req); err != nil {
		a.logger.Error(err, "scrape exporter failed", "exporter", target.name)
		emit(&agentapi.MetricsFrame{Exporter: target.name, Done: true, Error: err.Error()})
		return
	}
	defer func() {
		if e := resp.Body.Close(); e != nil {
			a.logger.Error(e, "close scrape response body failed", "exporter", target.name)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		msg := fmt.Sprintf("unexpected status %d from %s", resp.StatusCode, target.reqURL)
		a.logger.Error(nil, "scrape exporter returned non-200", "exporter", target.name, "status", resp.StatusCode)
		emit(&agentapi.MetricsFrame{Exporter: target.name, Done: true, Error: msg})
		return
	}

	buf := make([]byte, scrapeChunkSize)
	var total int64
	for {
		var n int
		n, err = resp.Body.Read(buf)
		if n > 0 {
			total += int64(n)
			if total > scrapeMaxBodySize {
				a.logger.Error(nil, "scrape exporter exceeded size limit", "exporter", target.name)
				emit(&agentapi.MetricsFrame{Exporter: target.name, Done: true, Error: "exposition exceeds size limit"})
				return
			}
			// emit marshals the chunk synchronously, so reusing buf next read is safe.
			emit(&agentapi.MetricsFrame{Exporter: target.name, Chunk: buf[:n]})
		}
		if errors.Is(err, io.EOF) {
			emit(&agentapi.MetricsFrame{Exporter: target.name, Done: true})
			return
		}
		if err != nil {
			a.logger.Error(err, "read scrape response body failed", "exporter", target.name)
			emit(&agentapi.MetricsFrame{Exporter: target.name, Done: true, Error: err.Error()})
			return
		}
		if ctx.Err() != nil {
			return
		}
	}
}
