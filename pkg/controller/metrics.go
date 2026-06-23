package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-logr/logr"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/proto"

	"github.com/humble-mun/hostlink/pkg/agentapi"
)

const (
	// metricLabelAgent labels every fanned-out series with the agent it came from.
	metricLabelAgent = "agent"
	// metricLabelExporter labels every series with its source exporter (the scrape
	// target name). It is deliberately not "job"/"instance" so the labels survive
	// without requiring honor_labels in the Prometheus scrape config.
	metricLabelExporter = "exporter"

	// metricAgentUp is the synthesized per-agent health gauge: 1 when the agent
	// answered this scrape round, 0 when it was offline or timed out. It is the
	// only clean "an agent went down" signal, since Prometheus scrapes one target.
	metricAgentUp = "agent_up"
	// metricTargetUp is the synthesized per-exporter health gauge: 1 when the
	// agent scraped that exporter successfully this round, 0 otherwise.
	metricTargetUp = "hostlink_scrape_target_up"

	// metricsFanoutConcurrency bounds how many agents are scraped at once, so the
	// peak memory of the fan-out is the merged family set plus a handful of
	// in-flight exporter bodies rather than the whole fleet's expositions at once.
	metricsFanoutConcurrency = 16
)

// agentMetrics handles GET /api/v1/metrics: the cloud-side aggregation of every
// online agent's upstream exporters. It is served on a route distinct from the
// chassis /metrics (which exposes the controller's own metrics). It fans out a
// streaming metrics.scrape to every agent (local or relayed to the holding pod)
// with bounded concurrency, folds each exporter's exposition into a fleet-wide
// MetricFamily merge as it arrives, and encodes the result once. One slow or
// offline agent never sinks the scrape: it contributes only agent_up 0.
func (svc *service) agentMetrics(c *gin.Context) {
	logger := svc.logger.WithName("agentMetrics")

	listCtx, cancel := context.WithTimeout(c.Request.Context(), redisOpTimeout)
	defer cancel()
	var agentIDs []string
	var err error
	if agentIDs, err = svc.registry.listAll(listCtx); err != nil {
		logger.Error(err, "list online agents failed")
		c.JSON(http.StatusBadGateway, gin.H{"error": "list online agents failed"})
		return
	}

	merger := newFamilyMerger()
	sem := make(chan struct{}, metricsFanoutConcurrency)
	wg := new(sync.WaitGroup)
	wg.Add(len(agentIDs))
	for _, agentID := range agentIDs {
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			svc.scrapeAgent(c.Request.Context(), agentID, merger, logger)
		}()
	}
	wg.Wait()

	writeExposition(c, merger.families(), logger)
}

// agentScrapeTimeout is the per-agent deadline for the fan-out, kept below the
// Prometheus scrape_timeout so a slow agent is skipped rather than failing the
// whole scrape.
func (svc *service) agentScrapeTimeout() (d time.Duration) {
	if d = viper.GetDuration(flagAgentScrapeTimeout); d <= 0 {
		d = defaultAgentScrapeTimeout
	}
	return
}

// scrapeAgent drives a streaming metrics.scrape to one agent and folds each
// exporter's exposition into merger as its frames arrive. It records the
// synthetic agent_up gauge (1 only when the agent's stream completed normally),
// so an offline or failing agent is still visible in the output.
func (svc *service) scrapeAgent(ctx context.Context, agentID string, merger *familyMerger, logger logr.Logger) {
	scrapeCtx, cancel := context.WithTimeout(ctx, svc.agentScrapeTimeout())
	defer cancel()

	acc := newScrapeAccumulator(agentID, merger, logger)
	defer func() {
		var up float64
		if acc.completed {
			up = 1
		}
		merger.add(gaugeFamily(metricAgentUp, "1 if the agent was scraped this round, 0 otherwise.", up,
			labels{metricLabelAgent: agentID}))
	}()

	frames, done, cancelStream, err := svc.dispatchStream(scrapeCtx, agentID, agentapi.MethodMetricsScrape, nil)
	if err != nil {
		logger.Error(err, "dispatch metrics.scrape to agent failed", "agentID", agentID)
		return
	}
	defer cancelStream()

	consumeStream(scrapeCtx, frames, done, acc.onFrame, acc.onAbort)
}

// scrapeAccumulator reassembles one agent's streaming metrics.scrape: it buffers
// each exporter's chunks until its Done frame, then parses, labels, and folds the
// exposition into the shared merger, releasing the buffer. Because the agent
// streams exporters sequentially, at most one buffer is live per agent at a time.
type scrapeAccumulator struct {
	agentID   string
	merger    *familyMerger
	logger    logr.Logger
	buffers   map[string]*bytes.Buffer
	completed bool
}

func newScrapeAccumulator(agentID string, merger *familyMerger, logger logr.Logger) *scrapeAccumulator {
	return &scrapeAccumulator{agentID: agentID, merger: merger, logger: logger, buffers: make(map[string]*bytes.Buffer)}
}

// onFrame handles one stream frame. The terminal frame (Final) ends the stream
// and, on a 200, marks the agent scraped; a progress frame carries a MetricsFrame
// that is demultiplexed by exporter.
func (acc *scrapeAccumulator) onFrame(frame streamFrame) (finished bool) {
	if frame.Final {
		if frame.Code == http.StatusOK {
			acc.completed = true
		} else {
			acc.logger.Error(nil, "agent metrics.scrape returned error",
				"agentID", acc.agentID, "code", frame.Code, "error", frame.Error)
		}
		return true
	}

	var mf agentapi.MetricsFrame
	if err := json.Unmarshal(frame.Payload, &mf); err != nil {
		acc.logger.Error(err, "decode metrics frame failed", "agentID", acc.agentID)
		return false
	}
	acc.ingest(&mf)
	return false
}

// onAbort runs when the agent's stream ended without a terminal frame (the agent
// disconnected mid-scrape). completed stays false, so agent_up is recorded as 0.
func (acc *scrapeAccumulator) onAbort() {
	acc.logger.Error(nil, "agent metrics.scrape stream aborted", "agentID", acc.agentID)
}

// ingest appends a chunk to its exporter's buffer, or on the exporter's Done
// frame folds the completed body into the merger and records the exporter's up
// gauge.
func (acc *scrapeAccumulator) ingest(mf *agentapi.MetricsFrame) {
	if !mf.Done {
		if len(mf.Chunk) == 0 {
			return
		}
		buf, ok := acc.buffers[mf.Exporter]
		if !ok {
			buf = new(bytes.Buffer)
			acc.buffers[mf.Exporter] = buf
		}
		buf.Write(mf.Chunk)
		return
	}

	buf := acc.buffers[mf.Exporter]
	delete(acc.buffers, mf.Exporter)
	constLabels := labels{metricLabelAgent: acc.agentID, metricLabelExporter: mf.Exporter}
	var up float64
	defer func() {
		acc.merger.add(gaugeFamily(metricTargetUp,
			"1 if the agent scraped this exporter successfully, 0 otherwise.", up, constLabels))
	}()

	if mf.Error != "" {
		acc.logger.Error(nil, "agent scrape exporter failed",
			"agentID", acc.agentID, "exporter", mf.Exporter, "error", mf.Error)
		return
	}
	if buf == nil {
		// No chunks and no error: the exporter served an empty exposition.
		up = 1
		return
	}
	if acc.foldBody(buf.Bytes(), constLabels, mf.Exporter) {
		up = 1
	}
}

// foldBody parses one exporter's exposition, injects the agent and exporter
// labels, and merges its families into the shared merger. UTF8Validation accepts
// every valid legacy exporter name plus UTF-8 names, so it is the permissive
// correct scheme for ingesting arbitrary exporter output; a zero-value TextParser
// leaves the scheme unset and panics on first parse.
func (acc *scrapeAccumulator) foldBody(body []byte, constLabels labels, exporter string) (ok bool) {
	parser := expfmt.NewTextParser(model.UTF8Validation)
	var parsed map[string]*dto.MetricFamily
	var err error
	if parsed, err = parser.TextToMetricFamilies(bytes.NewReader(body)); err != nil {
		acc.logger.Error(err, "parse agent exporter exposition failed", "agentID", acc.agentID, "exporter", exporter)
		return
	}
	for _, fam := range parsed {
		injectLabels(fam, constLabels)
		acc.merger.add(fam)
	}
	ok = true
	return
}

// labels is a set of constant labels injected into a fanned-out metric family.
type labels map[string]string

// familyMerger accumulates dto.MetricFamily values from concurrent agent scrapes,
// merging series that share a metric name under a single family (one HELP/TYPE).
// It is safe for concurrent use.
type familyMerger struct {
	mu     sync.Mutex
	byName map[string]*dto.MetricFamily
}

func newFamilyMerger() *familyMerger {
	return &familyMerger{byName: make(map[string]*dto.MetricFamily)}
}

// add merges mf into the accumulator. A first occurrence of a name is taken
// whole (keeping its HELP/TYPE); a later one with the same name appends its
// series, provided the type matches. A type conflict across exporter versions is
// dropped rather than producing an exposition the Prometheus parser rejects.
func (m *familyMerger) add(mf *dto.MetricFamily) {
	if mf.GetName() == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	existing, ok := m.byName[mf.GetName()]
	if !ok {
		m.byName[mf.GetName()] = mf
		return
	}
	if existing.GetType() != mf.GetType() {
		return
	}
	existing.Metric = append(existing.Metric, mf.GetMetric()...)
}

// families returns the merged families sorted by name for a stable exposition.
func (m *familyMerger) families() (out []*dto.MetricFamily) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out = make([]*dto.MetricFamily, 0, len(m.byName))
	for _, mf := range m.byName {
		out = append(out, mf)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GetName() < out[j].GetName() })
	return
}

// injectLabels adds each constant label to every series in mf, leaving a series
// that already carries a label of that name untouched so no duplicate label is
// produced. The per-series labels are then sorted by name.
func injectLabels(mf *dto.MetricFamily, constLabels labels) {
	for _, metric := range mf.GetMetric() {
		for name, value := range constLabels {
			if hasLabel(metric, name) {
				continue
			}
			metric.Label = append(metric.Label, &dto.LabelPair{Name: proto.String(name), Value: proto.String(value)})
		}
		sort.Slice(metric.Label, func(i, j int) bool { return metric.Label[i].GetName() < metric.Label[j].GetName() })
	}
}

func hasLabel(metric *dto.Metric, name string) (found bool) {
	for _, lp := range metric.GetLabel() {
		if lp.GetName() == name {
			found = true
			return
		}
	}
	return
}

// gaugeFamily builds a single-series gauge MetricFamily for a synthesized health
// metric (agent_up, hostlink_scrape_target_up).
func gaugeFamily(name, help string, value float64, constLabels labels) (mf *dto.MetricFamily) {
	metric := &dto.Metric{Gauge: &dto.Gauge{Value: proto.Float64(value)}}
	names := make([]string, 0, len(constLabels))
	for n := range constLabels {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		metric.Label = append(metric.Label, &dto.LabelPair{Name: proto.String(n), Value: proto.String(constLabels[n])})
	}
	mf = &dto.MetricFamily{
		Name:   proto.String(name),
		Help:   proto.String(help),
		Type:   dto.MetricType_GAUGE.Enum(),
		Metric: []*dto.Metric{metric},
	}
	return
}

// writeExposition encodes the merged families to the response, negotiating the
// wire format from the Accept header (defaulting to Prometheus text).
func writeExposition(c *gin.Context, families []*dto.MetricFamily, logger logr.Logger) {
	format := expfmt.Negotiate(c.Request.Header)
	c.Header("Content-Type", string(format))
	c.Status(http.StatusOK)

	enc := expfmt.NewEncoder(c.Writer, format)
	for _, mf := range families {
		if err := enc.Encode(mf); err != nil {
			logger.Error(err, "encode metric family failed; client gone", "family", mf.GetName())
			return
		}
	}
	if closer, ok := enc.(expfmt.Closer); ok {
		if err := closer.Close(); err != nil {
			logger.Error(err, "close metric encoder failed")
		}
	}
}
