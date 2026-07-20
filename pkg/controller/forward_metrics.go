package controller

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/humble-mun/chassis/pkg/metrics"
)

// Labels on forwardFailure beyond the failing step. Each stays bounded: port
// by the configured mappings, agent by the fleet (reusing metricLabelAgent so
// alert rules can join with agent_up), peer by the controller replica set. A
// label is empty when the failing step cannot know it yet.
const (
	forwardLabelReason = "reason"
	forwardLabelPort   = "port"
	forwardLabelPeer   = "peer"
)

// Reasons labelling forwardFailure with the step of the public port forwarding
// path that failed. A public connection increments at most one reason: the
// first step that aborted it.
const (
	// handleConn: resolving the accepted public port to an agent mapping.
	forwardFailureLookup        = "lookup_mapping"
	forwardFailurePortNotFound  = "port_not_found"
	forwardFailurePortSuspended = "port_suspended"

	// handleLocal: pairing the public connection with the agent stream.
	forwardFailureSessionID   = "session_id"
	forwardFailureSendCommand = "send_open_forward"
	forwardFailureAgentReject = "agent_rejected"
	forwardFailurePairTimeout = "pair_timeout"
	forwardFailureSplice      = "splice"

	// handleRemote: relaying the connection to the agent's holding pod.
	forwardFailureCrossPodDisabled  = "cross_pod_disabled"
	forwardFailureLocateHolder      = "locate_holder"
	forwardFailureHolderUnavailable = "holder_unavailable"
	forwardFailureHolderSelf        = "holder_self"
	forwardFailureRemoteOpen        = "remote_open"

	// runPortReconciler: keeping public listeners in sync with desired ports.
	forwardFailureListDesired = "list_desired"

	// listenerManager: binding public ports and accepting connections. These are
	// the intake errors: they drop or refuse public connections before any
	// forwarding logic runs.
	forwardFailureListenerBind  = "listener_bind"
	forwardFailureListenerClose = "listener_close"
	forwardFailureAccept        = "accept"
)

// forwardFailure counts public port forwarding failures by the step that
// failed. It is exposed on the chassis /metrics endpoint (the controller's own
// metrics), not on the aggregated /api/v1/metrics agent fan-out.
var forwardFailure = metrics.Register(func(factory promauto.Factory) *prometheus.CounterVec {
	return factory.NewCounterVec(prometheus.CounterOpts{
		Name: "hostlink_forward_failure",
		Help: "Total number of public port forwarding failures, labeled by the step that failed " +
			"and by the public port, agent, and peer controller where the step knows them",
	}, []string{forwardLabelReason, forwardLabelPort, metricLabelAgent, forwardLabelPeer})
})

// recordForwardFailure counts one forwarding failure. A zero port or empty
// agentID/peer means the step failed before that dimension was known.
func recordForwardFailure(reason string, port uint32, agentID, peer string) {
	forwardFailure.WithLabelValues(reason, forwardPortLabel(port), agentID, peer).Inc()
}

func forwardPortLabel(port uint32) string {
	if port == 0 {
		return ""
	}
	return strconv.FormatUint(uint64(port), 10)
}
