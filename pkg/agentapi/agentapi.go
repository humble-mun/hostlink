// Package agentapi defines the wire contract for the generic AgentRequest plane:
// the method names the controller drives to an agent and the JSON shapes of their
// payloads. The gRPC layer (AgentRequest/AgentResult) and the ControllerPeer relay
// treat method/payload as opaque; this package is the agreed schema both the
// controller and the agent encode/decode against.
package agentapi

const (
	// MethodImagesList lists the Docker images present on the agent host. It takes
	// no request payload; the result payload is a JSON array of ImageSummary.
	MethodImagesList = "images.list"
)

// ImageSummary is one entry of the MethodImagesList result. It mirrors the
// relevant fields of the Docker image summary, projected to a stable JSON shape
// the REST layer returns unchanged.
type ImageSummary struct {
	ID          string            `json:"id"`
	RepoTags    []string          `json:"repoTags"`
	RepoDigests []string          `json:"repoDigests"`
	Created     int64             `json:"created"`
	Size        int64             `json:"size"`
	Labels      map[string]string `json:"labels,omitempty"`
}
