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

	// MethodImagesPull pulls a Docker image onto the agent host. Its request
	// payload is a PullRequest. It is a streaming method: the agent emits zero or
	// more AgentProgress frames carrying a PullProgress JSON body before the
	// terminal AgentResult, which has an empty payload on success.
	MethodImagesPull = "images.pull"

	// MethodImagesRemove removes one or more Docker images from the agent host.
	// Its request payload is a RemoveRequest; the result payload is a
	// RemoveResult reporting per-reference deletions and errors.
	MethodImagesRemove = "images.remove"
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

// PullRequest is the MethodImagesPull request payload. Image is a Docker image
// reference (e.g. "ghcr.io/org/app:tag"); Auth, when set, carries the registry
// credentials used to pull from a private registry.
type PullRequest struct {
	Image string        `json:"image"`
	Auth  *RegistryAuth `json:"auth,omitempty"`
}

// RegistryAuth carries credentials for a private registry pull. It mirrors the
// fields the Docker daemon accepts; the agent encodes it into the registry-auth
// header the Docker client expects. Either username/password or an identity
// token may be supplied.
type RegistryAuth struct {
	Username      string `json:"username,omitempty"`
	Password      string `json:"password,omitempty"`
	ServerAddress string `json:"serverAddress,omitempty"`
	IdentityToken string `json:"identityToken,omitempty"`
}

// PullProgress is one AgentProgress frame of a MethodImagesPull operation. It
// projects the Docker pull JSON stream (jsonmessage) to a stable shape: ID names
// the layer the line refers to, Status is the human-readable phase, and
// Current/Total give byte counts for a progress bar when the daemon reports them.
// Error is set when the daemon reports a mid-stream error for this line.
type PullProgress struct {
	ID       string `json:"id,omitempty"`
	Status   string `json:"status,omitempty"`
	Progress string `json:"progress,omitempty"`
	Current  int64  `json:"current,omitempty"`
	Total    int64  `json:"total,omitempty"`
	Error    string `json:"error,omitempty"`
}

// RemoveRequest is the MethodImagesRemove request payload. Refs lists the image
// references or IDs to remove (e.g. "ubuntu:24.04", "sha256:abc..."). Force
// removes an image even if it is tagged in multiple repositories or used by a
// stopped container; NoPrune keeps untagged parent layers.
type RemoveRequest struct {
	Refs    []string `json:"refs"`
	Force   bool     `json:"force,omitempty"`
	NoPrune bool     `json:"noPrune,omitempty"`
}

// RemoveResult is the MethodImagesRemove result payload. Deleted aggregates the
// untagged/deleted layer records the daemon reported across all references;
// Errors lists the references that could not be removed, each with its reason.
type RemoveResult struct {
	Deleted []DeletedImage `json:"deleted"`
	Errors  []RemoveError  `json:"errors,omitempty"`
}

// DeletedImage is one layer record from a remove operation: exactly one of
// Untagged or Deleted is set, mirroring the Docker image-remove response.
type DeletedImage struct {
	Untagged string `json:"untagged,omitempty"`
	Deleted  string `json:"deleted,omitempty"`
}

// RemoveError reports that a single reference could not be removed.
type RemoveError struct {
	Ref   string `json:"ref"`
	Error string `json:"error"`
}
