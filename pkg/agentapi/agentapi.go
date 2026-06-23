// Package agentapi defines the wire contract for the generic AgentRequest plane:
// the method names the controller drives to an agent and the JSON shapes of their
// payloads. The gRPC layer (AgentRequest/AgentResult) and the ControllerPeer relay
// treat method/payload as opaque; this package is the agreed schema both the
// controller and the agent encode/decode against.
package agentapi

import "time"

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

	// MethodFsStat reports metadata for a single path under the agent working
	// directory. Its request payload is an FsPathRequest; the result payload is
	// an FsEntry. A missing path yields code 404.
	MethodFsStat = "fs.stat"

	// MethodFsList lists the immediate (non-recursive) entries of a directory
	// under the agent working directory. Its request payload is an FsPathRequest;
	// the result payload is an FsListResult.
	MethodFsList = "fs.list"

	// MethodFsRead streams a file's bytes from the agent working directory. Its
	// request payload is an FsPathRequest. It is a streaming method: the body is
	// delivered as a sequence of AgentProgress frames (raw bytes, not JSON)
	// followed by a terminal AgentResult. When the file cannot be opened (missing,
	// or a directory) the agent emits no body frames and reports the error in the
	// terminal result (code 404/400) so the REST layer can still set a status.
	MethodFsRead = "fs.read"

	// MethodFsWrite writes a file under the agent working directory. Its opening
	// request payload is an FsWriteRequest; the body is delivered as a sequence of
	// AgentRequestChunk messages on the Control stream. With Exclusive set, an
	// existing target yields code 409; otherwise the file is truncated and
	// overwritten. The terminal AgentResult reports the outcome.
	MethodFsWrite = "fs.write"

	// MethodFsMkdir creates a directory (and any missing parents) under the agent
	// working directory. Its request payload is an FsPathRequest. An existing
	// directory yields code 409.
	MethodFsMkdir = "fs.mkdir"

	// MethodFsRemove deletes a path under the agent working directory, recursively
	// for directories. Its request payload is an FsPathRequest. A missing path
	// yields code 404.
	MethodFsRemove = "fs.remove"

	// MethodMetricsScrape pulls the agent's configured upstream exporters (e.g.
	// node_exporter, dcgm-exporter) and streams their raw Prometheus exposition to
	// the controller. It takes no request payload. It is a streaming method: each
	// exporter's body is delivered as a sequence of AgentProgress frames carrying a
	// MetricsFrame, and the terminal AgentResult marks the whole scrape complete.
	// Streaming keeps the agent's memory bounded and removes the single-message
	// size limit a unary reply would impose on a large exposition.
	MethodMetricsScrape = "metrics.scrape"
)

// MetricsFrame is one AgentProgress frame of a streaming metrics.scrape. Frames
// for a given Exporter arrive in order: zero or more carrying a Chunk of its raw
// exposition, then exactly one with Done set (Error non-empty when that exporter
// could not be scraped). The controller demultiplexes by Exporter, reassembles
// each body, and folds it into the fleet aggregation.
type MetricsFrame struct {
	Exporter string `json:"exporter"`
	Chunk    []byte `json:"chunk,omitempty"`
	Done     bool   `json:"done,omitempty"`
	Error    string `json:"error,omitempty"`
}

// FsPathRequest is the request payload shared by the path-addressed fs methods
// (stat, list, read, mkdir, remove). Path is relative to the agent working
// directory; an empty Path addresses the working directory root.
type FsPathRequest struct {
	Path string `json:"path"`
}

// FsWriteRequest is the opening MethodFsWrite request payload. Path is the
// target file relative to the agent working directory. Exclusive requires the
// target not to exist (a failed create yields 409); when false the target is
// truncated and overwritten.
type FsWriteRequest struct {
	Path      string `json:"path"`
	Exclusive bool   `json:"exclusive,omitempty"`
}

// FsEntry is one directory entry (or a single path's metadata for stat). Dir
// reports whether the entry is a directory; Size is the file size in bytes (0
// for directories); ModTime is the last-modification time.
type FsEntry struct {
	Name    string    `json:"name"`
	Dir     bool      `json:"dir"`
	Size    int64     `json:"size"`
	ModTime time.Time `json:"modTime"`
}

// FsListResult is the MethodFsList result payload: the immediate entries of the
// listed directory, not recursed.
type FsListResult struct {
	Entries []FsEntry `json:"entries"`
}

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
