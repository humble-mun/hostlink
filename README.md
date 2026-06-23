# hostlink

> [中文文档](./README_CN.md)

> ⚠️ **EARLY STAGE — NOT PRODUCTION READY.** Implemented and verified end-to-end:
> the agent↔controller mTLS connection and `Control` stream (handshake +
> heartbeat); a generic request/response dispatch envelope with REST endpoint
> families for **Docker images** (`GET`/`POST`/`DELETE
> /api/v1/agents/<id>/images` — list, pull with streaming SSE progress, and
> remove) and an agent **working-directory filesystem** (`GET`/`POST`/`PUT`/`DELETE
> /api/v1/agents/<id>/files` — browse, download, streamed multipart upload, and
> recursive delete), served by an agent-side Docker client and a sandboxed data
> directory; the Redis-backed `agent→pod` registry; and cross-pod API relay via the
> `ControllerPeer` plane (multi-replica HA). Still **designed but not yet
> implemented**: container *lifecycle* ops (`DockerOp`/events), metrics fan-out,
> port forwarding, and the cross-pod port-forward data plane — see
> [Roadmap](#roadmap). Do not deploy this as a control plane for real workloads yet.

A control plane for managing Linux hosts that live **outside the cloud** (on-prem / colo servers). These hosts are **not** Kubernetes nodes; their workloads run as **Docker containers**, not pods. `hostlink` gives the cloud a persistent, mutually-authenticated channel to each NAT'd host for command dispatch, container lifecycle, metrics, and raw-TCP port forwarding — with no inbound connectivity required to the host.

**Scale assumptions.** Roughly a dozen agents, a few dozen at most. The design favors *controllable, debuggable, good-enough* over web-scale; do not over-engineer for massive fleets.

## Table of Contents

- [Overview](#overview)
- [How It Works](#how-it-works)
- [Architecture](#architecture)
  - [Connection Model](#connection-model)
  - [Command Channel](#command-channel)
  - [Metrics Reverse Fan-out](#metrics-reverse-fan-out)
  - [Port Forwarding](#port-forwarding)
  - [Multi-replica Affinity](#multi-replica-affinity)
  - [Container Lifecycle](#container-lifecycle)
- [Wire Protocol](#wire-protocol)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Build](#build)
  - [Certificates (mTLS)](#certificates-mtls)
- [CLI Flags](#cli-flags)
- [Deployment Shape](#deployment-shape)
- [Security Model](#security-model)
- [Hazards and Hard Constraints](#hazards-and-hard-constraints)
- [Roadmap](#roadmap)
- [License](#license)

---

## Overview

`hostlink` builds two binaries from a single Go module:

- **`hostlink-controller`** — runs in the cloud, inside Kubernetes. The control plane. It is the gRPC **server**, terminates mutual TLS for agents, and (by design) aggregates metrics, allocates public ports, and routes forwarded TCP across replicas.
- **`hostlink`** — runs on each external host as a systemd service. It is the gRPC **client**: it dials out to the controller (so it works behind NAT with no inbound firewall rules), drives the local Docker daemon, reports metrics, and carries tunnels.

The premise that shapes everything below: **under HTTP/2 a server cannot open a stream to a client.** The agent is behind NAT and must be the dialer, so every "controller → agent" interaction is modeled as a *reverse* flow over a connection the agent established.

### Scope and Boundaries

`hostlink` is the **infrastructure layer only** — connectivity, container orchestration, metrics, and port forwarding. It deliberately stays out of everything else:

- **It does** maintain one persistent, mTLS-authenticated gRPC connection per agent, multiplex commands / metrics / forwarded bytes over that connection, drive Docker container lifecycle, and expose container ports to public cloud ports.
- **It does not** implement billing, provisioning, quotas, or any other business logic — those belong to the larger Smoothcloud platform.
- **It does not** treat hosts as Kubernetes nodes. Workloads are Docker containers ("pets" with a preserved writable layer), not pods.
- **It does not** build its own L3 overlay today. Only L4 (port) forwarding is in scope; an L3/VPN story is explicitly deferred (see [Roadmap](#roadmap)).

---

## How It Works

```
external host (behind NAT)                cloud (Kubernetes)
  hostlink                          hostlink-controller (≥2 replicas)
       │                                          │
       │── dial out (mTLS, TLS 1.3) ────────────► │  one gRPC connection per agent
       │                                          │
       │── Control(stream AgentEvent) ──────────► │  long-lived bidi stream
       │     Hello (handshake) ─────────────────► │
       │     Heartbeat (refresh TTL) ───────────► │
       │                                          │
       │ ◄──────────── Command (push) ─────────── │  OpenForward / DockerOp / ExposeRule
       │                                          │
       │── Forward(stream Frame) ───────────────► │  one stream per forwarded TCP conn
       │     DATA / HALF_CLOSE / RESET           │  (opened by the agent on demand)
       │                                          │
```

One TCP connection, many HTTP/2 streams. The agent opens the `Control` stream once after connecting; the controller pushes commands down its response direction. Each forwarded public connection gets its own `Forward` stream so gRPC's per-stream flow control gives natural backpressure.

> **What is implemented today:** the mTLS dial, the `Control` stream and `Hello` / `Heartbeat` exchange; the generic `AgentRequest`/`AgentResult` dispatch envelope with the Docker **images** endpoints — `GET` (list), `POST` (pull, streaming SSE progress), and `DELETE` (remove) on `/api/v1/agents/<id>/images`; the Redis `agent→pod` registry; and cross-pod relay of API requests via `ControllerPeer.Dispatch` (unary) and `ControllerPeer.DispatchStream` (streaming, for the pull progress). `Forward`, `DockerOp` execution, metrics fan-out, and the cross-pod port-forward data plane are stubs or unimplemented. See [Roadmap](#roadmap).

---

## Architecture

### Connection Model

The agent is the gRPC **client**; the controller has the public entrypoint and is the gRPC **server**. Each agent maintains **one** gRPC connection to the controller, and every logical channel multiplexes onto HTTP/2 streams over it:

- **Commands** — one long-lived bidi `Control` stream, opened by the agent; the controller pushes commands down its response direction.
- **Metrics** — the controller reverse-pulls each agent's exposition over this connection.
- **Port forwarding** — each forwarded public connection is one `Forward` stream, opened by the agent.

> **Constraint:** "reuse one connection" means **one TCP connection with many streams** — not cramming all bytes into one stream and building a sub-mux. HTTP/2 streams *are* the mux; layering yamux/smux on top is redundant.

> **Constraint (transport security):** the agent↔controller connection is **mutual TLS (mTLS), TLS 1.3 minimum, with no insecure fallback**. The agent presents a client certificate and verifies the controller against a CA bundle; the controller presents a server certificate and requires-and-verifies the agent's client certificate. mTLS is the agent identity mechanism at the connection layer.

### Command Channel

On connect, the agent opens `Control(stream AgentEvent) returns (stream Command)`. The controller pushes `OpenForward`, `DockerOp` (run / stop / start / pause / unpause / rm), and `ExposeRule` down the `Command` stream. The agent reports its handshake, heartbeats, and Docker events up the `AgentEvent` stream.

### Metrics Reverse Fan-out

Prometheus stays in **pull mode** and scrapes a **single** target — the controller's `/metrics`. On scrape, the controller's handler:

1. **Concurrently** reverse-pulls each online agent's node_exporter exposition over its existing connection.
2. Gives each agent an **independent deadline strictly shorter than `scrape_timeout`** (~5s vs the 10s default); slow / failed agents are skipped so one slow agent does not blow up the whole scrape.
3. **Merges by `MetricFamily`** — decode each exposition with `expfmt.TextParser`, inject an `agent=<id>` label into every series, merge by metric name into one family (one HELP/TYPE), then encode once.
4. Synthesizes `agent_up{agent="<id>"}` (1 = scraped this round, 0 = offline) — the only clean signal for "an agent went down", since Prometheus sees only one target.

> **Constraint:** **never string-concatenate expositions.** Duplicate HELP/TYPE lines for the same metric name make the Prometheus parser reject the entire payload. Merge at the `MetricFamily` level.

> node_exporter runs as a **separate sidecar binary**; the agent GETs `127.0.0.1:9100` locally. Do **not** import node_exporter as a library — its collector package is not a stable public API.

### Port Forwarding

Goal: dynamically expose a container's internal port (e.g. vLLM on `:8080`) to a public cloud port (e.g. `:1025`) over **raw TCP** with half-close support. The data plane is **L4 stream proxying** (terminate TCP at each hop, carry only application bytes) — which inherently avoids TCP-over-TCP degradation.

Because the server cannot open a stream to the agent, the stream-open handshake is reversed:

1. A public connection lands on the controller's exposed port.
2. The controller pushes `OpenForward{session_id, target}` down that agent's `Control` stream.
3. The agent **opens** a `Forward` stream, first frame carrying `session_id`.
4. The controller **pairs** the public connection with that `Forward` stream by `session_id` and relays bidirectionally.

This handshake + chunking + session pairing is provided by [`openconfig/grpctunnel`](https://github.com/openconfig/grpctunnel) — do not rebuild it.

> **Constraint (half-close, the correctness crux):** a gRPC stream's own lifecycle (`CloseSend` / handler return) cannot represent TCP's independent per-direction half-close. It is modeled with **explicit frame types**: local EOF → send `HALF_CLOSE` and stop sending that direction but keep reading the other; receive `HALF_CLOSE` → `CloseWrite()` the local socket; `RESET` → `SetLinger(0)` + `Close()`. Before integrating `openconfig/grpctunnel`, **verify** its byte pipe fully preserves half-close; if not, patch it.

> **Constraint (backpressure):** never read from the local socket faster than you can `Send`. Rely on `Send` blocking when the HTTP/2 flow-control window is full. Do not buffer without bound.

### Multi-replica Affinity

The controller runs ≥2 replicas for HA. An agent's connection is pinned to the one pod it dialed, but public TCP arriving through the L4 load balancer lands on **any** replica — usually not the one holding that agent.

- **Routing key:** for raw TCP the only ingress key is the **destination port**. Each exposure = one distinct public port.
- **Registry (Redis dual maps):** `port:<P>` → `(agentID, container_target)`; `agent:<id>` → `holding_pod` (written when the agent attaches, **TTL refreshed by heartbeat**, deleted on disconnect).
- **Atomic port allocation:** take a free port from a reserved pool via Redis `INCR` / `SETNX`.
- **Routing flow:** a public connection reaches pod B on port P → B looks up `port:P` → agentX, then `agent:agentX` → podA. If `podA == B`, B drives the reverse-open directly; otherwise B forwards the connection to podA over **internal gRPC** (two byte-pipe hops in series; half-close must propagate end to end).
- **Stale window:** because the LB spreads connections, cross-pod forwarding is the **common** case (hit rate ≈ 1/N). A pod that receives "forwarded to me but I don't hold agentX" must **reject** and have the caller re-resolve and retry.

> Redis is already in the stack. Raft/gossip/mesh were evaluated and rejected: `agent→pod` is not contended state and needs no consensus.

### Container Lifecycle

"Power off / on" = `docker stop` / `docker start`. Docker containers are **stateful pets**: stop is SIGTERM → grace → SIGKILL, the writable layer is preserved on local disk, and killing the process frees the GPU; start brings back the same container ID with state intact. On plain Docker this is free — no K8s-style upperdir persistence needed.

Exposure rules are tied to lifecycle via `client.Events()`:

- on `stop` / `die` → suspend or remove that container's exposure (clear the Redis `port:` mapping).
- on `start` → re-resolve the container IP and re-establish exposure.

> The container IP can change after restart; if the port is re-allocated by the new holder, the **public port changes and clients must reconnect** — an accepted design tradeoff. For GPU containers, remember the nvidia runtime; `docker pause` / `unpause` (freezer cgroup, keeps RAM/VRAM) is a distinct "suspend, resume instantly" semantic.

---

## Wire Protocol

The services are defined in `pkg/api/hostlink/v1/`. `AgentLink` (agent↔controller) has two bidirectional-stream RPCs; `ControllerPeer` (controller↔controller) has one unary RPC:

```proto
service AgentLink {
  // Opened once after the agent connects; the controller pushes commands down
  // the response stream. Under HTTP/2 the server cannot initiate an RPC to the
  // client, so all server->agent commands travel over this already-open stream.
  rpc Control(stream AgentEvent) returns (stream Command);

  // One per forwarded public TCP connection; opened by the agent, first frame
  // carries session_id for pairing. Internal cross-pod forwarding reuses this
  // same service definition (just dialed to a sibling pod).
  rpc Forward(stream Frame) returns (stream Frame);
}

service ControllerPeer {
  // Cross-pod relay: a replica that does not hold the target agent forwards the
  // request to the holding pod (resolved via the Redis agent->pod map). A stale
  // holder rejects with FAILED_PRECONDITION so the caller re-resolves and retries.
  rpc Dispatch(DispatchRequest) returns (AgentResult);

  // Streaming variant for long-running ops (e.g. image pull): each streamed
  // AgentResult is a progress frame except the last, which has final=true and
  // carries the terminal code/payload/error.
  rpc DispatchStream(DispatchRequest) returns (stream AgentResult);
}
```

| Message | Direction | Purpose |
|---------|-----------|---------|
| `AgentEvent{Hello / Heartbeat / DockerEvent / AgentResult / AgentProgress}` | agent → controller | Handshake, TTL refresh, container reports, reply to an `AgentRequest`, or a progress frame for a long-running one |
| `Command{OpenForward / DockerOp / ExposeRule / AgentRequest / AgentRequestChunk}` | controller → agent | Open a forward stream, run a Docker op, add/remove an exposure rule, a generic method-dispatched request, or a body chunk for a streaming upload |
| `AgentRequest{request_id, method, payload}` / `AgentResult{request_id, code, payload, error, final}` | both | Generic API call: `method` names the op (e.g. `images.list`), `payload` is JSON, `code` mirrors an HTTP status, correlated by `request_id`; `final` marks the terminal frame of a streamed reply |
| `AgentRequestChunk{request_id, data, last}` | controller → agent | One body chunk of a streaming upload request (e.g. `fs.write`), correlated to its opening `AgentRequest` by `request_id`; `last` marks the final chunk |
| `AgentProgress{request_id, payload}` | agent → controller | A non-terminal progress update for a long-running `AgentRequest` (e.g. `images.pull` layer status, or `fs.read` file bytes); `payload` is method-specific. The op still completes with a `final` `AgentResult` |
| `DispatchRequest{agent_id, AgentRequest}` | controller → controller | Wraps an `AgentRequest` with the routing key for the `ControllerPeer` hop (`Dispatch` unary, `DispatchStream` streaming) |
| `UploadFrame{open / chunk, last}` | controller → controller | Streams a controller→agent upload across the `ControllerPeer.Upload` hop: the first frame carries the `DispatchRequest` open, the rest carry body chunks |
| `Frame{session_id, type, data}` | both (per forward) | Raw TCP bytes; `type` ∈ `DATA` / `HALF_CLOSE` / `RESET` |

The `Frame.Type` enum is what makes correct TCP half-close possible (see [Port Forwarding](#port-forwarding)). Regenerate and commit the generated code after any `.proto` change.

### REST API

The controller serves a small REST surface on its in-cluster default listener, on top of the generic dispatch envelope above:

| Method & path | Description |
|---------------|-------------|
| `GET /api/v1/agents` | List the connected agents. |
| `GET /api/v1/agents/<agentId>/images` | List the Docker images on the given agent. Dispatches `images.list` to the agent's `Control` stream (locally, or relayed to the holding pod via `ControllerPeer.Dispatch`); returns the agent's JSON unchanged. 404 if the agent is connected to no reachable replica. |
| `POST /api/v1/agents/<agentId>/images` | Pull a Docker image on the agent. JSON body `{"image":"<ref>","auth":{...optional registry auth...}}`. Responds with a **`text/event-stream`** (SSE): each event is `data: <PullProgress JSON>` (`id`/`status`/`current`/`total`/`progress`), terminated by `data: {"done":true,"code":...,"error":...}`. Dispatches the streaming `images.pull` (locally, or relayed via `ControllerPeer.DispatchStream`); no dispatch timeout (large pulls take minutes). 404 if the agent is unreachable. |
| `DELETE /api/v1/agents/<agentId>/images/<imageId>` | Remove a single image by ID (path param; works for an image ID or a digest, which contain no `/`). Optional `?force=true` and `?noPrune=true`. Dispatches `images.remove`; returns a `RemoveResult` JSON `{"deleted":[...],"errors":[...]}` (partial failures are reported, not fatal). |
| `DELETE /api/v1/agents/<agentId>/images?ref=A&ref=B` | Remove multiple images by repeated `ref` query params (use this form for full `repo/path:tag` references, which contain `/` and cannot be a path segment). Same options/response as the single-image form. |
| `GET /api/v1/agents/<agentId>/files?path=<p>` | Browse the agent's working directory (`--data-dir`). A directory returns JSON `{"entries":[{"name","dir","size","modTime"}]}` (non-recursive); a file is **streamed as a download** (`Content-Disposition`), or with `Accept: application/json` returns its `FsEntry` metadata. Empty `path` is the working-dir root. Dispatches `fs.stat` then `fs.list` / `fs.read`. |
| `POST /api/v1/agents/<agentId>/files?path=<p>` | `&dir=true` creates a directory (`fs.mkdir`, `409` if it exists). Otherwise uploads one or more files from a **multipart form**, streamed to the agent in chunks (`fs.write`) and created **exclusively** — an existing target is reported per-file. Response `{"written":[...],"errors":[...]}`. |
| `PUT /api/v1/agents/<agentId>/files?path=<p>` | Overwrite a single file with the request body (raw bytes, or the first multipart part); streamed via `fs.write` (truncate). |
| `DELETE /api/v1/agents/<agentId>/files?path=<p>` | Remove the path, **recursively** for directories (`fs.remove`). |

> The `files` endpoints stream both directions in 64 KB chunks (download via `AgentProgress` frames, upload via `AgentRequestChunk` / the `ControllerPeer.Upload` relay), so large files transfer with bounded memory. The agent resolves every `path` inside its working directory and rejects traversal (`..`) outside it.

---

## Getting Started

### Prerequisites

| Component | Version / Note |
|-----------|----------------|
| Go (build only) | 1.26+ |
| Controller runtime | Kubernetes (StatefulSet; default 1 replica, ≥2 for HA) |
| Agent runtime | A Linux host reachable *outbound* to the controller; systemd |
| Docker (on each host) | Required — workloads are Docker containers; nvidia runtime for GPU |
| Redis | The `agent→pod` registry — optional for a single replica, **required for HA** (`replicaCount > 1`) |
| cert-manager + CSI driver | Optional — issues controller/peer certs per-pod instead of mounting Secrets |
| node_exporter | Sidecar on each host, listening on `127.0.0.1:9100` |

This project is **Linux-only** (it manages a Linux Docker daemon and uses Linux-specific socket semantics for forwarding). It does not build or run natively on Windows; build inside a Linux toolchain.

### Build

The project is a single Go module with one binary per `cmd/` subdirectory, and it is **vendored** — build offline with `-mod=vendor`:

```bash
go build -mod=vendor -o bin/hostlink-controller ./cmd/controller
go build -mod=vendor -o bin/hostlink      ./cmd/agent
```

> The agent is the only hostlink binary installed on a host, so it is named simply `hostlink`; the cloud-side control plane keeps the `hostlink-controller` name.

On a Windows dev machine, compile inside a Linux container (the working tree and Go caches are bind-mounted, the container provides the Linux toolchain):

```bash
docker run --rm \
  -v "$PWD":/go/src/github.com/humble-mun/hostlink \
  -w /go/src/github.com/humble-mun/hostlink \
  golang:1.26.3-trixie go build -mod=vendor -v ./...
```

### Certificates (mTLS)

The agent↔controller connection requires a working PKI. You need a CA, a controller (server) certificate, and per-agent (client) certificates:

| Side | Presents | Verifies the peer against |
|------|----------|---------------------------|
| controller | server cert/key (`--grpc-tls-cert-path` / `--grpc-tls-key-path`) | agent CA bundle (`--grpc-tls-ca-path`) |
| agent | client cert/key (`--agent-tls-cert-path` / `--agent-tls-key-path`) | controller CA bundle (`--controller-tls-ca-path`) |

The controller requires-and-verifies the agent client certificate (`RequireAndVerifyClientCert`); the agent verifies the controller's server name (`--controller-tls-server-name`). If left empty, gRPC verifies against the dial endpoint's host, so set it explicitly whenever the certificate SAN differs from the dial address (e.g. dialing by IP). There is **no insecure fallback** — if certificates are missing or invalid, the connection fails hard.

---

## CLI Flags

All flags can also be set via environment variables: uppercase the flag, replace `-` with `_`, and prefix with `HM_` (e.g. `--controller-endpoint` → `HM_CONTROLLER_ENDPOINT`). Config may also be supplied as a YAML file at `/etc/humble-mun/<binary>.yaml` (`hostlink.yaml` or `controller.yaml`, after the binary's `version.Name`), with the flag names as keys; the file is watched and reloaded at runtime. Precedence: flags > env > config file.

### Agent flags

| Flag | Default | Description |
|------|---------|-------------|
| `--controller-endpoint` | — | Address of the `hostlink-controller` gRPC endpoint to dial, as `host:port` (required) |
| `--agent-tls-cert-path` | — | Client certificate the agent presents to the controller for mTLS |
| `--agent-tls-key-path` | — | Private key matching the client certificate |
| `--controller-tls-ca-path` | — | CA bundle used to verify the controller's certificate |
| `--controller-tls-server-name` | — | Server name to verify against the controller's certificate; if empty, gRPC verifies against the dial endpoint's host, so set it explicitly when the cert SAN differs from the dial address |

### Controller flags

| Flag | Default | Description |
|------|---------|-------------|
| `--grpc-bind-address` | — | Address to bind the mTLS gRPC listener for agent connections, as `host:port` |
| `--grpc-tls-cert-path` | — | Server certificate the controller presents to agents for mTLS |
| `--grpc-tls-key-path` | — | Private key matching the server certificate |
| `--grpc-tls-ca-path` | — | CA bundle used to verify agent client certificates |
| `--redis-url` | — | Redis URL backing the cross-pod `agent→pod` registry; empty = in-memory single-replica mode. Supports standalone (`redis://`), sentinel (`redis+sentinel://host?master_name=...&addr=...`), and cluster (`redis+cluster://host?addr=...`) topologies |
| `--peer-bind-address` | — | Bind address of the in-cluster ControllerPeer mTLS listener for cross-pod relay; empty = peer plane disabled |
| `--peer-advertise-address` | — | Address siblings dial to reach this pod's peer listener (written as the registry value); required in cross-pod mode |
| `--peer-tls-cert-path` / `--peer-tls-key-path` / `--peer-tls-ca-path` | — | mTLS material for the ControllerPeer plane (controller is both server and client) |
| `--peer-tls-server-name` | — | Server name to verify a sibling against when relaying; empty = the dialed peer host |

> `--redis-url` and `--peer-bind-address` are the two halves of one cross-pod switch: set both (plus `--peer-advertise-address`) or neither. The controller refuses to start half-configured.

The controller also inherits the chassis HTTP server flags (`--http-bind-address`, `--tls-cert-path`, `--tls-key-path`) for its **default listener**. Leave the default listener's cert/key empty to serve plaintext h2c for in-cluster probe and metrics traffic; the mTLS gRPC listener above is configured separately and exposed through the ingress.

---

## Deployment Shape

### hostlink-controller (cloud)

- **Form:** a Kubernetes **StatefulSet** (chart default `replicaCount: 1`). The chart at `charts/hostlink/` (`helm install <release> charts/hostlink`) renders a ConfigMap holding `/etc/humble-mun/controller.yaml`, the StatefulSet, a load-balanced ClusterIP Service (gRPC + in-cluster HTTP ports), a **headless `<release>-peer` Service** for stable per-pod DNS, and — when `ingress.host` is set — the agent-facing gRPC Ingress. **For HA, set `replicaCount > 1`, which requires `redis.url` + `peer.enabled`** (the chart fails the install otherwise — a half-configured multi-replica controller would silently 404 for agents held by sibling pods).
- **Three listeners** (the chassis HTTP/2 server multiplexes gRPC and Gin onto each listener — `Content-Type: application/grpc` with HTTP/2 routes to the gRPC server, everything else to Gin):
  1. an **mTLS gRPC listener** (`--grpc-bind-address` + `WithTLSCert` + `WithMTLS`) that agents dial out to; exposed externally through the ingress.
  2. a **plaintext (h2c) default listener**, bound in-cluster only, serving the REST API (`/api/v1/...`), `/metrics`, `/probe`, `/version`, `/logging`.
  3. (when `peer.enabled`) an **in-cluster ControllerPeer mTLS listener** on its own `grpc.Server` — separate from the shared chassis server so the relay plane is never reachable from the agent-facing/ingress listener.
- **Ingress (L4 LoadBalancer):** the mTLS gRPC port for agent dial-out. Because the controller terminates mTLS itself, the Ingress MUST do L4/TLS **passthrough** (asserted explicitly via controller-specific annotations) — terminating TLS would strip the agent client certificate and break the identity model. A **reserved TCP port range** (e.g. `1025–2025`) for tunnel exposure is design-only and the chart does not open it yet.
- **Dependencies:** **Redis** backs the `agent→pod` registry (optional single-replica, required for HA); the cross-pod relay of API requests rides the ControllerPeer plane. The `port:<P>` map + port allocation and the cross-pod port-forward data plane are still design-only.
- **Certificates:** per plane, from a mounted Secret (`grpc.tlsSecretName` / `peer.tlsSecretName`) or, with `certManager.enabled`, issued per-pod by the **cert-manager CSI driver** from a configured `Issuer`/`ClusterIssuer` (`certManager.issuerKind` / `issuerName`).

> **Bypass note.** The chassis server applies the same handler to every listener, so the plaintext default listener would also accept gRPC. The split relies on **network-layer isolation** — the default listener is reachable only inside the cluster, while agent gRPC is exposed solely through the mTLS listener via the ingress.

### hostlink (external host)

- **Form:** a static Go binary (`/usr/local/bin/hostlink`) running as a **systemd service**. The unit and an example config ship in `deploy/` (`deploy/hostlink.service`, `deploy/hostlink.yaml`).
- **Configuration:** the agent reads all settings from `/etc/humble-mun/hostlink.yaml` (chassis viper `SetConfigName("hostlink")` + `AddConfigPath("/etc/humble-mun")`; YAML keys are the flag names verbatim, each overridable by an `HM_*` env var). The systemd unit passes **no** command-line flags, and viper `WatchConfig` reloads the file at runtime, so changing config needs neither `systemctl daemon-reload` nor a unit edit.
- **Behavior:** dials out to the controller's public gRPC endpoint over mTLS and runs the `Control` stream (`Hello` + periodic `Heartbeat`), reconnecting **in-process** with exponential backoff + jitter (and HTTP/2 keepalive) so a controller redeploy is ridden out without a process restart. It serves controller-pushed `AgentRequest`s: the Docker **images** methods (`images.list` / `images.pull` / `images.remove`, via a lazy `client.FromEnv` Docker client) and the **filesystem** methods (`fs.stat` / `fs.list` / `fs.read` / `fs.write` / `fs.mkdir` / `fs.remove`) over the configured `--data-dir` working directory. Container *lifecycle* ops (`DockerOp`/events), carrying tunnels, and the node_exporter sidecar on `127.0.0.1:9100` are planned (see [Roadmap](#roadmap)). Because the Docker client is lazy, the unit treats `docker.service` as a soft (`Wants`) ordering dependency, not a hard requirement.
- **Network:** behind NAT; only needs **outbound** reachability to the controller.

---

## Security Model

This is a GPU platform; hosts may run **untrusted customer code**. Exposing ports — and, in the future, giving the cloud L3 reach into containers — significantly expands the attack surface.

- **Agent identity (connection layer):** mTLS, TLS 1.3 minimum, no insecure fallback. This is how agents authenticate to the controller and vice versa. The plaintext default listener (probe / metrics) must stay **in-cluster only**, since the shared chassis handler would otherwise accept gRPC there too.
- **Default-deny:** whitelist directions and ports, and audit. The ACL story must be thought through *before* any VPN / L3 capability is introduced.

---

## Hazards and Hard Constraints

Items below are **non-negotiable** — do not deviate during implementation.

- **mTLS, no fallback** — agent↔controller is always mutual TLS 1.3+. The default listener stays in-cluster only.
- **No TCP-over-TCP** — the L4 proxy terminates TCP and carries bytes; do **not** switch to L3 packet tunneling over TCP.
- **Half-close is explicit** — forwarding must do chunking + explicit `HALF_CLOSE` / `RESET` frames + backpressure. Verify `openconfig/grpctunnel` preserves half-close before integrating.
- **Never concatenate expositions** — merge metrics at the `MetricFamily` level; per-agent scrape deadline < `scrape_timeout`; skip slow agents.
- **Reject stale forwards** — a wrong-pod forward must be rejected and retried.
- **One TCP connection, many streams** — never build a sub-mux on top of HTTP/2.
- **Head-of-line blocking is accepted at this scale** — commands, metrics, and forwarded bytes share one TCP connection; on packet loss all streams stall together. If high-throughput traffic (e.g. vLLM) hurts control-plane latency, move the data plane onto its own `ClientConn` (same service/auth/endpoint, separate TCP), and consider QUIC.
- **Port reallocation is visible to clients** — the public port may change after an agent reconnects; document this in product docs.

### Explicitly rejected (do not adopt)

- **KubeEdge / treating hosts as K8s nodes** — the premise is Docker containers, not pods.
- **`jhump/grpctunnel`** — it tunnels gRPC-over-gRPC, not raw TCP. Wrong fit; use `openconfig/grpctunnel`.
- **Self-built tun device + userspace netstack** — only L4 forwarding is needed now; an L3 VPN is a future, WireGuard-family decision.
- **Raft / consensus between replicas** — `agent→pod` is not contended state.
- **`master` / `minion` naming.**

---

## Roadmap

`hostlink` is at an **early stage**. The MVP is being built in dependency order; the checklist below reflects actual implementation status.

### Implemented

- [x] `AgentLink` + `ControllerPeer` proto + generated code
- [x] Agent↔controller connection setup with **mTLS** (TLS 1.3, no insecure fallback), client-side and server-side
- [x] Two-listener controller wiring (in-cluster plaintext default listener + ingress-facing mTLS gRPC listener), plus an optional third in-cluster ControllerPeer mTLS listener
- [x] `Control` stream with `Hello` handshake and periodic `Heartbeat`, with in-process reconnect (exponential backoff + jitter) and HTTP/2 keepalive
- [x] Generic `AgentRequest`/`AgentResult` dispatch envelope + correlation, with a streaming variant (`AgentProgress` frames + a `final` `AgentResult`); Docker **images** endpoints served by an agent-side Docker client: `GET` (`images.list`), `POST` (`images.pull`, streaming SSE progress), `DELETE` (`images.remove`) on `/api/v1/agents/<id>/images`, plus `GET /api/v1/agents`
- [x] Agent **working-directory filesystem** endpoints on `/api/v1/agents/<id>/files`: browse/`fs.stat`+`fs.list`, chunked `fs.read` download, multipart chunked `fs.write` upload (exclusive create + `PUT` overwrite), `fs.mkdir`, recursive `fs.remove`; bounded-memory streaming both directions (`AgentRequestChunk` + cross-pod `ControllerPeer.Upload` relay) with a `--data-dir` working dir and path-traversal guard
- [x] Redis `agent→pod` registry (write/CAD-delete/TTL-refresh; standalone/sentinel/cluster topologies) and cross-pod relay of API requests via `ControllerPeer.Dispatch` + `ControllerPeer.DispatchStream`, with reject-and-retry on a stale mapping
- [x] Helm chart: StatefulSet + headless peer Service; optional Redis, peer plane, and cert-manager CSI cert issuance; multi-replica config guard

### In Progress / Planned (MVP)

- [ ] Container lifecycle ops: `DockerOp` (run/stop/start/rm) execution; `DockerEvent` reporting
- [ ] Metrics: node_exporter sidecar + agent local pull; controller `/metrics` concurrent fan-out + `MetricFamily` merge + `agent_up`
- [ ] Port forwarding: integrate `openconfig/grpctunnel` (verify half-close first); `ExposeRule` / `OpenForward`; `Frame` relay with half-close + backpressure (`Forward` is currently a stub)
- [ ] Multi-replica **port forwarding**: the `port:<P>` map + atomic port-pool allocation, and the cross-pod two-hop relay of the port-forward data plane (the API-request relay already exists)
- [ ] Lifecycle coupling: Docker events drive exposure establish / suspend / re-establish
- [ ] `internal/` decomposition into `{transport, tunnel, registry, metrics, docker, routing}` behind interfaces

### Future

- [ ] VPN / L3 overlay (WireGuard family — Nebula, or Headscale + Tailscale), subnet-router model; the L4 forwarding above then becomes a subset
- [ ] Container egress to cloud-internal private IPs (overlay, or tproxy / SOCKS5)
- [ ] HTTP service exposure via subdomain + wildcard cert + host routing (replaces port-per-exposure)
- [ ] `pause` / `unpause` exposure (suspend/resume without freeing VRAM)
- [ ] Per-container overlay identity / ACLs for stronger isolation

---

## License

See [LICENSE](./LICENSE) and [NOTICE](./NOTICE) for details.
