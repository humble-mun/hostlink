# hostlink

> [中文文档](./README_CN.md)

> ⚠️ **EARLY STAGE — NOT PRODUCTION READY.** Implemented and verified end-to-end:
> the agent↔controller mTLS connection and `Control` stream (handshake +
> heartbeat); a generic request/response dispatch envelope with REST endpoint
> families for **Docker images** (`GET`/`POST`/`DELETE
> /api/v1/agents/<id>/images` — list, pull with streaming SSE progress, and
> remove), **Docker containers** (`/api/v1/agents/<id>/containers` — list,
> create-and-start with docker run semantics, inspect, start/stop/restart,
> remove, and an SSE **log stream** with `?follow`), and an agent
> **working-directory filesystem** (`GET`/`POST`/`PUT`/`DELETE
> /api/v1/agents/<id>/files` — browse, download, streamed multipart upload, and
> recursive delete), served by an agent-side Docker client and a sandboxed data
> directory; a **metrics aggregation** endpoint (`GET /api/v1/metrics`) that
> streams and merges each agent's configured exporters; the Redis-backed
> `agent→pod` registry; and cross-pod API relay via the `ControllerPeer` plane
> (multi-replica HA), including cancel propagation for followed log streams;
> and **raw-TCP port forwarding** (`/api/v1/agents/<id>/forwards` — allocate a
> public port for a container `ip:port`, explicit half-close semantics, a
> cross-pod relay data plane, an all-replica bind activation barrier, and
> Docker-event-driven suspend/resume). Still **designed but not yet
> implemented**: the controller-side log relay into Loki — see
> [Roadmap](#roadmap). Do not deploy this as a control plane for real
> workloads yet.
>
> Released as **v0.x pre-releases**: the REST API, wire protocol, and
> configuration may change without notice until v1.0.0.

A control plane for managing Linux hosts that live **outside the cloud** (on-prem / colo servers). These hosts are **not** Kubernetes nodes; their workloads run as **Docker containers**, not pods. `hostlink` gives the cloud a persistent, mutually-authenticated channel to each NAT'd host for command dispatch, container lifecycle, metrics, and raw-TCP port forwarding — with no inbound connectivity required to the host.

**Scale assumptions.** Roughly a dozen agents, a few dozen at most. The design favors *controllable, debuggable, good-enough* over web-scale; do not over-engineer for massive fleets.

## Table of Contents

- [Overview](#overview)
- [How It Works](#how-it-works)
- [Architecture](#architecture)
  - [Connection Model](#connection-model)
  - [Command Channel](#command-channel)
  - [Metrics Reverse Fan-out](#metrics-reverse-fan-out)
  - [Container Logs](#container-logs)
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

`hostlink` builds two binaries from a single Go module — **both named `hostlink`**, disambiguated by where they run:

- the **controller** — runs in the cloud, inside Kubernetes, as a container (image `hostlink`). The control plane. It is the gRPC **server**, terminates mutual TLS for agents, aggregates metrics, allocates public ports, and routes forwarded TCP across replicas.
- the **agent** — runs on each external host as a systemd service (binary `/usr/local/bin/hostlink`). It is the gRPC **client**: it dials out to the controller (so it works behind NAT with no inbound firewall rules), drives the local Docker daemon, reports metrics, and carries tunnels.

> **Naming.** The former `hostlink-controller` name is retired: the controller only ever runs as a container inside Kubernetes and the agent is the only hostlink piece installed on a host, so a bare `hostlink` is unambiguous in each context. Docs use the role names "controller"/"agent"; both binaries read `/etc/humble-mun/hostlink.yaml`.

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
  hostlink (agent)                  hostlink (controller, ≥2 replicas)
       │                                          │
       │── dial out (mTLS, TLS 1.3) ────────────► │  one gRPC connection per agent
       │                                          │
       │── Control(stream AgentEvent) ──────────► │  long-lived bidi stream
       │     Hello (handshake) ─────────────────► │
       │     Heartbeat (refresh TTL) ───────────► │
       │                                          │
       │ ◄──────────── Command (push) ─────────── │  AgentRequest (images/containers/fs/…) / OpenForward / ExposeRule
       │                                          │
       │── Forward(stream Frame) ───────────────► │  one stream per forwarded TCP conn
       │     OPEN / DATA / HALF_CLOSE / RESET    │  (opened by the agent on demand)
       │                                          │
```

One TCP connection, many HTTP/2 streams. The agent opens the `Control` stream once after connecting; the controller pushes commands down its response direction. Each forwarded public connection gets its own `Forward` stream so gRPC's per-stream flow control gives natural backpressure.

> **What is implemented today:** the mTLS dial, the `Control` stream and `Hello` / `Heartbeat` exchange; the generic `AgentRequest`/`AgentResult` dispatch envelope with the Docker **images** endpoints — `GET` (list), `POST` (pull, streaming SSE progress), and `DELETE` (remove) on `/api/v1/agents/<id>/images`; the Docker **containers** endpoints on `/api/v1/agents/<id>/containers` — list, create-and-start (docker run semantics), inspect, start/stop/restart, remove, and an SSE **log stream** (`.../logs`, `?follow` supported, client disconnect cancels the agent-side stream via `request.cancel`); the agent **working-directory filesystem** API (`GET`/`POST`/`PUT`/`DELETE /api/v1/agents/<id>/files`); the **metrics aggregation** endpoint (`GET /api/v1/metrics`) that reverse-pulls, labels, and merges each agent's configured exporters; the Redis `agent→pod` registry; cross-pod relay of API requests via `ControllerPeer.Dispatch` (unary), `ControllerPeer.DispatchStream` (streaming), and `ControllerPeer.Upload` (client-streaming, for filesystem writes); and **port forwarding** end to end — the `forwards` REST API allocating public ports from `--forward-port-range`, per-port TCP listeners reconciled on every replica, the `OpenForward` reverse-open handshake with the `pkg/tunnel` half-close-correct byte pipe, the cross-pod `ControllerPeer.Forward` relay with stale-holder retry, the all-replica bind **activation barrier** (`pending`/`active`/`suspended`), and Docker **event**-driven suspend/resume of exposures. The controller-side Loki log relay is not yet implemented. See [Roadmap](#roadmap).

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

On connect, the agent opens `Control(stream AgentEvent) returns (stream Command)`. The controller pushes commands down the `Command` stream; the agent reports its handshake, heartbeats, and Docker events up the `AgentEvent` stream.

Most controller→agent work rides the generic **`AgentRequest`** envelope: `method` names the operation (`images.*`, `containers.*`, `fs.*`, `metrics.scrape`), `payload` is method-specific JSON, and the reply (`AgentResult`, plus `AgentProgress` frames for streaming methods) is correlated by `request_id`. A fire-and-forget `request.cancel` meta method cancels an in-flight streaming request — this is what tears down a followed log stream when its HTTP client goes away. Container lifecycle is implemented through this plane (the `containers.*` methods); the `DockerOp` command in the proto is an unused legacy placeholder. `OpenForward` sits outside the envelope: it tells the agent to dial a target and open a `Forward` stream (see [Port Forwarding](#port-forwarding)). `ExposeRule` remains an unused placeholder.

### Metrics: two endpoints, two concerns

Prometheus stays in **pull mode**. The controller exposes **two** distinct endpoints (the in-cluster plaintext listener serves both):

- **`/metrics`** (chassis-owned) — the controller's **own** process metrics.
- **`GET /api/v1/metrics`** — the **cloud-side aggregation of every agent's upstream exporters**. This is the reverse fan-out.

On a scrape of `/api/v1/metrics`, the controller:

1. enumerates the online fleet (the Redis `agent→pod` directory; in-memory mode = locally-held agents) and, with **bounded concurrency**, dispatches a streaming `metrics.scrape` to each agent — over its local `Control` stream, or relayed to the holding pod via `ControllerPeer.DispatchStream`;
2. each agent pulls its configured `scrape-targets` (node_exporter, dcgm-exporter, …) locally and **streams** each exposition back in chunks, so agent memory stays bounded and no single-message size limit applies;
3. gives each agent an **independent deadline shorter than `scrape_timeout`** (`--agent-scrape-timeout`, default 5s vs the 10s default); a slow / offline agent is skipped and contributes only `agent_up 0`;
4. **merges by `MetricFamily`** — parse each exposition with `expfmt.NewTextParser(model.UTF8Validation)`, inject `agent=<id>` + `exporter=<name>` labels into every series, fold series sharing a metric name under one family (one HELP/TYPE), then encode once;
5. synthesizes `agent_up{agent="<id>"}` (1 = the stream completed this round, 0 = offline / timed out) and `hostlink_scrape_target_up{agent,exporter}` (per-exporter health). `agent_up` is the only clean "an agent went down" signal, since Prometheus' native `up` only reflects the controller endpoint.

> **Constraint:** **never string-concatenate expositions.** Duplicate HELP/TYPE lines for the same metric name make the Prometheus parser reject the entire payload. Merge at the `MetricFamily` level.

> The `exporter` label (not `job` / `instance`) is used deliberately so the injected labels survive **without** requiring `honor_labels` in the Prometheus scrape config. Exporters run as **separate sidecar binaries**; the agent GETs them locally (e.g. `127.0.0.1:9100`). Do **not** import node_exporter as a library — its collector package is not a stable public API.

### Container Logs

`GET /api/v1/agents/<id>/containers/<containerId>/logs` streams a container's logs as **SSE**: one `data:` event per log line (`{"stream":"stdout"|"stderr","line":"..."}`), terminated by `{"done":true,...}`. Query parameters mirror `docker logs`: `follow` keeps the stream open for new output, `tail` bounds the initial backlog, `since` bounds the start time, `timestamps` prefixes each line. The agent inspects the container's TTY mode to pick the right wire format (raw vs `stdcopy`-multiplexed), demuxes stdout/stderr, and emits line-framed `AgentProgress` events over the **lossless** stream class (no dropped lines, backpressure applies).

A followed stream is unbounded, so teardown is explicit: when the HTTP client disconnects (or the request is otherwise abandoned), the controller sends a fire-and-forget `request.cancel` down the agent's `Control` stream, and the agent cancels the underlying `docker logs` follow. This propagates across pods — cancelling the `ControllerPeer.DispatchStream` relay makes the holding pod issue the cancel.

> **Log collection into Loki (decided, not yet implemented).** Agent hosts sit on a **restricted network whose only permitted path is the agent→controller mTLS connection**, so per-host shippers (Alloy/promtail pushing to Loki directly) are not an option. The decided design relays logs through the controller: a controller-side log-relay follows `containers.logs` for the containers of each agent it holds, stamps `{agent_id, container_name}` labels, and pushes lines to the in-cluster Alloy `loki.source.api` endpoint (Alloy owns batching/retry/relabel into Loki). Open points: resume via `since` after reconnect, relay ownership following agent reconnects across replicas, and bandwidth budgeting on the shared Control stream. See [Roadmap](#roadmap).

### Port Forwarding

Goal: dynamically expose a container's internal port (e.g. vLLM on `:8080`) to a public cloud port (e.g. `:1025`) over **raw TCP** with half-close support. The data plane is **L4 stream proxying** (terminate TCP at each hop, carry only application bytes) — which inherently avoids TCP-over-TCP degradation.

Because the server cannot open a stream to the agent, the stream-open handshake is reversed:

1. A public connection lands on the controller's exposed port.
2. The controller pushes `OpenForward{session_id, target}` down that agent's `Control` stream.
3. The agent **opens** a `Forward` stream whose first frame is a typed `OPEN` frame carrying `session_id` (on a failed local dial it sends `RESET` with the `session_id` instead).
4. The controller **pairs** the public connection with that `Forward` stream by `session_id` and relays bidirectionally.

This handshake + chunking + session pairing is implemented in-repo: `pkg/tunnel` is the byte pipe, and the controller pairs streams by `session_id`. [`openconfig/grpctunnel`](https://github.com/openconfig/grpctunnel) was evaluated and dropped in favor of this small, fully-tested implementation.

> **Constraint (half-close, the correctness crux):** a gRPC stream's own lifecycle (`CloseSend` / handler return) cannot represent TCP's independent per-direction half-close. It is modeled with **explicit frame types**: local EOF → send `HALF_CLOSE` and stop sending that direction but keep reading the other; receive `HALF_CLOSE` → `CloseWrite()` the local socket; `RESET` → `SetLinger(0)` + `Close()`. `pkg/tunnel` implements exactly this contract and its half-close semantics are locked by tests — keep them intact when touching it.

> **Constraint (backpressure):** never read from the local socket faster than you can `Send`. Rely on `Send` blocking when the HTTP/2 flow-control window is full. Do not buffer without bound.

> **Implementation status: implemented end to end.** `--forward-port-range` (chart: `portForward.range`) reserves the public pool; every replica binds every exposed port and reconciles its listeners from the shared store (5s tick + a pub/sub nudge); a suspended exposure answers new connections with an immediate RST.

### Multi-replica Affinity

The controller runs ≥2 replicas for HA. An agent's connection is pinned to the one pod it dialed, but public TCP arriving through the L4 load balancer lands on **any** replica — usually not the one holding that agent.

- **Routing key:** for raw TCP the only ingress key is the **destination port**. Each exposure = one distinct public port.
- **Registry (Redis dual maps):** `hostlink:port:<P>` → JSON `{agent_id, target, container_id?, suspended?}` (no TTL — an exposure survives agent churn until deleted); `hostlink:agent:<id>` → `holding_pod` (written when the agent attaches, **TTL refreshed by heartbeat**, deleted on disconnect).
- **Atomic port allocation:** free-port candidates are found with batched `MGET` over the reserved range, then claimed atomically via `SETNX` (in-memory store in single-replica mode).
- **Routing flow:** a public connection reaches pod B on port P → B looks up `port:P` → agentX, then `agent:agentX` → podA. If `podA == B`, B drives the reverse-open directly; otherwise B opens `ControllerPeer.Forward` to podA, first frame `OPEN{session_id, open:{agent_id, target}}` — podA pairs it with the agent's `Forward` stream and answers `READY` **before** B reads any public bytes (retry-before-read). Two byte-pipe hops in series; half-close propagates end to end.
- **Stale window:** because the LB spreads connections, cross-pod forwarding is the **common** case (hit rate ≈ 1/N). A pod that receives "forwarded to me but I don't hold agentX" **rejects** with `FAILED_PRECONDITION`; the caller re-resolves the holder once and retries.
- **Activation barrier:** kube-proxy does not retry a connection-refused backend, so a Service selecting all controller pods must not include port P until **every** live replica has bound it. Each replica reports `hostlink:bound:<P>:<pod>` (30s TTL) alongside a liveness key `hostlink:controller:<pod>` (45s TTL); a forward is `active` only when all live pods report the bind, `pending` otherwise, and `suspended` while its container is down.

> Redis is already in the stack. Raft/gossip/mesh were evaluated and rejected: `agent→pod` is not contended state and needs no consensus.

### Container Lifecycle

"Power off / on" = `docker stop` / `docker start`. Docker containers are **stateful pets**: stop is SIGTERM → grace → SIGKILL, the writable layer is preserved on local disk, and killing the process frees the GPU; start brings back the same container ID with state intact. On plain Docker this is free — no K8s-style upperdir persistence needed.

Exposure rules are tied to lifecycle via `client.Events()`:

- on `stop` / `die` → **suspend** that container's exposures: the mapping is marked `suspended`, the port and listeners are retained, and new connections get an immediate RST.
- on `start` → re-inspect the container, re-resolve its network IP, rewrite the mapping target, and clear the suspension — the public port does not change.

> The container IP can change after restart; if the port is re-allocated by the new holder, the **public port changes and clients must reconnect** — an accepted design tradeoff. For GPU containers, remember the nvidia runtime; `docker pause` / `unpause` (freezer cgroup, keeps RAM/VRAM) is a distinct "suspend, resume instantly" semantic.

---

## Wire Protocol

The services are defined in `pkg/api/hostlink/v1/`. `AgentLink` (agent↔controller) has two bidirectional-stream RPCs; `ControllerPeer` (controller↔controller) has three request-relay RPCs (unary, server-streaming, and client-streaming) plus a fourth bidirectional `Forward` RPC for the cross-pod port-forward data plane:

```proto
service AgentLink {
  // Opened once after the agent connects; the controller pushes commands down
  // the response stream. Under HTTP/2 the server cannot initiate an RPC to the
  // client, so all server->agent commands travel over this already-open stream.
  rpc Control(stream AgentEvent) returns (stream Command);

  // One per forwarded public TCP connection; opened by the agent. The first
  // frame is a typed OPEN frame carrying session_id for pairing (or RESET with
  // the session_id when the agent's local dial fails). Cross-pod forwarding
  // uses ControllerPeer.Forward, not this RPC.
  rpc Forward(stream Frame) returns (stream Frame);
}

service ControllerPeer {
  // Cross-pod relay: a replica that does not hold the target agent forwards the
  // request to the holding pod (resolved via the Redis agent->pod map). A stale
  // holder rejects with FAILED_PRECONDITION so the caller re-resolves and retries.
  rpc Dispatch(DispatchRequest) returns (AgentResult);

  // Streaming variant for long-running ops (e.g. image pull, container logs):
  // each streamed AgentResult is a progress frame except the last, which has
  // final=true and carries the terminal code/payload/error.
  rpc DispatchStream(DispatchRequest) returns (stream AgentResult);

  // Client-streaming relay of a controller->agent upload (e.g. fs.write): the
  // first UploadFrame carries the open (DispatchRequest), the rest body chunks.
  rpc Upload(stream UploadFrame) returns (AgentResult);

  // Cross-pod data plane for port forwarding: the pod that accepted the public
  // TCP connection opens this stream to the pod holding the agent. The first
  // frame is OPEN with session_id + open{agent_id, target}; the holder pairs it
  // with the agent's Forward stream and answers READY before any public bytes
  // flow (retry-before-read). A stale holder rejects with FAILED_PRECONDITION
  // so the caller re-resolves and retries.
  rpc Forward(stream Frame) returns (stream Frame);
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
| `Frame{session_id, type, data, open}` | both (per forward) | Raw TCP bytes; `type` ∈ `DATA` / `HALF_CLOSE` / `RESET` / `OPEN` / `READY`. The first frame of a stream is `OPEN` carrying `session_id` (`READY` acks the peer hop; `RESET` as a first frame reports a failed dial) |
| `PeerForwardOpen{agent_id, target}` | controller → controller | Routing payload on the first `OPEN` frame of a `ControllerPeer.Forward` relay: which agent to reverse-open and which `ip:port` it should dial |

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
| `GET /api/v1/agents/<agentId>/containers` | List the containers on the agent (`containers.list`). `?all=true` includes stopped containers (docker ps -a). |
| `POST /api/v1/agents/<agentId>/containers` | Create **and start** a container (docker run semantics, `containers.create`). JSON body: `image` (required, must already be pulled), plus optional `name`, `cmd`, `entrypoint`, `env` (`KEY=value` strings), `workingDir`, `labels`, `ports` (`{containerPort, protocol?, hostIp?, hostPort?}`), `binds` (`/host:/container[:opts]`), `networkMode` / `pidMode` (e.g. `"host"`), `restartPolicy` (+ `maxRetryCount`), `autoRemove`. Returns `201` with `{"id":"...","warnings":[...]}`. |
| `GET /api/v1/agents/<agentId>/containers/<containerId>` | Inspect a container (`containers.inspect`): identity, state, and the effective run configuration. `404` for an unknown container. |
| `GET /api/v1/agents/<agentId>/containers/<containerId>/logs` | Stream the container's logs as **SSE** (`containers.logs`): one event per line `{"stream","line"}`, terminated by `{"done":true,...}`. Options mirror docker logs: `?follow=true`, `?tail=100`, `?since=<RFC3339|unix>`, `?timestamps=true`. Client disconnect cancels the agent-side stream (`request.cancel`), including across pods. |
| `POST /api/v1/agents/<agentId>/containers/<containerId>/start` | Start a stopped container (`containers.start`). `204` on success. |
| `POST /api/v1/agents/<agentId>/containers/<containerId>/stop` | Stop a running container (`containers.stop`). Optional `?timeout=<seconds>` grace period before the daemon kills it. `204` on success. |
| `POST /api/v1/agents/<agentId>/containers/<containerId>/restart` | Restart a container (`containers.restart`), same `?timeout` as stop. `204` on success. |
| `DELETE /api/v1/agents/<agentId>/containers/<containerId>` | Remove a container (`containers.remove`). `?force=true` removes a running one (docker rm -f), `?volumes=true` also removes its anonymous volumes. `409` when removing a running container without `force`. |
| `GET /api/v1/agents/<agentId>/files?path=<p>` | Browse the agent's working directory (`--data-dir`). A directory returns JSON `{"entries":[{"name","dir","size","modTime"}]}` (non-recursive); a file is **streamed as a download** (`Content-Disposition`), or with `Accept: application/json` returns its `FsEntry` metadata. Empty `path` is the working-dir root. Dispatches `fs.stat` then `fs.list` / `fs.read`. |
| `POST /api/v1/agents/<agentId>/files?path=<p>` | `&dir=true` creates a directory (`fs.mkdir`, `409` if it exists). Otherwise uploads one or more files from a **multipart form**, streamed to the agent in chunks (`fs.write`) and created **exclusively** — an existing target is reported per-file. Response `{"written":[...],"errors":[...]}`. |
| `PUT /api/v1/agents/<agentId>/files?path=<p>` | Overwrite a single file with the request body (raw bytes, or the first multipart part); streamed via `fs.write` (truncate). |
| `DELETE /api/v1/agents/<agentId>/files?path=<p>` | Remove the path, **recursively** for directories (`fs.remove`). |
| `POST /api/v1/agents/<agentId>/forwards` | Allocate a public port that forwards raw TCP to a container `ip:port` on the agent. JSON body `{"target":"<ip:port>","container_id":"..."}` (`container_id` optional, ties the forward to container lifecycle events). Returns `201` with the mapping incl. `port` and `state` (starts `pending`; poll until `active` before wiring external infrastructure to it). `409` when the reserved range is exhausted, `503` when `--forward-port-range` is unset. |
| `GET /api/v1/agents/<agentId>/forwards` | List the agent's port forwards, each with its `state`: `pending` (not yet bound on every live replica), `active` (safe to expose), or `suspended` (container down; connections are refused). |
| `GET /api/v1/forwards` | List all port forwards across agents. |
| `DELETE /api/v1/forwards/<port>` | Release a forwarded public port. `204` on success, `404` for an unknown port. |

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
| Redis | The `agent→pod` registry + the port-forward exposure map — optional for a single replica, **required for HA** (`replicaCount > 1`) |
| cert-manager + CSI driver | Optional — issues controller/peer certs per-pod instead of mounting Secrets |
| node_exporter | Sidecar on each host, listening on `127.0.0.1:9100` |

This project is **Linux-only** (it manages a Linux Docker daemon and uses Linux-specific socket semantics for forwarding). It does not build or run natively on Windows; build inside a Linux toolchain.

### Build

The project is a single Go module with one binary per `cmd/` subdirectory, and it is **vendored** — build offline with `-mod=vendor`:

```bash
go build -mod=vendor -o bin/controller/hostlink ./cmd/controller
go build -mod=vendor -o bin/agent/hostlink      ./cmd/agent
```

> Both binaries are named `hostlink` (separate output directories keep local builds apart); in production the controller ships as the `hostlink` container image (`make build`) and the agent as the `hostlink` host binary (`make agent`).

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

**Certificate hot-reload.** All TLS material (server/client certificates, keys, and CA bundles) is reloaded from disk transparently when the file rotates in place — the new material takes effect on the next handshake with **no process restart**. This covers the agent-facing gRPC listener, the ControllerPeer plane (both its server and client sides), and the chassis default listener. As a result, short-lived certificates issued by **cert-manager** (or the cert-manager CSI driver) are picked up automatically as they are renewed; a long-running controller or agent never serves a stale, expired certificate. Reload failures (e.g. a transiently missing or malformed file) are logged and the previously loaded material continues to be served.

---

## CLI Flags

All flags can also be set via environment variables: uppercase the flag, replace `-` with `_`, and prefix with `HM_` (e.g. `--controller-endpoint` → `HM_CONTROLLER_ENDPOINT`). Config may also be supplied as a YAML file at `/etc/humble-mun/hostlink.yaml` (the name follows the binary's `version.Name`, which is `hostlink` for both the controller and the agent), with the flag names as keys; the file is watched and reloaded at runtime. Precedence: flags > env > config file.

### Agent flags

| Flag | Default | Description |
|------|---------|-------------|
| `--controller-endpoint` | — | Address of the controller's gRPC endpoint to dial, as `host:port` (required) |
| `--agent-tls-cert-path` | — | Client certificate the agent presents to the controller for mTLS |
| `--agent-tls-key-path` | — | Private key matching the client certificate |
| `--controller-tls-ca-path` | — | CA bundle used to verify the controller's certificate |
| `--controller-tls-server-name` | — | Server name to verify against the controller's certificate; if empty, gRPC verifies against the dial endpoint's host, so set it explicitly when the cert SAN differs from the dial address |
| `--data-dir` | — | Working directory served by the `files` API (browse/download/upload/delete). Empty disables the `files` API |

> `scrape-targets` (the agent's upstream exporter list for the metrics fan-out) is a structured YAML list and is configured via the config file only — see `deploy/hostlink.yaml` for the documented shape.

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
| `--agent-scrape-timeout` | `5s` | Per-agent deadline for the `GET /api/v1/metrics` fan-out; keep it below the Prometheus `scrape_timeout` |
| `--grpc-max-recv-msg-size` | 16 MiB | Ceiling for a single gRPC message received from an agent or sibling (large unary results; streaming methods chunk and are unaffected) |
| `--forward-port-range` | — | Public TCP port range reserved for port forwarding, as `from-to` (e.g. `1025-2025`) or a single port. Every replica binds every allocated port in this range. Empty = port forwarding disabled (the `forwards` API returns 503) |

> `--redis-url` and `--peer-bind-address` are the two halves of one cross-pod switch: set both (plus `--peer-advertise-address`) or neither. The controller refuses to start half-configured.

The controller also inherits the chassis HTTP server flags (`--http-bind-address`, `--tls-cert-path`, `--tls-key-path`) for its **default listener**. Leave the default listener's cert/key empty to serve plaintext h2c for in-cluster probe and metrics traffic; the mTLS gRPC listener above is configured separately and exposed through the ingress.

---

## Deployment Shape

### Controller (cloud; container image `hostlink`)

- **Form:** a Kubernetes **StatefulSet** (chart default `replicaCount: 1`). The chart at `charts/hostlink/` (`helm install <release> charts/hostlink`) renders a ConfigMap holding `/etc/humble-mun/hostlink.yaml`, the StatefulSet, a load-balanced ClusterIP Service (gRPC + in-cluster HTTP ports), a **headless `<release>-peer` Service** for stable per-pod DNS, and — when `ingress.host` is set — the agent-facing gRPC Ingress. **For HA, set `replicaCount > 1`, which requires `redis.url` + `peer.enabled`** (the chart fails the install otherwise — a half-configured multi-replica controller would silently 404 for agents held by sibling pods).
- **Three listeners** (the chassis HTTP/2 server multiplexes gRPC and Gin onto each listener — `Content-Type: application/grpc` with HTTP/2 routes to the gRPC server, everything else to Gin):
  1. an **mTLS gRPC listener** (`--grpc-bind-address` + `WithTLSCert` + `WithMTLS`) that agents dial out to; exposed externally through the ingress.
  2. a **plaintext (h2c) default listener**, bound in-cluster only, serving the REST API (`/api/v1/...`, including the aggregated agent metrics at `/api/v1/metrics`), the controller's own `/metrics`, `/probe`, `/version`, `/logging`.
  3. (when `peer.enabled`) an **in-cluster ControllerPeer mTLS listener** on its own `grpc.Server` — separate from the shared chassis server so the relay plane is never reachable from the agent-facing/ingress listener.
- **Ingress (L4 LoadBalancer):** the mTLS gRPC port for agent dial-out. Because the controller terminates mTLS itself, the Ingress MUST do L4/TLS **passthrough** (asserted explicitly via controller-specific annotations) — terminating TLS would strip the agent client certificate and break the identity model. The reserved TCP port range for tunnel exposure is configured via `portForward.range`; with `portForward.service.enabled` the chart renders a dedicated `<release>-forward` Service that enumerates every port in the range (Kubernetes Service ports are a list, not a range — mind your cloud LB listener quotas).
- **Dependencies:** **Redis** backs the `agent→pod` registry, the `port:<P>` exposure map, and the bind/liveness keys behind the forward activation barrier (optional single-replica, required for HA); the cross-pod relay of API requests rides the ControllerPeer plane, and the cross-pod port-forward data plane rides `ControllerPeer.Forward`.
- **Certificates:** per plane, from a mounted Secret (`grpc.tlsSecretName` / `peer.tlsSecretName`) or, with `certManager.enabled`, issued per-pod by the **cert-manager CSI driver** from a configured `Issuer`/`ClusterIssuer` (`certManager.issuerKind` / `issuerName`).

> **Bypass note.** The chassis server applies the same handler to every listener, so the plaintext default listener would also accept gRPC. The split relies on **network-layer isolation** — the default listener is reachable only inside the cluster, while agent gRPC is exposed solely through the mTLS listener via the ingress.

#### Scraping the metrics

The controller exposes **two** scrape paths on the in-cluster HTTP Service (`http` port): `/metrics` (the controller's own process metrics) and `/api/v1/metrics` (the aggregated agent-exporter fan-out, §4.3). The chart does **not** ship a scrape config — wire it to your monitoring stack the way that stack expects, pointing at the `http` Service port. Examples:

- **Prometheus annotation-based discovery** (one target per path):
  ```yaml
  prometheus.io/scrape: "true"
  prometheus.io/port: "8080"
  prometheus.io/path: "/api/v1/metrics"   # add a second scrape for /metrics
  ```
- **Prometheus Operator `ServiceMonitor`** — two `endpoints` on the `http` port, `path: /metrics` and `path: /api/v1/metrics`.
- **VictoriaMetrics `VMServiceScrape`** — same shape: two `endpoints` (or a `VMPodScrape`) targeting the `http` port and the two paths.

Keep the Prometheus job's `scrape_timeout` **above** the controller's `--agent-scrape-timeout` (default 5s) so the fan-out's per-agent deadline fires first. The `/api/v1/metrics` series already carry `agent`/`exporter` labels, so `honor_labels` is not required.

### Agent (external host; binary `hostlink`)

- **Form:** a static Go binary (`/usr/local/bin/hostlink`) running as a **systemd service**. The unit and an example config ship in `deploy/` (`deploy/hostlink.service`, `deploy/hostlink.yaml`).
- **Configuration:** the agent reads all settings from `/etc/humble-mun/hostlink.yaml` (chassis viper `SetConfigName("hostlink")` + `AddConfigPath("/etc/humble-mun")`; YAML keys are the flag names verbatim, each overridable by an `HM_*` env var). The systemd unit passes **no** command-line flags, and viper `WatchConfig` reloads the file at runtime, so changing config needs neither `systemctl daemon-reload` nor a unit edit.
- **Behavior:** dials out to the controller's public gRPC endpoint over mTLS and runs the `Control` stream (`Hello` + periodic `Heartbeat`), reconnecting **in-process** with exponential backoff + jitter (and HTTP/2 keepalive) so a controller redeploy is ridden out without a process restart. It serves controller-pushed `AgentRequest`s via a lazy `client.FromEnv` Docker client: the Docker **images** methods (`images.list` / `images.pull` / `images.remove`), the **container** methods (`containers.list` / `containers.create` / `containers.inspect` / `containers.start` / `containers.stop` / `containers.restart` / `containers.remove` / `containers.logs`), and the **filesystem** methods (`fs.stat` / `fs.list` / `fs.read` / `fs.write` / `fs.mkdir` / `fs.remove`) over the configured `--data-dir` working directory. It also watches Docker container lifecycle events (`start` / `die` / `stop`) and reports them up the `Control` stream (driving exposure suspend/resume), and carries forwarded TCP: on `OpenForward` it dials the target and opens a `Forward` stream. The node_exporter sidecar on `127.0.0.1:9100` is a deployment expectation for the metrics fan-out. Because the Docker client is lazy, the unit treats `docker.service` as a soft (`Wants`) ordering dependency, not a hard requirement.
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
- **Half-close is explicit** — forwarding does chunking + explicit `HALF_CLOSE` / `RESET` frames + backpressure. `pkg/tunnel` implements this contract and tests lock it; keep the semantics intact when touching it.
- **Never concatenate expositions** — merge metrics at the `MetricFamily` level; per-agent scrape deadline < `scrape_timeout`; skip slow agents.
- **Reject stale forwards** — a wrong-pod forward must be rejected and retried.
- **One TCP connection, many streams** — never build a sub-mux on top of HTTP/2.
- **Head-of-line blocking is accepted at this scale** — commands, metrics, and forwarded bytes share one TCP connection; on packet loss all streams stall together. If high-throughput traffic (e.g. vLLM) hurts control-plane latency, move the data plane onto its own `ClientConn` (same service/auth/endpoint, separate TCP), and consider QUIC.
- **Port reallocation is visible to clients** — the public port may change after an agent reconnects; document this in product docs.

### Explicitly rejected (do not adopt)

- **KubeEdge / treating hosts as K8s nodes** — the premise is Docker containers, not pods.
- **`jhump/grpctunnel`** — it tunnels gRPC-over-gRPC, not raw TCP. Wrong fit. (`openconfig/grpctunnel` was evaluated too and dropped in favor of the small in-repo `pkg/tunnel` pipe with explicit half-close.)
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
- [x] Metrics aggregation: agent `scrape-targets` (node_exporter / dcgm / …) pulled on demand and **streamed** back; controller `GET /api/v1/metrics` bounded-concurrency reverse fan-out + `agent`/`exporter` label injection + `MetricFamily` merge + synthesized `agent_up` / `hostlink_scrape_target_up`, cross-pod aware via `ControllerPeer.DispatchStream`
- [x] Container CRUD + lifecycle on `/api/v1/agents/<id>/containers` (`containers.list` / `containers.create` with docker run semantics incl. ports, binds, `--net`/`--pid` host modes, restart policy / `containers.inspect` / `containers.start` / `containers.stop` / `containers.restart` / `containers.remove`)
- [x] Container **log streaming** (`.../containers/<id>/logs`, SSE, `?follow`/`?tail`/`?since`/`?timestamps`): TTY-aware stdout/stderr demux, line framing, lossless delivery; plus the `request.cancel` meta method so an abandoned followed stream is torn down on the agent, propagated across pods
- [x] Port forwarding end to end: `--forward-port-range` port pool (Redis `SETNX` allocation, in-memory single-replica mode) + the `forwards` REST API; per-port TCP listeners reconciled on **every** replica; the `OpenForward` reverse-open handshake and the `pkg/tunnel` byte pipe with explicit half-close (`OPEN` / `READY` / `DATA` / `HALF_CLOSE` / `RESET`); cross-pod two-hop relay via `ControllerPeer.Forward` with stale-holder reject-and-retry; the all-replica bind **activation barrier** surfacing `pending` / `active` / `suspended` per forward; chart `portForward.range` + optional enumerated `<release>-forward` Service
- [x] Docker **event** reporting (`DockerEvent`: container `start` / `die` / `stop`) driving exposure lifecycle — suspend on `die` / `stop` (fast RST, port retained), re-resolve the container IP and resume on `start`

### In Progress / Planned (MVP)

- [ ] Controller-side **log relay into Loki** ("Plan B" — agents can only reach the controller, so a relay follows `containers.logs` per held agent, labels `{agent_id, container_name}`, and pushes to in-cluster Alloy `loki.source.api`; needs `since`-based resume, ownership follow-on-reconnect, and Control-stream bandwidth budgeting)
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
