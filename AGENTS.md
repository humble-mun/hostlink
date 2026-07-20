# AGENTS.md — hostlink

> This file is the development reference for the `hostlink` project, intended for AI coding tools (Claude Code, etc.) and human contributors. It captures the technology choices and their rationale, the architecture and data flows, the wire protocol, the current implementation scope, known hazards and hard constraints, future capabilities, and the deployment shape. **Items marked "Constraint" or "Do not" are non-negotiable — do not deviate from them during implementation.**

---

## 1. Overview and scope

`hostlink` manages Linux hosts that live **outside the cloud** (on-prem / colo servers). These hosts are **not Kubernetes nodes**; their workloads run as **Docker containers**, not K8s pods.

It builds two binaries, **both named `hostlink`** — the deployment context disambiguates them:

- the **controller** — runs in the cloud as a container (image `hostlink`, inside Kubernetes). The control plane.
- the **agent** — a static host binary (`/usr/local/bin/hostlink`) on each external host (behind NAT). Executes commands, reports metrics, carries tunnels.

> **Naming.** The former `hostlink-controller` binary/image name is retired: the controller only ever runs as a container inside Kubernetes and the agent is the only hostlink piece installed on a host, so in each context a bare `hostlink` is unambiguous. Prose and code keep the *role* names "controller"/"agent". Both binaries carry `version.Name=hostlink` and therefore both read `/etc/humble-mun/hostlink.yaml`.

**Scale assumptions:** roughly a dozen agents, a few dozen at most. Many choices favor "good enough, controllable, debuggable." Do not over-engineer for massive scale.

**Out of scope:** billing, provisioning, quotas, and other business logic (these belong to the larger Smoothcloud platform). This project is the infrastructure layer only: connectivity, container orchestration, metrics, and port forwarding.

---

## 2. Project layout and build

A single Go module; one binary per subdirectory under `cmd/` (the standard Go layout for multiple binaries):

```
github.com/humble-mun/hostlink
├── cmd/
│   ├── controller/   main.go    # builds the controller (cloud; container image `hostlink`)
│   └── agent/        main.go    # builds the agent (host binary `hostlink`)
├── pkg/
│   ├── agent/                    # agent runtime: dial-out, mTLS client creds, Control stream, docker client, request handlers (commands.go)
│   ├── agentapi/                 # shared wire contract for the generic AgentRequest plane (method names + JSON payload shapes)
│   ├── controller/               # controller runtime: AgentLink gRPC service, REST API (api.go), redis registry (registry.go), ControllerPeer relay (peer.go), mTLS listener wiring, HTTP/metrics hooks
│   └── api/hostlink/v1/          # AgentLink + ControllerPeer .proto and generated code
├── charts/hostlink/             # Helm chart (controller StatefulSet, services, optional redis/peer/cert-manager)
└── deploy/                       # agent systemd unit + example config, debug PKI helper
```

> The substantive logic currently lives under `pkg/agent` and `pkg/controller` (one file group per binary). As the MVP fills out (§7), the cross-cutting concerns are expected to be split into focused `internal/` subpackages — `internal/{transport, tunnel, registry, metrics, docker, routing}` — so the transport, port-forward relay, Redis registry, metrics fan-out, docker lifecycle, and multi-replica routing each become an independently testable unit behind an interface. Treat that `internal/` decomposition as the target shape, not the current one.

Build:

```bash
go build -mod=vendor -o bin/controller/hostlink ./cmd/controller
go build -mod=vendor -o bin/agent/hostlink      ./cmd/agent
```

> Both output binaries are named `hostlink` (separate output directories keep local builds apart). `make build` builds the controller container image (`Dockerfile`, entrypoint `hostlink`); `make agent` builds the Linux agent binary (`hostlink.elf`) inside a throwaway golang container.

---

## 3. Technology choices and rationale

| Concern | Choice | Rationale / key constraint |
|---|---|---|
| Language | Go | Team stack; a single static binary is easy to ship to external hosts |
| Application chassis | `github.com/humble-mun/chassis` (`pkg/app`, `pkg/server`, `pkg/metrics`, `pkg/version`) | Shared startup/flag/logging scaffolding; `pkg/server` runs a single HTTP/2 listener that routes `application/grpc` traffic to the gRPC server and everything else to Gin, and supports per-listener TLS/mTLS options (see §6). `pkg/metrics` provides the controller's own `/metrics` endpoint; the §4.3 agent-exporter fan-out is a separate concern served by the controller on `/api/v1/metrics` (it cannot share the chassis fixed registry, which holds no dynamic agent metric names) |
| Transport / command channel | gRPC bidirectional streaming (HTTP/2) | The agent is behind NAT and must dial out with a persistent connection; HTTP/2 gives stream multiplexing for free |
| Multiplexing | HTTP/2 streams directly | **Do not** layer yamux/smux on top of gRPC — HTTP/2 streams *are* the mux; two layers is redundant |
| Containers | `github.com/docker/docker/client` | Docker containers are stateful "pets"; stop/start preserve the writable layer (see §4.6) |
| Metrics | `prometheus/client_golang` + `prometheus/common/expfmt` + `client_model` | See §4.3 |
| Node metric collection | node_exporter as a **separate sidecar binary**; agent GETs `127.0.0.1:9100` locally | **Do not** import node_exporter as a library — its collector package is not a stable public API |
| Tunnel byte pipe | self-built: the §5 `Frame` protocol + `pkg/tunnel` splice engine | Implemented in-repo (`SpliceConn` TCP↔stream, `SpliceStream` stream↔stream): 32 KiB chunking, explicit `HALF_CLOSE`/`RESET` frames, backpressure inherited from HTTP/2 flow control. `openconfig/grpctunnel` was evaluated and dropped — the frame protocol is small enough that owning it (with tests) beats auditing/patching a dependency's half-close behavior |
| Coordination / registry | Redis | Already in the stack (used by Asynq); atomic allocation + TTL, simple and debuggable; see §4.5 |
| K8s interaction | `client-go` (minimal) | The port range is statically reserved (see §6); dynamic exposure is purely application-level. **Do not** create a k8s object per exposure |

**Explicitly rejected (do not adopt):**

- **KubeEdge / treating hosts as K8s nodes** — this project's premise is Docker containers, not pods; and the K8s pod lifecycle cannot cheaply give "power off/on with preserved state."
- **`jhump/grpctunnel`** — it tunnels gRPC calls (gRPC-over-gRPC), not raw TCP. Wrong fit.
- **`openconfig/grpctunnel`** — was the original pick for the tunnel byte pipe; dropped when the pipe was built in-repo (§4.4): the §5 `Frame` protocol is small, and owning it means half-close semantics are implemented and tested directly instead of audited in a dependency.
- **Self-built tun device + userspace netstack overlay** — only L4 port forwarding is needed right now; rolling your own L3 is self-inflicted difficulty (for a future VPN, see §8).
- **Raft/consensus between replicas** — `agent→pod` is not contended state and needs no consensus (see §4.5).
- **`master`/`minion` naming** — being phased out across the industry; don't use it.

---

## 4. Architecture and data flows

### 4.1 Connection model (the core premise)

The agent is behind NAT and is the gRPC **client**; the controller has a public entrypoint and is the gRPC **server**. **Under HTTP/2 a server cannot initiate an RPC or open a stream to a client** — this is the root cause of every "reverse" design that follows.

Each agent maintains **one** gRPC connection to the controller. Every logical channel multiplexes onto HTTP/2 streams over that connection:

- **Commands**: one long-lived bidi control stream (`Control`), opened by the agent; the controller pushes commands down its response direction.
- **Metrics**: the controller reverse-pulls each agent's exposition over this connection (see §4.3).
- **Port forwarding**: each forwarded public connection is one `Forward` stream, opened by the agent (see §4.4).

> **Constraint:** "reuse one connection" means **one TCP connection with many streams** — not cramming all bytes into one stream and building your own sub-mux. Each forwarded connection gets its own `Forward` stream, preserving gRPC's per-stream flow control.

> **Constraint (transport security):** the agent↔controller gRPC connection is **mutually authenticated TLS (mTLS), TLS 1.3 minimum, with no insecure fallback**. The agent presents a client certificate and verifies the controller against a CA bundle; the controller presents a server certificate and requires-and-verifies the agent's client certificate. mTLS is the identity-authentication mechanism for agents at the connection layer (see §6 for how the controller terminates it on a dedicated listener, and §9 for the security rationale).

### 4.2 Command channel

On connect, the agent opens `Control(stream AgentEvent) returns (stream Command)`. The controller pushes down the `Command` stream: `OpenForward` (open a §4.4 tunnel) and `AgentRequest`/`AgentRequestChunk` (the generic dispatch plane below); `DockerOp` and `ExposeRule` remain unused proto placeholders. The agent reports handshake, heartbeats, and Docker events up the `AgentEvent` stream.

**Generic request/response dispatch (implemented).** On top of the command channel there is a generic, method-dispatched request envelope used to serve API calls: the controller pushes `Command.AgentRequest{request_id, method, payload}` and the agent replies with `AgentEvent.AgentResult{request_id, code, payload, error, final}`, correlated by `request_id`. `method`/`payload` are opaque JSON; `code` mirrors an HTTP status so the REST layer maps it back directly. The controller-side dispatcher keeps a per-`request_id` channel and a single Recv loop fans results back to the waiting handler; sends are serialized (a gRPC stream allows one concurrent Send). Long-running methods stream: the agent emits `AgentEvent.AgentProgress{request_id, payload}` frames and a terminal `AgentResult{final: true}`; the dispatcher keeps the channel open until `final`. Implemented methods: `images.list` (`agentapi.MethodImagesList`, single-shot), `images.pull` (`agentapi.MethodImagesPull`, streaming progress), `images.remove` (`agentapi.MethodImagesRemove`, single-shot batch); the **container** methods — `containers.list` / `containers.create` (docker run semantics: create + start, `--net`/`--pid`/ports/binds/restart-policy supported) / `containers.inspect` / `containers.start` / `containers.stop` / `containers.restart` / `containers.remove` (all single-shot) and `containers.logs` (streaming, line-framed, optional follow; a single line is capped at 64 KiB so a pathological line cannot grow memory or exceed the gRPC message size); the filesystem methods `fs.stat`/`fs.list`/`fs.read` (streaming)/`fs.write` (streaming upload)/`fs.mkdir`/`fs.remove`; and `metrics.scrape` (`agentapi.MethodMetricsScrape`, streaming exporter exposition — see §4.3).

**Cancellation of in-flight streams (implemented).** `request.cancel` (`agentapi.MethodRequestCancel`) is a fire-and-forget meta method riding the same opaque method plane (no proto change): its payload names the `request_id` to cancel, the agent cancels that handler's context and sends no reply, and an unknown id is ignored. The controller-side `dispatchStream` cancel sends it whenever a stream is torn down before its terminal frame — this is what ends an unbounded stream (a followed `containers.logs`) whose HTTP client went away, and it propagates across pods automatically (cancelling the relay RPC makes the holding pod's handler run its own cancel). The agent registers each streaming handler's cancel **synchronously in the receive loop** (like the `fs.write` chunk channels) so a cancel arriving right after the opening request can never miss it. Streaming delivery has two reliability classes (`streamReliable`): `fs.read`/`metrics.scrape`/`containers.logs` are lossless with backpressure; `images.pull` progress is advisory and may drop frames under a slow consumer.

**REST surface (implemented).** The controller exposes (Gin, on the in-cluster default listener): `GET /api/v1/agents` (list connected agents); `GET /api/v1/agents/<agentId>/images` (dispatch `images.list`, return JSON unchanged); `POST /api/v1/agents/<agentId>/images` (dispatch the streaming `images.pull`, body `{image, auth?}`, response is **`text/event-stream`** SSE — each `data:` line a `PullProgress` JSON, terminated by `data: {done:true,...}`); `DELETE /api/v1/agents/<agentId>/images/<imageId>` and `DELETE /api/v1/agents/<agentId>/images?ref=A&ref=B` (dispatch `images.remove`, single by path param or batch by repeated `ref`, optional `?force`/`?noPrune`). The **container** endpoints: `GET /api/v1/agents/<agentId>/containers` (`?all=true` includes stopped), `POST .../containers` (docker run: create + start, 201 with `{id}`), `GET .../containers/<containerId>` (inspect), `GET .../containers/<containerId>/logs` (SSE log stream; `?follow`/`?tail`/`?since`/`?timestamps`; client disconnect propagates `request.cancel` to the agent), `POST .../containers/<containerId>/start|stop|restart` (`?timeout` grace seconds on stop/restart), `DELETE .../containers/<containerId>` (`?force`/`?volumes`). Also: `GET`/`POST`/`PUT`/`DELETE /api/v1/agents/<agentId>/files` (the working-directory filesystem API — `fs.*`), and `GET /api/v1/metrics` (the aggregated agent-exporter fan-out — §4.3, distinct from the controller's own `/metrics`). Resolution is local-or-relay: if the agent's `Control` stream is on this replica, dispatch directly; otherwise resolve the holding pod from Redis and relay via `ControllerPeer.Dispatch` (unary), `ControllerPeer.DispatchStream` (streaming pulls, incl. `metrics.scrape`), or `ControllerPeer.Upload` (filesystem writes) (§4.5). With the peer plane or Redis disabled, a miss returns 404.
The **port-forward** endpoints (§4.4/§4.5): `POST /api/v1/agents/<agentId>/forwards` (body `{target, container_id?, port?}` — allocates a public port from `--forward-port-range`; the optional `port` pins a specific port and is validated against the range, `201` with `{port, state, agent_id, target, ...}`, `409` when the range is exhausted or the requested port is already taken, `503` when forwarding is disabled), `GET /api/v1/agents/<agentId>/forwards` and `GET /api/v1/forwards` (list, each entry carrying its `state`: `pending`/`active`/`suspended`), and `DELETE /api/v1/forwards/<port>` (release; 404 unknown).

### 4.3 Metrics: controller-self `/metrics` + aggregated agent `/api/v1/metrics`

Two **independent** endpoints on the in-cluster default listener:

- **`/metrics`** (chassis-owned): the controller's **own** process metrics.
- **`GET /api/v1/metrics`** (`service.agentMetrics`): the **cloud-side aggregation of every agent's upstream exporters** — the reverse fan-out. Deliberately a separate route, not the chassis `/metrics`: controller-self metrics and the dynamic-name agent passthrough are different concerns, and the chassis single fixed registry cannot carry arbitrary agent metric names.

On a scrape of `/api/v1/metrics`, the controller:

1. enumerates the online fleet (`registry.listAll`: Redis `agent→pod` scan, or locally-held agents in in-memory mode) and dispatches a **streaming** `metrics.scrape` (`agentapi.MethodMetricsScrape`) to each agent with **bounded concurrency** (`metricsFanoutConcurrency`), local-or-relay (`ControllerPeer.DispatchStream` for an agent held by a sibling);
2. each agent pulls its configured `scrape-targets` (a structured list of `{name, url}` with optional `path` — `url` is `http(s)://host[:port][/path]`, path defaulting to `/metrics`, or `unix:///path/to.sock` for a unix-domain-socket exporter — node_exporter, dcgm-exporter, …) locally and **streams** each exposition back as `MetricsFrame` `AgentProgress` chunks, so the agent buffers only a 64KB chunk and no single-message size limit applies (each exporter's total exposition is capped at 16 MiB by the agent's `scrapeMaxBodySize`, which also bounds the controller's per-exporter assembly buffer before MetricFamily parsing); the stream is **reliable** (lossless backpressure, like `fs.read`);
3. gives each agent an **independent deadline shorter than `scrape_timeout` (default 10s) — `--agent-scrape-timeout`, default 5s**; slow/offline agents are skipped, contributing only `agent_up 0`, so one slow agent never times out the whole scrape;
4. **merges by MetricFamily**, folding each exporter's body as its stream completes: decode with `expfmt.NewTextParser(model.UTF8Validation)` (a zero-value `TextParser` leaves the validation scheme unset and **panics** on first parse), inject `agent=<id>` + `exporter=<name>` labels, merge by metric name into one family (one HELP/TYPE with all series beneath it), then encode once with `expfmt.NewEncoder`;
5. synthesizes `agent_up{agent="<id>"}` (1 = the stream completed this round, 0 = offline/timed-out — the only clean "an agent went down" signal, since native `up` reflects only the controller endpoint) and `hostlink_scrape_target_up{agent,exporter}` (per-exporter health).

> **Constraint:** **Never string-concatenate expositions** — duplicate HELP/TYPE lines for the same metric name cause the Prometheus parser to reject the entire payload. You must merge at the MetricFamily level. The merge tolerates cross-exporter/version skew: same-name families keep the first HELP/TYPE, and a TYPE conflict drops the later family.

> The injected label is `exporter` (not `job`/`instance`) so it survives **without** `honor_labels` in the scrape config. Exporters are **separate sidecar binaries** GET'd locally (e.g. `127.0.0.1:9100`); do **not** import node_exporter as a library — its collector package is not a stable public API. Large **unary** agent results (a different path from this streaming fan-out) are bounded by `--grpc-max-recv-msg-size` (default 16 MiB), applied to the agent-facing and ControllerPeer servers/clients.

### 4.4 Port forwarding (raw TCP, reverse tunnel)

Requirement: dynamically expose a container's internal port (e.g. vLLM listening on `:8080`) to a public port on the cloud side (e.g. `:1025`) so it can serve external traffic. **The protocol is raw TCP (not HTTP) and must support half-close.**

The data plane is **L4 stream proxying** (terminate TCP at each hop, carry only application bytes), **not L3 packet tunneling** — so it inherently avoids TCP-over-TCP degradation.

> **Implementation status: implemented** end to end. `--forward-port-range` (chart `portForward.range`) reserves the public range; the REST API allocates a port per exposure (§4.5); **every replica binds every allocated port** (a listener manager reconciles against the port registry on change signals plus a 5s tick); an accepted public connection is paired with the agent's `Forward` stream and spliced by `pkg/tunnel`; suspended mappings (§4.6) are rejected with an immediate RST.

Stream-open handshake (because the server cannot open a stream to the agent):

1. A public connection lands on the controller's exposed port.
2. The controller pushes `OpenForward{session_id, target}` down that agent's `Control` stream.
3. The agent **opens** a `Forward` stream, first frame carrying `session_id`.
4. The controller **pairs** the public connection with that `Forward` stream by `session_id` and relays bidirectionally.

This handshake + chunking + session pairing is **implemented in-repo**: the controller's session table pairs the public connection with the agent's `Forward` stream by `session_id`, and `pkg/tunnel` carries the bytes. The agent's first frame is an `OPEN` — or, when its dial to the target fails, a `RESET` carrying the `session_id`, which tears down the pending public connection with an RST.

**Half-close (the correctness crux):** a gRPC stream's own lifecycle (`CloseSend` / handler return) **cannot represent TCP's independent per-direction half-close**. It must be modeled as **explicit frame types** (see §5 `Frame.Type`):

- Local socket read returns EOF → send a `HALF_CLOSE` frame, stop sending on that direction, **but keep reading the other direction**;
- Receive `HALF_CLOSE` → call `conn.(*net.TCPConn).CloseWrite()` on the local socket, keep reading;
- Receive/produce `RESET` → `SetLinger(0)` + `Close()`.

**Backpressure:** never read from the local socket faster than you can `Send` on the stream — rely on `Send` blocking when the HTTP/2 flow-control window is full to apply natural backpressure. **Do not buffer without bound.**

### 4.5 Multi-replica HA and connection affinity

The controller runs multiple replicas (required for HA). **The affinity problem:** an agent's connection is pinned to the **one** pod it dialed; public TCP arriving through the L4 LB lands on **any** replica, and the receiving pod is usually not the one holding that agent.

> **Implementation status.** All of the below is **implemented**: the `hostlink:agent:<id> → holding_pod` Redis map (write on connect, compare-and-delete on disconnect, TTL refreshed by heartbeat); the cross-pod relay for **API requests** — `ControllerPeer.Dispatch` (unary), `ControllerPeer.DispatchStream` (streaming, e.g. the SSE image pull or `fs.read` download), and `ControllerPeer.Upload` (client-streaming, e.g. the `fs.write` upload) — over a dedicated in-cluster mTLS listener, with reject-and-retry on a stale mapping (§6); the `hostlink:port:<P>` map with atomic allocation; the cross-pod relay of the **port-forward data plane** (`ControllerPeer.Forward`, the two byte-pipe hops below); and the **activation barrier** — each replica refreshes `hostlink:controller:<pod>` (45s TTL) and per-port `hostlink:bound:<P>:<pod>` (30s TTL) keys as it binds listeners, and the REST API reports a port `active` only once **every** live replica has bound it (`pending` otherwise; `suspended` per §4.6) — so business logic adds P to a Service only after `active` and never hits a replica that would refuse the connection.

- **Routing key:** for raw TCP the only key available at ingress is the **destination port** (there is no Host header). Each exposure = one distinct public port.
- **Registry (Redis dual maps):**
  - `hostlink:port:<P>` → JSON `{agent_id, target, container_id?, suspended?}` (no TTL — freed only by an explicit release);
  - `hostlink:agent:<id>` → `holding_pod` (written by the pod holding the connection when the agent attaches, **with a TTL refreshed by heartbeat**, deleted on disconnect).
- **Atomic port allocation:** take a free port from the reserved pool atomically via Redis `SETNX` (`MGET` batch to find candidates, `SETNX` to claim); single-replica mode runs the same store interface in memory. The store publishes every change on the Redis pub/sub channel `hostlink:ports`, which each replica's listener manager subscribes to alongside its 5s tick for prompt reconciliation.
- **Routing flow:** a public connection reaches pod B on port P → B looks up `port:P` to get agentX, then `hostlink:agent:agentX` to get podA.
  - `podA == B`: B drives the §4.4 reverse-open directly.
  - `podA != B`: B opens `ControllerPeer.Forward` to podA, first frame `OPEN{session_id, open: {agent_id, target}}`; podA pushes `OpenForward` down the agent's Control stream, pairs the agent's `Forward` stream by `session_id`, and replies `READY` — only then does B start consuming the public socket (**retry-before-read**: a stale-holder `FAILED_PRECONDITION` is retried after one re-resolve with no byte loss). B splices "public conn ↔ peer stream"; podA splices "peer stream ↔ agent Forward stream." **Two byte-pipe hops in series; half-close signaling propagates end to end.**
- **Note:** because the LB spreads connections, **cross-pod forwarding is the common case, not the exception** (hit rate ≈ 1/N). This path must be solid.
- **Stale-window handling:** after an agent reconnects to a different holder but before the Redis TTL expires, a connection may be forwarded to a pod that no longer holds it → **a pod that receives "forwarded to me but I don't hold agentX" must reject and have the caller re-resolve and retry.**

> gossip/mesh was evaluated and rejected: the state is not contended and needs no consensus; and since Redis is already in the stack, gossip would only add moving parts without actually removing a dependency. Reconsider mesh only if the controller must ship as a **self-contained appliance with zero external dependencies.**

### 4.6 Container lifecycle (power off/on)

"Power off/on" = `docker stop` / `docker start`. Docker containers are **stateful pets**: stop goes SIGTERM → grace period → SIGKILL, the **writable layer is preserved on local disk**, and killing the process frees the GPU; start brings back the same container ID with state intact. **On plain Docker this is free — you do not need the K8s-style upperdir persistence.**

> **Implementation status.** Container CRUD + lifecycle (list/create-run/inspect/start/stop/restart/remove) and log streaming are **implemented via the generic `AgentRequest` plane** (the `containers.*` methods, §4.2) — **not** via the legacy `DockerOp` command, which remains an unused placeholder in the proto. Docker **event** reporting (`DockerEvent`) and the event→exposure coupling below are also **implemented**.

Exposure follows lifecycle: the agent subscribes to Docker events via `client.Events()` (filtered to container `start`/`stop`/`die`; one watcher per Control session, resubscribing with backoff) and reports them up the `AgentEvent` stream —

- on `stop`/`die` → the controller marks that container's port mappings `suspended` (the allocation and public port are **kept**; new connections are refused with an immediate RST);
- on `start` → the controller re-inspects the container via the holding pod (`containers.inspect` projects `Networks` name→IP), rewrites the mapping's target IP (**the container IP may change after restart**), and clears `suspended`. The public port itself never changes — the allocation was never released.

> For GPU containers, remember nvidia-container-toolkit / the nvidia runtime. `docker pause`/`unpause` (freezer cgroup — freezes the process but keeps RAM/VRAM) is a different "suspend, resume instantly, don't free resources" semantic; expose it or not depending on the billing model.

---

## 5. Wire protocol (proto)

```proto
syntax = "proto3";
package hostlink.v1;
option go_package = "github.com/humble-mun/hostlink/pkg/api/hostlink/v1;hostlinkv1";

service AgentLink {
  // Opened once after the agent connects; the controller pushes commands to the agent
  // down the response stream. Under HTTP/2 the server cannot initiate an RPC to the
  // client, so all server->agent commands travel over this already-open stream.
  rpc Control(stream AgentEvent) returns (stream Command);

  // One per forwarded public TCP connection; opened by the agent, first frame is an
  // OPEN carrying session_id for pairing (or a RESET carrying session_id when the
  // agent's dial to the target failed). Cross-pod relay uses ControllerPeer.Forward.
  rpc Forward(stream Frame) returns (stream Frame);
}

// ControllerPeer is the controller<->controller (sibling pod) plane: a replica that
// receives an API request for an agent it does not hold relays it to the holding pod
// (resolved via the Redis agent->pod map). Pure routing hop; method/payload are opaque.
service ControllerPeer {
  // A pod that no longer holds the agent rejects with FAILED_PRECONDITION so the
  // caller re-resolves and retries.
  rpc Dispatch(DispatchRequest) returns (AgentResult);

  // Streaming variant for long-running ops (e.g. images.pull, fs.read): each
  // streamed AgentResult is a progress frame except the last (final = true),
  // which carries the terminal code/payload/error.
  rpc DispatchStream(DispatchRequest) returns (stream AgentResult);

  // Client-streaming relay of a controller->agent upload (e.g. fs.write): the
  // first UploadFrame carries the open (DispatchRequest), the rest body chunks.
  rpc Upload(stream UploadFrame) returns (AgentResult);

  // Cross-pod port-forward data plane (§4.5): the pod that accepted the public
  // connection opens this to the holding pod with an OPEN frame (session_id +
  // the open routing key); the holder pushes OpenForward to the agent, pairs the
  // agent's Forward stream, replies READY, then both sides splice. A pod that no
  // longer holds the agent rejects with FAILED_PRECONDITION (re-resolve + retry).
  rpc Forward(stream Frame) returns (stream Frame);
}

message AgentEvent {
  string agent_id = 1;
  oneof kind {
    Hello hello = 2;          // initial handshake: auth, capability declaration
    Heartbeat heartbeat = 3;  // refresh the TTL of the agent->pod mapping in Redis
    DockerEvent event = 4;    // container start/stop/die reports
    AgentResult result = 5;   // reply to a controller->agent AgentRequest (final = true)
    AgentProgress progress = 6; // non-terminal progress for a long-running AgentRequest
  }
}
message Hello { string token = 1; }
message Heartbeat {}
message DockerEvent { string type = 1; string container_id = 2; }

message Command {
  oneof cmd {
    OpenForward  open_forward = 1; // tell the agent to open a Forward stream for a session
    DockerOp     docker_op    = 2; // run/stop/start/pause/unpause/rm
    ExposeRule   expose_rule  = 3; // add/remove a port-exposure rule
    AgentRequest request      = 4; // generic API-driven request (containers/images/...)
    AgentRequestChunk chunk   = 5; // body chunk for a streaming upload (e.g. fs.write)
  }
}
message OpenForward { string session_id = 1; string target = 2; } // target = container-side addr, e.g. 172.30.1.5:8080
message DockerOp    { string op = 1; string container_id = 2; bytes spec = 3; }
message ExposeRule  { string container_target = 1; uint32 public_port = 2; bool remove = 3; }

// Generic method-dispatched request/result, correlated by request_id. method names
// the operation (e.g. "images.list"); payload is its JSON body; code mirrors an HTTP
// status. DispatchRequest wraps it with the agent_id routing key for the peer hop.
message AgentRequest      { string request_id = 1; string method = 2; bytes payload = 3; }
message AgentRequestChunk { string request_id = 1; bytes data = 2; bool last = 3; } // one body chunk of a streaming upload request
message AgentResult       { string request_id = 1; uint32 code = 2; bytes payload = 3; string error = 4; bool final = 5; } // final marks the terminal frame of a (possibly streamed) reply
message AgentProgress     { string request_id = 1; bytes payload = 2; } // method-specific progress (e.g. images.pull layer status JSON, or fs.read file bytes)
message DispatchRequest   { string agent_id = 1; AgentRequest request = 2; }
message UploadFrame       { oneof kind { DispatchRequest open = 1; bytes chunk = 2; } bool last = 3; } // ControllerPeer.Upload stream: first frame open, rest body chunks

message Frame {
  string session_id = 1;            // set on the first frame of a Forward stream (the OPEN frame, or a failed-dial RESET), for pairing
  enum Type { DATA = 0; HALF_CLOSE = 1; RESET = 2; OPEN = 3; READY = 4; }
  Type   type = 2;
  bytes  data = 3;                  // valid only when Type == DATA
  PeerForwardOpen open = 4;         // set only on the OPEN frame of a ControllerPeer.Forward pod->pod hop
}
message PeerForwardOpen { string agent_id = 1; string target = 2; } // pod->pod routing key: which agent + container target
```

Relay contract (pseudocode; reused at every hop — agent↔container, controller↔public, pod↔pod are all the same shape):

```
relay(localTCP, frameStream):
  goroutine A: localTCP.Read -> Frame{DATA} (Send blocks = backpressure); EOF -> Frame{HALF_CLOSE} then stop this direction; err -> Frame{RESET}
  goroutine B: frameStream.Recv -> DATA: localTCP.Write; HALF_CLOSE: localTCP.CloseWrite() then stop this direction; RESET: SetLinger(0)+Close
  each direction half-closes independently; only fully Close once both directions are done. On a hard error, cancel the ctx to unblock the other direction.
```

---

## 6. Deployment shape

### Controller (cloud; container image `hostlink`)

- Form: a Kubernetes **StatefulSet** (not a Deployment — the ControllerPeer plane needs stable per-pod DNS). Chart default `replicaCount: 1`, which runs out of the box with no external dependency. For **HA set `replicaCount > 1`, which REQUIRES `redis.url` + `peer.enabled`** (the chart fails the install/upgrade otherwise); see §4.5. A Helm chart is at `charts/hostlink/`.
- Listeners (the chassis HTTP/2 server multiplexes gRPC and Gin onto each listener):
  1. an **mTLS gRPC listener** (`WithTCPListener` + `WithMTLS`) that agents dial out to for their `Control`/`Forward` connections; exposed externally through the ingress;
  2. a **plaintext (h2c) default listener** bound in-cluster only, serving the REST API (`/api/v1/...`, incl. the aggregated agent metrics at `/api/v1/metrics`), the controller's own `/metrics`, `/probe`, `/version`, `/logging` — no client-cert requirement so K8s probes, Prometheus, and in-cluster API callers can reach it;
  3. (when `peer.enabled`) a separate **in-cluster ControllerPeer mTLS listener** on its own `grpc.Server` — deliberately NOT the shared chassis server, so the relay plane is never reachable from the agent-facing/ingress listener (an agent running untrusted code must not be able to call `ControllerPeer.Dispatch` and target other agents).
- Ingress (L4 LoadBalancer, Cilium environment):
  1. the mTLS gRPC port above, for agent dial-out — the chart provides this Ingress (gated on `ingress.host`), and it MUST do L4/TLS passthrough so the controller still terminates mTLS itself (see §9);
  2. a **reserved TCP port range** (e.g. `1025–2025`) for tunnel exposure — chart `portForward.range` writes `forward-port-range` into the controller config and, with `portForward.service.enabled`, renders a `<release>-forward` Service (type/annotations configurable) enumerating every port in the range (Service ports are an enumerated list, not a range — mind cloud-LB listener quotas when sizing).
- Bypass note: the chassis server applies the same handler to every listener, so the plaintext default listener would also accept gRPC. The split relies on **network-layer isolation** — the default listener is reachable only inside the cluster, while agent gRPC is exposed solely through the mTLS listener via ingress.
- Services: a load-balanced ClusterIP Service carrying the gRPC + http ports (the stateless `/api/v1/metrics` fan-out and REST API are answerable by any replica), plus a **headless `<release>-peer` Service** giving each pod stable DNS (`<pod>.<release>-peer.<ns>.svc:<peerPort>`) that the ControllerPeer plane dials.
- Dependency: **Redis** backs the `hostlink:agent:<id> → holding_pod` registry, the `hostlink:port:<P>` exposure map (atomic allocation), and the activation-barrier bind/liveness keys — optional (single-replica runs in-memory), required for HA.
- Certificates: per plane, sourced either from a mounted Secret (`grpc.tlsSecretName` / `peer.tlsSecretName`) or, when `certManager.enabled`, issued per-pod by the **cert-manager CSI driver** (`csi.cert-manager.io`) from a configured Issuer/ClusterIssuer — the latter fits the peer cert whose SAN must carry the pod's own headless DNS.

> **Infra decision (decoupled from the Go code):** confirm that the Cilium LB / your NLB can handle a port range of the needed size; if not, shrink the range or evaluate the Gateway API `TCPRoute`.

### Agent (external host; binary `hostlink`)

- Form: a static Go binary (installed as `/usr/local/bin/hostlink`) running as a **systemd service**; the unit and an example config live in `deploy/` (`deploy/hostlink.service`, `deploy/hostlink.yaml`).
- Configuration: the agent reads all settings from `/etc/humble-mun/hostlink.yaml` (the chassis registers viper `SetConfigName("hostlink")` + `AddConfigPath("/etc/humble-mun")`; the binary's `version.Name` is `hostlink`). The systemd unit passes **no** command-line flags so config edits need no `daemon-reload`, and viper `WatchConfig` reloads the file at runtime. YAML keys are the flag names verbatim (`controller-endpoint`, `controller-tls-ca-path`, `agent-tls-cert-path`, `agent-tls-key-path`, `controller-tls-server-name`, `node-name`); each can also be overridden by an `HM_*` env var.
- Behavior: **dials out** to the controller's public gRPC endpoint with mTLS and runs the `Control` stream (`Hello` + heartbeats), **reconnecting in-process** with exponential backoff + jitter and HTTP/2 keepalive (a controller redeploy is ridden out without a process restart). It serves controller-pushed `AgentRequest`s via a lazy `client.FromEnv` Docker client: the Docker **images** methods — `images.list` / `images.pull` (streaming) / `images.remove`; the **container** methods — `containers.list` / `containers.create` (run) / `containers.inspect` / `containers.start` / `containers.stop` / `containers.restart` / `containers.remove` / `containers.logs` (streaming, follow cancellable via `request.cancel`); and the **filesystem** methods — `fs.stat` / `fs.list` / `fs.read` (chunked download) / `fs.write` (chunked upload) / `fs.mkdir` / `fs.remove` — over the configured `--data-dir` working directory, with every path resolved inside it (traversal rejected, and the working-dir root refused on delete). It also carries the §4.4 forward tunnels (`OpenForward` → dial the container target → open a `Forward` stream → splice) and reports Docker events (`start`/`stop`/`die`) that drive the §4.6 exposure suspend/resume. The node_exporter sidecar on `127.0.0.1:9100` is a deployment-time concern (§4.3). Because the docker client is lazy, the systemd unit keeps `docker.service` as a soft (`Wants`) ordering dependency, not a hard requirement.
- Network: behind NAT; only needs outbound reachability to the controller.

---

## 7. Current implementation scope (MVP)

In dependency order (`[done]` / `[partial]` / `[todo]` reflect current status):

1. `[done]` `AgentLink` proto + generated code; agent↔controller connection setup, `Hello`/`Heartbeat`, with **in-process reconnect** (exponential backoff + jitter) and HTTP/2 keepalive.
2. `[done]` Command dispatch and execution. The **generic `AgentRequest`/`AgentResult` envelope** (with the streaming `AgentProgress`/`final` extension, the `AgentRequestChunk` upload extension, and the `request.cancel` fire-and-forget cancel meta method) is done, along with: the Docker **images** methods — **`images.list`**, **`images.pull`** (streaming SSE progress), **`images.remove`**; the **container** methods — **`containers.list`**, **`containers.create`** (docker run: create + start), **`containers.inspect`**, **`containers.start`**/**`containers.stop`**/**`containers.restart`**, **`containers.remove`**, and **`containers.logs`** (streaming SSE, optional follow with cross-pod cancel propagation); and the **filesystem** methods over `--data-dir` — **`fs.stat`**/**`fs.list`**, **`fs.read`** (chunked download), **`fs.write`** (chunked multipart upload, with `ControllerPeer.Upload` for the cross-pod hop), **`fs.mkdir`**, recursive **`fs.remove`** — all with their REST endpoints and the controller dispatcher. `DockerEvent` reporting is **done** (§4.6); the legacy `DockerOp` command is superseded by the `containers.*` methods and remains an unused proto placeholder.
3. `[done]` Metrics: agent `scrape-targets` (node_exporter / dcgm / … sidecars) pulled on demand and **streamed** back (`metrics.scrape`); controller `GET /api/v1/metrics` bounded-concurrency reverse fan-out + `agent`/`exporter` label injection + MetricFamily merge + synthesized `agent_up` / `hostlink_scrape_target_up`, cross-pod aware via `ControllerPeer.DispatchStream`. Served on a route distinct from the controller's own `/metrics`. `--grpc-max-recv-msg-size` bounds large unary results.
4. `[done]` Port forwarding: the self-built `pkg/tunnel` splice engine (chunking, explicit half-close/reset frames, backpressure), the `OpenForward` handshake + `Forward` session pairing, per-port public listeners reconciled from the port registry on every replica, and the REST forwards API + `--forward-port-range`. (`ExposeRule` remains an unused placeholder.)
5. `[done]` Multi-replica affinity: the `hostlink:agent:<id> → holding_pod` Redis map (write/CAD-delete/TTL-refresh; standalone/sentinel/cluster topologies); the **cross-pod relay for API requests** (`ControllerPeer.Dispatch` unary + `ControllerPeer.DispatchStream` streaming + `ControllerPeer.Upload` client-streaming, on a dedicated in-cluster mTLS listener, optional via `peer.enabled`, with reject-and-retry on a stale mapping); the `hostlink:port:<P>` map + atomic port-pool allocation; the cross-pod **port-forward** two-hop relay (`ControllerPeer.Forward`, retry-before-read); and the activation barrier (`pending`/`active`/`suspended` per-port state).
6. `[done]` Lifecycle coupling: agent Docker-event watcher → the controller suspends exposures on `stop`/`die` (fast RST, allocation kept) and re-resolves the container IP + unsuspends on `start`.

---

## 8. Future capabilities (not implemented now, but don't wall off the extension)

- **Container log collection into Loki (decided: controller-relayed, "Plan B").** Agent hosts sit on a **restricted network whose only permitted path is the agent→controller mTLS connection**, so host-local shipping (an Alloy/promtail per host pushing to Loki) is not an option — logs must ride the existing Control stream. Design: a controller-side log-relay follows `containers.logs` for the target containers of each agent this replica holds, stamps `{agent_id, container_name}` labels on every line, and pushes to the in-cluster Alloy `loki.source.api` endpoint (Alloy owns batching/retry/relabel into Loki). The `containers.logs` streaming method (lossless, line-framed) and the `request.cancel` teardown are the transport primitives, both implemented. Open points for the relay itself: resume-after-reconnect via `since` (track the last shipped timestamp per container), relay ownership must follow the agent when it reconnects to a different replica, and bandwidth budgeting on the shared Control stream.
- **VPN / L3 overlay**: let the cloud reach container IPs directly (bidirectional L3). When that time comes, use the **WireGuard family**, not a self-built netstack: for DC/colo networks where UDP is reachable and untrusted workloads need least privilege → **Nebula** (built-in host firewall); for networks that may block UDP → **Headscale + Tailscale** (DERP fallback over 443). Containers use the **subnet-router model** (each host gets a non-overlapping subnet; containers stay unmodified). With L3, the §4.4 port forwarding becomes a subset of it.
- **Container egress to cloud-internal private IPs**: via the overlay above, or an L4 transparent proxy (tproxy/REDIRECT) / SOCKS5 (SOCKS5's exit-side resolution is cleaner when access is by hostname).
- **HTTP service exposure**: if the service is HTTP, "subdomain + Cilium wildcard cert + Ingress host routing" can replace port-per-exposure and remove the port bookkeeping.
- **`pause`/`unpause`**: suspend/resume in seconds without freeing VRAM.
- **Per-container overlay identity / ACLs**: when untrusted workloads need stronger isolation.

---

## 9. Hazards and hard constraints (must follow)

- **Security (top priority):** this is a GPU platform; hosts may run untrusted customer code. Exposing ports — and, in the future, giving the cloud L3 reach into containers — significantly expands the attack surface. **Default-deny, whitelist directions and ports, audit.** Think the ACL story through *before* introducing a VPN.
- **Agent identity (connection layer):** the agent↔controller gRPC connection is **mutually authenticated TLS (mTLS), TLS 1.3 minimum, no insecure fallback** — this is how agents authenticate to the controller and vice versa (§4.1). The controller terminates mTLS on a dedicated listener; the plaintext default listener (probe/metrics) must stay **in-cluster only**, since the shared chassis handler would otherwise accept gRPC there too (§6).
- **TCP-over-TCP:** the current L4 proxy (terminate TCP, carry bytes) avoids it inherently; **do not** switch to L3 packet tunneling over TCP.
- **Head-of-line blocking:** commands, metrics, and forwarded bytes share one TCP connection (HTTP/2); on packet loss all streams stall together. Acceptable at a dozen agents with low concurrency; if high-throughput traffic (e.g. vLLM) hurts control-plane latency, move the **data plane onto its own `ClientConn`** (same service, same auth, same endpoint, just a separate TCP connection), and switch the data plane to QUIC if needed.
- **gRPC is message-bounded, not a byte stream:** forwarding must do chunking + explicit half-close + backpressure (§4.4).
- **/api/v1/metrics:** bounded-concurrency **streaming** fan-out, per-agent deadline < `scrape_timeout`, skip slow agents, inject `agent`/`exporter` labels; **never string-concatenate expositions** (§4.3). Distinct from the controller's own `/metrics`.
- **Multi-replica stale window:** a wrong-pod forward must be rejected and retried (§4.5).
- **Port reallocation:** the public port may change after an agent reconnects, so clients must reconnect — this is an acceptable design tradeoff, but document it in the product docs.
- **Tunnel half-close:** the `pkg/tunnel` splice engine models half-close as explicit frames (§4.4); its tests cover echo, half-close propagation, reset, and the two-hop relay — any change must preserve those semantics.

---

## 10. Coding conventions

- Follow Go community idioms and the Go conventions in the machine-global `~/.claude/CLAUDE.md`.
- Handle errors explicitly, never swallow them; thread `context` for cancellation on concurrent paths; decouple transport/registry/tunnel behind interfaces for testability and swappable implementations.
- Regenerate and commit generated code after proto changes (or pin the protoc version in CI).
```