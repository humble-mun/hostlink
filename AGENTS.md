# AGENTS.md — hostlink

> This file is the development reference for the `hostlink` project, intended for AI coding tools (Claude Code, etc.) and human contributors. It captures the technology choices and their rationale, the architecture and data flows, the wire protocol, the current implementation scope, known hazards and hard constraints, future capabilities, and the deployment shape. **Items marked "Constraint" or "Do not" are non-negotiable — do not deviate from them during implementation.**

---

## 1. Overview and scope

`hostlink` manages Linux hosts that live **outside the cloud** (on-prem / colo servers). These hosts are **not Kubernetes nodes**; their workloads run as **Docker containers**, not K8s pods.

It builds two binaries:

- **`hostlink-controller`** — runs in the cloud (inside Kubernetes). The control plane.
- **`hostlink-agent`** — runs on each external host (behind NAT). Executes commands, reports metrics, carries tunnels.

**Scale assumptions:** roughly a dozen agents, a few dozen at most. Many choices favor "good enough, controllable, debuggable." Do not over-engineer for massive scale.

**Out of scope:** billing, provisioning, quotas, and other business logic (these belong to the larger Smoothcloud platform). This project is the infrastructure layer only: connectivity, container orchestration, metrics, and port forwarding.

---

## 2. Project layout and build

A single Go module; one binary per subdirectory under `cmd/` (the standard Go layout for multiple binaries):

```
github.com/humble-mun/hostlink
├── cmd/
│   ├── controller/   main.go    # builds hostlink-controller (cloud)
│   └── agent/        main.go    # builds hostlink-agent (host)
├── pkg/
│   ├── agent/                    # agent runtime: dial-out, mTLS client creds, Control stream, docker client
│   ├── controller/               # controller runtime: AgentLink gRPC service, mTLS gRPC listener wiring, HTTP/metrics hooks
│   └── api/hostlink/v1/          # AgentLink .proto and generated code
└── deploy/                       # k8s manifests, systemd unit, etc.
```

> The substantive logic currently lives under `pkg/agent` and `pkg/controller` (one file group per binary). As the MVP fills out (§7), the cross-cutting concerns are expected to be split into focused `internal/` subpackages — `internal/{transport, tunnel, registry, metrics, docker, routing}` — so the transport, port-forward relay, Redis registry, metrics fan-out, docker lifecycle, and multi-replica routing each become an independently testable unit behind an interface. Treat that `internal/` decomposition as the target shape, not the current one.

Build:

```bash
go build -o bin/hostlink-controller ./cmd/controller
go build -o bin/hostlink-agent      ./cmd/agent
```

> Binaries are deliberately prefixed with `hostlink-` so that on the host they don't collide with some other `agent` in `ps`, in packaging, or in systemd units. Optionally prefix the project itself as `ruyun-hostlink` if you want to make Smoothcloud ownership explicit.

---

## 3. Technology choices and rationale

| Concern | Choice | Rationale / key constraint |
|---|---|---|
| Language | Go | Team stack; a single static binary is easy to ship to external hosts |
| Application chassis | `github.com/humble-mun/chassis` (`pkg/app`, `pkg/server`, `pkg/metrics`, `pkg/version`) | Shared startup/flag/logging scaffolding; `pkg/server` runs a single HTTP/2 listener that routes `application/grpc` traffic to the gRPC server and everything else to Gin, and supports per-listener TLS/mTLS options (see §6). `pkg/metrics` provides the `/metrics` endpoint plus scrape hooks for the §4.3 reverse fan-out |
| Transport / command channel | gRPC bidirectional streaming (HTTP/2) | The agent is behind NAT and must dial out with a persistent connection; HTTP/2 gives stream multiplexing for free |
| Multiplexing | HTTP/2 streams directly | **Do not** layer yamux/smux on top of gRPC — HTTP/2 streams *are* the mux; two layers is redundant |
| Containers | `github.com/docker/docker/client` | Docker containers are stateful "pets"; stop/start preserve the writable layer (see §4.6) |
| Metrics | `prometheus/client_golang` + `prometheus/common/expfmt` + `client_model` | See §4.3 |
| Node metric collection | node_exporter as a **separate sidecar binary**; agent GETs `127.0.0.1:9100` locally | **Do not** import node_exporter as a library — its collector package is not a stable public API |
| Tunnel byte pipe | `github.com/openconfig/grpctunnel` (TCP-over-gRPC reverse tunnel) | It already implements the register stream + session_id + reverse stream open + chunking; see §4.4. **Before building, you must verify it fully preserves TCP half-close semantics; if not, patch it or implement that part yourself** |
| Coordination / registry | Redis | Already in the stack (used by Asynq); atomic allocation + TTL, simple and debuggable; see §4.5 |
| K8s interaction | `client-go` (minimal) | The port range is statically reserved (see §6); dynamic exposure is purely application-level. **Do not** create a k8s object per exposure |

**Explicitly rejected (do not adopt):**

- **KubeEdge / treating hosts as K8s nodes** — this project's premise is Docker containers, not pods; and the K8s pod lifecycle cannot cheaply give "power off/on with preserved state."
- **`jhump/grpctunnel`** — it tunnels gRPC calls (gRPC-over-gRPC), not raw TCP. Wrong fit.
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

On connect, the agent opens `Control(stream AgentEvent) returns (stream Command)`. The controller pushes down the `Command` stream: `OpenForward`, `DockerOp` (run/stop/start/pause/unpause/rm), `ExposeRule`. The agent reports handshake, heartbeats, and Docker events up the `AgentEvent` stream.

### 4.3 Metrics: single aggregated /metrics with reverse fan-out

Prometheus **stays in pull mode** and scrapes a **single** target — the controller's `/metrics`. The controller's `/metrics` handler:

1. **Concurrently** reverse-pulls each online agent's node_exporter exposition;
2. Gives each agent an **independent deadline strictly shorter than `scrape_timeout` (default 10s) — use ~5s**; slow/failed agents are skipped and do not block the rest (otherwise one slow agent makes the whole scrape time out and **all** metrics are lost that round);
3. **Merges by MetricFamily**: decode each exposition into a `dto.MetricFamily` with `expfmt.TextParser`, inject an `agent=<id>` label into every series, **merge by metric name into one family** (one HELP/TYPE with all agents' series beneath it), then encode once;
4. Synthesizes `agent_up{agent="<id>"}`: 1 if scraped successfully this round, 0 if timed out/offline — this is the only clean signal for "an agent went down" (to Prometheus there is only one target, so native `up` reflects only the controller).

> **Constraint:** **Never string-concatenate expositions** — duplicate HELP/TYPE lines for the same metric name cause the Prometheus parser to reject the entire payload. You must merge at the MetricFamily level.

### 4.4 Port forwarding (raw TCP, reverse tunnel)

Requirement: dynamically expose a container's internal port (e.g. vLLM listening on `:8080`) to a public port on the cloud side (e.g. `:1025`) so it can serve external traffic. **The protocol is raw TCP (not HTTP) and must support half-close.**

The data plane is **L4 stream proxying** (terminate TCP at each hop, carry only application bytes), **not L3 packet tunneling** — so it inherently avoids TCP-over-TCP degradation.

Stream-open handshake (because the server cannot open a stream to the agent):

1. A public connection lands on the controller's exposed port.
2. The controller pushes `OpenForward{session_id, target}` down that agent's `Control` stream.
3. The agent **opens** a `Forward` stream, first frame carrying `session_id`.
4. The controller **pairs** the public connection with that `Forward` stream by `session_id` and relays bidirectionally.

This handshake + chunking + session pairing is **provided by `openconfig/grpctunnel`** — do not rebuild it.

**Half-close (the correctness crux):** a gRPC stream's own lifecycle (`CloseSend` / handler return) **cannot represent TCP's independent per-direction half-close**. It must be modeled as **explicit frame types** (see §5 `Frame.Type`):

- Local socket read returns EOF → send a `HALF_CLOSE` frame, stop sending on that direction, **but keep reading the other direction**;
- Receive `HALF_CLOSE` → call `conn.(*net.TCPConn).CloseWrite()` on the local socket, keep reading;
- Receive/produce `RESET` → `SetLinger(0)` + `Close()`.

**Backpressure:** never read from the local socket faster than you can `Send` on the stream — rely on `Send` blocking when the HTTP/2 flow-control window is full to apply natural backpressure. **Do not buffer without bound.**

### 4.5 Multi-replica HA and connection affinity

The controller runs multiple replicas (required for HA). **The affinity problem:** an agent's connection is pinned to the **one** pod it dialed; public TCP arriving through the L4 LB lands on **any** replica, and the receiving pod is usually not the one holding that agent.

- **Routing key:** for raw TCP the only key available at ingress is the **destination port** (there is no Host header). Each exposure = one distinct public port.
- **Registry (Redis dual maps):**
  - `port:<P>` → `(agentID, container_target)`;
  - `agent:<id>` → `holding_pod` (written by the pod holding the connection when the agent attaches, **with a TTL refreshed by heartbeat**, deleted on disconnect).
- **Atomic port allocation:** take a free port from a reserved pool atomically via Redis `INCR`/`SETNX`.
- **Routing flow:** a public connection reaches pod B on port P → B looks up `port:P` to get agentX, then `agent:agentX` to get podA.
  - `podA == B`: B drives the §4.4 reverse-open directly.
  - `podA != B`: B forwards the connection to podA over **internal gRPC**, relaying "public conn ↔ internal stream"; podA relays "internal stream ↔ agent Forward stream." **Two byte-pipe hops in series; half-close signaling must propagate end to end.**
- **Note:** because the LB spreads connections, **cross-pod forwarding is the common case, not the exception** (hit rate ≈ 1/N). This path must be solid.
- **Stale-window handling:** after an agent reconnects to a different holder but before the Redis TTL expires, a connection may be forwarded to a pod that no longer holds it → **a pod that receives "forwarded to me but I don't hold agentX" must reject and have the caller re-resolve and retry.**

> gossip/mesh was evaluated and rejected: the state is not contended and needs no consensus; and since Redis is already in the stack, gossip would only add moving parts without actually removing a dependency. Reconsider mesh only if the controller must ship as a **self-contained appliance with zero external dependencies.**

### 4.6 Container lifecycle (power off/on)

"Power off/on" = `docker stop` / `docker start`. Docker containers are **stateful pets**: stop goes SIGTERM → grace period → SIGKILL, the **writable layer is preserved on local disk**, and killing the process frees the GPU; start brings back the same container ID with state intact. **On plain Docker this is free — you do not need the K8s-style upperdir persistence.**

Tie exposure rules to lifecycle: the agent subscribes to Docker events via `client.Events()` —

- on `stop`/`die` → suspend/remove that container's exposure (clear the Redis `port:` mapping or mark it unavailable);
- on `start` → re-resolve the container IP and re-establish exposure (**note: the container IP may change after restart; if the port is re-allocated by the new holder, the public port changes and clients must reconnect**).

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

  // One per forwarded public TCP connection; opened by the agent, first frame carries
  // session_id for pairing. Internal cross-pod forwarding reuses this same service
  // definition (just dialed to a sibling pod).
  rpc Forward(stream Frame) returns (stream Frame);
}

message AgentEvent {
  string agent_id = 1;
  oneof kind {
    Hello hello = 2;          // initial handshake: auth, capability declaration
    Heartbeat heartbeat = 3;  // refresh the TTL of the agent->pod mapping in Redis
    DockerEvent event = 4;    // container start/stop/die reports
  }
}
message Hello { string token = 1; }
message Heartbeat {}
message DockerEvent { string type = 1; string container_id = 2; }

message Command {
  oneof cmd {
    OpenForward open_forward = 1; // tell the agent to open a Forward stream for a session
    DockerOp    docker_op    = 2; // run/stop/start/pause/unpause/rm
    ExposeRule  expose_rule  = 3; // add/remove a port-exposure rule
  }
}
message OpenForward { string session_id = 1; string target = 2; } // target = container-side addr, e.g. 172.30.1.5:8080
message DockerOp    { string op = 1; string container_id = 2; bytes spec = 3; }
message ExposeRule  { string container_target = 1; uint32 public_port = 2; bool remove = 3; }

message Frame {
  string session_id = 1;            // set on the first frame of a Forward stream, for pairing
  enum Type { DATA = 0; HALF_CLOSE = 1; RESET = 2; }
  Type   type = 2;
  bytes  data = 3;                  // valid only when Type == DATA
}
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

### hostlink-controller (cloud)

- Form: a Kubernetes **Deployment, ≥2 replicas** (HA).
- Listeners (the chassis HTTP/2 server multiplexes gRPC and Gin onto each listener; the controller runs two):
  1. an **mTLS gRPC listener** (dedicated `WithTCPListener` + `WithMTLS`) that agents dial out to and establish their `Control`/`Forward` connections on; exposed externally through the ingress;
  2. a **plaintext (h2c) default listener** bound in-cluster only, serving `/metrics` (scraped by Prometheus), `/probe`, `/version`, and `/logging` — no client-cert requirement so K8s probes and Prometheus can reach it.
- Ingress (L4 LoadBalancer, Cilium environment):
  1. the mTLS gRPC port above, for agent dial-out — the chart provides this Ingress (gated on `ingress.host`), and it MUST do L4/TLS passthrough so the controller still terminates mTLS itself (see §9);
  2. a **reserved TCP port range** (e.g. `1025–2025`), all routed to every replica, used for tunnel exposure — "exposing" is an application-level "allocate a port from the pool + write Redis"; **pods already listen on the whole range, so no Service/LB change is needed per exposure**. Design-only: the port-forward/Redis path is not implemented, so the chart does not yet open this range.
- Bypass note: the chassis server applies the same handler to every listener, so the plaintext default listener would also accept gRPC. The split relies on **network-layer isolation** — the default listener is reachable only inside the cluster, while agent gRPC is exposed solely through the mTLS listener via ingress.
- Dependency: Redis (registry + port allocation) — design-only; not yet wired in the controller code.
- Replicas forward to each other over internal gRPC for cross-pod routing (§4.5). That path is design-only and not yet implemented, so the chart ships a single normal (load-balanced) ClusterIP Service — the `/metrics` fan-out is stateless and any replica can answer. Pod-to-pod addressing (a headless Service or equivalent) is to be added only when the cross-pod forward path lands.

> **Infra decision (decoupled from the Go code):** confirm that the Cilium LB / your NLB can handle a port range of the needed size; if not, shrink the range or evaluate the Gateway API `TCPRoute`.

### hostlink-agent (external host)

- Form: a static Go binary (installed as `/usr/local/bin/hostlink-agent`) running as a **systemd service**; the unit and an example config live in `deploy/` (`deploy/hostlink-agent.service`, `deploy/agent.yaml`).
- Configuration: the agent reads all settings from `/etc/humble-mun/agent.yaml` (the chassis registers viper `SetConfigName("agent")` + `AddConfigPath("/etc/humble-mun")`; the binary's `version.Name` is `agent`). The systemd unit passes **no** command-line flags so config edits need no `daemon-reload`, and viper `WatchConfig` reloads the file at runtime. YAML keys are the flag names verbatim (`controller-endpoint`, `controller-tls-ca-path`, `agent-tls-cert-path`, `agent-tls-key-path`, `controller-tls-server-name`, `node-name`); each can also be overridden by an `HM_*` env var.
- Behavior: **dials out** to the controller's public gRPC endpoint with mTLS (the only behavior implemented today is the `Control` stream: `Hello` + heartbeats). Managing the local Docker daemon, carrying tunnels, and the node_exporter sidecar on `127.0.0.1:9100` are design goals (§7) — no code path opens Docker or talks to node_exporter yet, so the systemd unit treats `docker.service` as a soft (`Wants`) ordering dependency, not a hard requirement.
- Network: behind NAT; only needs outbound reachability to the controller.

---

## 7. Current implementation scope (MVP)

In dependency order:

1. `AgentLink` proto + generated code; agent↔controller connection setup, `Hello`/`Heartbeat`, reconnection.
2. Command dispatch and execution: `DockerOp` (run/stop/start/rm) via the docker client; `DockerEvent` reporting.
3. Metrics: node_exporter sidecar + agent local pull; controller `/metrics` concurrent fan-out + MetricFamily merge + `agent_up`.
4. Port forwarding: integrate `openconfig/grpctunnel` (**verify half-close first**); `ExposeRule`/`OpenForward`; Frame relay (with half-close and backpressure).
5. Multi-replica affinity: Redis dual-map read/write + TTL/heartbeat; atomic port-pool allocation; `accept → look up → handle locally / cross-pod two-hop`; reject-and-retry for the stale window.
6. Lifecycle coupling: Docker events drive exposure establish/suspend/re-establish.

---

## 8. Future capabilities (not implemented now, but don't wall off the extension)

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
- **/metrics:** concurrent fan-out, per-agent deadline < `scrape_timeout`, skip slow agents; **never string-concatenate expositions** (§4.3).
- **Multi-replica stale window:** a wrong-pod forward must be rejected and retried (§4.5).
- **Port reallocation:** the public port may change after an agent reconnects, so clients must reconnect — this is an acceptable design tradeoff, but document it in the product docs.
- **`openconfig/grpctunnel` half-close:** before integrating, **verify** that its byte pipe fully preserves half-close semantics; if not, patch it or implement that part yourself.

---

## 10. Coding conventions

- Follow Go community idioms and the Go conventions in the machine-global `~/.claude/CLAUDE.md`.
- Handle errors explicitly, never swallow them; thread `context` for cancellation on concurrent paths; decouple transport/registry/tunnel behind interfaces for testability and swappable implementations.
- Regenerate and commit generated code after proto changes (or pin the protoc version in CI).
```