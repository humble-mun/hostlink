# hostlink

> [English](./README.md)

> ⚠️ **早期阶段 — 尚不可用于生产环境。** 已实现并经过端到端验证的能力：agent↔controller 的 mTLS 连接与 `Control` 流（握手 + 心跳）；通用的请求/响应分发信封（dispatch envelope），及其之上的多组 REST 端点族 —— **Docker 镜像**（`GET`/`POST`/`DELETE /api/v1/agents/<id>/images` —— 列表、带 SSE 流式进度的拉取、删除）、**Docker 容器**（`/api/v1/agents/<id>/containers` —— 列表、以 docker run 语义创建并启动、查看详情、启动/停止/重启、删除，以及支持 `?follow` 的 SSE **日志流**）、agent **工作目录文件系统**（`GET`/`POST`/`PUT`/`DELETE /api/v1/agents/<id>/files` —— 浏览、下载、流式 multipart 上传、递归删除），由 agent 侧 Docker 客户端和一个沙箱化的数据目录提供服务；**指标聚合**端点（`GET /api/v1/metrics`），流式拉取并合并每个 agent 配置的 exporter；基于 Redis 的 `agent→pod` 注册表；经由 `ControllerPeer` 平面的跨 Pod API 中继（多副本 HA），包括对 follow 日志流的取消传播；以及 **raw-TCP 端口转发**（`/api/v1/agents/<id>/forwards` —— 为容器 `ip:port` 分配公网端口、显式半关闭语义、跨 Pod 中继数据面、全副本绑定激活屏障、由 Docker 事件驱动的暂停/恢复）。**已设计但尚未实现**：controller 侧到 Loki 的日志中继 —— 见[路线图](#路线图)。请勿将其部署为真实工作负载的控制面。
>
> 以 **v0.x 预发布版本**形式发布：在 v1.0.0 之前，REST API、线协议与配置都可能在不另行通知的情况下变更。

一个用于管理**云外** Linux 主机（自有机房 / 托管服务器）的控制面。这些主机**不是** Kubernetes 节点；其工作负载以 **Docker 容器**运行，而非 Pod。`hostlink` 为云侧提供一条到每台 NAT 后主机的持久、双向认证的通道，用于命令分发、容器生命周期管理、指标采集和 raw-TCP 端口转发 —— 主机侧无需任何入站连通性。

**规模假设。** 大约十几个 agent，最多几十个。设计取向是*可控、可调试、够用*，而非 web-scale；不要为超大规模集群过度设计。

---

## 目录

- [概览](#概览)
- [工作原理](#工作原理)
- [架构](#架构)
  - [连接模型](#连接模型)
  - [命令通道](#命令通道)
  - [指标反向扇出](#指标两个端点两种职责)
  - [容器日志](#容器日志)
  - [端口转发](#端口转发)
  - [多副本亲和](#多副本亲和)
  - [容器生命周期](#容器生命周期)
- [线协议](#线协议)
- [快速开始](#快速开始)
  - [前置条件](#前置条件)
  - [构建](#构建)
  - [证书（mTLS）](#证书mtls)
- [命令行参数](#命令行参数)
- [部署形态](#部署形态)
- [安全模型](#安全模型)
- [风险与硬性约束](#风险与硬性约束)
- [路线图](#路线图)
- [许可证](#许可证)

---

## 概览

`hostlink` 从同一个 Go module 构建出两个二进制 —— **都叫 `hostlink`**，靠运行位置区分：

- **controller** —— 运行在云端 Kubernetes 内的容器（镜像名 `hostlink`）。控制面本体：gRPC **服务端**，为 agent 终结双向 TLS，聚合指标，分配公网端口，并在副本间路由被转发的 TCP 连接。
- **agent** —— 以 systemd 服务形式运行在每台外部主机上（二进制 `/usr/local/bin/hostlink`）。gRPC **客户端**：向 controller 主动拨号（在 NAT 后无需任何入站防火墙规则即可工作），驱动本地 Docker daemon，上报指标，并承载隧道流量。

> **命名。** 旧名称 `hostlink-controller` 已弃用：controller 只会以容器形式运行在 Kubernetes 内，而 agent 是唯一安装到主机上的 hostlink 组件，因此在各自语境下直接叫 `hostlink` 不会产生歧义。文档统一使用角色名 "controller" / "agent"；两个二进制都读取 `/etc/humble-mun/hostlink.yaml`。

塑造下文一切设计的前提是：**在 HTTP/2 下，服务端无法主动向客户端打开流。** agent 位于 NAT 之后，必须由它发起拨号，因此每一次 "controller → agent" 的交互，都被建模为在 agent 已建立的连接上的一次*反向*流动。

### 范围与边界

`hostlink` 只做**基础设施层** —— 连接、容器编排、指标、端口转发，刻意不涉足其他一切：

- **它做**：为每个 agent 维护一条持久的、mTLS 认证的 gRPC 连接；在这条连接上复用命令、指标与被转发的字节流；驱动 Docker 容器生命周期；把容器端口暴露到云侧公网端口。
- **它不做**：计费、开通、配额或任何其他业务逻辑 —— 这些属于更上层的 Smoothcloud 平台。
- **它不做**：把主机当作 Kubernetes 节点。工作负载是 Docker 容器（保留可写层的"宠物"），不是 Pod。
- **它不做**：目前不构建自己的 L3 overlay。当前范围只有 L4（端口）转发；L3/VPN 方案被明确推迟（见[路线图](#路线图)）。

---

## 工作原理

```
external host (behind NAT)                cloud (Kubernetes)
  hostlink (agent)                  hostlink (controller, ≥2 replicas)
       │                                          │
       │── dial out (mTLS, TLS 1.3) ────────────► │  每个 agent 一条 gRPC 连接
       │                                          │
       │── Control(stream AgentEvent) ──────────► │  长生命周期双向流
       │     Hello (handshake) ─────────────────► │
       │     Heartbeat (refresh TTL) ───────────► │
       │                                          │
       │ ◄──────────── Command (push) ────────── │  AgentRequest (images/containers/fs/…) / OpenForward / ExposeRule
       │                                          │
       │── Forward(stream Frame) ───────────────► │  每条被转发的 TCP 连接一个流
       │     OPEN / DATA / HALF_CLOSE / RESET    │  （由 agent 按需打开）
       │                                          │
```

一条 TCP 连接，多个 HTTP/2 流。agent 连接后只打开一次 `Control` 流；controller 沿其响应方向下推命令。每条被转发的公网连接都拥有自己的 `Forward` 流，因此 gRPC 的按流流控天然提供背压。

> **当前已实现的内容：** mTLS 拨号、`Control` 流及 `Hello`/`Heartbeat` 交换；通用 `AgentRequest`/`AgentResult` 分发信封，及其之上的 Docker **镜像**端点 —— `/api/v1/agents/<id>/images` 上的 `GET`（列表）、`POST`（拉取，SSE 流式进度）、`DELETE`（删除）；`/api/v1/agents/<id>/containers` 上的 Docker **容器**端点 —— 列表、创建并启动（docker run 语义）、查看详情、启动/停止/重启、删除，以及 SSE **日志流**（`.../logs`，支持 `?follow`，客户端断开会通过 `request.cancel` 取消 agent 侧的流）；agent **工作目录文件系统** API（`GET`/`POST`/`PUT`/`DELETE /api/v1/agents/<id>/files`）；**指标聚合**端点（`GET /api/v1/metrics`），反向拉取、打标签并合并每个 agent 配置的 exporter；Redis `agent→pod` 注册表；经由 `ControllerPeer.Dispatch`（一元）、`ControllerPeer.DispatchStream`（流式）、`ControllerPeer.Upload`（客户端流式，用于文件系统写入）的跨 Pod API 请求中继；以及端到端的**端口转发** —— `forwards` REST API 从 `--forward-port-range` 分配公网端口、每个副本上按端口调和的 TCP 监听器、`OpenForward` 反向打开握手配合 `pkg/tunnel` 的半关闭正确字节管道、带陈旧持有者重试的跨 Pod `ControllerPeer.Forward` 中继、全副本绑定**激活屏障**（`pending`/`active`/`suspended`）、由 Docker **事件**驱动的暴露暂停/恢复。controller 侧的 Loki 日志中继尚未实现。见[路线图](#路线图)。

---

## 架构

### 连接模型

agent 是 gRPC **客户端**；controller 拥有公网入口，是 gRPC **服务端**。每个 agent 只维护**一条** gRPC 连接，所有逻辑通道都复用在它的 HTTP/2 流上：

- **命令** —— 一条长生命周期的双向 `Control` 流，由 agent 打开；controller 沿响应方向下推命令。
- **指标** —— controller 通过这条连接反向拉取每个 agent 的指标数据。
- **端口转发** —— 每条被转发的公网连接对应一个 `Forward` 流，由 agent 打开。

> **约束：**"复用一条连接"的意思是**一条 TCP 连接承载多个流** —— 而不是把所有字节塞进单个流再自建子复用层。HTTP/2 流*本身就是*复用机制；在其上再叠加 yamux/smux 是多余的。

> **约束（传输安全）：** agent↔controller 连接是**双向 TLS（mTLS），最低 TLS 1.3，无不安全回退**。agent 出示客户端证书并用 CA bundle 验证 controller；controller 出示服务端证书并强制校验 agent 的客户端证书。mTLS 就是连接层的 agent 身份机制。

### 命令通道

连接建立后，agent 打开 `Control(stream AgentEvent) returns (stream Command)`。controller 沿 `Command` 流下推命令；agent 沿 `AgentEvent` 流上报握手、心跳和 Docker 事件。

大多数 controller→agent 的工作都承载在通用的 **`AgentRequest`** 信封上：`method` 标识操作（`images.*`、`containers.*`、`fs.*`、`metrics.scrape`），`payload` 是方法专属的 JSON，应答（`AgentResult`，流式方法还有 `AgentProgress` 帧）通过 `request_id` 关联。一个即发即弃的 `request.cancel` 元方法可取消进行中的流式请求 —— 当 HTTP 客户端断开时，正是它拆除被 follow 的日志流。容器生命周期就通过这个平面实现（`containers.*` 系列方法）；proto 中的 `DockerOp` 命令是未使用的历史占位。`OpenForward` 位于信封之外：它通知 agent 拨号目标地址并打开 `Forward` 流（见[端口转发](#端口转发)）。`ExposeRule` 仍是未使用的占位。

### 指标：两个端点，两种职责

Prometheus 保持**拉取模式**。controller 暴露**两个**彼此独立的端点（都由集群内明文监听器提供）：

- `/metrics`（由 chassis 框架托管）—— controller **自身**的进程指标。
- `GET /api/v1/metrics` —— **云侧聚合所有 agent 的上游 exporter** —— 即反向扇出。

当 `/api/v1/metrics` 被抓取时，controller 会：

1. 枚举在线的 agent 群（Redis `agent→pod` 目录；内存模式下即本地持有的 agent），并以**有界并发**向每个 agent 分发流式的 `metrics.scrape` —— 走本地 `Control` 流，或经 `ControllerPeer.DispatchStream` 中继给持有它的 Pod；
2. 每个 agent 在本地拉取其配置的 `scrape-targets`（node_exporter、dcgm-exporter 等），并把每份指标数据**分块流式**回传，agent 内存占用有界，且不受单条消息大小限制；
3. 给每个 agent 一个**短于 `scrape_timeout` 的独立超时**（`--agent-scrape-timeout`，默认 5s，对应 Prometheus 默认的 10s）；慢或离线的 agent 被跳过，只贡献 `agent_up 0`；
4. **按 `MetricFamily` 合并** —— 用 `expfmt.NewTextParser(model.UTF8Validation)` 解析每份数据，向每条序列注入 `agent=<id>` 和 `exporter=<name>` 标签，把同名指标的序列折叠进同一个 family（一份 HELP/TYPE），再一次性编码输出；
5. 合成 `agent_up{agent="<id>"}`（1 = 本轮流成功完成，0 = 离线/超时）和 `hostlink_scrape_target_up{agent,exporter}`（各 exporter 的健康状况）。`agent_up` 是唯一干净的"某个 agent 掉线了"信号，因为 Prometheus 原生的 `up` 只反映 controller 端点本身。

> **约束：** **绝不能字符串拼接指标数据。** 同一指标名重复的 HELP/TYPE 行会让 Prometheus 解析器拒绝整个响应。必须在 `MetricFamily` 层面合并。

> 刻意使用 `exporter` 标签（而非 `job`/`instance`），使注入的标签**无需**在 Prometheus 抓取配置中开启 `honor_labels` 也能保留。exporter 以**独立的 sidecar 二进制**运行；agent 在本地 GET 它们（例如 `127.0.0.1:9100`）。**不要**把 node_exporter 作为库引入 —— 它的 collector 包不是稳定的公开 API。

### 容器日志

`GET /api/v1/agents/<id>/containers/<containerId>/logs` 以 **SSE** 流式返回容器日志：每行日志一个 `data:` 事件（`{"stream":"stdout"|"stderr","line":"..."}`），以 `{"done":true,...}` 结束。查询参数与 `docker logs` 对应：`follow` 保持流打开以接收新输出，`tail` 限定初始回溯行数，`since` 限定起始时间，`timestamps` 给每行加时间戳前缀。agent 会检查容器的 TTY 模式来选择正确的线格式（raw 或 `stdcopy` 多路复用），解复用 stdout/stderr，并以行为单位封帧成 `AgentProgress` 事件，走**无损**流类别传输（不丢行，受背压约束）。

follow 的流是无界的，因此拆除是显式的：当 HTTP 客户端断开（或请求被以其他方式放弃）时，controller 沿该 agent 的 `Control` 流发送即发即弃的 `request.cancel`，agent 随即取消底层的 `docker logs` follow。这一机制可跨 Pod 传播 —— 取消 `ControllerPeer.DispatchStream` 中继会让持有该 agent 的 Pod 发出取消。

> **日志收集进 Loki（已定方案，尚未实现）。** agent 主机位于**受限网络，唯一被允许的路径是 agent→controller 的 mTLS 连接**，因此每机日志代理（Alloy/promtail 直接推送 Loki）不可行。已定的设计是经 controller 中继日志：controller 侧的日志中继为它持有的每个 agent 的容器 follow `containers.logs`，打上 `{agent_id, container_name}` 标签，并把日志行推送到集群内 Alloy 的 `loki.source.api` 端点（批处理/重试/relabel 进 Loki 由 Alloy 负责）。待定问题：重连后基于 `since` 的续传、agent 在副本间重连时中继归属的跟随、共享 Control 流上的带宽预算。见[路线图](#路线图)。

### 端口转发

目标：把容器内部端口（如 vLLM 的 `:8080`）动态暴露为云侧公网端口（如 `:1025`），走 **raw TCP** 并支持半关闭。数据面是 **L4 流代理**（在每一跳终结 TCP，只搬运应用层字节）—— 天然避免 TCP-over-TCP 劣化。

由于服务端无法主动向 agent 打开流，流的建立握手是反向的：

1. 一条公网连接到达 controller 暴露的端口。
2. controller 沿该 agent 的 `Control` 流下推 `OpenForward{session_id, target}`。
3. agent **打开**一个 `Forward` 流，其首帧是携带 `session_id` 的类型化 `OPEN` 帧（本地拨号失败则改发携带该 `session_id` 的 `RESET`）。
4. controller 按 `session_id` 把公网连接与该 `Forward` 流**配对**，双向中继。

这套握手 + 分块 + 会话配对在仓库内实现：`pkg/tunnel` 是字节管道，controller 按 `session_id` 配对流。[`openconfig/grpctunnel`](https://github.com/openconfig/grpctunnel) 曾被评估，最终弃用，改为这个小而全测试覆盖的实现。

> **约束（半关闭，正确性的关键）：** gRPC 流自身的生命周期（`CloseSend` / handler 返回）无法表达 TCP 每方向独立的半关闭。改用**显式帧类型**建模：本地读到 EOF → 发送 `HALF_CLOSE`，停止该方向发送但继续读另一方向；收到 `HALF_CLOSE` → 对本地 socket 执行 `CloseWrite()`；收到 `RESET` → `SetLinger(0)` + `Close()`。`pkg/tunnel` 精确实现了这份契约，其半关闭语义由测试锁定 —— 改动它时必须保持语义不变。

> **约束（背压）：** 从本地 socket 读取的速度绝不能超过 `Send` 的速度。依赖 HTTP/2 流控窗口占满时 `Send` 的阻塞。不得无界缓冲。

> **实现状态：已端到端实现。** `--forward-port-range`（chart 中为 `portForward.range`）预留公网端口池；每个副本绑定每个已暴露端口，并从共享存储调和其监听器（5s 周期 + pub/sub 触发）；处于 suspended 状态的暴露对新连接立即回 RST。

### 多副本亲和

controller 以 ≥2 副本运行以实现 HA。agent 的连接固定在它拨号到的那个 Pod 上，但经 L4 负载均衡进来的公网 TCP 会落到**任意**副本 —— 通常不是持有该 agent 的那个。

- **路由键：** 对 raw TCP 而言，唯一的入口路由键是**目标端口**。每个暴露 = 一个独立公网端口。
- **注册表（Redis 双映射）：** `hostlink:port:<P>` → JSON `{agent_id, target, container_id?, suspended?}`（无 TTL —— 暴露在 agent 反复上下线之间持续存在，直到被删除）；`hostlink:agent:<id>` → `holding_pod`（agent 接入时写入，**由心跳刷新 TTL**，断开时删除）。
- **原子端口分配：** 在预留范围内用批量 `MGET` 找空闲端口候选，再用 `SETNX` 原子抢占（单副本模式为内存存储）。
- **路由流程：** 公网连接到达 Pod B 的端口 P → B 查 `port:P` → agentX，再查 `agent:agentX` → podA。若 `podA == B`，B 直接驱动反向打开；否则 B 向 podA 打开 `ControllerPeer.Forward`，首帧为 `OPEN{session_id, open:{agent_id, target}}` —— podA 将其与 agent 的 `Forward` 流配对，并在 B 读取任何公网字节**之前**应答 `READY`（先重试后读取）。两段字节管道串联；半关闭端到端传播。
- **陈旧窗口：** 由于 LB 会打散连接，跨 Pod 转发是**常态**（命中率 ≈ 1/N）。收到"转发给我但我并不持有 agentX"的 Pod 会以 `FAILED_PRECONDITION` **拒绝**；调用方重新解析持有者一次并重试。
- **激活屏障：** kube-proxy 不会对 connection-refused 的后端重试，因此选中全部 controller Pod 的 Service 在**每个**存活副本都绑定端口 P 之前不得包含它。每个副本上报 `hostlink:bound:<P>:<pod>`（30s TTL），并维护存活键 `hostlink:controller:<pod>`（45s TTL）；只有所有存活 Pod 都上报绑定后转发才是 `active`，否则为 `pending`，容器停止期间为 `suspended`。

> Redis 本就在技术栈里。Raft/gossip/mesh 均被评估并否决：`agent→pod` 不是有竞争的状态，不需要共识。

### 容器生命周期

"关机 / 开机" = `docker stop` / `docker start`。Docker 容器是**有状态的宠物**：stop 是 SIGTERM → 宽限期 → SIGKILL，可写层保留在本地磁盘，杀掉进程即释放 GPU；start 恢复的是同一个容器 ID，状态原样保留。在原生 Docker 上这是免费能力 —— 无需 K8s 风格的 upperdir 持久化。

暴露规则通过 `client.Events()` 与生命周期联动：

- `stop`/`die` 时 → **暂停**该容器的暴露：映射被标记为 `suspended`，端口与监听器保留，新连接立即收到 RST。
- `start` 时 → 重新 inspect 容器、重新解析其网络 IP、改写映射目标并解除暂停 —— 公网端口不变。

> 容器 IP 在重启后可能变化；若端口被新的持有者重新分配，则**公网端口会变化，客户端必须重连** —— 这是已接受的设计取舍。GPU 容器要记得 nvidia runtime；`docker pause`/`unpause`（freezer cgroup，保留 RAM/VRAM）是另一种"挂起、瞬时恢复"的语义。

---

## 线协议

服务定义在 `pkg/api/hostlink/v1/`。`AgentLink`（agent↔controller）有两个双向流 RPC；`ControllerPeer`（controller↔controller）有三个请求中继 RPC（一元、服务端流式、客户端流式），外加第四个双向 `Forward` RPC 作为跨 Pod 端口转发数据面。

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

| 消息 | 方向 | 用途 |
| --- | --- | --- |
| `AgentEvent{Hello / Heartbeat / DockerEvent / AgentResult / AgentProgress}` | agent → controller | 握手、TTL 刷新、容器上报、对 `AgentRequest` 的应答，或长任务的进度帧。 |
| `Command{OpenForward / DockerOp / ExposeRule / AgentRequest / AgentRequestChunk}` | controller → agent | 打开转发流、执行 Docker 操作、增删暴露规则、通用的按方法分发请求，或流式上传的一个 body 分块。 |
| `AgentRequest{request_id, method, payload}` / `AgentResult{request_id, code, payload, error, final}` | 双向 | 通用 API 调用：`method` 标识操作（如 `images.list`），`payload` 为 JSON，`code` 对应 HTTP 状态码，以 `request_id` 关联；`final` 标记流式应答的终止帧。 |
| `AgentRequestChunk{request_id, data, last}` | controller → agent | 流式上传请求（如 `fs.write`）的一个 body 分块，以 `request_id` 关联到其起始 `AgentRequest`；`last` 标记最后一块。 |
| `AgentProgress{request_id, payload}` | agent → controller | 长任务 `AgentRequest` 的非终止进度更新（如 `images.pull` 的分层状态、`fs.read` 的文件字节）；`payload` 为方法专属。操作仍以 `final` 的 `AgentResult` 收尾。 |
| `DispatchRequest{agent_id, AgentRequest}` | controller → controller | 为 `ControllerPeer` 中继跳包装 `AgentRequest` 并附上路由键（`Dispatch` 一元，`DispatchStream` 流式）。 |
| `UploadFrame{open / chunk, last}` | controller → controller | 经 `ControllerPeer.Upload` 中继 controller→agent 的上传：首帧携带 `DispatchRequest`（open），其余为 body 分块。 |
| `Frame{session_id, type, data, open}` | 双向（每个转发） | 原始 TCP 字节；`type` ∈ `DATA`/`HALF_CLOSE`/`RESET`/`OPEN`/`READY`。流的首帧是携带 `session_id` 的 `OPEN`（`READY` 用于确认 peer 跳；首帧为 `RESET` 则表示拨号失败）。 |
| `PeerForwardOpen{agent_id, target}` | controller → controller | `ControllerPeer.Forward` 中继首个 `OPEN` 帧上的路由载荷：向哪个 agent 反向打开、让它拨号哪个 `ip:port`。 |

`Frame.Type` 枚举正是实现正确 TCP 半关闭的关键（见[端口转发](#端口转发)）。任何 `.proto` 变更后都要重新生成并提交生成代码。

### REST API

controller 在其集群内默认监听器上提供一小组 REST 接口，构建在上述通用分发信封之上：

| 方法与路径 | 说明 |
| --- | --- |
| `GET /api/v1/agents` | 列出已连接的 agent。 |
| `GET /api/v1/agents/<agentId>/images` | 列出 Docker 镜像。向 agent 的 `Control` 流分发 `images.list`（本地或经 `ControllerPeer.Dispatch` 中继）；原样返回 agent 的 JSON；若 agent 未连接到任何可达副本则返回 404。 |
| `POST /api/v1/agents/<agentId>/images` | 拉取镜像。JSON body 为 `{"image":"<ref>","auth":{...可选的 registry 认证...}}`；响应为 **`text/event-stream`**（SSE）：每个事件是 `data: <PullProgress JSON>`（`id`/`status`/`current`/`total`/`progress`），以 `data: {"done":true,"code":...,"error":...}` 结束。分发流式 `images.pull`（本地或经 `ControllerPeer.DispatchStream`）；分发无超时（大镜像拉取需要数分钟）；agent 不可达返回 404。 |
| `DELETE /api/v1/agents/<agentId>/images/<imageId>` | 按 ID 删除单个镜像（路径参数；适用于不含 `/` 的镜像 ID 或 digest）。可选 `?force=true`、`?noPrune=true`。分发 `images.remove`；返回 `RemoveResult` JSON `{"deleted":[...],"errors":[...]}`（部分失败会报告，不视为致命）。 |
| `DELETE /api/v1/agents/<agentId>/images?ref=A&ref=B` | 通过重复的 `ref` 查询参数删除多个镜像（用于含 `/` 的完整 `repo/path:tag` 引用）。选项与响应同上。 |
| `GET /api/v1/agents/<agentId>/containers` | 列出容器（`containers.list`）；`?all=true` 包含已停止的（docker ps -a）。 |
| `POST /api/v1/agents/<agentId>/containers` | 创建**并启动**容器（docker run 语义，`containers.create`）。JSON body：`image`（必填，须已拉取），可选 `name`、`cmd`、`entrypoint`、`env`（`KEY=value` 字符串）、`workingDir`、`labels`、`ports`（`{containerPort, protocol?, hostIp?, hostPort?}`）、`binds`（`/host:/container[:opts]`）、`networkMode`/`pidMode`（如 `"host"`）、`restartPolicy`（+ `maxRetryCount`）、`autoRemove`。返回 `201` 及 `{"id":"...","warnings":[...]}`。 |
| `GET /api/v1/agents/<agentId>/containers/<containerId>` | 查看容器详情（`containers.inspect`）：身份、状态、生效的运行配置；未知容器返回 `404`。 |
| `GET /api/v1/agents/<agentId>/containers/<containerId>/logs` | 以 **SSE** 流式返回日志（`containers.logs`）：每行一个事件 `{"stream","line"}`，以 `{"done":true,...}` 结束。选项与 docker logs 对应：`?follow=true`、`?tail=100`、`?since=<RFC3339|unix>`、`?timestamps=true`。客户端断开会取消 agent 侧的流（`request.cancel`），包括跨 Pod 场景。 |
| `POST /api/v1/agents/<agentId>/containers/<containerId>/start` | 启动已停止的容器（`containers.start`）；`204`。 |
| `POST /api/v1/agents/<agentId>/containers/<containerId>/stop` | 停止运行中的容器（`containers.stop`）；可选 `?timeout=<seconds>` 指定 daemon 强杀前的宽限期；`204`。 |
| `POST /api/v1/agents/<agentId>/containers/<containerId>/restart` | 重启容器（`containers.restart`），同样支持 `?timeout`；`204`。 |
| `DELETE /api/v1/agents/<agentId>/containers/<containerId>` | 删除容器（`containers.remove`）；`?force=true` 可删除运行中的（docker rm -f），`?volumes=true` 一并删除匿名卷；未加 force 删除运行中容器返回 `409`。 |
| `GET /api/v1/agents/<agentId>/files?path=<p>` | 浏览 agent 工作目录（`--data-dir`）。目录返回 JSON `{"entries":[{"name","dir","size","modTime"}]}`（非递归）；文件则**以下载方式流式返回**（`Content-Disposition`），或带 `Accept: application/json` 时返回 `FsEntry` 元数据。`path` 为空 = 工作目录根。分发 `fs.stat`，再按需 `fs.list`/`fs.read`。 |
| `POST /api/v1/agents/<agentId>/files?path=<p>` | 带 `&dir=true` 时创建目录（`fs.mkdir`，已存在返回 `409`）；否则从 **multipart 表单**上传一个或多个文件，分块流式传输（`fs.write`），以**独占方式**创建 —— 目标已存在按文件逐个报告。响应 `{"written":[...],"errors":[...]}`。 |
| `PUT /api/v1/agents/<agentId>/files?path=<p>` | 用请求体覆盖单个文件（原始字节或第一个 multipart part）；经 `fs.write` 流式写入（截断）。 |
| `DELETE /api/v1/agents/<agentId>/files?path=<p>` | 删除路径，目录**递归删除**（`fs.remove`）。 |
| `POST /api/v1/agents/<agentId>/forwards` | 分配一个公网端口，把 raw TCP 转发到容器 `ip:port`。JSON body 为 `{"target":"<ip:port>","container_id":"..."}`（`container_id` 可选，用于把转发关联到容器生命周期事件）。返回 `201` 及映射信息（含 `port` 与 `state`；初始为 `pending`，应轮询至 `active` 后再接入外部基础设施）。预留范围耗尽返回 `409`；未设置 `--forward-port-range` 返回 `503`。 |
| `GET /api/v1/agents/<agentId>/forwards` | 列出该 agent 的转发，每条带 `state`：`pending`（尚未在每个存活副本上绑定）、`active`（可安全对外暴露）、`suspended`（容器已停止；连接被拒绝）。 |
| `GET /api/v1/forwards` | 列出所有 agent 的全部转发。 |
| `DELETE /api/v1/forwards/<port>` | 释放被转发的公网端口；`204`，未知端口返回 `404`。 |

> `files` 端点双向均以 64 KB 分块流式传输（下载经 `AgentProgress` 帧，上传经 `AgentRequestChunk` / `ControllerPeer.Upload` 中继），大文件传输内存占用有界。agent 把每个 `path` 都解析到其工作目录之内，拒绝越界的路径穿越（`..`）。

---

## 快速开始

### 前置条件

| 组件 | 版本 / 说明 |
| --- | --- |
| Go（仅构建需要） | 1.26+ |
| Controller 运行时 | Kubernetes（StatefulSet；默认 1 副本，HA 需 ≥2） |
| Agent 运行时 | 一台可*出站*访问 controller 的 Linux 主机；systemd |
| Docker（每台主机） | 必需 —— 工作负载是 Docker 容器；GPU 需 nvidia runtime |
| Redis | `agent→pod` 注册表 + 端口转发暴露映射 —— 单副本可选，**HA 必需**（`replicaCount > 1`） |
| cert-manager + CSI driver | 可选 —— 按 Pod 签发 controller/peer 证书，替代挂载 Secret |
| node_exporter | 每台主机上的 sidecar，监听 `127.0.0.1:9100` |

本项目**仅支持 Linux**（它管理 Linux 的 Docker daemon，转发实现使用 Linux 特有的 socket 语义），不支持在 Windows 上原生构建或运行；请在 Linux 工具链内构建。

### 构建

单一 Go module，`cmd/` 下每个子目录对应一个二进制，依赖已 **vendor** —— 可用 `-mod=vendor` 离线构建：

```bash
go build -mod=vendor -o bin/controller/hostlink ./cmd/controller
go build -mod=vendor -o bin/agent/hostlink      ./cmd/agent
```

> 两个二进制都叫 `hostlink`（各自独立的输出目录用于区分本地构建产物）；生产环境中 controller 以 `hostlink` 容器镜像交付（`make build`），agent 以 `hostlink` 主机二进制交付（`make agent`）。

在 Windows 开发机上，请在 Linux 容器内编译（工作目录与 Go 缓存以 bind mount 挂入，容器提供 Linux 工具链）：

```bash
docker run --rm \
  -v "$PWD":/go/src/github.com/humble-mun/hostlink \
  -w /go/src/github.com/humble-mun/hostlink \
  golang:1.26.3-trixie go build -mod=vendor -v ./...
```

### 证书（mTLS）

agent↔controller 连接需要一套可用的 PKI：一个 CA、一张 controller（服务端）证书、每个 agent 一张（客户端）证书：

| 侧 | 出示 | 校验对端所用 |
| --- | --- | --- |
| controller | 服务端证书/私钥（`--grpc-tls-cert-path` / `--grpc-tls-key-path`） | agent CA bundle（`--grpc-tls-ca-path`） |
| agent | 客户端证书/私钥（`--agent-tls-cert-path` / `--agent-tls-key-path`） | controller CA bundle（`--controller-tls-ca-path`） |

controller 强制校验 agent 的客户端证书（`RequireAndVerifyClientCert`）；agent 校验 controller 的 server name（`--controller-tls-server-name`）。该项留空时，gRPC 会按拨号端点的主机名校验，因此只要证书 SAN 与拨号地址不一致（例如按 IP 拨号），就应显式设置。**没有不安全回退** —— 证书缺失或无效时连接直接失败。

**证书热重载。** 所有 TLS 材料（服务端/客户端证书、私钥、CA bundle）在文件被原地轮换时会自动从磁盘透明重载 —— 新材料在下一次握手时生效，**无需重启进程**。覆盖范围包括面向 agent 的 gRPC 监听器、ControllerPeer 平面（其服务端与客户端两侧）以及 chassis 默认监听器。因此由 **cert-manager**（或 cert-manager CSI driver）签发的短期证书会在续期后被自动拾取；长期运行的 controller 或 agent 永远不会继续使用过期的旧证书。重载失败（如文件暂时缺失或格式损坏）会被记录日志，并继续使用之前已加载的材料。

---

## 命令行参数

所有参数也可通过环境变量设置：参数名大写、`-` 换成 `_`、加 `HM_` 前缀（如 `--controller-endpoint` → `HM_CONTROLLER_ENDPOINT`）。配置也可以放在 YAML 文件 `/etc/humble-mun/hostlink.yaml`（文件名跟随二进制的 `version.Name`，controller 与 agent 均为 `hostlink`），以参数名为键；文件会被监视并在运行时重载。优先级：参数 > 环境变量 > 配置文件。

### Agent 参数

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `--controller-endpoint` | — | 要拨号的 controller gRPC 端点地址，格式 `host:port`（必填）。 |
| `--agent-tls-cert-path` | — | agent 向 controller 出示的 mTLS 客户端证书。 |
| `--agent-tls-key-path` | — | 与客户端证书匹配的私钥。 |
| `--controller-tls-ca-path` | — | 用于校验 controller 证书的 CA bundle。 |
| `--controller-tls-server-name` | — | 校验 controller 证书所用的 server name；留空时 gRPC 按拨号端点主机名校验，证书 SAN 与拨号地址不一致时应显式设置。 |
| `--data-dir` | — | `files` API 提供服务的工作目录（浏览/下载/上传/删除）。留空则禁用 `files` API。 |

> `scrape-targets`（agent 用于指标扇出的上游 exporter 列表）是结构化 YAML 列表，只能通过配置文件设置 —— 其格式说明见 `deploy/hostlink.yaml`。

### Controller 参数

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `--grpc-bind-address` | — | 面向 agent 连接的 mTLS gRPC 监听器绑定地址，格式 `host:port`。 |
| `--grpc-tls-cert-path` | — | controller 向 agent 出示的 mTLS 服务端证书。 |
| `--grpc-tls-key-path` | — | 与服务端证书匹配的私钥。 |
| `--grpc-tls-ca-path` | — | 用于校验 agent 客户端证书的 CA bundle。 |
| `--redis-url` | — | 支撑跨 Pod `agent→pod` 注册表的 Redis URL；留空 = 内存式单副本模式。支持 standalone（`redis://`）、sentinel（`redis+sentinel://host?master_name=...&addr=...`）、cluster（`redis+cluster://host?addr=...`）三种拓扑。 |
| `--peer-bind-address` | — | 集群内 ControllerPeer mTLS 监听器（跨 Pod 中继用）的绑定地址；留空 = 禁用 peer 平面。 |
| `--peer-advertise-address` | — | 兄弟副本拨号本 Pod peer 监听器所用的地址（作为注册表值写入）；跨 Pod 模式下必填。 |
| `--peer-tls-cert-path` / `--peer-tls-key-path` / `--peer-tls-ca-path` | — | ControllerPeer 平面的 mTLS 材料（controller 既是服务端也是客户端）。 |
| `--peer-tls-server-name` | — | 中继时校验兄弟副本所用的 server name；留空 = 使用所拨 peer 的主机名。 |
| `--agent-scrape-timeout` | `5s` | `GET /api/v1/metrics` 扇出中每个 agent 的超时；应低于 Prometheus 的 `scrape_timeout`。 |
| `--grpc-max-recv-msg-size` | 16 MiB | 从 agent 或兄弟副本接收的单条 gRPC 消息上限（针对大体积一元结果；流式方法分块传输，不受影响）。 |
| `--forward-port-range` | — | 为端口转发预留的公网 TCP 端口范围，格式 `from-to`（如 `1025-2025`）或单个端口。每个副本会绑定该范围内每个已分配端口。留空 = 禁用端口转发（`forwards` API 返回 503）。 |

> `--redis-url` 与 `--peer-bind-address` 是同一个跨 Pod 开关的两半：要么两者都设（外加 `--peer-advertise-address`），要么都不设。半配置状态下 controller 拒绝启动。

controller 还继承了 chassis HTTP 服务器的参数（`--http-bind-address`、`--tls-cert-path`、`--tls-key-path`），用于其**默认监听器**。默认监听器的证书/私钥留空即以明文 h2c 服务集群内的探针与指标流量；上文的 mTLS gRPC 监听器独立配置，经 ingress 对外暴露。

---

## 部署形态

### Controller（云端；容器镜像 `hostlink`）

- **形态：** Kubernetes **StatefulSet**（chart 默认 `replicaCount: 1`）。chart 位于 `charts/hostlink/`（`helm install <release> charts/hostlink`），渲染出：承载 `/etc/humble-mun/hostlink.yaml` 的 ConfigMap、StatefulSet、负载均衡的 ClusterIP Service（gRPC + 集群内 HTTP 端口）、用于稳定按 Pod DNS 的 **headless `<release>-peer` Service**，以及在设置了 `ingress.host` 时面向 agent 的 gRPC Ingress。**HA 需设置 `replicaCount > 1`，且要求 `redis.url` + `peer.enabled`**（否则 chart 安装直接失败 —— 半配置的多副本 controller 会对兄弟 Pod 持有的 agent 静默返回 404）。
- **三个监听器**（chassis HTTP/2 服务器在每个监听器上复用 gRPC 与 Gin —— HTTP/2 且 `Content-Type: application/grpc` 的请求路由到 gRPC 服务器，其余走 Gin）：
  1. **mTLS gRPC 监听器**（`--grpc-bind-address` + `WithTLSCert` + `WithMTLS`），agent 向其拨号；经 ingress 对外暴露。
  2. **明文（h2c）默认监听器**，仅绑定在集群内，提供 REST API（`/api/v1/...`，含聚合 agent 指标的 `/api/v1/metrics`）、controller 自身的 `/metrics`、`/probe`、`/version`、`/logging`。
  3. （`peer.enabled` 时）**集群内 ControllerPeer mTLS 监听器**，运行在独立的 `grpc.Server` 上 —— 与共享的 chassis 服务器分离，确保中继平面永远无法从面向 agent / ingress 的监听器触达。
- **Ingress（L4 LoadBalancer）：** 供 agent 拨号的 mTLS gRPC 端口。由于 controller 自行终结 mTLS，Ingress 必须做 L4/TLS **透传**（passthrough，通过 controller 专属注解显式声明）—— 若在 Ingress 终结 TLS 会剥掉 agent 客户端证书，破坏身份模型。隧道暴露用的预留 TCP 端口范围经 `portForward.range` 配置；开启 `portForward.service.enabled` 时 chart 会渲染专门的 `<release>-forward` Service，逐个枚举范围内每个端口（Kubernetes Service 端口是列表不是范围 —— 注意云 LB 的监听器配额）。
- **依赖：** **Redis** 支撑 `agent→pod` 注册表、`port:<P>` 暴露映射，以及转发激活屏障背后的绑定/存活键（单副本可选，HA 必需）；跨 Pod API 请求中继走 ControllerPeer 平面，跨 Pod 端口转发数据面走 `ControllerPeer.Forward`。
- **证书：** 按平面分别提供，来自挂载的 Secret（`grpc.tlsSecretName` / `peer.tlsSecretName`），或开启 `certManager.enabled` 后由 **cert-manager CSI driver** 按 Pod 从配置的 `Issuer`/`ClusterIssuer` 签发（`certManager.issuerKind` / `issuerName`）。

> **旁路提示。** chassis 服务器对每个监听器应用同一个 handler，因此明文默认监听器同样能接受 gRPC。这种切分依赖**网络层隔离** —— 默认监听器只在集群内可达，而 agent gRPC 只经 ingress 通过 mTLS 监听器对外暴露。

#### 指标抓取

controller 在集群内 HTTP Service（`http` 端口）上暴露**两条**抓取路径：`/metrics`（controller 自身进程指标）和 `/api/v1/metrics`（聚合的 agent-exporter 扇出，见 §4.3）。chart **不**附带抓取配置 —— 按你的监控栈自己的方式接入，指向 `http` Service 端口即可。示例：

- **Prometheus 基于注解的发现**（每条路径一个 target）：

  ```yaml
  prometheus.io/scrape: "true"
  prometheus.io/port: "8080"
  prometheus.io/path: "/api/v1/metrics"   # add a second scrape for /metrics
  ```

- **Prometheus Operator `ServiceMonitor`** —— 在 `http` 端口上配两个 `endpoints`，分别 `path: /metrics` 和 `path: /api/v1/metrics`。
- **VictoriaMetrics `VMServiceScrape`** —— 同样形态：两个 `endpoints`（或用 `VMPodScrape`），指向 `http` 端口和这两条路径。

Prometheus 任务的 `scrape_timeout` 要保持**高于** controller 的 `--agent-scrape-timeout`（默认 5s），让扇出内部的按 agent 超时先触发。`/api/v1/metrics` 的序列已带 `agent`/`exporter` 标签，无需 `honor_labels`。

### Agent（外部主机；二进制 `hostlink`）

- **形态：** 静态 Go 二进制（`/usr/local/bin/hostlink`），以 **systemd 服务**运行。unit 文件和示例配置随仓库提供于 `deploy/`（`deploy/hostlink.service`、`deploy/hostlink.yaml`）。
- **配置：** agent 从 `/etc/humble-mun/hostlink.yaml` 读取全部设置（chassis viper `SetConfigName("hostlink")` + `AddConfigPath("/etc/humble-mun")`；YAML 键就是参数名原文，均可被 `HM_*` 环境变量覆盖）。systemd unit **不**传任何命令行参数，且 viper `WatchConfig` 会在运行时重载文件，因此改配置既不需要 `systemctl daemon-reload` 也不需要改 unit。
- **行为：** 通过 mTLS 拨号 controller 的公网 gRPC 端点并运行 `Control` 流（`Hello` + 周期 `Heartbeat`），**进程内**重连（指数退避 + 抖动，外加 HTTP/2 keepalive），因此 controller 重新部署无需 agent 进程重启即可平稳度过。它通过惰性初始化的 `client.FromEnv` Docker 客户端处理 controller 下推的 `AgentRequest`：Docker **镜像**方法（`images.list`/`images.pull`/`images.remove`）、**容器**方法（`containers.list`/`containers.create`/`containers.inspect`/`containers.start`/`containers.stop`/`containers.restart`/`containers.remove`/`containers.logs`），以及作用于配置的 `--data-dir` 工作目录的**文件系统**方法（`fs.stat`/`fs.list`/`fs.read`/`fs.write`/`fs.mkdir`/`fs.remove`）。它还监视 Docker 容器生命周期事件（`start`/`die`/`stop`）并沿 `Control` 流上报（驱动暴露的暂停/恢复），并承载被转发的 TCP：收到 `OpenForward` 时拨号目标并打开 `Forward` 流。`127.0.0.1:9100` 上的 node_exporter sidecar 是指标扇出的部署预期。由于 Docker 客户端是惰性的，unit 把 `docker.service` 视为软性（`Wants`）顺序依赖，而非硬性要求。
- **网络：** 位于 NAT 后；只需要到 controller 的**出站**可达性。

---

## 安全模型

这是一个 GPU 平台；主机上可能运行**不受信任的客户代码**。暴露端口 —— 以及未来让云侧获得进入容器的 L3 可达性 —— 会显著扩大攻击面。

- **agent 身份（连接层）：** mTLS，最低 TLS 1.3，无不安全回退。这是 agent 向 controller 认证（以及反向认证）的方式。明文默认监听器（探针 / 指标）必须保持**仅集群内可达**，否则共享的 chassis handler 在那里同样会接受 gRPC。
- **默认拒绝：** 白名单方向与端口，并做审计。ACL 方案必须在引入任何 VPN / L3 能力*之前*想清楚。

---

## 风险与硬性约束

以下条目**不可协商** —— 实现时不得偏离。

- **mTLS，无回退** —— agent↔controller 永远是双向 TLS 1.3+。默认监听器保持仅集群内可达。
- **禁止 TCP-over-TCP** —— L4 代理终结 TCP、只搬运字节；**不得**改为在 TCP 上做 L3 包隧道。
- **半关闭必须显式** —— 转发实现为分块 + 显式 `HALF_CLOSE`/`RESET` 帧 + 背压。`pkg/tunnel` 实现了这份契约且由测试锁定；改动时保持语义不变。
- **绝不拼接指标数据** —— 在 `MetricFamily` 层面合并；按 agent 的抓取超时 < `scrape_timeout`；跳过慢的 agent。
- **拒绝陈旧转发** —— 发错 Pod 的转发必须被拒绝并重试。
- **一条 TCP 连接，多个流** —— 永远不要在 HTTP/2 之上再造子复用层。
- **在当前规模下接受队头阻塞** —— 命令、指标、被转发的字节共享一条 TCP 连接；丢包时所有流一起停顿。若高吞吐流量（如 vLLM）拖累控制面延迟，就把数据面挪到独立的 `ClientConn`（同一服务/认证/端点，独立 TCP），并考虑 QUIC。
- **端口重分配对客户端可见** —— agent 重连后公网端口可能变化；要写进产品文档。

### 明确否决（不得采用）

- **KubeEdge / 把主机当 K8s 节点** —— 前提是 Docker 容器，不是 Pod。
- **`jhump/grpctunnel`** —— 它隧道的是 gRPC-over-gRPC，不是 raw TCP，方向不符。（`openconfig/grpctunnel` 也评估过，最终弃用，改为仓库内小巧且带显式半关闭的 `pkg/tunnel` 管道。）
- **自研 tun 设备 + 用户态网络栈** —— 当前只需要 L4 转发；L3 VPN 是未来的 WireGuard 系决策。
- **副本间 Raft / 共识** —— `agent→pod` 不是有竞争的状态。
- **`master` / `minion` 命名。**

---

## 路线图

`hostlink` 处于**早期阶段**。MVP 按依赖顺序构建；下面的清单反映真实实现状态。

### 已实现

- [x] `AgentLink` + `ControllerPeer` proto 及生成代码。
- [x] agent↔controller 的 **mTLS** 连接建立（TLS 1.3，无不安全回退），客户端与服务端两侧。
- [x] controller 双监听器接线（集群内明文默认监听器 + 面向 ingress 的 mTLS gRPC 监听器），外加可选的第三个集群内 ControllerPeer mTLS 监听器。
- [x] `Control` 流：`Hello` 握手与周期 `Heartbeat`，进程内重连（指数退避 + 抖动）及 HTTP/2 keepalive。
- [x] 通用 `AgentRequest`/`AgentResult` 分发信封 + 关联，含流式变体（`AgentProgress` 帧 + `final` 的 `AgentResult`）；由 agent 侧 Docker 客户端提供的 Docker **镜像**端点：`/api/v1/agents/<id>/images` 上的 `GET`（`images.list`）、`POST`（`images.pull`，SSE 流式进度）、`DELETE`（`images.remove`），外加 `GET /api/v1/agents`。
- [x] `/api/v1/agents/<id>/files` 上的 agent **工作目录文件系统**端点：浏览（`fs.stat`+`fs.list`）、分块 `fs.read` 下载、multipart 分块 `fs.write` 上传（独占创建 + `PUT` 覆盖）、`fs.mkdir`、递归 `fs.remove`；双向有界内存流式传输（`AgentRequestChunk` + 跨 Pod `ControllerPeer.Upload` 中继），基于 `--data-dir` 工作目录并带路径穿越防护。
- [x] Redis `agent→pod` 注册表（写入/CAD 删除/TTL 刷新；standalone/sentinel/cluster 拓扑），及经 `ControllerPeer.Dispatch` + `ControllerPeer.DispatchStream` 的跨 Pod API 请求中继，对陈旧映射拒绝并重试。
- [x] Helm chart：StatefulSet + headless peer Service；可选 Redis、peer 平面、cert-manager CSI 证书签发；多副本配置守卫。
- [x] 指标聚合：agent 的 `scrape-targets`（node_exporter / dcgm / …）按需拉取并**流式**回传；controller `GET /api/v1/metrics` 有界并发反向扇出 + `agent`/`exporter` 标签注入 + `MetricFamily` 合并 + 合成的 `agent_up` / `hostlink_scrape_target_up`，经 `ControllerPeer.DispatchStream` 支持跨 Pod。
- [x] `/api/v1/agents/<id>/containers` 上的容器 CRUD + 生命周期（`containers.list` / `containers.create`（docker run 语义，含 ports、binds、`--net`/`--pid` host 模式、重启策略）/ `containers.inspect` / `containers.start` / `containers.stop` / `containers.restart` / `containers.remove`）。
- [x] 容器**日志流**（`.../containers/<id>/logs`，SSE，`?follow`/`?tail`/`?since`/`?timestamps`）：感知 TTY 的 stdout/stderr 解复用、按行封帧、无损投递；外加 `request.cancel` 元方法，被放弃的 follow 流会在 agent 侧被拆除，并跨 Pod 传播。
- [x] 端到端端口转发：`--forward-port-range` 端口池（Redis `SETNX` 分配，单副本内存模式）+ `forwards` REST API；在**每个**副本上调和的按端口 TCP 监听器；`OpenForward` 反向打开握手与 `pkg/tunnel` 字节管道（显式半关闭：`OPEN`/`READY`/`DATA`/`HALF_CLOSE`/`RESET`）；经 `ControllerPeer.Forward` 的跨 Pod 两跳中继（陈旧持有者拒绝并重试）；全副本绑定**激活屏障**，按转发呈现 `pending`/`active`/`suspended`；chart 的 `portForward.range` + 可选的枚举式 `<release>-forward` Service。
- [x] Docker **事件**上报（`DockerEvent`：容器 `start`/`die`/`stop`）驱动暴露生命周期 —— `die`/`stop` 时暂停（快速 RST，端口保留），`start` 时重新解析容器 IP 并恢复。

### 进行中 / 计划中（MVP）

- [ ] controller 侧**日志中继进 Loki**（"Plan B" —— agent 只能触达 controller，因此由中继为所持有的每个 agent follow `containers.logs`，打上 `{agent_id, container_name}` 标签，推送到集群内 Alloy 的 `loki.source.api`；还需要基于 `since` 的续传、随 agent 重连的归属跟随、共享 Control 流上的带宽预算）。
- [ ] `internal/` 分解为 `{transport, tunnel, registry, metrics, docker, routing}` 等接口化模块。

### 未来

- [ ] VPN / L3 overlay（WireGuard 系 —— Nebula，或 Headscale + Tailscale），subnet-router 模型；届时上文的 L4 转发成为其子集。
- [ ] 容器出站访问云内私有 IP（overlay，或 tproxy / SOCKS5）。
- [ ] 经子域名 + 泛域名证书 + host 路由的 HTTP 服务暴露（取代按端口暴露）。
- [ ] 暴露的 `pause`/`unpause`（暂停/恢复而不释放 VRAM）。
- [ ] 按容器的 overlay 身份 / ACL，实现更强隔离。

---

## 许可证

详见 [LICENSE](./LICENSE) 与 [NOTICE](./NOTICE)。
