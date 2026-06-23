# hostlink

> [English](./README.md)

> ⚠️ **早期阶段 —— 尚未达到生产可用。** 已实现并经端到端验证：agent↔controller
> 的 mTLS 连接与 `Control` 流（握手 + 心跳）；通用的请求/响应分发信封及两个 REST 接口族
> —— **Docker 镜像**（`GET`/`POST`/`DELETE /api/v1/agents/<id>/images`：列举、SSE 流式
> 拉取、删除）与 agent **工作目录文件系统**（`GET`/`POST`/`PUT`/`DELETE
> /api/v1/agents/<id>/files`：浏览、下载、流式 multipart 上传、递归删除）；一个**指标聚合**端点（`GET /api/v1/metrics`，流式拉取并合并各 agent 配置的 exporter）；基于 Redis 的 `agent→pod`
> 注册表；以及经 `ControllerPeer` 平面（`Dispatch` 一元 + `DispatchStream` 流式）的跨 pod API
> （`DockerOp`/事件）、端口转发，以及跨 pod 的端口转发数据面 ——
> 参见 [路线图](#路线图)。请勿将其作为承载真实业务的控制面部署。

一个用于管理**云外**（自建机房 / 托管机房服务器）Linux 主机的控制面。这些主机**不是** Kubernetes 节点；其上的工作负载以 **Docker 容器**形式运行，而非 Pod。`hostlink` 为云端提供一条到每台 NAT 后主机的持久、双向认证通道，用于命令下发、容器生命周期管理、指标采集以及裸 TCP 端口转发 —— 且无需主机具备任何入站连通性。

**规模假设。** 大约一打 agent，最多几十个。设计取向是*可控、可调试、够用*而非追求 web 级扩展性；不要为庞大集群过度设计。

## 目录

- [概述](#概述)
- [工作原理](#工作原理)
- [架构](#架构)
  - [连接模型](#连接模型)
  - [命令通道](#命令通道)
  - [指标反向聚合](#指标反向聚合)
  - [端口转发](#端口转发)
  - [多副本亲和性](#多副本亲和性)
  - [容器生命周期](#容器生命周期)
- [通信协议](#通信协议)
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

## 概述

`hostlink` 由单个 Go module 构建出两个二进制：

- **`hostlink-controller`** —— 运行在云端、Kubernetes 之内。它是控制面，是 gRPC **服务端**，为 agent 终结双向 TLS，并（按设计）聚合指标、分配公网端口、在副本间路由被转发的 TCP。
- **`hostlink`** —— 以 systemd 服务的形式运行在每台外部主机上。它是 gRPC **客户端**：主动拨出连接到 controller（因此可在 NAT 后工作、无需任何入站防火墙规则），驱动本地 Docker 守护进程，上报指标，承载隧道。

贯穿下文的核心前提是：**在 HTTP/2 下，服务端无法向客户端开启一条 stream。** agent 处于 NAT 之后，必须是拨号方，因此每一个“controller → agent”的交互都被建模为基于 agent 已建立连接之上的*反向*数据流。

### 范围与边界

`hostlink` **仅是基础设施层** —— 连通性、容器编排、指标与端口转发。它刻意不涉足其余一切：

- **它负责**为每个 agent 维护一条持久、经 mTLS 认证的 gRPC 连接，在该连接上多路复用命令 / 指标 / 被转发字节，驱动 Docker 容器生命周期，并将容器端口暴露到公网云端端口。
- **它不负责**计费、开通、配额或任何其他业务逻辑 —— 这些属于更上层的 Smoothcloud 平台。
- **它不会**把主机当作 Kubernetes 节点。工作负载是 Docker 容器（保留可写层的“宠物”），而非 Pod。
- **它当前不会**构建自有的 L3 overlay。当前范围只有 L4（端口）转发；L3/VPN 方案被明确推迟（参见[路线图](#路线图)）。

---

## 工作原理

```
外部主机（NAT 之后）                       云端（Kubernetes）
  hostlink                          hostlink-controller（≥2 副本）
       │                                          │
       │── 拨出（mTLS，TLS 1.3） ────────────────► │  每个 agent 一条 gRPC 连接
       │                                          │
       │── Control(stream AgentEvent) ──────────► │  长生命周期双向流
       │     Hello（握手） ─────────────────────► │
       │     Heartbeat（刷新 TTL） ─────────────► │
       │                                          │
       │ ◄──────────── Command（推送） ─────────── │  OpenForward / DockerOp / ExposeRule
       │                                          │
       │── Forward(stream Frame) ───────────────► │  每条被转发的 TCP 连接对应一条流
       │     DATA / HALF_CLOSE / RESET           │  （由 agent 按需开启）
       │                                          │
```

一条 TCP 连接，多条 HTTP/2 stream。agent 在连接后开启一次 `Control` 流；controller 沿其响应方向推送命令。每条被转发的公网连接拥有自己独立的 `Forward` 流，从而借助 gRPC 的每流级流控获得天然的背压（backpressure）。

> **当前已实现的部分：** mTLS 拨号、`Control` 流与 `Hello` / `Heartbeat` 交互；通用的 `AgentRequest`/`AgentResult` 分发信封（含 `AgentProgress` / `final` 流式扩展）及 Docker 镜像 REST 接口（`GET` 列举 `images.list`、`POST` 经 SSE 流式拉取 `images.pull`、`DELETE` 删除 `images.remove`，位于 `/api/v1/agents/<id>/images`）；agent **工作目录文件系统**接口（`GET`/`POST`/`PUT`/`DELETE /api/v1/agents/<id>/files`）；**指标聚合**端点（`GET /api/v1/metrics`，反向拉取、打标并合并每个 agent 配置的 exporter）；Redis 的 `agent→pod` 注册表；以及经 `ControllerPeer.Dispatch`（一元）、`ControllerPeer.DispatchStream`（流式）与 `ControllerPeer.Upload`（客户端流式，用于文件系统写入）的跨 pod API 请求中继。`Forward`、`DockerOp` 执行，以及跨 pod 的端口转发数据面仍为占位桩或尚未实现。参见[路线图](#路线图)。

---

## 架构

### 连接模型

agent 是 gRPC **客户端**；controller 拥有公网入口、是 gRPC **服务端**。每个 agent 维护**一条**到 controller 的 gRPC 连接，所有逻辑通道都在其上多路复用为 HTTP/2 stream：

- **命令** —— 一条由 agent 开启的长生命周期双向 `Control` 流；controller 沿其响应方向推送命令。
- **指标** —— controller 在这条连接上反向拉取每个 agent 的指标暴露内容（exposition）。
- **端口转发** —— 每条被转发的公网连接是一条由 agent 开启的 `Forward` 流。

> **约束：** “复用一条连接”指的是**一条 TCP 连接承载多条 stream** —— 而不是把所有字节塞进一条 stream 再自建子多路复用层。HTTP/2 的 stream *本身就是*多路复用；在其上再叠加 yamux/smux 是冗余的。

> **约束（传输安全）：** agent↔controller 连接为**双向 TLS（mTLS），最低 TLS 1.3，无不安全回退**。agent 出示客户端证书并依据 CA bundle 验证 controller；controller 出示服务端证书并要求且验证 agent 的客户端证书。mTLS 即连接层的 agent 身份认证机制。

### 命令通道

连接建立后，agent 开启 `Control(stream AgentEvent) returns (stream Command)`。controller 沿 `Command` 流推送 `OpenForward`、`DockerOp`（run / stop / start / pause / unpause / rm）与 `ExposeRule`。agent 沿 `AgentEvent` 流上报其握手、心跳与 Docker 事件。

### 指标：两个端点，两类关注点

Prometheus 维持**拉取模式**。controller 暴露**两个**彼此独立的端点（集群内明文 listener 同时提供两者）：

- **`/metrics`**（由 chassis 提供）—— controller **自身**的进程指标。
- **`GET /api/v1/metrics`** —— **云端对全部 agent 上游 exporter 的聚合**。这才是反向聚合（reverse fan-out）。

当 `/api/v1/metrics` 被抓取时，controller：

1. 枚举在线 agent 全集（Redis `agent→pod` 目录；内存模式下即本副本持有的 agent），以**有界并发**向每个 agent 下发流式 `metrics.scrape` —— 走其本地 `Control` 流，或经 `ControllerPeer.DispatchStream` 中继到持有该 agent 的 pod；
2. 每个 agent 在本地拉取其配置的 `scrape-targets`（node_exporter、dcgm-exporter……）并将每份暴露内容**分块流式**回传，因此 agent 内存有界、且不受单条消息大小上限限制；
3. 为每个 agent 设置**严格短于 `scrape_timeout` 的独立超时**（`--agent-scrape-timeout`，默认 5s，对应默认的 10s）；缓慢 / 离线的 agent 被跳过，仅贡献 `agent_up 0`；
4. **按 `MetricFamily` 合并** —— 用 `expfmt.TextParser` 解码每份暴露内容，为每条 series 注入 `agent=<id>` 与 `exporter=<name>` 标签，将同名指标的 series 合并到同一个 family（共享一条 HELP/TYPE），最后统一编码一次；
5. 合成 `agent_up{agent="<id>"}`（1 = 本轮流式抓取完成，0 = 离线 / 超时）与 `hostlink_scrape_target_up{agent,exporter}`（单个 exporter 健康度）。`agent_up` 是“某个 agent 掉线”的唯一干净信号，因为 Prometheus 原生 `up` 只反映 controller 端点本身。

> **约束：** **绝不可对暴露内容做字符串拼接。** 同一指标名出现重复的 HELP/TYPE 行会让 Prometheus 解析器拒绝整份载荷。必须在 `MetricFamily` 层级合并。

> 刻意使用 `exporter` 标签（而非 `job` / `instance`），这样注入的标签**无需**在 Prometheus 抓取配置里开启 `honor_labels` 即可保留。exporter 作为**独立 sidecar 二进制**运行；agent 在本地 GET（例如 `127.0.0.1:9100`）。**不要**把 node_exporter 作为库引入 —— 其 collector 包不是稳定的公开 API。

### 端口转发

目标：把容器的内部端口（例如监听 `:8080` 的 vLLM）以**裸 TCP**、支持半关闭（half-close）的方式动态暴露到公网云端端口（例如 `:1025`）。数据面是 **L4 流代理**（在每一跳终结 TCP，只承载应用字节）—— 这天然避免了 TCP-over-TCP 退化。

由于服务端无法向 agent 开启 stream，开流握手是反向的：

1. 一条公网连接落在 controller 暴露的端口上。
2. controller 沿该 agent 的 `Control` 流推送 `OpenForward{session_id, target}`。
3. agent **开启**一条 `Forward` 流，首帧携带 `session_id`。
4. controller 依据 `session_id` 将该公网连接与那条 `Forward` 流**配对**，并双向中继。

这套握手 + 分块 + 会话配对由 [`openconfig/grpctunnel`](https://github.com/openconfig/grpctunnel) 提供 —— 不要重复造轮子。

> **约束（半关闭，正确性关键）：** gRPC 流自身的生命周期（`CloseSend` / handler 返回）无法表达 TCP 各方向独立的半关闭。它通过**显式帧类型**建模：本地 EOF → 发送 `HALF_CLOSE` 并停止该方向的发送，但继续读另一方向；收到 `HALF_CLOSE` → 对本地 socket 调用 `CloseWrite()`；`RESET` → `SetLinger(0)` + `Close()`。在集成 `openconfig/grpctunnel` 之前，必须**验证**其字节管道完整保留半关闭语义；若不满足，则打补丁。

> **约束（背压）：** 从本地 socket 读取的速度绝不可超过你能 `Send` 出去的速度。依赖 HTTP/2 流控窗口写满时 `Send` 阻塞来实现背压。不要无界缓冲。

### 多副本亲和性

controller 为高可用运行 ≥2 副本。一个 agent 的连接被钉在它拨号到的那一个 pod 上，但经 L4 负载均衡器到达的公网 TCP 会落在**任意**副本 —— 通常不是持有该 agent 的那个。

- **路由键：** 对裸 TCP 而言，入口处唯一可用的键是**目的端口**。每个暴露 = 一个独立公网端口。
- **注册表（Redis 双映射）：** `port:<P>` → `(agentID, container_target)`；`agent:<id>` → `holding_pod`（agent 接入时写入，**TTL 由心跳刷新**，断连时删除）。
- **原子端口分配：** 通过 Redis `INCR` / `SETNX` 从预留池中取一个空闲端口。
- **路由流程：** 一条公网连接经端口 P 到达 pod B → B 查 `port:P` → agentX，再查 `agent:agentX` → podA。若 `podA == B`，B 直接驱动反向开流；否则 B 经**内部 gRPC** 把连接转发给 podA（两段串联的字节管道；半关闭必须端到端传播）。
- **陈旧窗口：** 由于 LB 会打散连接，跨 pod 转发是**常见**情形（命中率 ≈ 1/N）。一个收到“被转发给我但我并不持有 agentX”的 pod 必须**拒绝**，并让调用方重新解析后重试。

> Redis 已在技术栈中。Raft / gossip / mesh 经评估后被否决：`agent→pod` 不是竞争状态，无需共识。

### 容器生命周期

“关机 / 开机” = `docker stop` / `docker start`。Docker 容器是**有状态的宠物**：stop 走 SIGTERM → 宽限期 → SIGKILL，可写层在本地磁盘上被保留，杀掉进程即释放 GPU；start 带回相同的容器 ID 且状态完好。在原生 Docker 上这是免费的 —— 无需 K8s 式的 upperdir 持久化。

暴露规则通过 `client.Events()` 与生命周期绑定：

- 收到 `stop` / `die` → 挂起或移除该容器的暴露（清除 Redis `port:` 映射）。
- 收到 `start` → 重新解析容器 IP 并重建暴露。

> 容器 IP 在重启后可能改变；若端口被新的持有 pod 重新分配，则**公网端口随之改变、客户端必须重连** —— 这是一个被接受的设计权衡。对 GPU 容器，记得 nvidia 运行时；`docker pause` / `unpause`（freezer cgroup，保留 RAM/VRAM）是一种不同的“挂起、瞬时恢复”语义。

---

## 通信协议

服务定义于 `pkg/api/hostlink/v1/`。`AgentLink`（agent↔controller）有两个双向流 RPC；`ControllerPeer`（controller↔controller）有一个一元 RPC 与一个服务端流式 RPC：

```proto
service AgentLink {
  // agent 连接后开启一次；controller 沿响应流推送命令。
  // 在 HTTP/2 下服务端无法向客户端发起 RPC，因此所有
  // server->agent 命令都经由这条已开启的流传输。
  rpc Control(stream AgentEvent) returns (stream Command);

  // 每条被转发的公网 TCP 连接对应一个；由 agent 开启，
  // 首帧携带 session_id 用于配对。内部跨 pod 转发复用
  // 同一服务定义（只是拨号到一个兄弟 pod）。
  rpc Forward(stream Frame) returns (stream Frame);
}

service ControllerPeer {
  // 跨 pod 中继：未持有目标 agent 的副本把请求转发给持有
  // 它的 pod（经 Redis agent->pod 映射解析）。映射陈旧的
  // pod 以 FAILED_PRECONDITION 拒绝，调用方据此重新解析并重试。
  rpc Dispatch(DispatchRequest) returns (AgentResult);

  // 流式跨 pod 中继（例如 SSE 镜像拉取的进度）：同样解析到
  // 持有该 agent 的 pod，但以流式逐帧返回多个 AgentResult
  // （进度帧 final=false，终结帧 final=true）。
  rpc DispatchStream(DispatchRequest) returns (stream AgentResult);

  // 客户端流式跨 pod 中继 controller->agent 的上传（例如 fs.write）：首帧携带
  // open（DispatchRequest），其余帧为请求体分块。
  rpc Upload(stream UploadFrame) returns (AgentResult);
}
```

| 消息 | 方向 | 用途 |
|---------|-----------|---------|
| `AgentEvent{Hello / Heartbeat / DockerEvent / AgentResult / AgentProgress}` | agent → controller | 握手、TTL 刷新、容器上报、对 `AgentRequest` 的回复（`AgentResult`）、以及流式中间进度（`AgentProgress`） |
| `Command{OpenForward / DockerOp / ExposeRule / AgentRequest / AgentRequestChunk}` | controller → agent | 开启转发流、执行 Docker 操作、增删暴露规则、一条通用的按 method 分发的请求，或流式上传的请求体分块 |
| `AgentRequest{request_id, method, payload}` / `AgentResult{request_id, code, payload, error, final}` | 双向 | 通用 API 调用：`method` 命名操作（如 `images.list`），`payload` 为 JSON，`code` 对应 HTTP 状态，按 `request_id` 关联；`final` 标记流式响应的终结帧 |
| `AgentRequestChunk{request_id, data, last}` | controller → agent | 流式上传请求（如 `fs.write`）的一个请求体分块，按 `request_id` 关联到其 `AgentRequest`；`last` 标记末块 |
| `AgentProgress{request_id, payload}` | agent → controller | 长操作（如 `images.pull` 层进度、或 `fs.read` 文件字节）的中间帧，`payload` 视方法而定，按 `request_id` 关联，终结时以一条 `final=true` 的 `AgentResult` 收尾 |
| `DispatchRequest{agent_id, AgentRequest}` | controller → controller | 为 `ControllerPeer.Dispatch`（一元）与 `DispatchStream`（流式）这一跳给 `AgentRequest` 包上路由键 |
| `UploadFrame{open / chunk, last}` | controller → controller | 经 `ControllerPeer.Upload` 这一跳流式中继 controller→agent 的上传：首帧为 `DispatchRequest` open，其余帧为请求体分块 |
| `Frame{session_id, type, data}` | 双向（每条转发） | 裸 TCP 字节；`type` ∈ `DATA` / `HALF_CLOSE` / `RESET` |

`Frame.Type` 枚举正是实现正确 TCP 半关闭的关键（参见[端口转发](#端口转发)）。任何 `.proto` 变更后，需重新生成并提交生成代码。

### REST API

controller 在其集群内默认 listener 上、基于上述通用分发信封，提供一小组 REST 接口：

| 方法与路径 | 说明 |
|---------------|-------------|
| `GET /api/v1/agents` | 列出当前连到可达副本的所有 agent。 |
| `GET /api/v1/agents/<agentId>/images` | 列出指定 agent 上的 Docker 镜像。向该 agent 的 `Control` 流分发 `images.list`（本地处理，或经 `ControllerPeer.Dispatch` 中继到持有它的 pod），原样返回 agent 的 JSON。若该 agent 未连接到任何可达副本则返回 404。 |
| `POST /api/v1/agents/<agentId>/images` | 拉取镜像。请求体 `{image, auth?}`（`auth` 为可选私有镜像仓凭证）。响应为 `text/event-stream`（SSE）：逐帧 `data: <PullProgress JSON>`（id/status/current/total/progress/error）表示拉取进度，最后以 `data: {"done":true,"code":...,"error":...}` 收尾。经 `images.pull` 流式分发（本地或经 `DispatchStream` 中继）。 |
| `DELETE /api/v1/agents/<agentId>/images/<imageId>` | 删除单个镜像（`imageId` 为镜像 ID 或无 `/` 的引用）。可选 `?force=true` 与 `?noPrune=true`。经 `images.remove` 分发。 |
| `DELETE /api/v1/agents/<agentId>/images?ref=A&ref=B` | 批量删除镜像（可重复 `ref` 查询参数，适用于含 `/` 的引用）。返回 `RemoveResult{deleted, errors}`—部分失败仅在 `errors` 中报告而不使整个请求失败。 |
| `GET /api/v1/agents/<agentId>/files?path=<p>` | 浏览 agent 工作目录（`--data-dir`）。目录返回 JSON `{"entries":[{"name","dir","size","modTime"}]}`（不递归）；文件**流式下载**（`Content-Disposition`），或在 `Accept: application/json` 时返回其 `FsEntry` 元信息。`path` 为空表示工作目录根。先分发 `fs.stat`，再 `fs.list` / `fs.read`。 |
| `POST /api/v1/agents/<agentId>/files?path=<p>` | `&dir=true` 创建目录（`fs.mkdir`，已存在返回 `409`）。否则从 **multipart 表单**上传一个或多个文件，分块流式（`fs.write`）写入并以**独占**方式创建——已存在的目标按文件单独报告。响应 `{"written":[...],"errors":[...]}`。 |
| `PUT /api/v1/agents/<agentId>/files?path=<p>` | 用请求体（裸字节或首个 multipart part）覆盖单个文件；经 `fs.write` 流式（截断）写入。 |
| `DELETE /api/v1/agents/<agentId>/files?path=<p>` | 删除该路径，目录**递归**删除（`fs.remove`）。 |

> `files` 接口两个方向均按 64 KB 分块流式传输（下载经 `AgentProgress` 帧，上传经 `AgentRequestChunk` / `ControllerPeer.Upload` 中继），大文件以有界内存传输。agent 将每个 `path` 解析在工作目录内，拒绝越界（`..`）。

---

## 快速开始

### 前置条件

| 组件 | 版本 / 说明 |
|-----------|----------------|
| Go（仅构建） | 1.26+ |
| Controller 运行环境 | Kubernetes（StatefulSet；默认 1 副本，高可用需 ≥2） |
| Agent 运行环境 | 一台可*出站*访问 controller 的 Linux 主机；systemd |
| Docker（每台主机） | 必需 —— 工作负载是 Docker 容器；GPU 需 nvidia 运行时 |
| Redis | `agent→pod` 注册表 —— 单副本可选，**高可用（`replicaCount > 1`）必需** |
| cert-manager + CSI driver | 可选 —— 逐 pod 签发 controller/peer 证书，替代挂载 Secret |
| node_exporter | 每台主机上的 sidecar，监听 `127.0.0.1:9100` |

本项目**仅支持 Linux**（它管理一个 Linux Docker 守护进程，并在转发中使用 Linux 特有的 socket 语义）。它无法在 Windows 上原生构建或运行；请在 Linux 工具链中构建。

### 构建

本项目是单个 Go module，`cmd/` 下每个子目录对应一个二进制，且已 **vendored** —— 用 `-mod=vendor` 离线构建：

```bash
go build -mod=vendor -o bin/hostlink-controller ./cmd/controller
go build -mod=vendor -o bin/hostlink      ./cmd/agent
```

> 二进制以 `hostlink-` 为前缀，使其在主机上的 `ps`、打包或 systemd 单元中不会与其他某个 `agent` 冲突。

在 Windows 开发机上，请在 Linux 容器内编译（工作树与 Go 缓存以 bind mount 挂入，容器提供 Linux 工具链）：

```bash
docker run --rm \
  -v "$PWD":/go/src/github.com/humble-mun/hostlink \
  -w /go/src/github.com/humble-mun/hostlink \
  golang:1.26.3-trixie go build -mod=vendor -v ./...
```

### 证书（mTLS）

agent↔controller 连接需要一套可用的 PKI。你需要一个 CA、一份 controller（服务端）证书，以及逐 agent 的（客户端）证书：

| 一方 | 出示 | 据以验证对端 |
|------|----------|---------------------------|
| controller | 服务端 cert/key（`--grpc-tls-cert-path` / `--grpc-tls-key-path`） | agent CA bundle（`--grpc-tls-ca-path`） |
| agent | 客户端 cert/key（`--agent-tls-cert-path` / `--agent-tls-key-path`） | controller CA bundle（`--controller-tls-ca-path`） |

controller 要求且验证 agent 客户端证书（`RequireAndVerifyClientCert`）；agent 验证 controller 的服务名（`--controller-tls-server-name`）。若留空，gRPC 会以拨号 endpoint 的主机名进行验证，因此当证书 SAN 与拨号地址不一致时（例如以 IP 拨号）必须显式设置。**没有不安全回退** —— 若证书缺失或无效，连接将硬性失败。

---

## 命令行参数

所有参数也可通过环境变量设置：将参数名大写、把 `-` 替换为 `_`，并加上 `HM_` 前缀（例如 `--controller-endpoint` → `HM_CONTROLLER_ENDPOINT`）。配置也可以 YAML 文件形式置于 `/etc/humble-mun/<binary>.yaml`（即 `hostlink.yaml` 或 `controller.yaml`，取决于二进制的 `version.Name`），以参数名为键；该文件会被监听并在运行时重新加载。优先级：命令行参数 > 环境变量 > 配置文件。

### Agent 参数

| 参数 | 默认值 | 说明 |
|------|---------|-------------|
| `--controller-endpoint` | —— | 要拨号的 `hostlink-controller` gRPC endpoint 地址，形如 `host:port`（必填） |
| `--agent-tls-cert-path` | —— | agent 为 mTLS 向 controller 出示的客户端证书 |
| `--agent-tls-key-path` | —— | 与客户端证书匹配的私钥 |
| `--controller-tls-ca-path` | —— | 用于验证 controller 证书的 CA bundle |
| `--controller-tls-server-name` | —— | 据以验证 controller 证书的服务名；若留空，gRPC 以拨号 endpoint 的主机名验证，故当证书 SAN 与拨号地址不一致时必须显式设置 |

### Controller 参数

| 参数 | 默认值 | 说明 |
|------|---------|-------------|
| `--grpc-bind-address` | —— | 用于接受 agent 连接的 mTLS gRPC listener 绑定地址，形如 `host:port` |
| `--grpc-tls-cert-path` | —— | controller 为 mTLS 向 agent 出示的服务端证书 |
| `--grpc-tls-key-path` | —— | 与服务端证书匹配的私钥 |
| `--grpc-tls-ca-path` | —— | 用于验证 agent 客户端证书的 CA bundle |
| `--redis-url` | —— | 承载跨 pod `agent→pod` 注册表的 Redis URL；留空即内存态单副本模式。支持三种拓扑：单机 `redis://`（或 `rediss://`）、哨兵 `redis+sentinel://host:26379?master_name=mymaster&addr=host2:26379`、集群 `redis+cluster://node1:6379?addr=node2:6379&addr=node3:6379`（额外节点经可重复的 `?addr=` 传入）。 |
| `--peer-bind-address` | —— | 集群内 ControllerPeer mTLS listener 的绑定地址，用于跨 pod 中继；留空即关闭 peer 平面 |
| `--peer-advertise-address` | —— | 兄弟副本拨号以到达本 pod peer listener 的地址（写入注册表作为值）；跨 pod 模式下必填 |
| `--peer-tls-cert-path` / `--peer-tls-key-path` / `--peer-tls-ca-path` | —— | ControllerPeer 平面的 mTLS 材料（controller 既是服务端也是客户端） |
| `--peer-tls-server-name` | —— | 中继时据以验证兄弟副本的服务名；留空即用拨号的 peer 主机名 |

> `--redis-url` 与 `--peer-bind-address` 是同一个跨 pod 开关的两半：要么都设（外加 `--peer-advertise-address`），要么都不设。半配置状态下 controller 拒绝启动。

controller 还继承 chassis 的 HTTP server 参数（`--http-bind-address`、`--tls-cert-path`、`--tls-key-path`）用于其**默认 listener**。把默认 listener 的 cert/key 留空，即可为集群内的探针（probe）与指标流量提供明文 h2c；上面的 mTLS gRPC listener 单独配置并通过 ingress 对外暴露。

---

## 部署形态

### hostlink-controller（云端）

- **形态：** 一个 Kubernetes **StatefulSet**（chart 默认 `replicaCount: 1`）。`charts/hostlink/` 的 chart（`helm install <release> charts/hostlink`）渲染出承载 `/etc/humble-mun/controller.yaml` 的 ConfigMap、StatefulSet、一个负载均衡 ClusterIP Service（gRPC + 集群内 HTTP 端口）、一个提供稳定逐 pod DNS 的**无头 `<release>-peer` Service**，以及（当设置了 `ingress.host` 时）面向 agent 的 gRPC Ingress。**高可用请设 `replicaCount > 1`，这要求同时配置 `redis.url` + `peer.enabled`**（否则 chart 在安装时直接报错 —— 半配置的多副本 controller 会对落在兄弟 pod 上的 agent 静默返回 404）。
- **三个 listener**（chassis 的 HTTP/2 server 在每个 listener 上同时多路复用 gRPC 与 Gin —— `Content-Type: application/grpc` 且 HTTP/2 的流量路由到 gRPC server，其余路由到 Gin）：
  1. 一个 **mTLS gRPC listener**（`--grpc-bind-address` + `WithTLSCert` + `WithMTLS`），供 agent 拨出连接；通过 ingress 对外暴露。
  2. 一个**明文（h2c）默认 listener**，仅绑定集群内，提供 REST API（`/api/v1/...`，含聚合的 agent 指标 `/api/v1/metrics`）、controller 自身的 `/metrics`、`/probe`、`/version` 与 `/logging`。
  3.（当 `peer.enabled` 时）一个**集群内 ControllerPeer mTLS listener**，运行在它自己的 `grpc.Server` 上 —— 与共享的 chassis server 分离，使中继平面永不会从面向 agent / ingress 的 listener 上可达。
- **Ingress（L4 LoadBalancer）：** 供 agent 拨出的 mTLS gRPC 端口。由于 controller 自己终结 mTLS，该 Ingress 必须做 L4/TLS **透传（passthrough）**（通过各 ingress controller 专有的 annotation 显式声明）—— 若在 ingress 处终结 TLS，会剥离 agent 的客户端证书并破坏身份模型。用于隧道暴露的**预留 TCP 端口段**（例如 `1025–2025`）仍为设计项，chart 暂未开放。
- **依赖：** **Redis** 承载 `agent→pod` 注册表（单副本可选，高可用必需）；API 请求的跨 pod 中继走 ControllerPeer 平面。`port:<P>` 映射 + 端口分配，以及跨 pod 的端口转发数据面，仍为设计项。
- **证书：** 逐平面，来自挂载的 Secret（`grpc.tlsSecretName` / `peer.tlsSecretName`），或在 `certManager.enabled` 时由 **cert-manager CSI driver** 从配置的 `Issuer`/`ClusterIssuer`（`certManager.issuerKind` / `issuerName`）逐 pod 签发。

> **绕过提示。** chassis server 对每个 listener 都套用同一个 handler，因此明文默认 listener 也会接受 gRPC。这种切分依赖**网络层隔离** —— 默认 listener 只在集群内可达，而 agent gRPC 仅经 ingress 通过 mTLS listener 暴露。

#### 抓取指标

controller 在集群内 HTTP Service（`http` 端口）上暴露**两个**抓取路径：`/metrics`（controller 自身进程指标）与 `/api/v1/metrics`（聚合的 agent exporter 反向聚合，见 §4.3）。chart **不**内置抓取配置 —— 请按你所用监控栈的约定，指向 `http` Service 端口接入。示例：

- **Prometheus 注解发现**（每个路径一个 target）：
  ```yaml
  prometheus.io/scrape: "true"
  prometheus.io/port: "8080"
  prometheus.io/path: "/api/v1/metrics"   # 再为 /metrics 配一份
  ```
- **Prometheus Operator `ServiceMonitor`** —— 在 `http` 端口上配两个 `endpoints`，`path: /metrics` 与 `path: /api/v1/metrics`。
- **VictoriaMetrics `VMServiceScrape`** —— 同样结构：两个 `endpoints`（或 `VMPodScrape`）指向 `http` 端口与上述两个路径。

将 Prometheus job 的 `scrape_timeout` 设得**高于** controller 的 `--agent-scrape-timeout`（默认 5s），让 fan-out 的逐 agent 超时先触发。`/api/v1/metrics` 的 series 已带 `agent`/`exporter` 标签，无需 `honor_labels`。

### hostlink（外部主机）

- **形态：** 一个以 **systemd 服务**运行的静态 Go 二进制（`/usr/local/bin/hostlink`）。单元文件与示例配置位于 `deploy/`（`deploy/hostlink.service`、`deploy/hostlink.yaml`）。
- **配置：** agent 的全部设置均从 `/etc/humble-mun/hostlink.yaml` 读取（chassis 通过 viper `SetConfigName("hostlink")` + `AddConfigPath("/etc/humble-mun")` 装配；YAML 的键即参数名本身，每项均可由 `HM_*` 环境变量覆盖）。systemd 单元**不传任何命令行参数**，且 viper `WatchConfig` 会在运行时热加载该文件，因此修改配置既不需要 `systemctl daemon-reload` 也不需要改单元文件。
- **行为：** 经 mTLS 拨出连接到 controller 的公网 gRPC endpoint，运行 `Control` 流（`Hello` + 周期性 `Heartbeat`），并以**进程内重连**（指数退避 + 抖动，配合 HTTP/2 keepalive）扛过 controller 重启/发版而无需进程退出。它服务 controller 推送的 `AgentRequest`：Docker **镜像**方法（`images.list` / `images.pull` 流式 / `images.remove`，经惰性 `client.FromEnv` Docker 客户端）与**文件系统**方法（`fs.stat` / `fs.list` / `fs.read` 分块下载 / `fs.write` 分块上传 / `fs.mkdir` / `fs.remove`，作用于 `--data-dir` 工作目录，所有路径均解析在其内、拒绝越界）。更广的 Docker 生命周期（`DockerOp`/事件）、承载隧道，以及监听 `127.0.0.1:9100` 的 node_exporter sidecar 仍为计划项（参见[路线图](#路线图)）。由于 Docker 客户端是惰性的，单元文件把 `docker.service` 保留为软（`Wants`）顺序依赖，而非硬性要求。
- **网络：** 处于 NAT 之后；只需具备到 controller 的**出站**连通性。

---

## 安全模型

这是一个 GPU 平台；主机上可能运行**不受信任的客户代码**。暴露端口 —— 以及将来让云端获得对容器的 L3 直达能力 —— 会显著扩大攻击面。

- **Agent 身份（连接层）：** mTLS，最低 TLS 1.3，无不安全回退。这是 agent 向 controller 认证、以及反之的方式。明文默认 listener（探针 / 指标）必须保持**仅集群内可达**，否则共享的 chassis handler 也会在那里接受 gRPC。
- **默认拒绝：** 对方向与端口做白名单，并审计。ACL 方案必须在引入任何 VPN / L3 能力*之前*想清楚。

---

## 风险与硬性约束

以下条目**不可协商** —— 实现期间不得偏离。

- **mTLS，无回退** —— agent↔controller 始终是双向 TLS 1.3+。默认 listener 保持仅集群内可达。
- **不做 TCP-over-TCP** —— L4 代理终结 TCP 并承载字节；**不要**改用基于 TCP 的 L3 报文隧道。
- **半关闭是显式的** —— 转发必须做分块 + 显式 `HALF_CLOSE` / `RESET` 帧 + 背压。集成 `openconfig/grpctunnel` 前先验证其保留半关闭。
- **绝不拼接暴露内容** —— 在 `MetricFamily` 层级合并指标；逐 agent 抓取超时 < `scrape_timeout`；跳过缓慢 agent。
- **拒绝陈旧转发** —— 落错 pod 的转发必须被拒绝并重试。
- **一条 TCP 连接，多条 stream** —— 绝不在 HTTP/2 之上自建子多路复用层。
- **该规模下接受队头阻塞** —— 命令、指标与被转发字节共享一条 TCP 连接；丢包时所有 stream 一起停顿。若高吞吐流量（如 vLLM）损害控制面延迟，则把数据面挪到它自己的 `ClientConn`（同服务/同认证/同 endpoint，独立 TCP），并考虑 QUIC。
- **端口重分配对客户端可见** —— agent 重连后公网端口可能改变；在产品文档中说明这一点。

### 明确否决（不要采用）

- **KubeEdge / 把主机当作 K8s 节点** —— 前提是 Docker 容器，而非 Pod。
- **`jhump/grpctunnel`** —— 它隧道的是 gRPC-over-gRPC，而非裸 TCP。不合适；用 `openconfig/grpctunnel`。
- **自建 tun 设备 + 用户态 netstack** —— 当前只需要 L4 转发；L3 VPN 是未来的、WireGuard 家族的决策。
- **副本间 Raft / 共识** —— `agent→pod` 不是竞争状态。
- **`master` / `minion` 命名。**

---

## 路线图

`hostlink` 处于**早期阶段**。MVP 正按依赖顺序构建；下面的清单反映实际实现状态。

### 已实现

- [x] `AgentLink` + `ControllerPeer` proto + 生成代码
- [x] 带 **mTLS** 的 agent↔controller 连接建立（TLS 1.3，无不安全回退），客户端与服务端两侧
- [x] 双 listener 的 controller 接线（集群内明文默认 listener + 面向 ingress 的 mTLS gRPC listener），外加可选的第三个集群内 ControllerPeer mTLS listener
- [x] 带 `Hello` 握手与周期性 `Heartbeat` 的 `Control` 流，含进程内重连（指数退避 + 抖动）与 HTTP/2 keepalive
- [x] 通用 `AgentRequest`/`AgentResult` 分发信封与关联（含 `AgentProgress` / `final` 流式扩展）；`GET /api/v1/agents`；以及由 agent 端 Docker 客户端服务的镜像接口：`GET .../images`（`images.list`）、`POST .../images`（`images.pull`、SSE 流式进度）、`DELETE .../images[/:imageId]`（`images.remove`、单个与批量）
- [x] agent **工作目录文件系统**接口（`/api/v1/agents/<id>/files`）：浏览（`fs.stat`+`fs.list`）、`fs.read` 分块下载、multipart 分块 `fs.write` 上传（独占创建 + `PUT` 覆盖）、`fs.mkdir`、递归 `fs.remove`；两个方向均有界内存流式（`AgentRequestChunk` + 跨 pod `ControllerPeer.Upload` 中继），含 `--data-dir` 工作目录与路径越界防护
- [x] Redis `agent→pod` 注册表（写入/比较删除/TTL 刷新，支持单机/哨兵/集群三种拓扑）与经 `ControllerPeer.Dispatch`（一元）及 `DispatchStream`（流式）的跨 pod API 请求中继，含陈旧映射的拒绝并重试
- [x] Helm chart：StatefulSet + 无头 peer Service；可选 Redis、peer 平面与 cert-manager CSI 证书签发；多副本配置守卫
- [x] 指标聚合：agent `scrape-targets`（node_exporter / dcgm / …）按需拉取并**流式**回传；controller `GET /api/v1/metrics` 有界并发反向聚合 + `agent`/`exporter` 标签注入 + `MetricFamily` 合并 + 合成 `agent_up` / `hostlink_scrape_target_up`，经 `ControllerPeer.DispatchStream` 跨 pod 感知

### 进行中 / 计划中（MVP）

- [ ] 容器生命周期操作：`DockerOp`（run/stop/start/rm）执行；`DockerEvent` 上报
- [ ] 端口转发：集成 `openconfig/grpctunnel`（先验证半关闭）；`ExposeRule` / `OpenForward`；带半关闭 + 背压的 `Frame` 中继（`Forward` 目前是占位桩）
- [ ] 多副本**端口转发**：`port:<P>` 映射 + 原子端口池分配，以及端口转发数据面的跨 pod 两跳中继（API 请求中继已实现）
- [ ] 生命周期联动：Docker 事件驱动暴露的建立 / 挂起 / 重建
- [ ] 将 `internal/` 拆分为接口背后的 `{transport, tunnel, registry, metrics, docker, routing}`

### 未来

- [ ] VPN / L3 overlay（WireGuard 家族 —— Nebula，或 Headscale + Tailscale），子网路由器模型；上面的 L4 转发届时成为其子集
- [ ] 容器出站到云端内网私有 IP（overlay，或 tproxy / SOCKS5）
- [ ] 经子域名 + 通配符证书 + host 路由的 HTTP 服务暴露（替代逐端口暴露）
- [ ] `pause` / `unpause` 暴露（挂起/恢复而不释放 VRAM）
- [ ] 逐容器 overlay 身份 / ACL 以实现更强隔离

---

## 许可证

详见 [LICENSE](./LICENSE) 与 [NOTICE](./NOTICE)。
