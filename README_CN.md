# hostlink

> [English](./README.md)

> ⚠️ **早期阶段 —— 尚未达到生产可用。** 当前仅 agent↔controller 之间的 mTLS
> 连接以及 helloworld 级的 `Control` 流（握手 + 心跳）已实现并经过端到端验证。
> 容器编排、指标聚合（fan-out）、端口转发与多副本路由均**已完成设计但尚未
> 实现** —— 参见 [路线图](#路线图)。请勿将其作为承载真实业务的控制面部署。

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
- **`hostlink-agent`** —— 以 systemd 服务的形式运行在每台外部主机上。它是 gRPC **客户端**：主动拨出连接到 controller（因此可在 NAT 后工作、无需任何入站防火墙规则），驱动本地 Docker 守护进程，上报指标，承载隧道。

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
  hostlink-agent                          hostlink-controller（≥2 副本）
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

> **当前已实现的部分：** mTLS 拨号、`Control` 流，以及 `Hello` / `Heartbeat` 交互（helloworld 级 —— controller 仅记录收到的事件日志）。`Forward`、`DockerOp` 执行、指标聚合以及跨副本路由仍为占位桩或尚未实现。参见[路线图](#路线图)。

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

### 指标反向聚合

Prometheus 维持**拉取模式**，只抓取**单一**目标 —— controller 的 `/metrics`。在被抓取时，controller 的 handler：

1. 在每个 agent 的既有连接上**并发**反向拉取其 node_exporter 暴露内容。
2. 为每个 agent 设置**严格短于 `scrape_timeout` 的独立超时**（约 5s，对应默认的 10s）；缓慢 / 失败的 agent 被跳过，从而避免单个慢 agent 拖垮整轮抓取。
3. **按 `MetricFamily` 合并** —— 用 `expfmt.TextParser` 解码每份暴露内容，为每条 series 注入 `agent=<id>` 标签，按指标名合并为同一个 family（共享一条 HELP/TYPE），最后统一编码一次。
4. 合成 `agent_up{agent="<id>"}`（1 = 本轮成功抓取，0 = 离线）—— 这是“某个 agent 掉线”的唯一干净信号，因为 Prometheus 只看到一个目标。

> **约束：** **绝不可对暴露内容做字符串拼接。** 同一指标名出现重复的 HELP/TYPE 行会让 Prometheus 解析器拒绝整份载荷。必须在 `MetricFamily` 层级合并。

> node_exporter 作为**独立 sidecar 二进制**运行；agent 在本地 GET `127.0.0.1:9100`。**不要**把 node_exporter 作为库引入 —— 其 collector 包不是稳定的公开 API。

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

服务定义于 `pkg/api/hostlink/v1/`。两个 RPC，均为双向流：

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
```

| 消息 | 方向 | 用途 |
|---------|-----------|---------|
| `AgentEvent{Hello / Heartbeat / DockerEvent}` | agent → controller | 握手、TTL 刷新、容器 start/stop/die 上报 |
| `Command{OpenForward / DockerOp / ExposeRule}` | controller → agent | 开启转发流、执行 Docker 操作、增删暴露规则 |
| `Frame{session_id, type, data}` | 双向（每条转发） | 裸 TCP 字节；`type` ∈ `DATA` / `HALF_CLOSE` / `RESET` |

`Frame.Type` 枚举正是实现正确 TCP 半关闭的关键（参见[端口转发](#端口转发)）。任何 `.proto` 变更后，需重新生成并提交生成代码。

---

## 快速开始

### 前置条件

| 组件 | 版本 / 说明 |
|-----------|----------------|
| Go（仅构建） | 1.26+ |
| Controller 运行环境 | Kubernetes（Deployment，≥2 副本） |
| Agent 运行环境 | 一台可*出站*访问 controller 的 Linux 主机；systemd |
| Docker（每台主机） | 必需 —— 工作负载是 Docker 容器；GPU 需 nvidia 运行时 |
| Redis | 注册表 + 原子端口分配（controller 依赖） |
| node_exporter | 每台主机上的 sidecar，监听 `127.0.0.1:9100` |

本项目**仅支持 Linux**（它管理一个 Linux Docker 守护进程，并在转发中使用 Linux 特有的 socket 语义）。它无法在 Windows 上原生构建或运行；请在 Linux 工具链中构建。

### 构建

本项目是单个 Go module，`cmd/` 下每个子目录对应一个二进制，且已 **vendored** —— 用 `-mod=vendor` 离线构建：

```bash
go build -mod=vendor -o bin/hostlink-controller ./cmd/controller
go build -mod=vendor -o bin/hostlink-agent      ./cmd/agent
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

所有参数也可通过环境变量设置：将参数名大写、把 `-` 替换为 `_`，并加上 `HM_` 前缀（例如 `--controller-endpoint` → `HM_CONTROLLER_ENDPOINT`）。配置也可以 YAML 文件形式置于 `/etc/humble-mun/<binary>.yaml`（即 `agent.yaml` 或 `controller.yaml`，取决于二进制的 `version.Name`），以参数名为键；该文件会被监听并在运行时重新加载。优先级：命令行参数 > 环境变量 > 配置文件。

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

controller 还继承 chassis 的 HTTP server 参数（`--http-bind-address`、`--tls-cert-path`、`--tls-key-path`）用于其**默认 listener**。把默认 listener 的 cert/key 留空，即可为集群内的探针（probe）与指标流量提供明文 h2c；上面的 mTLS gRPC listener 单独配置并通过 ingress 对外暴露。

---

## 部署形态

### hostlink-controller（云端）

- **形态：** 一个 Kubernetes **Deployment，≥2 副本**（高可用）。仓库在 `charts/hostlink/` 提供了 Helm chart（`helm install <release> charts/hostlink`）；它渲染出一个承载 `/etc/humble-mun/controller.yaml` 的 ConfigMap、Deployment、一个同时承载 gRPC 与集群内 HTTP 两个端口的 ClusterIP Service，以及（当设置了 `ingress.host` 时）面向 agent 的 gRPC Ingress。
- **两个 listener**（chassis 的 HTTP/2 server 在每个 listener 上同时多路复用 gRPC 与 Gin —— `Content-Type: application/grpc` 且 HTTP/2 的流量路由到 gRPC server，其余路由到 Gin）：
  1. 一个 **mTLS gRPC listener**（`--grpc-bind-address` + `WithTLSCert` + `WithMTLS`），供 agent 拨出连接；通过 ingress 对外暴露。
  2. 一个**明文（h2c）默认 listener**，仅绑定集群内，提供 `/metrics`（Prometheus）、`/probe`、`/version` 与 `/logging`。
- **Ingress（L4 LoadBalancer）：** 供 agent 拨出的 mTLS gRPC 端口。由于 controller 自己终结 mTLS，该 Ingress 必须做 L4/TLS **透传（passthrough）**（通过各 ingress controller 专有的 annotation 显式声明）—— 若在 ingress 处终结 TLS，会剥离 agent 的客户端证书并破坏身份模型。按设计还会有一段路由到每个副本、用于隧道暴露的**预留 TCP 端口段**（例如 `1025–2025`；从池中分配 + 写 Redis，pod 已监听整段端口，故无需为每次暴露改动 Service/LB）—— 该端口段为设计项，chart 暂未开放。
- **依赖：** Redis（注册表 + 端口分配）与副本间的内部 gRPC 转发均为设计项、尚未在代码中接线，因此 chart 只提供一个普通的负载均衡 ClusterIP Service（`/metrics` 聚合是无状态的，任一副本均可应答）。

> **绕过提示。** chassis server 对每个 listener 都套用同一个 handler，因此明文默认 listener 也会接受 gRPC。这种切分依赖**网络层隔离** —— 默认 listener 只在集群内可达，而 agent gRPC 仅经 ingress 通过 mTLS listener 暴露。

### hostlink-agent（外部主机）

- **形态：** 一个以 **systemd 服务**运行的静态 Go 二进制（`/usr/local/bin/hostlink-agent`）。单元文件与示例配置位于 `deploy/`（`deploy/hostlink-agent.service`、`deploy/agent.yaml`）。
- **配置：** agent 的全部设置均从 `/etc/humble-mun/agent.yaml` 读取（chassis 通过 viper `SetConfigName("agent")` + `AddConfigPath("/etc/humble-mun")` 装配；YAML 的键即参数名本身，每项均可由 `HM_*` 环境变量覆盖）。systemd 单元**不传任何命令行参数**，且 viper `WatchConfig` 会在运行时热加载该文件，因此修改配置既不需要 `systemctl daemon-reload` 也不需要改单元文件。
- **行为：** 经 mTLS 拨出连接到 controller 的公网 gRPC endpoint。当前仅实现了 `Control` 流（`Hello` + `Heartbeat`）；管理本地 Docker 守护进程、承载隧道，以及监听 `127.0.0.1:9100` 的 node_exporter sidecar 均为计划项（参见[路线图](#路线图)）。因此单元文件把 `docker.service` 作为软（`Wants`）顺序依赖，而非硬性要求。
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

- [x] `AgentLink` proto + 生成代码
- [x] 带 **mTLS** 的 agent↔controller 连接建立（TLS 1.3，无不安全回退），客户端与服务端两侧
- [x] 双 listener 的 controller 接线（集群内明文默认 listener + 面向 ingress 的 mTLS gRPC listener）
- [x] 带 `Hello` 握手与周期性 `Heartbeat` 的 `Control` 流（helloworld 级 —— controller 记录收到的事件日志）

### 进行中 / 计划中（MVP）

- [ ] 命令下发与执行：经 Docker 客户端的 `DockerOp`（run/stop/start/rm）；`DockerEvent` 上报
- [ ] 指标：node_exporter sidecar + agent 本地拉取；controller `/metrics` 并发聚合 + `MetricFamily` 合并 + `agent_up`
- [ ] 端口转发：集成 `openconfig/grpctunnel`（先验证半关闭）；`ExposeRule` / `OpenForward`；带半关闭 + 背压的 `Frame` 中继
- [ ] 多副本亲和性：Redis 双映射 + TTL/心跳；原子端口池分配；接受 → 查表 → 本地处理 / 跨 pod 两跳；陈旧窗口的拒绝并重试
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
