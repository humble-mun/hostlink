# hostlink-agent — manual installation

This directory holds everything needed to install `hostlink-agent` on an
external Linux host (on-prem / colo, behind NAT) as a **systemd service**:

```
deploy/
├── hostlink-agent.service   the systemd unit (no flags — config lives in the YAML)
└── agent.yaml               documented example config, installs to /etc/humble-mun/agent.yaml
```

The agent is a single static Go binary. It dials **outbound** to the cloud-side
`hostlink-controller` over a mutually-authenticated TLS (mTLS) gRPC connection
and keeps it open — so the host needs no inbound firewall rules, only outbound
reachability to the controller.

> This guide installs an agent on a host. It assumes you already have the mTLS
> material (`ca.crt`, `tls.crt`, `tls.key`) for this agent. For a workstation /
> scratch-cluster PKI you can generate those throwaway certs with
> [`debug/`](../debug/README.md) — see [Step 2](#step-2--install-the-mtls-material).
> For production, provision a real, short-lived PKI out-of-band instead.

## Prerequisites

| Requirement | Note |
|-------------|------|
| Linux host with systemd | The agent manages a Linux Docker daemon; it is Linux-only |
| Outbound reachability to the controller | `host:port` of the controller's mTLS gRPC endpoint |
| The agent's mTLS material | `ca.crt` (verifies the controller), `tls.crt` + `tls.key` (this agent's client identity) |
| `root` / `sudo` | To install the binary, create the service user, and write under `/etc/humble-mun/` |

Docker is used **lazily**: the agent opens a Docker client (`client.FromEnv`) but
does not dial the daemon until a request needs it. The only implemented request,
`images.list` (behind the controller's `GET /api/v1/agents/<id>/images`), reads
the local image list — so for that endpoint to work the daemon must be reachable
and the service user must be able to access the Docker socket (add `hostlink` to
the `docker` group — see [Step 4](#step-4--create-the-service-user)). The agent
itself still starts and runs the `Control` stream (`Hello` + `Heartbeat`) without
Docker, so the unit keeps `docker.service` as a soft (`Wants`) dependency.

## Step 1 — build and install the binary

Build the agent (the module is vendored; build offline with `-mod=vendor`). On a
non-Linux dev machine, cross-compile or build inside a Linux container — see the
top-level [README](../README.md#build):

```bash
GOOS=linux GOARCH=amd64 go build -mod=vendor -o bin/hostlink-agent ./cmd/agent
```

Copy it onto the host and install it where the unit expects it:

```bash
sudo install -m 0755 bin/hostlink-agent /usr/local/bin/hostlink-agent
```

> The binary is prefixed `hostlink-` so it does not collide with some other
> `agent` in `ps`, in packaging, or in systemd units.

## Step 2 — install the mTLS material

The agent reads three files from `/etc/humble-mun/agent/`:

| File | Mode | Purpose |
|------|------|---------|
| `ca.crt`  | 0644 | CA bundle used to **verify the controller's** server certificate |
| `tls.crt` | 0644 | **Client** certificate the agent presents to the controller (`clientAuth`) |
| `tls.key` | 0640 | Private key matching `tls.crt` — keep it readable only by the agent user |

Create the directory and drop the files in:

```bash
sudo install -d -m 0750 /etc/humble-mun/agent
sudo install -m 0644 <src>/ca.crt  /etc/humble-mun/agent/ca.crt
sudo install -m 0644 <src>/tls.crt /etc/humble-mun/agent/tls.crt
sudo install -m 0640 <src>/tls.key /etc/humble-mun/agent/tls.key
```

For a debug PKI generated under [`debug/`](../debug/README.md), `<src>` is
`debug/pki/agent/<agent-id>/` (this is exactly Step F of the debug guide):

```bash
sudo install -m 0644 debug/pki/agent/agent-demo/ca.crt  /etc/humble-mun/agent/ca.crt
sudo install -m 0644 debug/pki/agent/agent-demo/tls.crt /etc/humble-mun/agent/tls.crt
sudo install -m 0640 debug/pki/agent/agent-demo/tls.key /etc/humble-mun/agent/tls.key
```

> **mTLS, no insecure fallback.** If any of these files are missing or invalid
> the connection fails hard. The controller runs `RequireAndVerifyClientCert`,
> so `tls.crt` **must** carry `extendedKeyUsage=clientAuth`. The agent verifies
> the controller's certificate against `ca.crt` **and** against `controller-tls-server-name`
> (see [Step 3](#step-3--write-the-configuration)).

## Step 3 — write the configuration

The agent reads **all** of its settings from `/etc/humble-mun/agent.yaml`. The
lookup path is fixed (the chassis registers `SetConfigName("agent")` +
`AddConfigPath("/etc/humble-mun")`, and the binary's name is `agent`), so the
file must live exactly there. Copy the documented template from this directory
and edit it:

```bash
sudo install -m 0644 deploy/agent.yaml /etc/humble-mun/agent.yaml
sudo $EDITOR /etc/humble-mun/agent.yaml
```

The keys are the flag names verbatim:

| Key | Required | Description |
|-----|----------|-------------|
| `controller-endpoint` | **yes** | `host:port` the agent dials; the agent exits immediately if empty |
| `controller-tls-server-name` | yes (in practice) | Name verified against the controller cert's SAN — **must be a SAN entry of the controller cert**; the "defaults to endpoint host" behavior is not implemented yet, so always set it |
| `controller-tls-ca-path` | yes | Path to `ca.crt` (default `/etc/humble-mun/agent/ca.crt`) |
| `agent-tls-cert-path` | yes | Path to `tls.crt` (default `/etc/humble-mun/agent/tls.crt`) |
| `agent-tls-key-path` | yes | Path to `tls.key` (default `/etc/humble-mun/agent/tls.key`) |
| `node-name` | recommended | Logical name for this host; becomes the `agent_id` the controller sees — set it to the hostname or another stable identifier |

A minimal config (matching the debug PKI's `agent-demo`):

```yaml
controller-endpoint: hostlink-controller:8443   # host:port the agent dials
controller-tls-server-name: hostlink-controller    # MUST be a SAN entry of the controller cert
controller-tls-ca-path: /etc/humble-mun/agent/ca.crt
agent-tls-cert-path: /etc/humble-mun/agent/tls.crt
agent-tls-key-path: /etc/humble-mun/agent/tls.key
node-name: agent-demo
```

> Any key can also be overridden by an `HM_*` environment variable (uppercase
> the key, replace `-` with `_`, prefix `HM_` — e.g. `controller-endpoint` →
> `HM_CONTROLLER_ENDPOINT`). Precedence: flags > env > config file.
>
> The agent watches this file (`viper.WatchConfig`), so later edits are picked
> up **at runtime** without `systemctl daemon-reload` and without restarting the
> unit. That is why the systemd unit passes **no** command-line flags.

## Step 4 — create the service user

The unit runs as a dedicated, stable system user (`hostlink`). A static user —
not `DynamicUser` — is required because the mTLS material is provisioned
out-of-band and must have a predictable owner (and because the agent will later
need to join the `docker` group). Create it once and hand it the cert directory:

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin hostlink
sudo chown -R hostlink:hostlink /etc/humble-mun/agent
sudo chmod 0750 /etc/humble-mun/agent
sudo chmod 0640 /etc/humble-mun/agent/tls.key
```

## Step 5 — install and start the unit

```bash
sudo install -m 0644 deploy/hostlink-agent.service /etc/systemd/system/hostlink-agent.service
sudo systemctl daemon-reload
sudo systemctl enable --now hostlink-agent
```

The unit reconnects on any exit with a short backoff (`Restart=always`,
`RestartSec=5s`), but a hard-failing config (bad or missing certs, empty
`controller-endpoint`) is rate-limited by `StartLimitIntervalSec=60` /
`StartLimitBurst=5` so it does not hot-loop forever.

## Step 6 — verify

```bash
# Is the unit up?
systemctl status hostlink-agent

# Follow the logs (the unit logs to the journal under SyslogIdentifier=hostlink-agent):
journalctl -u hostlink-agent -f
```

A healthy agent logs `agent started`, establishes the mTLS connection, sends a
`Hello`, and then emits periodic heartbeats. On the controller side you should
see the corresponding events for this agent's `node-name`.

If the connection fails hard, it is almost always the certs or the server name:

```bash
# Confirm the agent's client cert is signed by the same CA and carries clientAuth:
openssl verify -CAfile /etc/humble-mun/agent/ca.crt /etc/humble-mun/agent/tls.crt
openssl x509 -in /etc/humble-mun/agent/tls.crt -noout -ext extendedKeyUsage -dates

# controller-tls-server-name MUST match a SAN entry of the controller's server certificate.
```

See [`debug/README.md`](../debug/README.md) for the full mTLS model and how the
certificate extensions (`serverAuth` vs `clientAuth`, the controller SAN list)
must line up.

## Uninstall

```bash
sudo systemctl disable --now hostlink-agent
sudo rm /etc/systemd/system/hostlink-agent.service
sudo systemctl daemon-reload
sudo rm -rf /etc/humble-mun/agent /etc/humble-mun/agent.yaml
sudo rm /usr/local/bin/hostlink-agent
sudo userdel hostlink
```
