# hostlink debug PKI

**Debug only.** This directory contains a throwaway certificate authority and a
set of **10-year** certificates for exercising hostlink's mutual-TLS link on a
workstation or in a scratch cluster. The private keys are committed in the clear
under [`pki/`](./pki). **Never use any of this in production** — regenerate a
real PKI with short-lived, properly protected keys instead.

## The mTLS model (why the certs look the way they do)

The agent ↔ controller gRPC connection is **mutually authenticated TLS, TLS 1.3,
with no insecure fallback**:

- The **controller** presents a *server* certificate and runs
  `RequireAndVerifyClientCert`, so it verifies that every agent's *client*
  certificate chains to the agent **CA**. It does **not** check the agent's
  hostname/SAN.
- The **agent** presents a *client* certificate and verifies the controller's
  *server* certificate against the **CA** *and* against the server name it was
  told to expect (`controller-tls-server-name`).

That asymmetry dictates the certificate extensions:

| Certificate | Extended Key Usage | Subject Alternative Name |
|-------------|--------------------|--------------------------|
| controller (server) | `serverAuth` | **must** contain the name the agent dials / sets as `controller-tls-server-name` |
| agent (client) | `clientAuth` | optional (controller ignores it; we set `DNS:<agent-id>` for readability) |

All keys are EC P-256, signatures SHA-256, validity 3650 days.

## Prerequisites

- An `openssl` CLI (3.x) available on your `PATH`.

## Quick start

Everything below is already pre-generated under [`pki/`](./pki). To regenerate,
run from the repo root:

```bash
bash debug/gen-certs.sh
# or, for multiple agents:
AGENT_IDS="agent-demo gpu-host-1" bash debug/gen-certs.sh
```

The sections below explain each step the script performs, so you can reproduce
or adapt it by hand.

### Step A — self-signed CA

```bash
openssl ecparam -name prime256v1 -genkey -noout -out pki/ca.key
openssl req -x509 -new -key pki/ca.key -sha256 -days 3650 \
  -subj "/CN=hostlink-debug-ca" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -out pki/ca.crt
```

This single CA signs both the controller and every agent. Both sides trust it as
their verification bundle (`ca.crt`).

### Step B — controller (server) certificate

```bash
openssl ecparam -name prime256v1 -genkey -noout -out pki/controller/tls.key
openssl req -new -key pki/controller/tls.key -subj "/CN=hostlink-controller" \
  -out /tmp/controller.csr
openssl x509 -req -in /tmp/controller.csr \
  -CA pki/ca.crt -CAkey pki/ca.key -CAcreateserial \
  -days 3650 -sha256 \
  -extfile <(printf '%s\n' \
    "basicConstraints=critical,CA:FALSE" \
    "keyUsage=critical,digitalSignature,keyEncipherment" \
    "extendedKeyUsage=serverAuth" \
    "subjectAltName=DNS:localhost,DNS:hostlink-controller,DNS:hostlink-controller.default.svc,DNS:hostlink-controller.default.svc.cluster.local,DNS:controller.hostlink.local,IP:127.0.0.1") \
  -out pki/controller/tls.crt
```

**The SAN list is the part you must get right.** The agent verifies the
controller certificate's SAN against whatever it has configured as
`controller-tls-server-name`. The default list above covers `localhost`, the bare service
name, the in-cluster DNS names, and a sample external name. **For a real ingress
you must regenerate with your actual ingress host in the SAN** and set the
agent's `controller-tls-server-name` to one of these entries. (Note: the documented
"defaults to the endpoint host" behaviour for an empty `controller-tls-server-name` is *not*
implemented in the agent yet — always set it explicitly.)

### Step C — per-agent (client) certificate

Repeat this for every agent; give each a distinct id (it becomes the CN):

```bash
AGENT_ID=agent-demo
openssl ecparam -name prime256v1 -genkey -noout -out pki/agent/$AGENT_ID/tls.key
openssl req -new -key pki/agent/$AGENT_ID/tls.key -subj "/CN=$AGENT_ID" \
  -out /tmp/agent.csr
openssl x509 -req -in /tmp/agent.csr \
  -CA pki/ca.crt -CAkey pki/ca.key -CAcreateserial \
  -days 3650 -sha256 \
  -extfile <(printf '%s\n' \
    "basicConstraints=critical,CA:FALSE" \
    "keyUsage=critical,digitalSignature" \
    "extendedKeyUsage=clientAuth" \
    "subjectAltName=DNS:$AGENT_ID") \
  -out pki/agent/$AGENT_ID/tls.crt
```

`extendedKeyUsage=clientAuth` is **required** — the controller rejects a client
certificate without it.

### Step D — verify

```bash
openssl verify -CAfile pki/ca.crt pki/controller/tls.crt
openssl verify -CAfile pki/ca.crt pki/agent/agent-demo/tls.crt
# inspect SAN / EKU / dates:
openssl x509 -in pki/controller/tls.crt -noout -ext subjectAltName,extendedKeyUsage -dates
openssl x509 -in pki/agent/agent-demo/tls.crt -noout -ext extendedKeyUsage -dates
```

### Step E — create the Kubernetes Secret the chart consumes

The Helm chart (`charts/hostlink`) mounts a Secret named after
`.Values.grpc.tlsSecretName` (default `hostlink-controller-grpc-tls`) at
`/etc/humble-mun/controller/`, and the controller reads `tls.crt`, `tls.key`,
and `ca.crt` from there. The Secret therefore needs **all three keys** — a
`kubernetes.io/tls` typed Secret only carries `tls.crt`/`tls.key`, which is
**insufficient** because the controller also needs `ca.crt` to verify agents.
Use a generic Secret with three explicit keys:

```bash
kubectl create secret generic hostlink-controller-grpc-tls \
  --namespace <ns> \
  --from-file=tls.crt=debug/pki/controller/tls.crt \
  --from-file=tls.key=debug/pki/controller/tls.key \
  --from-file=ca.crt=debug/pki/controller/ca.crt
```

Declarative variant (GitOps-friendly):

```bash
kubectl create secret generic hostlink-controller-grpc-tls \
  --namespace <ns> \
  --from-file=tls.crt=debug/pki/controller/tls.crt \
  --from-file=tls.key=debug/pki/controller/tls.key \
  --from-file=ca.crt=debug/pki/controller/ca.crt \
  --dry-run=client -o yaml | kubectl apply -f -
```

If you change the Secret name, override it at install time:
`helm install <release> charts/hostlink --set grpc.tlsSecretName=<name>`.

### Step F — deploy the agent certificates on a host

Copy the agent's three files into the path the systemd unit expects, then point
the agent at the controller:

```bash
sudo install -d -m 0750 /etc/humble-mun/agent
sudo install -m 0644 debug/pki/agent/agent-demo/ca.crt  /etc/humble-mun/agent/ca.crt
sudo install -m 0644 debug/pki/agent/agent-demo/tls.crt /etc/humble-mun/agent/tls.crt
sudo install -m 0640 debug/pki/agent/agent-demo/tls.key /etc/humble-mun/agent/tls.key
```

Then set the connection details in `/etc/humble-mun/hostlink.yaml` (see
[`deploy/hostlink.yaml`](../deploy/hostlink.yaml) for the full template):

```yaml
controller-endpoint: hostlink-controller:8443   # host:port the agent dials
controller-tls-server-name: hostlink-controller    # MUST be a SAN entry of the controller cert
controller-tls-ca-path: /etc/humble-mun/agent/ca.crt
agent-tls-cert-path: /etc/humble-mun/agent/tls.crt
agent-tls-key-path: /etc/humble-mun/agent/tls.key
node-name: agent-demo
```

## File layout

```
debug/
├── gen-certs.sh                 regenerate everything (idempotent)
└── pki/
    ├── ca.crt  ca.key           the debug CA (ca.srl is an openssl artifact)
    ├── controller/
    │   ├── tls.crt tls.key      controller server cert + key (SAN, serverAuth)
    │   └── ca.crt               CA bundle (verifies agent client certs)
    └── agent/
        ├── tls.crt tls.key ca.crt   convenience copy of the first agent
        └── <agent-id>/
            ├── tls.crt tls.key  agent client cert + key (clientAuth)
            └── ca.crt           CA bundle (verifies the controller)
```
