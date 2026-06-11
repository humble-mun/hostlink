#!/usr/bin/env bash
# Regenerate the hostlink debug PKI: a self-signed CA, a controller (server)
# certificate, and one client certificate per agent. DEBUG ONLY -- 10-year
# validity, committed into the repo for out-of-the-box local testing. Never use
# these for production.
#
# Usage (from the repo root):
#   bash debug/gen-certs.sh                       # default agent id "agent-demo"
#   AGENT_IDS="agent-demo gpu-host-1" bash debug/gen-certs.sh
#
# Output goes to debug/pki/ in the exact on-disk layout the runtime expects:
#   pki/ca.crt  pki/ca.key
#   pki/controller/{tls.crt,tls.key,ca.crt}     <- chart Secret keys
#   pki/agent/<id>/{tls.crt,tls.key,ca.crt}     <- copy to /etc/humble-mun/agent/
# The first agent is also linked at pki/agent/{tls.crt,tls.key,ca.crt} for the
# single-host default case.
set -euo pipefail

# Resolve the directory this script lives in so it works from any CWD.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PKI_DIR="${SCRIPT_DIR}/pki"

DAYS=3650                       # 10 years -- debug only
AGENT_IDS="${AGENT_IDS:-agent-demo}"

# Subject Alternative Names for the controller server certificate. The agent's
# Go TLS client verifies the controller cert's SAN against the configured
# controller-tls-server-name, so every name an agent might dial / set as
# controller-tls-server-name
# MUST appear here. For a real ingress, regenerate with your ingress host added.
CONTROLLER_SANS="DNS:localhost,DNS:hostlink-controller,DNS:hostlink-controller.default.svc,DNS:hostlink-controller.default.svc.cluster.local,DNS:controller.hostlink.local,IP:127.0.0.1"

echo ">> output dir: ${PKI_DIR}"
rm -rf "${PKI_DIR}"
mkdir -p "${PKI_DIR}/controller"

# ---------------------------------------------------------------------------
# 1. Self-signed root CA (EC P-256).
# ---------------------------------------------------------------------------
echo ">> [1/3] generating CA"
openssl ecparam -name prime256v1 -genkey -noout -out "${PKI_DIR}/ca.key"
openssl req -x509 -new -key "${PKI_DIR}/ca.key" -sha256 -days "${DAYS}" \
  -subj "/CN=hostlink-debug-ca" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -out "${PKI_DIR}/ca.crt"

# ---------------------------------------------------------------------------
# 2. Controller (server) certificate, signed by the CA.
#    EKU serverAuth + SAN (verified by the agent's TLS client).
# ---------------------------------------------------------------------------
echo ">> [2/3] generating controller server cert"
openssl ecparam -name prime256v1 -genkey -noout -out "${PKI_DIR}/controller/tls.key"
openssl req -new -key "${PKI_DIR}/controller/tls.key" \
  -subj "/CN=hostlink-controller" \
  -out "${PKI_DIR}/controller/tls.csr"
openssl x509 -req -in "${PKI_DIR}/controller/tls.csr" \
  -CA "${PKI_DIR}/ca.crt" -CAkey "${PKI_DIR}/ca.key" -CAcreateserial \
  -days "${DAYS}" -sha256 \
  -extfile <(printf '%s\n' \
    "basicConstraints=critical,CA:FALSE" \
    "keyUsage=critical,digitalSignature,keyEncipherment" \
    "extendedKeyUsage=serverAuth" \
    "subjectAltName=${CONTROLLER_SANS}") \
  -out "${PKI_DIR}/controller/tls.crt"
rm -f "${PKI_DIR}/controller/tls.csr"
# The controller mounts the whole Secret and needs the CA bundle to verify
# agent client certs, under the key ca.crt.
cp "${PKI_DIR}/ca.crt" "${PKI_DIR}/controller/ca.crt"

# ---------------------------------------------------------------------------
# 3. One client certificate per agent, signed by the CA.
#    EKU clientAuth (verified by the controller via RequireAndVerifyClientCert).
#    The controller does NOT check the agent SAN, so SAN is optional; we add a
#    DNS entry matching the agent id purely for human readability.
# ---------------------------------------------------------------------------
echo ">> [3/3] generating agent client cert(s): ${AGENT_IDS}"
first_agent=""
for id in ${AGENT_IDS}; do
  [ -n "${first_agent}" ] || first_agent="${id}"
  out="${PKI_DIR}/agent/${id}"
  mkdir -p "${out}"
  openssl ecparam -name prime256v1 -genkey -noout -out "${out}/tls.key"
  openssl req -new -key "${out}/tls.key" -subj "/CN=${id}" -out "${out}/tls.csr"
  openssl x509 -req -in "${out}/tls.csr" \
    -CA "${PKI_DIR}/ca.crt" -CAkey "${PKI_DIR}/ca.key" -CAcreateserial \
    -days "${DAYS}" -sha256 \
    -extfile <(printf '%s\n' \
      "basicConstraints=critical,CA:FALSE" \
      "keyUsage=critical,digitalSignature" \
      "extendedKeyUsage=clientAuth" \
      "subjectAltName=DNS:${id}") \
    -out "${out}/tls.crt"
  rm -f "${out}/tls.csr"
  cp "${PKI_DIR}/ca.crt" "${out}/ca.crt"
done

# Convenience: expose the first agent's material at pki/agent/ directly so a
# single-host default deploy can copy pki/agent/{ca.crt,tls.crt,tls.key}.
cp "${PKI_DIR}/agent/${first_agent}/tls.crt" "${PKI_DIR}/agent/tls.crt"
cp "${PKI_DIR}/agent/${first_agent}/tls.key" "${PKI_DIR}/agent/tls.key"
cp "${PKI_DIR}/ca.crt" "${PKI_DIR}/agent/ca.crt"

# ---------------------------------------------------------------------------
# Verify the chains validate before we declare success.
# ---------------------------------------------------------------------------
echo ">> verifying chains"
openssl verify -CAfile "${PKI_DIR}/ca.crt" "${PKI_DIR}/controller/tls.crt"
for id in ${AGENT_IDS}; do
  openssl verify -CAfile "${PKI_DIR}/ca.crt" "${PKI_DIR}/agent/${id}/tls.crt"
done

cat <<EOF

Done. Generated under debug/pki/:
  ca.crt / ca.key                          self-signed debug CA (10y)
  controller/{tls.crt,tls.key,ca.crt}      controller server cert (SAN+serverAuth)
  agent/<id>/{tls.crt,tls.key,ca.crt}      per-agent client certs (clientAuth)
  agent/{tls.crt,tls.key,ca.crt}           -> ${first_agent} (single-host default)

Create the controller Secret the Helm chart consumes (key names matter):
  kubectl create secret generic hostlink-controller-grpc-tls \\
    --namespace <ns> \\
    --from-file=tls.crt=debug/pki/controller/tls.crt \\
    --from-file=tls.key=debug/pki/controller/tls.key \\
    --from-file=ca.crt=debug/pki/controller/ca.crt

On each agent host, copy debug/pki/agent/<id>/{ca.crt,tls.crt,tls.key} to
/etc/humble-mun/agent/ and set controller-endpoint + controller-tls-server-name (one of the
controller SAN entries, e.g. hostlink-controller) in /etc/humble-mun/agent.yaml.
EOF
