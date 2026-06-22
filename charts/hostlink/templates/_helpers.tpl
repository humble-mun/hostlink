{{/*
hostlink.certVolumeSource renders the volume source for a TLS cert/key/ca bundle.
When certManager.enabled, it emits a cert-manager CSI volume that issues a
short-lived per-pod certificate; otherwise it mounts a pre-created Secret.

Args (dict):
  root       - the root context ($)
  secretName - Secret name to mount when cert-manager is disabled
  dnsNames   - comma-separated SANs for the issued cert (cert-manager mode);
               supports the CSI driver's ${POD_NAME}/${POD_NAMESPACE} expansion
*/}}
{{- define "hostlink.certVolumeSource" -}}
{{- $cm := .root.Values.certManager -}}
{{- if $cm.enabled -}}
csi:
  driver: csi.cert-manager.io
  readOnly: true
  volumeAttributes:
    csi.cert-manager.io/issuer-name: {{ $cm.issuerName | quote }}
    csi.cert-manager.io/issuer-kind: {{ $cm.issuerKind | quote }}
    csi.cert-manager.io/issuer-group: {{ $cm.issuerGroup | quote }}
    csi.cert-manager.io/dns-names: {{ .dnsNames | quote }}
    csi.cert-manager.io/duration: {{ $cm.duration | quote }}
    csi.cert-manager.io/renew-before: {{ $cm.renewBefore | quote }}
{{- else }}
secret:
  secretName: {{ .secretName | quote }}
{{- end -}}
{{- end -}}
