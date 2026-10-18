{{- define "arbordb.tag" -}}
{{- required "image.tag is required (git commit SHA)" .Values.image.tag -}}
{{- end -}}

{{- define "arbordb.labels" -}}
app.kubernetes.io/part-of: arbordb
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ include "arbordb.tag" . | quote }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{- define "arbordb.podSecurityContext" -}}
runAsNonRoot: true
seccompProfile:
  type: RuntimeDefault
{{- end -}}

{{- define "arbordb.containerSecurityContext" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities:
  drop: ["ALL"]
{{- end -}}
