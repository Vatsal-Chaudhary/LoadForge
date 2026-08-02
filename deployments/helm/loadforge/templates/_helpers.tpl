{{- define "loadforge.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- define "loadforge.fullname" -}}
{{- if .Values.fullnameOverride }}{{ .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}{{ else }}{{ printf "%s-%s" .Release.Name (include "loadforge.name" .) | trunc 63 | trimSuffix "-" }}{{ end -}}
{{- end -}}
{{- define "loadforge.labels" -}}
app.kubernetes.io/name: {{ include "loadforge.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}
{{- define "loadforge.selectorLabels" -}}
app.kubernetes.io/name: {{ include "loadforge.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
{{- define "loadforge.postgresHost" -}}{{ printf "%s-postgresql" .Release.Name }}{{- end -}}
{{- define "loadforge.redisAddr" -}}{{- if .Values.redis.enabled -}}{{ printf "%s-redis-master:6379" .Release.Name }}{{- else -}}{{ required "external.redisAddr is required when redis.enabled=false" .Values.external.redisAddr }}{{- end -}}{{- end -}}
{{- define "loadforge.natsURL" -}}{{- if .Values.nats.enabled -}}{{ printf "nats://%s-nats:4222" .Release.Name }}{{- else -}}{{ required "external.natsURL is required when nats.enabled=false" .Values.external.natsURL }}{{- end -}}{{- end -}}
{{- define "loadforge.image" -}}{{ printf "%s:%s" .repository .tag }}{{- end -}}
