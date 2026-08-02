#!/usr/bin/env bash
set -euo pipefail

chart_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

helm lint "$chart_dir"
helm template loadforge "$chart_dir" --namespace loadforge-system >"$tmp_dir/default.yaml"
helm template loadforge "$chart_dir" --namespace loadforge-system \
  --set postgresql.enabled=false,redis.enabled=false,nats.enabled=false \
  --set monitoring.serviceMonitor.enabled=false,monitoring.podMonitor.enabled=false,monitoring.prometheusRule.enabled=false \
  --set-string secrets.externalPostgresDSN='postgres://user:password@postgres.example:5432/loadforge?sslmode=require' \
  --set-string external.redisAddr='redis.example:6379' \
  --set-string external.natsURL='nats://nats.example:4222' >"$tmp_dir/external.yaml"

ruby -e '
require "yaml"
docs = YAML.load_stream(File.read(ARGV[0])).compact
raise "no rendered resources" if docs.empty?
owned = docs.select { |d| d.dig("metadata", "labels", "app.kubernetes.io/name") == "loadforge" }
counts = owned.group_by { |d| d["kind"] }.transform_values(&:length)
raise "expected 3 Deployments, got #{counts["Deployment"]}" unless counts["Deployment"] == 3
raise "expected 3 Services, got #{counts["Service"]}" unless counts["Service"] == 3
raise "expected 4 ServiceAccounts, got #{counts["ServiceAccount"]}" unless counts["ServiceAccount"] == 4
raise "expected worker NetworkPolicy" unless counts["NetworkPolicy"] == 1
raise "expected PrometheusRule" unless counts["PrometheusRule"] == 1
pod_role = owned.find { |d| d["kind"] == "ClusterRole" }
pod_rule = pod_role.fetch("rules").first
raise "unexpected pod RBAC resources" unless pod_rule["resources"] == ["pods"]
raise "unexpected pod RBAC verbs" unless pod_rule["verbs"].sort == %w[create delete get update watch]
lease_role = owned.find { |d| d["kind"] == "Role" }
lease_rule = lease_role.fetch("rules").first
raise "unexpected lease RBAC" unless lease_rule["resources"] == ["leases"] && lease_rule["verbs"].sort == %w[create get update]
policy = owned.find { |d| d["kind"] == "NetworkPolicy" }
ports = policy.dig("spec", "egress").flat_map { |rule| rule.fetch("ports", []) }.map { |port| port["port"] }.sort
raise "unexpected worker egress ports #{ports}" unless ports == [53, 53, 4222, 50051]
' "$tmp_dir/default.yaml"

ruby -e '
require "yaml"
docs = YAML.load_stream(File.read(ARGV[0])).compact
forbidden = docs.any? { |d| %w[StatefulSet].include?(d["kind"]) || d.dig("metadata", "name").to_s.match?(/postgresql|redis-master|-nats$/) }
raise "bundled dependency resource leaked into external render" if forbidden
' "$tmp_dir/external.yaml"

echo "Helm lint, default render, external-services render, YAML parse, and resource assertions passed."
