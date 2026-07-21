# LoadForge — Functional Design Document
### Distributed Load Testing Platform | v1.0
**Author:** Vatsal Chaudhary  
**Stack:** Go · Kubernetes · gRPC · Prometheus · PostgreSQL · Redis · NATS

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Goals & Non-Goals](#2-goals--non-goals)
3. [System Architecture](#3-system-architecture)
4. [Component Deep Dive](#4-component-deep-dive)
   - 4.1 Control Plane (Orchestrator)
   - 4.2 Worker Agent
   - 4.3 CLI
   - 4.4 Metrics Aggregator
   - 4.5 Dashboard / API Server
5. [Tech Stack Decisions](#5-tech-stack-decisions)
6. [Functional Requirements](#6-functional-requirements)
7. [Non-Functional Requirements](#7-non-functional-requirements)
8. [Data Models](#8-data-models)
9. [API Design](#9-api-design)
10. [Worker Lifecycle & Scaling Logic](#10-worker-lifecycle--scaling-logic)
11. [Monitoring Architecture](#11-monitoring-architecture)
12. [Storage Design](#12-storage-design)
13. [Security Design](#13-security-design)
14. [Configuration Design](#14-configuration-design)
15. [Error Handling & Fault Tolerance](#15-error-handling--fault-tolerance)
16. [Deployment Architecture](#16-deployment-architecture)
17. [Milestones & Implementation Order](#17-milestones--implementation-order)
18. [Open Questions](#18-open-questions)

---

## 1. Project Overview

LoadForge is a **self-hosted, open-source distributed load testing platform** written entirely in Go. It allows engineering teams to run realistic, large-scale load tests against any HTTP/gRPC/WebSocket service by automatically spinning up a **fleet of containerized worker agents** that generate distributed traffic, scaling the fleet up or down dynamically based on the target load configuration.

Unlike commercial tools that charge per virtual user, LoadForge runs on your own Kubernetes cluster. The fleet is ephemeral — containers appear when a test starts, scale dynamically as load ramps up, and are destroyed when the test ends. While the test runs, LoadForge simultaneously monitors both **the test fleet itself** (are workers healthy? are they bottlenecked?) and **the application under test** (latency, error rates, throughput).

### The Core Loop

```
User defines test plan (YAML)
        ↓
Control Plane provisions N worker pods on Kubernetes
        ↓
Workers execute load against target service
        ↓
Workers stream metrics → Metrics Aggregator
        ↓
Aggregator writes to Prometheus + PostgreSQL
        ↓
Dashboard shows real-time graphs of both fleet health & app behavior
        ↓
Control Plane auto-scales worker count based on ramp-up profile
        ↓
Test ends → fleet destroyed → final report generated
```

---

## 2. Goals & Non-Goals

### Goals

- **G1** — Run distributed HTTP/gRPC/WebSocket load tests from a Kubernetes-native fleet of worker containers
- **G2** — Auto-scale the worker fleet up and down according to the test's load profile (ramp-up, hold, ramp-down)
- **G3** — Monitor the test fleet (CPU/mem/goroutines per worker, request dispatch rate) in real time
- **G4** — Monitor the application under test (latency percentiles, error rate, throughput, status code distribution) in real time
- **G5** — Support multiple test types: constant load, step ramp, spike, soak, and stress
- **G6** — Support JMeter-compatible scenario scripts AND a native YAML test plan format
- **G7** — Produce a structured final report (HTML + JSON) per test run
- **G8** — CLI-first UX with an optional web dashboard
- **G9** — Deployable via a single Helm chart
- **G10** — Pluggable protocol support (HTTP first, gRPC and WebSocket in later milestones)

### Non-Goals

- Not a SaaS product (no multi-tenancy billing, no hosted runners)
- Not a browser/Selenium-based UI testing tool
- Not a chaos engineering tool (no fault injection, no kill signals to the app under test)
- Not a CI/CD system (integrates with CI but does not replace it)
- No built-in APM agent for the application under test (relies on existing observability)

---

## 3. System Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        CONTROL PLANE                                │
│                                                                     │
│  ┌──────────────┐   gRPC    ┌────────────────────────────────────┐  │
│  │   REST API   │◄─────────►│         Orchestrator               │  │
│  │   Server     │           │  - Test lifecycle FSM              │  │
│  └──────┬───────┘           │  - Fleet provisioner (K8s client)  │  │
│         │                   │  - Ramp-up / scale controller      │  │
│         │                   │  - Health watchdog                 │  │
│  ┌──────▼───────┐           └──────────┬─────────────────────────┘  │
│  │  PostgreSQL  │                      │ Kubernetes API             │
│  │  (test runs, │           ┌──────────▼─────────────────────────┐  │
│  │   results,   │           │       Kubernetes Cluster           │  │
│  │   configs)   │           │                                    │  │
│  └──────────────┘           │  ┌─────────┐ ┌─────────┐          │  │
│                             │  │Worker-0 │ │Worker-1 │  ...     │  │
│  ┌──────────────┐           │  │(Go pod) │ │(Go pod) │          │  │
│  │    Redis     │           │  └────┬────┘ └────┬────┘          │  │
│  │  (live stats,│           │       │            │               │  │
│  │   job queue) │           └───────┼────────────┼───────────────┘  │
│  └──────────────┘                   │            │                   │
│                                     │ NATS       │                   │
│  ┌──────────────┐           ┌───────▼────────────▼───────────────┐  │
│  │  Prometheus  │◄──scrape──│      Metrics Aggregator            │  │
│  │  + Grafana   │           │  - Collects worker streams         │  │
│  └──────────────┘           │  - Aggregates p50/p95/p99          │  │
│                             │  - Writes to Prom + Postgres       │  │
│                             └────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘

        CLI (loadforge) ──────────────────────────► REST API
        Web Dashboard   ──────────────────────────► REST API
```

### Communication Boundaries

| From | To | Protocol | Why |
|------|----|----------|-----|
| CLI / Dashboard | API Server | REST/HTTP | Simple, human-readable, easy to curl |
| Orchestrator | Kubernetes | k8s client-go | Native K8s pod management |
| Orchestrator ↔ Workers | gRPC (bidirectional stream) | Low-latency control signals, typed |
| Workers → Aggregator | NATS (pub/sub) | High-throughput, fire-and-forget metrics |
| Aggregator → Prometheus | Prometheus exposition format | Standard observability |
| Orchestrator → Redis | Redis client | Job queue, live counters |

---

## 4. Component Deep Dive

### 4.1 Control Plane — Orchestrator

The Orchestrator is the **brain** of LoadForge. It is a long-running Go service that owns the entire lifecycle of a test run.

**Responsibilities:**
- Receives a test run request (from the API Server via internal gRPC call)
- Parses and validates the test plan
- Provisions the initial worker pod fleet via the Kubernetes client
- Executes the ramp-up controller (scales fleet based on the load profile)
- Maintains a health watchdog for each worker pod
- Terminates the fleet when the test ends or when a fatal error occurs
- Writes test run state transitions to PostgreSQL

**Key internal packages:**
```
orchestrator/
  fsm/          — Test run state machine (Pending → Running → Scaling → Draining → Done)
  provisioner/  — Kubernetes pod creation, deletion, watch
  scaler/       — Ramp-up algorithm, desired vs actual worker count reconciliation
  watchdog/     — Per-worker health check via gRPC heartbeat
  planner/      — Parses YAML test plan into internal TestPlan struct
```

**Test Run State Machine:**

```
PENDING ──── validated ──── PROVISIONING ──── pods ready ──── RUNNING
                                                                  │
                                                          ┌───────┴────────┐
                                                      scaling?           completed
                                                          │                │
                                                       SCALING           DRAINING
                                                          │                │
                                                       RUNNING          DONE / FAILED
```

**Orchestrator Config (loaded from env + ConfigMap):**
```go
type OrchestratorConfig struct {
    KubeConfigPath     string
    WorkerNamespace    string
    WorkerImage        string
    MaxWorkersPerTest  int
    HeartbeatInterval  time.Duration
    ScaleCheckInterval time.Duration
    NATSUrl            string
    PostgresDSN        string
    RedisAddr          string
}
```

---

### 4.2 Worker Agent

Each Worker is a **Go binary running inside a Docker container** deployed as a Kubernetes Pod. Workers are stateless and ephemeral — they receive their full test configuration from the Orchestrator via gRPC stream on startup.

**Responsibilities:**
- Connect to Orchestrator via gRPC on startup (the Orchestrator address is injected as an env var)
- Receive test plan segment (which endpoints to hit, which virtual users to simulate)
- Execute load: spawn N goroutines, each acting as a virtual user
- Collect per-request metrics (latency, status code, bytes received, error)
- Publish metrics batches to NATS every 500ms
- Respond to Orchestrator heartbeats
- Gracefully drain when Orchestrator sends STOP signal

**Virtual User Loop:**
```go
// Each virtual user is a goroutine running this loop
func (vu *VirtualUser) Run(ctx context.Context) {
    for {
        select {
        case <-ctx.Done():
            return
        default:
            for _, step := range vu.scenario.Steps {
                start := time.Now()
                resp, err := vu.client.Do(step.Request)
                latency := time.Since(start)
                vu.metrics.Record(latency, resp, err)
                time.Sleep(step.ThinkTime)
            }
        }
    }
}
```

**Key internal packages:**
```
worker/
  executor/     — Virtual user goroutine pool management
  scenario/     — Parses and runs test scenario steps
  reporter/     — Batches metrics and publishes to NATS
  client/       — HTTP/gRPC/WebSocket client implementations
  grpcclient/   — gRPC connection to Orchestrator for control signals
```

**Worker environment (injected by Orchestrator at pod creation):**
```yaml
env:
  - name: LOADFORGE_ORCHESTRATOR_ADDR
    value: "orchestrator.loadforge.svc.cluster.local:50051"
  - name: LOADFORGE_TEST_RUN_ID
    value: "run-abc123"
  - name: LOADFORGE_WORKER_ID
    value: "worker-3"
  - name: LOADFORGE_NATS_URL
    value: "nats://nats.loadforge.svc.cluster.local:4222"
```

---

### 4.3 CLI — `loadforge`

The CLI is a Go binary (using `cobra`) that users install locally. It talks to the API Server over REST.

**Commands:**
```
loadforge run      <test-plan.yaml>    — Submit and stream a test run
loadforge status   <run-id>           — Get current state of a test run
loadforge stop     <run-id>           — Gracefully stop a test run
loadforge report   <run-id>           — Download the final report
loadforge list                        — List recent test runs
loadforge config   set/get            — Configure API Server endpoint, auth token
loadforge validate <test-plan.yaml>   — Validate a test plan without running it
loadforge worker   logs <run-id>      — Stream worker logs for a run
loadforge dashboard                   — Open the web dashboard in browser
```

**Live streaming during `run`:**
```
$ loadforge run api-stress.yaml

  LoadForge v1.0  ─────────────────────────────────────
  Test Plan : API Stress Test
  Target    : https://api.example.com
  Duration  : 5m | Workers: 1 → 10 (step ramp)

  Time     Workers  RPS      p50      p95      p99     Errors
  ─────────────────────────────────────────────────────────
  0:00:10    2       412     23ms     87ms     204ms   0.0%
  0:00:20    4       891     19ms     72ms     189ms   0.1%
  0:00:30    6      1402     21ms     91ms     231ms   0.3%
  ...

  ✓ Test complete. Run ID: run-abc123
  → Report: loadforge report run-abc123
```

---

### 4.4 Metrics Aggregator

A standalone Go service that receives raw metrics from all workers via NATS and computes aggregated statistics.

**Responsibilities:**
- Subscribe to NATS topic `loadforge.metrics.<run-id>.*`
- Accumulate raw data points in a sliding time window (default: 10s)
- Compute p50, p95, p99 latency using HDR histogram (codahale/hdrhistogram)
- Compute RPS, error rate, bytes/sec
- Expose a Prometheus `/metrics` endpoint (scraped by Prometheus every 10s)
- Write per-second snapshots to PostgreSQL for final report generation
- Write live counters to Redis for the dashboard's real-time view

**NATS Message Format (per worker batch, published every 500ms):**
```go
type MetricsBatch struct {
    RunID     string         `json:"run_id"`
    WorkerID  string         `json:"worker_id"`
    Timestamp time.Time      `json:"ts"`
    Samples   []RequestSample `json:"samples"`
}

type RequestSample struct {
    Endpoint   string        `json:"endpoint"`
    Method     string        `json:"method"`
    StatusCode int           `json:"status_code"`
    LatencyMs  float64       `json:"latency_ms"`
    BytesRecv  int64         `json:"bytes_recv"`
    Error      string        `json:"error,omitempty"`
}
```

---

### 4.5 REST API Server

A Go HTTP server (using `chi` router) that acts as the user-facing interface for the CLI and web dashboard.

**Responsibilities:**
- Accept test run submissions
- Forward control commands to the Orchestrator (via internal gRPC)
- Query PostgreSQL for historical run data
- Stream live metrics from Redis for in-progress runs (SSE endpoint)
- Serve the web dashboard (embedded static files via `embed.FS`)
- Handle authentication (API key, future: OIDC)

---

## 5. Tech Stack Decisions

### Core Language: Go

Go is chosen because:
- Native goroutine model is ideal for simulating thousands of virtual users cheaply
- Fast compilation, easy cross-compilation for Docker builds
- Excellent Kubernetes ecosystem (client-go, controller-runtime)
- Strong standard library for HTTP clients, TLS, JSON
- Good gRPC support (google.golang.org/grpc)

### Container Orchestration: Kubernetes

- Workers are K8s Pods in a dedicated namespace (`loadforge-workers`)
- Orchestrator uses `client-go` to create/delete pods dynamically
- No Kubernetes operator needed for v1 — direct pod management is sufficient
- Future: migrate to a custom Kubernetes Job or CRD-based approach

### Messaging: NATS

- Chosen over Kafka for simplicity — no ZooKeeper/KRaft, no broker cluster to manage
- Workers publish at high frequency (every 500ms); NATS handles this trivially
- NATS JetStream for durable message delivery if needed in v2
- Alternative considered: gRPC streams directly — rejected because NATS decouples workers from aggregator; workers don't need to know about the aggregator

### Databases

| Component | Storage | Why |
|-----------|---------|-----|
| Test run metadata, final results | PostgreSQL | Relational, ACID, easy to query for reports |
| Live counters during test | Redis | Sub-millisecond reads, natural TTL, Pub/Sub |
| Time-series metrics | Prometheus | Standard, integrates with Grafana |

### Observability: Prometheus + Grafana

- Aggregator exposes `/metrics`; Prometheus scrapes it
- Two separate Grafana dashboards:
  - **Fleet Health Dashboard** — per-worker CPU, memory, goroutine count, dispatch rate
  - **App Under Test Dashboard** — latency percentiles, RPS, error rate, status code distribution
- Alert rules via Prometheus AlertManager (e.g., error rate > 5% → alert)

### gRPC: Orchestrator ↔ Workers

```protobuf
service WorkerControl {
  rpc Register(WorkerInfo) returns (TestPlan);
  rpc Heartbeat(stream HeartbeatRequest) returns (stream HeartbeatResponse);
  rpc Stop(StopRequest) returns (StopResponse);
}
```

Workers call `Register` on startup → receive their test plan. Then open a bidirectional `Heartbeat` stream for liveness tracking. Orchestrator sends STOP via the heartbeat response stream.

### CLI Framework: Cobra + Viper

- `cobra` for command structure
- `viper` for config file (`~/.loadforge/config.yaml`) and env var override

### Router: chi

- Lightweight, idiomatic Go HTTP router
- No magic, easy middleware composition
- Good for SSE (Server-Sent Events) streaming

### Serialization: Protocol Buffers + JSON

- gRPC messages: protobuf
- REST API: JSON
- Internal config: YAML (gopkg.in/yaml.v3)

### Histogram: HdrHistogram

- `codahale/hdrhistogram` for accurate p95/p99 computation
- Standard sliding window (10s) to avoid stale data distorting percentiles

---

## 6. Functional Requirements

### 6.1 Test Plan Definition

**FR-001** — The system SHALL accept a test plan defined in YAML format.

**FR-002** — A test plan SHALL define at minimum:
- Target base URL
- Load profile (type, duration, parameters)
- One or more scenario steps (method, path, headers, body, think time)
- Worker resource requests (CPU, memory)

**FR-003** — The system SHALL validate the test plan before provisioning any workers. Validation errors SHALL be returned to the user with specific field-level messages.

**FR-004** — The system SHALL support named variables in test plans (e.g., `{{ .base_url }}`), resolved from a separate variables file or environment.

**Example test plan:**
```yaml
name: "API Stress Test"
version: "1"

target:
  base_url: "https://api.example.com"
  tls_skip_verify: false

load_profile:
  type: step_ramp        # constant | step_ramp | spike | soak | stress
  initial_workers: 1
  max_workers: 10
  step_size: 2           # add 2 workers every step_interval
  step_interval: 30s
  hold_duration: 2m
  ramp_down: true

scenarios:
  - name: "User flow"
    weight: 1.0           # relative weight if multiple scenarios
    steps:
      - name: "Login"
        method: POST
        path: /auth/login
        headers:
          Content-Type: application/json
        body: |
          {"username": "testuser", "password": "testpass"}
        extract:
          - name: token
            from: response_body
            jsonpath: $.access_token
        think_time: 500ms

      - name: "Get profile"
        method: GET
        path: /user/profile
        headers:
          Authorization: "Bearer {{ .token }}"
        think_time: 1s

      - name: "List items"
        method: GET
        path: /items?page=1&limit=20
        headers:
          Authorization: "Bearer {{ .token }}"

workers:
  resources:
    cpu: "500m"
    memory: "256Mi"
  virtual_users_per_worker: 50

thresholds:
  p95_latency_ms: 500
  error_rate_percent: 1.0
  min_rps: 100
```

---

### 6.2 Load Profile Types

**FR-010 — Constant Load**
Workers remain at a fixed count for the entire duration. Useful for baseline benchmarking.
```
Workers: ──────────────── (flat)
Time:    0 ─────────────► end
```

**FR-011 — Step Ramp**
Workers increase in discrete steps. Useful for finding the saturation point.
```
Workers: ─────┐    ┌─────┐    ┌─────
              └────┘    └────┘
Time:    0 ─────────────────────────► end
```

**FR-012 — Spike**
Workers jump suddenly to a high count, then drop back. Tests elasticity and recovery.
```
Workers:      ┌──┐
─────────────┘  └────────────────
```

**FR-013 — Soak**
Workers stay at a moderate level for a long duration (hours). Tests for memory leaks, connection pool exhaustion, gradual degradation.

**FR-014 — Stress**
Workers increase continuously until thresholds are breached or the maximum is hit. Finds the breaking point.
```
Workers:                   /
                          /
                         /
────────────────────────/
```

---

### 6.3 Fleet Management

**FR-020** — When a test run is submitted, the Orchestrator SHALL provision the initial worker count as Kubernetes Pods in the `loadforge-workers` namespace.

**FR-021** — Each worker Pod SHALL be labeled with `loadforge.io/run-id`, `loadforge.io/worker-id` for easy selection.

**FR-022** — The Orchestrator SHALL watch Pod status and wait for all initial workers to reach `Running` state before starting the test (with a configurable timeout, default: 2 minutes).

**FR-023** — During a step-ramp test, the Orchestrator SHALL create additional worker Pods at each step boundary. New workers SHALL join the active test seamlessly.

**FR-024** — The Orchestrator SHALL track each worker's health via gRPC heartbeat. A worker is considered unhealthy if it misses 3 consecutive heartbeats (default interval: 5s).

**FR-025** — If a worker becomes unhealthy during a test, the Orchestrator SHALL:
1. Log the event with the worker ID and last known metrics
2. Mark the test run with a `DEGRADED` flag (not failed)
3. Optionally provision a replacement worker (configurable)

**FR-026** — When a test ends (duration reached OR thresholds breached), the Orchestrator SHALL:
1. Send STOP signal to all workers via gRPC
2. Wait up to 30s for workers to drain in-flight requests
3. Delete all worker Pods for the run
4. Update the test run record in PostgreSQL to `DONE` or `FAILED`

**FR-027** — All worker Pods for a test run SHALL be deleted even if the control plane crashes. This is enforced by a Pod TTL annotation (`loadforge.io/ttl`) that an independent cleanup controller respects.

---

### 6.4 Test Execution — Worker Behavior

**FR-030** — Each worker SHALL establish a gRPC connection to the Orchestrator within 10 seconds of pod start. Failure to connect SHALL cause the pod to exit with a non-zero code.

**FR-031** — After registration, the worker SHALL receive the full test plan and begin executing virtual users immediately.

**FR-032** — Workers SHALL support configurable think time between requests (per step, per scenario, or global default).

**FR-033** — Workers SHALL support response extraction — pulling values from response body (JSONPath) or headers, and using them in subsequent requests within the same virtual user session.

**FR-034** — Workers SHALL support connection pooling and keep-alive for HTTP, configurable per test plan.

**FR-035** — Workers SHALL enforce per-request timeouts (configurable, default: 30s).

**FR-036** — Workers SHALL categorize errors:
- Connection refused / timeout (network error)
- HTTP 4xx (client error)
- HTTP 5xx (server error)
- Request timeout
- TLS error

**FR-037** — Workers SHALL publish a final drain report to NATS before exiting on STOP, containing their total request count and error summary.

---

### 6.5 Metrics & Monitoring

**FR-040** — The Metrics Aggregator SHALL compute the following per endpoint, per 10-second window:
- Request count
- Requests per second (RPS)
- Error rate (%)
- Latency: p50, p90, p95, p99, max
- Bytes received per second
- Status code distribution (200, 201, 400, 401, 403, 404, 500, 502, 503...)

**FR-041** — The Metrics Aggregator SHALL also compute fleet-level metrics per 10-second window:
- Total active workers
- Total goroutines (sum across workers)
- Per-worker: CPU usage, memory usage, request dispatch rate
- Worker error count (workers that failed, not request errors)

**FR-042** — The system SHALL expose a real-time SSE (Server-Sent Events) endpoint for live metric streaming during an active test. The CLI and web dashboard SHALL consume this stream.

**FR-043** — The system SHALL write per-second snapshots to PostgreSQL during the test for post-test report generation.

**FR-044** — The system SHALL expose a Prometheus `/metrics` endpoint on the Aggregator with the following metric names:

```
# Application under test
loadforge_request_total{run_id, endpoint, method, status_code}
loadforge_request_duration_ms{run_id, endpoint, quantile}
loadforge_rps{run_id, endpoint}
loadforge_error_rate{run_id, endpoint}
loadforge_bytes_received_total{run_id}

# Fleet health
loadforge_active_workers{run_id}
loadforge_worker_goroutines{run_id, worker_id}
loadforge_worker_cpu_usage{run_id, worker_id}
loadforge_worker_memory_bytes{run_id, worker_id}
```

---

### 6.6 Thresholds & Alerting

**FR-050** — A test plan MAY define thresholds. Thresholds are checked every 10 seconds during the test.

**FR-051** — Supported threshold conditions:
```yaml
thresholds:
  p95_latency_ms: 500       # fail if p95 > 500ms
  error_rate_percent: 1.0   # fail if errors > 1%
  min_rps: 100              # fail if RPS drops below 100
  max_p99_latency_ms: 2000  # fail if p99 > 2s
```

**FR-052** — If a threshold is breached, the system SHALL:
1. Mark the test run as `THRESHOLD_BREACHED`
2. Log which threshold, at what value, at what timestamp
3. Optionally stop the test immediately (`on_breach: stop`) or continue and report at end (`on_breach: report`)

**FR-053** — The system SHALL support Prometheus AlertManager integration for push-based alerting on threshold breaches.

---

### 6.7 Reporting

**FR-060** — After a test run completes, the system SHALL generate a structured report containing:
- Test plan summary (name, duration, load profile)
- Overall stats (total requests, total errors, overall p50/p95/p99)
- Per-endpoint breakdown (RPS, latency percentiles, error rate, status codes)
- Timeline graphs (RPS, latency, worker count over time)
- Threshold check results (pass/fail per threshold)
- Worker fleet summary (how many workers ran, any degraded workers)

**FR-061** — Reports SHALL be available in two formats:
- JSON (`GET /api/runs/{id}/report.json`)
- HTML (standalone, embeds all charts as inline SVG, `GET /api/runs/{id}/report.html`)

**FR-062** — The CLI command `loadforge report <run-id>` SHALL download and open the HTML report in the default browser.

---

### 6.8 Multi-Protocol Support

**FR-070** — v1: HTTP/1.1 and HTTP/2 (via Go's `net/http`)

**FR-071** — v2: gRPC load testing (send protobuf messages, measure streaming RPC latency)

**FR-072** — v2: WebSocket load testing (open WS connections, send/receive frames, measure frame latency)

---

### 6.9 Concurrency & Distributed Coordination

**FR-080** — Multiple test runs SHALL be able to execute simultaneously, each with their own isolated fleet.

**FR-081** — The system SHALL enforce a global maximum worker count across all active runs (configurable, default: 100) to prevent cluster resource exhaustion.

**FR-082** — A test run SHALL be uniquely identified by a `run-id` (UUID v4). All resources (pods, metrics, results) SHALL be namespaced under this ID.

---

### 6.10 CLI Features (Extended)

**FR-090** — `loadforge run --watch` SHALL stream live metrics to the terminal in a table format, updating every 5 seconds.

**FR-091** — `loadforge run --ci` SHALL run without live streaming, exit 0 on pass, exit 1 on threshold breach. Suitable for CI pipelines.

**FR-092** — `loadforge validate <test-plan.yaml>` SHALL:
- Check YAML schema validity
- Resolve all variable references
- Verify target URL is reachable (optional, `--check-target`)
- Return detailed validation errors

**FR-093** — `loadforge list` SHALL show the last 20 test runs with their status, duration, and a brief result summary.

**FR-094** — `loadforge run --dry-run` SHALL simulate the orchestration (print what pods would be created, what the ramp profile looks like) without actually hitting the target.

---

## 7. Non-Functional Requirements

**NFR-001 — Scalability**
A single LoadForge deployment SHALL support up to 100 concurrent worker pods and 10 concurrent test runs without degradation of the control plane.

**NFR-002 — Latency of Metric Delivery**
Raw metrics from workers SHALL appear in the Aggregator within 1 second of the request completing.

**NFR-003 — Control Plane Availability**
The Orchestrator SHALL be deployable with 2 replicas using leader election (via Kubernetes leader election with `coordination.k8s.io` lease). Only the leader provisions workers; followers watch.

**NFR-004 — Worker Isolation**
Worker pods SHALL run in a dedicated namespace with resource quotas. A misbehaving worker SHALL not be able to consume more than its declared resource limit.

**NFR-005 — Graceful Degradation**
If the Metrics Aggregator is unavailable, workers SHALL continue generating load and buffer metrics locally (up to 10,000 samples, then drop oldest). When the Aggregator recovers, it SHALL request workers to flush their buffers.

**NFR-006 — Test Plan Size**
The system SHALL support test plans up to 1MB in size (multiple scenarios, hundreds of steps).

**NFR-007 — Report Retention**
Test run reports SHALL be retained in PostgreSQL for 30 days by default (configurable). A cleanup job SHALL run nightly.

**NFR-008 — Resource Efficiency**
A single worker pod at 50 virtual users SHALL consume no more than 200MB RAM and 200m CPU under normal conditions.

**NFR-009 — Startup Time**
A worker pod SHALL be ready to receive test plans within 30 seconds of pod creation (including image pull if cached).

**NFR-010 — Observability of LoadForge Itself**
The Orchestrator, Aggregator, and API Server SHALL each expose their own health check endpoints (`/healthz`, `/readyz`) and their own Prometheus `/metrics` for self-monitoring.

---

## 8. Data Models

### 8.1 TestRun (PostgreSQL)

```sql
CREATE TABLE test_runs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,
    status          TEXT NOT NULL,   -- PENDING, PROVISIONING, RUNNING, DRAINING, DONE, FAILED, THRESHOLD_BREACHED
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at      TIMESTAMPTZ,
    ended_at        TIMESTAMPTZ,
    duration_seconds INT,
    test_plan       JSONB NOT NULL,  -- full test plan stored for replay
    result_summary  JSONB,           -- populated when done: overall stats
    threshold_results JSONB,
    worker_count    INT,
    peak_rps        FLOAT,
    total_requests  BIGINT,
    total_errors    BIGINT,
    error_rate      FLOAT,
    p50_latency_ms  FLOAT,
    p95_latency_ms  FLOAT,
    p99_latency_ms  FLOAT,
    degraded        BOOLEAN DEFAULT false,
    created_by      TEXT             -- user or API key identifier
);
```

### 8.2 MetricSnapshot (PostgreSQL)

```sql
CREATE TABLE metric_snapshots (
    id          BIGSERIAL PRIMARY KEY,
    run_id      UUID NOT NULL REFERENCES test_runs(id),
    ts          TIMESTAMPTZ NOT NULL,
    endpoint    TEXT NOT NULL,
    method      TEXT NOT NULL,
    rps         FLOAT,
    p50_ms      FLOAT,
    p95_ms      FLOAT,
    p99_ms      FLOAT,
    error_rate  FLOAT,
    req_count   INT,
    err_count   INT,
    status_codes JSONB    -- {"200": 412, "500": 3}
);

CREATE INDEX idx_metric_snapshots_run_ts ON metric_snapshots(run_id, ts);
```

### 8.3 WorkerEvent (PostgreSQL)

```sql
CREATE TABLE worker_events (
    id          BIGSERIAL PRIMARY KEY,
    run_id      UUID NOT NULL REFERENCES test_runs(id),
    worker_id   TEXT NOT NULL,
    event_type  TEXT NOT NULL,  -- REGISTERED, HEALTHY, UNHEALTHY, STOPPED, REPLACED
    ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
    metadata    JSONB
);
```

### 8.4 Redis Keys

```
# Live run counters (expires when run ends + 10min)
loadforge:run:{run_id}:rps                  → float (current RPS, updated every 5s)
loadforge:run:{run_id}:p95                  → float
loadforge:run:{run_id}:error_rate           → float
loadforge:run:{run_id}:active_workers       → int
loadforge:run:{run_id}:status               → string

# Job queue for pending test runs
loadforge:queue:pending                     → List of run_id

# Worker registry for a run
loadforge:run:{run_id}:workers              → Set of worker_id
loadforge:run:{run_id}:worker:{worker_id}:last_heartbeat → timestamp
```

### 8.5 Go Structs (Internal)

```go
// TestPlan is the parsed internal representation of a YAML test plan
type TestPlan struct {
    Name        string      `yaml:"name"`
    Version     string      `yaml:"version"`
    Target      Target      `yaml:"target"`
    LoadProfile LoadProfile `yaml:"load_profile"`
    Scenarios   []Scenario  `yaml:"scenarios"`
    Workers     WorkerSpec  `yaml:"workers"`
    Thresholds  *Thresholds `yaml:"thresholds,omitempty"`
}

type Target struct {
    BaseURL       string `yaml:"base_url"`
    TLSSkipVerify bool   `yaml:"tls_skip_verify"`
    Timeout       string `yaml:"timeout"`  // e.g., "30s"
}

type LoadProfile struct {
    Type            string `yaml:"type"`    // constant|step_ramp|spike|soak|stress
    InitialWorkers  int    `yaml:"initial_workers"`
    MaxWorkers      int    `yaml:"max_workers"`
    StepSize        int    `yaml:"step_size"`
    StepInterval    string `yaml:"step_interval"`
    HoldDuration    string `yaml:"hold_duration"`
    RampDown        bool   `yaml:"ramp_down"`
}

type Scenario struct {
    Name   string  `yaml:"name"`
    Weight float64 `yaml:"weight"`
    Steps  []Step  `yaml:"steps"`
}

type Step struct {
    Name      string            `yaml:"name"`
    Method    string            `yaml:"method"`
    Path      string            `yaml:"path"`
    Headers   map[string]string `yaml:"headers"`
    Body      string            `yaml:"body"`
    Extract   []Extraction      `yaml:"extract"`
    ThinkTime string            `yaml:"think_time"`
    Assert    []Assertion       `yaml:"assert"`
}

type Extraction struct {
    Name     string `yaml:"name"`
    From     string `yaml:"from"`      // response_body | header
    JSONPath string `yaml:"jsonpath"`
    Header   string `yaml:"header"`
}

type Assertion struct {
    Field    string `yaml:"field"`     // status_code | latency_ms | body_contains
    Operator string `yaml:"operator"`  // eq | lt | gt | contains
    Value    string `yaml:"value"`
}

type WorkerSpec struct {
    Resources          ResourceSpec `yaml:"resources"`
    VirtualUsersPerWorker int       `yaml:"virtual_users_per_worker"`
}

type Thresholds struct {
    P95LatencyMs    *float64 `yaml:"p95_latency_ms"`
    ErrorRatePercent *float64 `yaml:"error_rate_percent"`
    MinRPS          *float64 `yaml:"min_rps"`
    MaxP99LatencyMs *float64 `yaml:"max_p99_latency_ms"`
    OnBreach        string   `yaml:"on_breach"` // stop | report
}
```

---

## 9. API Design

### Base URL: `https://loadforge.internal/api/v1`

### Authentication

Every request requires an `Authorization: Bearer <api-key>` header. API keys are generated by the admin via `loadforge config set-key`.

---

### 9.1 Test Runs

**Submit a test run**
```
POST /runs
Content-Type: application/json

{
  "test_plan": { ...parsed plan... },
  "variables": { "base_url": "https://staging.api.example.com" }
}

Response 202 Accepted:
{
  "run_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "PENDING",
  "created_at": "2026-06-24T10:00:00Z"
}
```

**Get run status**
```
GET /runs/{run_id}

Response 200:
{
  "run_id": "550e8400...",
  "status": "RUNNING",
  "started_at": "2026-06-24T10:00:05Z",
  "active_workers": 4,
  "live": {
    "rps": 891.2,
    "p95_ms": 72.4,
    "error_rate": 0.001
  }
}
```

**Stop a run**
```
POST /runs/{run_id}/stop

Response 202 Accepted:
{ "status": "DRAINING" }
```

**List runs**
```
GET /runs?limit=20&offset=0&status=DONE

Response 200:
{
  "runs": [ ...array of run summaries... ],
  "total": 142
}
```

**Get final report (JSON)**
```
GET /runs/{run_id}/report

Response 200: { ...full report... }
```

**Get final report (HTML)**
```
GET /runs/{run_id}/report.html

Response 200: text/html (standalone HTML file)
```

---

### 9.2 Live Metrics Stream (SSE)

```
GET /runs/{run_id}/stream
Accept: text/event-stream

→ Server-Sent Events:
event: metrics
data: {"ts":"2026-06-24T10:00:10Z","rps":891,"p50":19,"p95":72,"p99":189,"errors":0.001,"workers":4}

event: metrics
data: {...}

event: done
data: {"status":"DONE","total_requests":53210,"total_errors":53,"p99_ms":204}
```

---

### 9.3 Validate

```
POST /validate
Content-Type: application/json

{ "test_plan": { ... } }

Response 200 (valid):
{ "valid": true }

Response 422 (invalid):
{
  "valid": false,
  "errors": [
    { "field": "load_profile.max_workers", "message": "must be > 0" },
    { "field": "scenarios[0].steps[1].method", "message": "must be one of GET|POST|PUT|PATCH|DELETE" }
  ]
}
```

---

### 9.4 Health

```
GET /healthz   → 200 OK (always, if process is up)
GET /readyz    → 200 OK (if DB connected and Orchestrator reachable)
GET /metrics   → Prometheus exposition format
```

---

## 10. Worker Lifecycle & Scaling Logic

### 10.1 Worker Startup Sequence

```
1. Pod starts → Go binary executes
2. Worker reads env: ORCHESTRATOR_ADDR, RUN_ID, WORKER_ID, NATS_URL
3. Worker dials Orchestrator gRPC (retries with exponential backoff, max 10s)
4. Worker calls Register(WorkerInfo{RunID, WorkerID, NodeName, PodIP})
5. Orchestrator returns TestPlan
6. Worker parses TestPlan → initializes virtual user goroutine pool
7. Worker dials NATS
8. Worker opens bidirectional Heartbeat stream with Orchestrator
9. Worker begins executing virtual users
10. Worker publishes MetricsBatch to NATS every 500ms
```

### 10.2 Ramp-Up Controller Algorithm

The scaler runs every `ScaleCheckInterval` (default: 5s). It computes the desired worker count based on the load profile and reconciles against actual running pods.

```go
func (s *Scaler) Reconcile(ctx context.Context, run *TestRun) {
    desired := s.profile.ComputeDesired(time.Since(run.StartedAt))
    actual := s.workerRegistry.Count(run.ID)

    if desired > actual {
        toAdd := desired - actual
        s.provisioner.CreateWorkers(ctx, run, toAdd)
    } else if desired < actual && s.profile.RampDown {
        toRemove := actual - desired
        s.provisioner.DrainAndRemoveWorkers(ctx, run, toRemove)
    }
}

// Step ramp profile
func (p *StepRampProfile) ComputeDesired(elapsed time.Duration) int {
    steps := int(elapsed / p.StepInterval)
    desired := p.InitialWorkers + (steps * p.StepSize)
    if desired > p.MaxWorkers {
        desired = p.MaxWorkers
    }
    return desired
}
```

### 10.3 Worker Drain on Scale-Down

When the scaler removes workers, it does NOT kill pods abruptly. It:
1. Picks the workers with the lowest request dispatch rate (least disruptive to remove)
2. Sends SCALE_DOWN signal via gRPC heartbeat stream to those workers
3. Workers stop spawning new requests but finish in-flight ones
4. Workers send final drain report to NATS
5. Orchestrator deletes the pods once drain is confirmed or timeout (30s) passes

### 10.4 Health Watchdog

```go
// Runs per worker, in the Orchestrator
func (w *Watchdog) Watch(ctx context.Context, workerID string) {
    missedBeats := 0
    ticker := time.NewTicker(w.interval) // default: 5s
    for {
        select {
        case <-ticker.C:
            if !w.registry.HeartbeatReceived(workerID, w.interval*2) {
                missedBeats++
                if missedBeats >= 3 {
                    w.handleUnhealthy(workerID)
                    return
                }
            } else {
                missedBeats = 0
            }
        case <-ctx.Done():
            return
        }
    }
}
```

---

## 11. Monitoring Architecture

### 11.1 Fleet Health Dashboard (Grafana)

Panels:
- Active Workers over time (line chart)
- Per-Worker CPU Usage (multi-line)
- Per-Worker Memory Usage (multi-line)
- Per-Worker Goroutine Count (multi-line)
- Per-Worker Request Dispatch Rate (RPS per worker)
- Worker Events Timeline (registered, unhealthy, replaced events as annotations)

### 11.2 Application Under Test Dashboard (Grafana)

Panels:
- RPS over time (line chart)
- Latency Percentiles over time (p50, p95, p99 — multi-line)
- Error Rate over time (line chart, red threshold line at 1%)
- Status Code Distribution over time (stacked bar: 2xx, 4xx, 5xx)
- Per-Endpoint Breakdown (table: endpoint, method, RPS, p95, errors)
- Bytes Received/sec

### 11.3 Alerting Rules (Prometheus AlertManager)

```yaml
groups:
  - name: loadforge.test
    rules:
      - alert: HighErrorRate
        expr: loadforge_error_rate > 0.05
        for: 30s
        labels:
          severity: warning
        annotations:
          summary: "Error rate above 5% for run {{ $labels.run_id }}"

      - alert: HighP99Latency
        expr: loadforge_request_duration_ms{quantile="0.99"} > 2000
        for: 30s
        labels:
          severity: warning
        annotations:
          summary: "p99 latency above 2s for run {{ $labels.run_id }}"

      - alert: WorkerUnhealthy
        expr: increase(loadforge_worker_events_total{event="UNHEALTHY"}[1m]) > 0
        labels:
          severity: critical
```

---

## 12. Storage Design

### 12.1 PostgreSQL Schema

All tables in a `loadforge` schema. Migrations managed by `golang-migrate`.

```sql
-- Additional indexes for common query patterns
CREATE INDEX idx_test_runs_status ON test_runs(status);
CREATE INDEX idx_test_runs_created_at ON test_runs(created_at DESC);
CREATE INDEX idx_metric_snapshots_run_endpoint ON metric_snapshots(run_id, endpoint, ts);
```

### 12.2 Data Lifecycle

- Active test: metrics flow to Prometheus (ephemeral) + PostgreSQL (durable) + Redis (live)
- Test done: Redis keys expire after TTL. PostgreSQL retains all snapshots.
- Nightly cleanup job: deletes `metric_snapshots` and `worker_events` older than 30 days. Retains `test_runs` header rows indefinitely.
- Report generation reads from `metric_snapshots` — no dependency on Prometheus for historical reports.

---

## 13. Security Design

**SEC-001 — API Authentication**
All API endpoints require a Bearer token. Tokens are UUIDs stored hashed in PostgreSQL. Future: OIDC integration.

**SEC-002 — Worker Pod Security**
- Workers run as non-root user (`runAsUser: 1000`)
- No privileged containers
- `allowPrivilegeEscalation: false`
- `readOnlyRootFilesystem: true`
- Resource limits enforced (prevent runaway pods)

**SEC-003 — Network Isolation**
- Workers in `loadforge-workers` namespace
- NetworkPolicy: workers can only talk to the Orchestrator (port 50051) and NATS (port 4222)
- Workers cannot reach the LoadForge control plane database or Redis

**SEC-004 — TLS**
- All gRPC connections between Orchestrator and Workers use mTLS (cert-manager generates certs)
- All external HTTPS connections from workers support TLS skip verify (configurable per test)
- REST API behind TLS termination (ingress or service mesh)

**SEC-005 — Test Plan Sanitization**
- The API Server validates test plans before forwarding to the Orchestrator
- Reject test plans targeting private/internal IP ranges (RFC 1918) unless `allow_internal: true` is explicitly set by admin
- Reject test plans with excessively large bodies (>10MB per step)

---

## 14. Configuration Design

### 14.1 Control Plane Config (`loadforge-config` ConfigMap)

```yaml
orchestrator:
  worker_namespace: loadforge-workers
  worker_image: ghcr.io/yourorg/loadforge-worker:latest
  max_workers_global: 100
  heartbeat_interval: 5s
  scale_check_interval: 5s
  pod_ready_timeout: 2m
  worker_drain_timeout: 30s

api_server:
  port: 8080
  cors_origins: ["*"]
  rate_limit_rps: 100

aggregator:
  nats_url: nats://nats:4222
  window_size: 10s
  publish_interval: 5s
  prometheus_port: 9090

storage:
  postgres_dsn: postgres://loadforge:secret@postgres:5432/loadforge?sslmode=require
  redis_addr: redis:6379
  retention_days: 30
```

### 14.2 Worker Config (injected via Pod env)

Workers have no config file. All configuration comes from:
- Environment variables (orchestrator address, run ID, NATS URL)
- gRPC Register response (full test plan)

---

## 15. Error Handling & Fault Tolerance

### 15.1 Control Plane Crash

If the Orchestrator crashes mid-test:
- Worker pods continue running (they don't stop on lost gRPC connection)
- Workers buffer metrics locally
- On Orchestrator restart, it queries PostgreSQL for `RUNNING` tests
- It reconstructs the worker registry by listing pods with `loadforge.io/run-id` label
- It re-establishes gRPC connections with each worker
- Test continues from current state

### 15.2 Worker Pod OOMKilled

If a worker is OOMKilled by Kubernetes:
- Kubernetes restarts the pod (if `restartPolicy: OnFailure`)
- On restart, the worker reconnects to Orchestrator and re-registers
- Orchestrator recognizes the worker_id and continues — metrics gap noted in report

### 15.3 NATS Unavailable

- Workers buffer up to 10,000 samples in memory (ring buffer, drops oldest on overflow)
- Workers retry NATS connection with exponential backoff
- On reconnect, workers flush the buffer in order
- Aggregator marks a gap in the timeline for affected workers

### 15.4 PostgreSQL Unavailable

- Orchestrator queues state transitions in memory (bounded queue)
- Retries writes with exponential backoff
- Test continues — worst case is metric snapshots are lost for the outage window

### 15.5 Target Service Unavailable

- Workers receive connection refused / timeout errors
- These are recorded as errors, not worker failures
- If error rate threshold is exceeded, the test is marked `THRESHOLD_BREACHED`
- Workers continue attempting requests (do not bail out)

---

## 16. Deployment Architecture

### 16.1 Helm Chart Structure

```
loadforge/
  Chart.yaml
  values.yaml
  templates/
    orchestrator-deployment.yaml
    orchestrator-service.yaml
    api-server-deployment.yaml
    api-server-service.yaml
    api-server-ingress.yaml
    aggregator-deployment.yaml
    aggregator-service.yaml
    nats-deployment.yaml             # or use NATS Helm chart as subchart
    postgres-deployment.yaml         # or use Bitnami Postgres subchart
    redis-deployment.yaml            # or use Bitnami Redis subchart
    prometheus-servicemonitor.yaml
    workers-namespace.yaml
    workers-networkpolicy.yaml
    workers-resourcequota.yaml
    rbac.yaml                        # ClusterRole for Orchestrator to manage pods
    configmap.yaml
    secret.yaml                      # postgres DSN, API keys
```

### 16.2 Kubernetes RBAC for Orchestrator

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: loadforge-orchestrator
rules:
  - apiGroups: [""]
    resources: ["pods", "pods/log"]
    verbs: ["get", "list", "watch", "create", "delete"]
    resourceNames: []
  - apiGroups: [""]
    resources: ["namespaces"]
    verbs: ["get"]
  - apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    verbs: ["get", "create", "update"]   # for leader election
```

### 16.3 Resource Allocation (Recommended)

| Component | CPU Request | CPU Limit | Memory Request | Memory Limit |
|-----------|-------------|-----------|----------------|--------------|
| Orchestrator | 100m | 500m | 128Mi | 512Mi |
| API Server | 50m | 200m | 64Mi | 256Mi |
| Aggregator | 200m | 1000m | 256Mi | 1Gi |
| NATS | 100m | 500m | 128Mi | 512Mi |
| Each Worker | 200m | 500m | 128Mi | 256Mi |

---

## 17. Milestones & Implementation Order

### Milestone 1 — Skeleton & Local Dev (Week 1–2)

- [ ] Repo structure: `cmd/`, `internal/`, `pkg/`, `proto/`, `deployments/`
- [ ] Proto definitions for `WorkerControl` service
- [ ] Basic Orchestrator that accepts a hardcoded test plan and logs what it would do
- [ ] Basic Worker that connects to Orchestrator via gRPC, receives plan, prints it
- [ ] Docker images for both
- [ ] `docker-compose.yaml` for local dev (orchestrator + 1 worker + NATS + postgres + redis)

### Milestone 2 — Worker HTTP Execution (Week 3–4)

- [ ] Virtual user goroutine pool in worker
- [ ] HTTP client with configurable timeout, keep-alive, connection pool
- [ ] Step execution loop (request → response → extraction → think time)
- [ ] Per-request metric recording
- [ ] NATS publisher (MetricsBatch every 500ms)
- [ ] Drain on STOP signal

### Milestone 3 — Orchestrator Core (Week 5–6)

- [ ] Test run state machine (FSM)
- [ ] Kubernetes pod provisioner (create/delete/watch)
- [ ] Worker registry (tracks active workers per run)
- [ ] Heartbeat watchdog
- [ ] Ramp-up scaler (step ramp first)
- [ ] PostgreSQL integration (persist run state)

### Milestone 4 — Metrics Aggregator (Week 7)

- [ ] NATS subscriber
- [ ] HDR histogram aggregation per window
- [ ] Prometheus metrics exposition
- [ ] Redis live counter writes
- [ ] PostgreSQL snapshot writes

### Milestone 5 — CLI (Week 8)

- [ ] `loadforge run` with `--watch` live streaming
- [ ] `loadforge status`, `loadforge stop`, `loadforge list`
- [ ] `loadforge validate`
- [ ] `loadforge report`
- [ ] Config file handling

### Milestone 6 — REST API Server (Week 9)

- [ ] All CRUD endpoints for runs
- [ ] SSE streaming endpoint
- [ ] API key authentication middleware
- [ ] Rate limiting middleware
- [ ] Health/readyz endpoints

### Milestone 7 — Thresholds & Reporting (Week 10)

- [ ] Threshold checker goroutine in Orchestrator
- [ ] HTML report generator (Go template + inline charts via go-echarts or Chart.js)
- [ ] JSON report endpoint
- [ ] `on_breach: stop` behavior

### Milestone 8 — Helm Chart & Observability (Week 11–12)

- [ ] Helm chart for all components
- [ ] Grafana dashboards (JSON provisioned via ConfigMap)
- [ ] Prometheus AlertManager rules
- [ ] RBAC and NetworkPolicy
- [ ] Leader election for Orchestrator HA

### Milestone 9 — Additional Load Profiles (Week 13)

- [ ] Constant load profile
- [ ] Spike profile
- [ ] Soak profile
- [ ] Stress profile (auto-increase until threshold breach)

### Milestone 10 — Polish & Docs (Week 14–15)

- [ ] `--dry-run` mode
- [ ] `--ci` mode (exit codes)
- [ ] Web dashboard (optional: Next.js or embedded Go template)
- [ ] README with quickstart
- [ ] Architecture diagram
- [ ] Example test plans

---

## 18. Open Questions

| # | Question | Default decision | Revisit when |
|---|----------|-----------------|--------------|
| OQ-1 | Should workers use a custom HTTP client or `net/http` stdlib? | stdlib + fasthttp for high concurrency | Milestone 2 |
| OQ-2 | Should NATS use JetStream (persistent) or core NATS (ephemeral)? | Core NATS for v1 (simpler) | v2 if metric loss is a problem |
| OQ-3 | Should test plans support conditional steps (if/else)? | No for v1 | After v1 GA |
| OQ-4 | Should the web dashboard be embedded in the API server or separate? | Embedded via `embed.FS` | Milestone 10 |
| OQ-5 | Support JMeter `.jmx` script import? | No for v1 | After v1 GA |
| OQ-6 | Leader election: use K8s lease or etcd? | K8s lease (no extra infra) | If etcd is already in cluster |
| OQ-7 | How to handle large body payloads in test steps? | Cap at 10MB per step, read from file in v2 | Milestone 2 |
| OQ-8 | Should workers pre-generate all requests or generate on the fly? | On the fly (lower memory) | If CPU becomes bottleneck |

---

## Appendix A — Repo Structure

```
loadforge/
├── cmd/
│   ├── orchestrator/    main.go
│   ├── worker/          main.go
│   ├── aggregator/      main.go
│   ├── apiserver/       main.go
│   └── loadforge/       main.go   (CLI)
├── internal/
│   ├── orchestrator/
│   │   ├── fsm/
│   │   ├── provisioner/
│   │   ├── scaler/
│   │   ├── watchdog/
│   │   └── planner/
│   ├── worker/
│   │   ├── executor/
│   │   ├── scenario/
│   │   ├── reporter/
│   │   └── client/
│   ├── aggregator/
│   │   ├── subscriber/
│   │   ├── histogram/
│   │   └── writer/
│   ├── apiserver/
│   │   ├── handlers/
│   │   ├── middleware/
│   │   └── sse/
│   └── store/
│       ├── postgres/
│       └── redis/
├── pkg/
│   ├── testplan/        (shared plan types, validation)
│   ├── metrics/         (shared metric types)
│   └── report/          (report generation)
├── proto/
│   └── worker.proto
├── deployments/
│   ├── helm/
│   │   └── loadforge/
│   ├── docker-compose.yaml
│   └── grafana/
│       └── dashboards/
├── examples/
│   ├── basic-http.yaml
│   ├── step-ramp.yaml
│   ├── spike.yaml
│   └── soak.yaml
├── Makefile
├── Dockerfile.orchestrator
├── Dockerfile.worker
├── Dockerfile.aggregator
├── Dockerfile.apiserver
└── go.mod
```

## Appendix B — Key Dependencies

```go
// go.mod key dependencies
require (
    google.golang.org/grpc                 v1.64.0
    google.golang.org/protobuf             v1.34.2
    k8s.io/client-go                       v0.30.0
    github.com/nats-io/nats.go             v1.36.0
    github.com/go-chi/chi/v5               v5.0.12
    github.com/prometheus/client_golang    v1.19.1
    github.com/jackc/pgx/v5               v5.6.0
    github.com/redis/go-redis/v9          v9.5.3
    github.com/spf13/cobra                 v1.8.1
    github.com/spf13/viper                 v1.19.0
    github.com/codahale/hdrhistogram       v0.9.0
    gopkg.in/yaml.v3                       v3.0.1
    github.com/google/uuid                 v1.6.0
    go.uber.org/zap                        v1.27.0
    github.com/golang-migrate/migrate/v4   v4.17.1
)
```

---

*End of LoadForge FDD v1.0*

---

## Appendix C — Full Proto Definition

```protobuf
syntax = "proto3";
package loadforge.worker.v1;
option go_package = "github.com/yourorg/loadforge/proto/worker/v1";

// ─── Worker Registration ───────────────────────────────────────────────────

message WorkerInfo {
  string run_id    = 1;
  string worker_id = 2;
  string pod_ip    = 3;
  string node_name = 4;
  string version   = 5;
}

message TestPlanResponse {
  string          run_id    = 1;
  bytes           plan_json = 2;   // JSON-encoded TestPlan struct
  int32           worker_index = 3;// which shard of virtual users this worker owns
  int32           total_workers = 4;
}

// ─── Heartbeat ────────────────────────────────────────────────────────────

message HeartbeatRequest {
  string   worker_id    = 1;
  string   run_id       = 2;
  int64    timestamp_ms = 3;
  WorkerStats stats     = 4;
}

message WorkerStats {
  int64  active_goroutines  = 1;
  double cpu_percent        = 2;
  int64  memory_bytes       = 3;
  double requests_per_sec   = 4;
  int64  total_requests     = 5;
  int64  total_errors       = 6;
}

message HeartbeatResponse {
  string signal = 1;  // CONTINUE | STOP | SCALE_DOWN
  int32  new_vu_count = 2; // if SCALE_DOWN: new target VU count for this worker
}

// ─── Stop ─────────────────────────────────────────────────────────────────

message StopRequest {
  string run_id    = 1;
  string worker_id = 2;
  bool   graceful  = 3;
}

message StopResponse {
  bool   acknowledged = 1;
  int64  drained_requests = 2;
}

// ─── Service Definition ───────────────────────────────────────────────────

service WorkerControl {
  // Worker calls on startup to get its test plan
  rpc Register(WorkerInfo) returns (TestPlanResponse);

  // Bidirectional stream: worker sends heartbeats, orchestrator sends signals
  rpc Heartbeat(stream HeartbeatRequest) returns (stream HeartbeatResponse);

  // Orchestrator calls to stop a specific worker
  rpc Stop(StopRequest) returns (StopResponse);
}
```

---

## Appendix D — Makefile

```makefile
.PHONY: build proto test docker-build helm-install clean dev

REGISTRY    := ghcr.io/yourorg
VERSION     := $(shell git describe --tags --always --dirty)
GOFLAGS     := -ldflags="-X main.Version=$(VERSION)"

# ── Proto generation ────────────────────────────────────────────────────────
proto:
	protoc \
	  --go_out=. --go_opt=paths=source_relative \
	  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	  proto/worker.proto

# ── Build all binaries ──────────────────────────────────────────────────────
build:
	go build $(GOFLAGS) -o bin/orchestrator ./cmd/orchestrator
	go build $(GOFLAGS) -o bin/worker        ./cmd/worker
	go build $(GOFLAGS) -o bin/aggregator    ./cmd/aggregator
	go build $(GOFLAGS) -o bin/apiserver     ./cmd/apiserver
	go build $(GOFLAGS) -o bin/loadforge     ./cmd/loadforge

# ── Test ────────────────────────────────────────────────────────────────────
test:
	go test ./... -race -cover -count=1

test-integration:
	go test ./... -tags=integration -race -count=1

# ── Docker ──────────────────────────────────────────────────────────────────
docker-build:
	docker build -f Dockerfile.orchestrator -t $(REGISTRY)/loadforge-orchestrator:$(VERSION) .
	docker build -f Dockerfile.worker       -t $(REGISTRY)/loadforge-worker:$(VERSION) .
	docker build -f Dockerfile.aggregator   -t $(REGISTRY)/loadforge-aggregator:$(VERSION) .
	docker build -f Dockerfile.apiserver    -t $(REGISTRY)/loadforge-apiserver:$(VERSION) .

docker-push:
	docker push $(REGISTRY)/loadforge-orchestrator:$(VERSION)
	docker push $(REGISTRY)/loadforge-worker:$(VERSION)
	docker push $(REGISTRY)/loadforge-aggregator:$(VERSION)
	docker push $(REGISTRY)/loadforge-apiserver:$(VERSION)

# ── Local dev with docker-compose ───────────────────────────────────────────
dev:
	docker compose up --build

dev-down:
	docker compose down -v

# ── Helm ────────────────────────────────────────────────────────────────────
helm-lint:
	helm lint deployments/helm/loadforge

helm-install:
	helm upgrade --install loadforge deployments/helm/loadforge \
	  --namespace loadforge --create-namespace \
	  --set image.tag=$(VERSION) \
	  -f deployments/helm/loadforge/values.yaml

helm-uninstall:
	helm uninstall loadforge --namespace loadforge

# ── DB migrations ───────────────────────────────────────────────────────────
migrate-up:
	migrate -path internal/store/postgres/migrations \
	        -database "$(DATABASE_URL)" up

migrate-down:
	migrate -path internal/store/postgres/migrations \
	        -database "$(DATABASE_URL)" down 1

# ── Clean ───────────────────────────────────────────────────────────────────
clean:
	rm -rf bin/
```

---

## Appendix E — CI/CD Pipeline (GitHub Actions)

```yaml
# .github/workflows/ci.yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:

jobs:
  test:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_DB: loadforge_test
          POSTGRES_USER: loadforge
          POSTGRES_PASSWORD: secret
        ports: ["5432:5432"]
      redis:
        image: redis:7
        ports: ["6379:6379"]
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: "1.22"
          cache: true
      - name: Generate proto
        run: |
          go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
          go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
          make proto
      - name: Run tests
        env:
          DATABASE_URL: postgres://loadforge:secret@localhost:5432/loadforge_test
          REDIS_ADDR: localhost:6379
        run: make test
      - name: Build binaries
        run: make build

  docker:
    needs: test
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'
    steps:
      - uses: actions/checkout@v4
      - uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}
      - name: Build and push
        run: |
          make docker-build docker-push

  helm-lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: azure/setup-helm@v4
      - run: make helm-lint
```

---

## Appendix F — Key Prometheus Queries (for Grafana panels)

```promql
# Current RPS across all endpoints for a run
sum(rate(loadforge_request_total{run_id="$run_id"}[30s]))

# p95 latency per endpoint
histogram_quantile(0.95,
  sum by (endpoint, le) (
    rate(loadforge_request_duration_ms_bucket{run_id="$run_id"}[30s])
  )
)

# Error rate (%)
sum(rate(loadforge_request_total{run_id="$run_id", status_code=~"5.."}[30s]))
/
sum(rate(loadforge_request_total{run_id="$run_id"}[30s]))
* 100

# Active worker count
loadforge_active_workers{run_id="$run_id"}

# Per-worker memory
loadforge_worker_memory_bytes{run_id="$run_id"}

# Status code distribution
sum by (status_code) (
  increase(loadforge_request_total{run_id="$run_id"}[$__range])
)
```

---

## Appendix G — Example Docker Compose (Local Dev)

```yaml
version: "3.9"
services:

  orchestrator:
    build:
      context: .
      dockerfile: Dockerfile.orchestrator
    environment:
      POSTGRES_DSN: postgres://loadforge:secret@postgres:5432/loadforge?sslmode=disable
      REDIS_ADDR: redis:6379
      NATS_URL: nats://nats:4222
      WORKER_IMAGE: loadforge-worker:dev
      WORKER_NAMESPACE: loadforge-workers
      KUBE_CONFIG_PATH: /root/.kube/config
    volumes:
      - ~/.kube:/root/.kube:ro
    ports:
      - "50051:50051"
    depends_on: [postgres, redis, nats]

  apiserver:
    build:
      context: .
      dockerfile: Dockerfile.apiserver
    environment:
      POSTGRES_DSN: postgres://loadforge:secret@postgres:5432/loadforge?sslmode=disable
      REDIS_ADDR: redis:6379
      ORCHESTRATOR_ADDR: orchestrator:50051
    ports:
      - "8080:8080"
    depends_on: [postgres, redis, orchestrator]

  aggregator:
    build:
      context: .
      dockerfile: Dockerfile.aggregator
    environment:
      NATS_URL: nats://nats:4222
      POSTGRES_DSN: postgres://loadforge:secret@postgres:5432/loadforge?sslmode=disable
      REDIS_ADDR: redis:6379
    ports:
      - "9090:9090"    # Prometheus metrics
    depends_on: [nats, postgres, redis]

  nats:
    image: nats:2.10-alpine
    ports:
      - "4222:4222"
      - "8222:8222"    # NATS monitoring

  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: loadforge
      POSTGRES_USER: loadforge
      POSTGRES_PASSWORD: secret
    volumes:
      - pgdata:/var/lib/postgresql/data
    ports:
      - "5432:5432"

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

  prometheus:
    image: prom/prometheus:v2.52.0
    volumes:
      - ./deployments/prometheus/prometheus.yaml:/etc/prometheus/prometheus.yml
    ports:
      - "9091:9090"

  grafana:
    image: grafana/grafana:10.4.0
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
    volumes:
      - ./deployments/grafana/dashboards:/etc/grafana/provisioning/dashboards
      - ./deployments/grafana/datasources:/etc/grafana/provisioning/datasources
    ports:
      - "3000:3000"
    depends_on: [prometheus]

volumes:
  pgdata:
```

---

*LoadForge FDD v1.0 — Complete*
