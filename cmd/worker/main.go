package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/vatsalchaudhary/loadforge/pkg/testplan"
	pb "github.com/vatsalchaudhary/loadforge/proto"
	lfclient "github.com/vatsalchaudhary/loadforge/worker/client"
	"github.com/vatsalchaudhary/loadforge/worker/executor"
	"github.com/vatsalchaudhary/loadforge/worker/reporter"
	"github.com/vatsalchaudhary/loadforge/worker/scenario"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	orchestratorAddr := envString("LOADFORGE_ORCHESTRATOR_ADDR", "localhost:50051")
	runID := envString("LOADFORGE_TEST_RUN_ID", "run-dummy-123")
	workerID := envString("LOADFORGE_WORKER_ID", "worker-dummy-456")
	natsURL := envString("LOADFORGE_NATS_URL", "nats://localhost:4222")
	metricsAddr := envString("LOADFORGE_WORKER_METRICS_ADDR", ":9102")

	startMetricsServer(metricsAddr, logger)

	rootCtx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()

	conn, err := grpc.Dial(orchestratorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Error("failed to connect to orchestrator", "error", err, "addr", orchestratorAddr)
		os.Exit(1)
	}
	defer conn.Close()

	control := pb.NewWorkerControlClient(conn)

	registerCtx, cancelRegister := context.WithTimeout(rootCtx, 10*time.Second)
	regResp, err := control.Register(registerCtx, &pb.WorkerInfo{
		RunId:    runID,
		WorkerId: workerID,
		PodIp:    localIP(),
		NodeName: envString("NODE_NAME", "localhost-node"),
		Version:  envString("LOADFORGE_VERSION", "dev"),
	})
	cancelRegister()
	if err != nil {
		logger.Error("registration failed", "error", err)
		os.Exit(1)
	}
	if regResp.RunId != "" {
		runID = regResp.RunId
	}

	var plan testplan.TestPlan
	if err := json.Unmarshal(regResp.PlanJson, &plan); err != nil {
		logger.Error("failed to parse test plan", "error", err)
		os.Exit(1)
	}

	targetVUs := plan.Workers.VirtualUsersPerWorker
	if targetVUs <= 0 {
		targetVUs = plan.LoadProfile.InitialWorkers
	}
	if targetVUs < 0 {
		targetVUs = 0
	}

	timeout := 30 * time.Second
	if plan.Target.Timeout != "" {
		parsed, err := time.ParseDuration(plan.Target.Timeout)
		if err != nil {
			logger.Warn("invalid target timeout; using default", "timeout", plan.Target.Timeout, "error", err)
		} else {
			timeout = parsed
		}
	}

	httpClient := lfclient.NewHTTPClient(lfclient.Config{
		Timeout:             timeout,
		KeepAlive:           envDuration("LOADFORGE_HTTP_KEEPALIVE", 30*time.Second, logger),
		MaxIdleConns:        envInt("LOADFORGE_HTTP_MAX_IDLE_CONNS", 4096, logger),
		MaxIdleConnsPerHost: envInt("LOADFORGE_HTTP_MAX_IDLE_CONNS_PER_HOST", max(512, targetVUs*2), logger),
		MaxConnsPerHost:     envInt("LOADFORGE_HTTP_MAX_CONNS_PER_HOST", 0, logger),
		TLSSkipVerify:       plan.Target.TLSSkipVerify,
	})

	rep, err := reporter.New(context.Background(), reporter.Config{
		RunID:          runID,
		WorkerID:       workerID,
		NATSURL:        natsURL,
		FlushInterval:  500 * time.Millisecond,
		BufferCapacity: envInt("LOADFORGE_REPORTER_BUFFER_CAPACITY", 10000, logger),
		Logger:         logger,
	})
	if err != nil {
		logger.Error("failed to start reporter", "error", err)
		os.Exit(1)
	}

	runner, err := scenario.NewRunner(scenario.Config{
		Plan:     plan,
		Client:   httpClient,
		Recorder: rep,
		Logger:   logger,
	})
	if err != nil {
		logger.Error("failed to initialize scenario runner", "error", err)
		os.Exit(1)
	}

	pool := executor.NewPool(rootCtx, runner, logger)
	if err := pool.Scale(targetVUs); err != nil {
		logger.Error("failed to start virtual users", "error", err)
		os.Exit(1)
	}

	logger.Info("worker execution started", "run_id", runID, "worker_id", workerID, "vus", targetVUs, "metrics_subject", rep.Subject())

	stopCh := make(chan string, 1)
	hbCtx, hbCancel := context.WithCancel(rootCtx)
	defer hbCancel()
	hbStream, err := control.Heartbeat(hbCtx)
	if err != nil {
		logger.Error("failed to open heartbeat stream", "error", err)
		stopCh <- "heartbeat_open_failed"
	} else {
		go receiveControlSignals(hbStream, pool, stopCh, logger)
		go sendHeartbeats(hbCtx, hbStream, runner, pool, workerID, runID, logger)
	}

	select {
	case <-rootCtx.Done():
		logger.Info("worker shutdown requested", "reason", rootCtx.Err())
	case reason := <-stopCh:
		logger.Info("worker stop requested", "reason", reason)
	}

	hbCancel()
	pool.Stop()

	drained, _ := runner.Stats()
	flushCtx, cancelFlush := context.WithTimeout(context.Background(), 10*time.Second)
	if err := rep.Close(flushCtx); err != nil {
		logger.Warn("reporter flush failed during shutdown", "error", err)
	}
	cancelFlush()

	ackCtx, cancelAck := context.WithTimeout(context.Background(), 5*time.Second)
	ack, err := control.Stop(ackCtx, &pb.StopRequest{RunId: runID, WorkerId: workerID, Graceful: true})
	cancelAck()
	if err != nil {
		logger.Warn("failed to confirm drain completion to orchestrator", "error", err)
	} else {
		logger.Info("drain completion confirmed", "acknowledged", ack.Acknowledged, "local_drained_requests", drained, "orchestrator_drained_requests", ack.DrainedRequests)
	}
}

func receiveControlSignals(stream pb.WorkerControl_HeartbeatClient, pool *executor.Pool, stopCh chan<- string, logger *slog.Logger) {
	for {
		resp, err := stream.Recv()
		if err != nil {
			logger.Warn("heartbeat receive loop stopped", "error", err)
			return
		}
		switch resp.Signal {
		case "STOP":
			select {
			case stopCh <- "orchestrator_stop":
			default:
			}
			return
		case "SCALE_DOWN", "SCALE_UP":
			if err := pool.Scale(int(resp.NewVuCount)); err != nil {
				logger.Warn("failed to scale virtual users", "target_vus", resp.NewVuCount, "error", err)
			} else {
				logger.Info("scaled virtual users", "target_vus", resp.NewVuCount)
			}
		case "", "CONTINUE":
		default:
			logger.Warn("unknown orchestrator signal", "signal", resp.Signal)
		}
	}
}

func sendHeartbeats(ctx context.Context, stream pb.WorkerControl_HeartbeatClient, runner *scenario.Runner, pool *executor.Pool, workerID, runID string, logger *slog.Logger) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	var prevRequests int64
	var prevAt = time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			totalRequests, totalErrors := runner.Stats()
			now := time.Now()
			elapsed := now.Sub(prevAt).Seconds()
			rps := 0.0
			if elapsed > 0 {
				rps = float64(totalRequests-prevRequests) / elapsed
			}
			prevRequests = totalRequests
			prevAt = now

			var mem runtime.MemStats
			runtime.ReadMemStats(&mem)
			stats := &pb.WorkerStats{
				ActiveGoroutines: pool.Active(),
				MemoryBytes:      int64(mem.Alloc),
				RequestsPerSec:   rps,
				TotalRequests:    totalRequests,
				TotalErrors:      totalErrors,
			}
			if err := stream.Send(&pb.HeartbeatRequest{
				WorkerId:    workerID,
				RunId:       runID,
				TimestampMs: now.UnixMilli(),
				Stats:       stats,
			}); err != nil {
				logger.Warn("failed to send heartbeat", "error", err)
				return
			}
		}
	}
}

func startMetricsServer(addr string, logger *slog.Logger) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		logger.Info("worker prometheus metrics server listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Warn("worker metrics server stopped", "error", err)
		}
	}()
}

func envString(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func envInt(key string, fallback int, logger *slog.Logger) int {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(val)
	if err != nil {
		logger.Warn("invalid integer environment value", "key", key, "value", val, "error", err)
		return fallback
	}
	return parsed
}

func envDuration(key string, fallback time.Duration, logger *slog.Logger) time.Duration {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(val)
	if err != nil {
		logger.Warn("invalid duration environment value", "key", key, "value", val, "error", err)
		return fallback
	}
	return parsed
}

func localIP() string {
	conn, err := net.DialTimeout("udp", "8.8.8.8:80", 100*time.Millisecond)
	if err != nil {
		return "127.0.0.1"
	}
	defer conn.Close()
	if addr, ok := conn.LocalAddr().(*net.UDPAddr); ok {
		return addr.IP.String()
	}
	return "127.0.0.1"
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
