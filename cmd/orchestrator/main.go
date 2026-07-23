package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/vatsalchaudhary/loadforge/orchestrator/fsm"
	"github.com/vatsalchaudhary/loadforge/orchestrator/planner"
	"github.com/vatsalchaudhary/loadforge/orchestrator/provisioner"
	"github.com/vatsalchaudhary/loadforge/orchestrator/run"
	"github.com/vatsalchaudhary/loadforge/orchestrator/scaler"
	"github.com/vatsalchaudhary/loadforge/orchestrator/watchdog"
	pb "github.com/vatsalchaudhary/loadforge/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type server struct {
	pb.UnimplementedWorkerControlServer
	pb.UnimplementedOperatorControlServer
	mu       sync.RWMutex
	runs     map[string]run.TestRun
	plans    map[string][]byte
	registry *memoryRegistry
	signals  *signalHub
	watchdog *watchdog.Manager
	rootCtx  context.Context
	store    fsm.Store
	prov     scaler.Provisioner
	cfg      run.OrchestratorConfig
	log      *slog.Logger
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	cfg := loadConfig()
	startMetricsServer(envString("ORCHESTRATOR_METRICS_ADDR", ":9101"), logger)

	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	registry := newMemoryRegistry()
	signals := newSignalHub()
	srv := &server{
		runs:     make(map[string]run.TestRun),
		plans:    make(map[string][]byte),
		registry: registry,
		signals:  signals,
		rootCtx:  rootCtx,
	}

	var fsmStore fsm.Store = &memoryFSMStore{}
	var eventStore watchdog.EventStore
	if cfg.PostgresDSN != "" {
		store, err := fsm.OpenPostgres(rootCtx, cfg.PostgresDSN)
		if err != nil {
			logger.Warn("postgres unavailable; using in-memory FSM store", "error", err)
		} else {
			defer store.Close()
			fsmStore = store
		}
		events, err := watchdog.OpenPostgresEventStore(rootCtx, cfg.PostgresDSN)
		if err != nil {
			logger.Warn("postgres worker event store unavailable", "error", err)
		} else {
			defer events.Close()
			eventStore = events
		}
	}

	var prov scaler.Provisioner
	if cfg.WorkerImage != "" {
		p, err := provisioner.NewForConfig(cfg, envString("ORCHESTRATOR_GRPC_ADDR", "orchestrator:50051"), logger)
		if err != nil {
			logger.Warn("kubernetes provisioner unavailable", "error", err)
		} else {
			p.Registry = registry
			p.Signaler = signals
			prov = p
		}
	}
	if prov == nil {
		prov = noopProvisioner{log: logger}
	}
	srv.store = fsmStore
	srv.prov = prov
	srv.cfg = cfg
	srv.log = logger

	srv.watchdog = &watchdog.Manager{
		Registry:    registry,
		Provisioner: prov,
		Events:      eventStore,
		Interval:    cfg.HeartbeatInterval,
		Log:         logger,
	}

	if path := os.Getenv("LOADFORGE_TEST_PLAN_PATH"); path != "" {
		if err := startManualRun(rootCtx, path, cfg, fsmStore, prov, registry, srv, logger); err != nil {
			logger.Error("failed to start configured test run", "error", err, "path", path)
			os.Exit(1)
		}
	} else {
		logger.Info("no LOADFORGE_TEST_PLAN_PATH configured; waiting for workers registered for known manual runs")
	}

	lis, err := net.Listen("tcp", envString("ORCHESTRATOR_LISTEN_ADDR", ":50051"))
	if err != nil {
		logger.Error("failed to listen", "error", err)
		os.Exit(1)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterWorkerControlServer(grpcServer, srv)
	pb.RegisterOperatorControlServer(grpcServer, srv)
	go func() {
		<-rootCtx.Done()
		grpcServer.GracefulStop()
	}()

	logger.Info("orchestrator grpc server listening", "addr", lis.Addr().String())
	if err := grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		logger.Error("grpc server stopped", "error", err)
		os.Exit(1)
	}
}

func startManualRun(ctx context.Context, path string, cfg run.OrchestratorConfig, store fsm.Store, prov scaler.Provisioner, registry scaler.Registry, srv *server, logger *slog.Logger) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	plan, err := planner.ParseYAML(data)
	if err != nil {
		return err
	}
	planJSON, err := json.Marshal(plan)
	if err != nil {
		return err
	}
	runID := envString("LOADFORGE_TEST_RUN_ID", "run-"+strconv.FormatInt(time.Now().Unix(), 10))
	testRun := run.TestRun{ID: runID, Plan: plan, State: run.StatePending, StartedAt: time.Now().UTC()}
	machine := fsm.New(runID, run.StatePending, store, logger)
	if err := machine.Transition(ctx, run.StateProvisioning, "plan accepted"); err != nil {
		return err
	}
	initial := plan.LoadProfile.InitialWorkers
	if cfg.MaxWorkersPerTest > 0 && initial > cfg.MaxWorkersPerTest {
		initial = cfg.MaxWorkersPerTest
	}
	testRun.State = run.StateProvisioning
	if err := prov.CreateWorkers(ctx, testRun, initial); err != nil {
		_ = machine.Transition(ctx, run.StateFailed, "initial provision failed")
		return err
	}
	if err := machine.Transition(ctx, run.StateRunning, "initial fleet provisioned"); err != nil {
		return err
	}
	testRun.State = run.StateRunning
	testRun.StartedAt = time.Now().UTC()
	srv.addRun(testRun, planJSON)

	if plan.LoadProfile.Type == "step_ramp" {
		stepInterval, _ := time.ParseDuration(plan.LoadProfile.StepInterval)
		maxWorkers := plan.LoadProfile.MaxWorkers
		if cfg.MaxWorkersPerTest > 0 && (maxWorkers == 0 || maxWorkers > cfg.MaxWorkersPerTest) {
			maxWorkers = cfg.MaxWorkersPerTest
		}
		sc := &scaler.Scaler{
			Profile: scaler.StepRampProfile{
				InitialWorkers: plan.LoadProfile.InitialWorkers,
				StepSize:       plan.LoadProfile.StepSize,
				StepInterval:   stepInterval,
				MaxWorkers:     maxWorkers,
				AllowRampDown:  plan.LoadProfile.RampDown,
			},
			Provisioner: prov,
			Registry:    registry,
			Interval:    cfg.ScaleCheckInterval,
			Log:         logger,
		}
		go func() { _ = sc.Run(ctx, testRun) }()
	}
	logger.Info("test run started", "run_id", runID, "profile", plan.LoadProfile.Type, "initial_workers", initial)
	return nil
}

func (s *server) addRun(testRun run.TestRun, planJSON []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runs[testRun.ID] = testRun
	s.plans[testRun.ID] = planJSON
}

func (s *server) SubmitRun(_ context.Context, req *pb.SubmitRunRequest) (*pb.SubmitRunResponse, error) {
	if req.RunId == "" || len(req.PlanJson) == 0 {
		return nil, status.Error(codes.InvalidArgument, "run_id and plan_json are required")
	}
	plan, err := planner.ParseYAML(req.PlanJson)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid test plan: %v", err)
	}
	s.mu.Lock()
	if existing, ok := s.runs[req.RunId]; ok {
		s.mu.Unlock()
		return &pb.SubmitRunResponse{Status: string(existing.State)}, nil
	}
	testRun := run.TestRun{ID: req.RunId, Plan: plan, State: run.StatePending}
	s.runs[req.RunId] = testRun
	s.plans[req.RunId] = append([]byte(nil), req.PlanJson...)
	s.mu.Unlock()

	go s.startSubmittedRun(testRun)
	return &pb.SubmitRunResponse{Status: string(run.StatePending)}, nil
}

func (s *server) startSubmittedRun(testRun run.TestRun) {
	machine := fsm.New(testRun.ID, run.StatePending, s.store, s.log)
	if err := machine.Transition(s.rootCtx, run.StateProvisioning, "API submission accepted"); err != nil {
		s.failRun(testRun.ID, machine, err)
		return
	}
	s.setRunState(testRun.ID, run.StateProvisioning)
	initial := testRun.Plan.LoadProfile.InitialWorkers
	if s.cfg.MaxWorkersPerTest > 0 && initial > s.cfg.MaxWorkersPerTest {
		initial = s.cfg.MaxWorkersPerTest
	}
	if err := s.prov.CreateWorkers(s.rootCtx, testRun, initial); err != nil {
		s.failRun(testRun.ID, machine, err)
		return
	}
	if err := machine.Transition(s.rootCtx, run.StateRunning, "initial fleet provisioned"); err != nil {
		s.failRun(testRun.ID, machine, err)
		return
	}
	testRun.State = run.StateRunning
	testRun.StartedAt = time.Now().UTC()
	s.mu.Lock()
	s.runs[testRun.ID] = testRun
	s.mu.Unlock()

	if testRun.Plan.LoadProfile.Type == "step_ramp" {
		stepInterval, _ := time.ParseDuration(testRun.Plan.LoadProfile.StepInterval)
		maxWorkers := testRun.Plan.LoadProfile.MaxWorkers
		if s.cfg.MaxWorkersPerTest > 0 && (maxWorkers == 0 || maxWorkers > s.cfg.MaxWorkersPerTest) {
			maxWorkers = s.cfg.MaxWorkersPerTest
		}
		controller := &scaler.Scaler{
			Profile: scaler.StepRampProfile{
				InitialWorkers: testRun.Plan.LoadProfile.InitialWorkers,
				StepSize:       testRun.Plan.LoadProfile.StepSize, StepInterval: stepInterval,
				MaxWorkers: maxWorkers, AllowRampDown: testRun.Plan.LoadProfile.RampDown,
			},
			Provisioner: s.prov, Registry: s.registry,
			Interval: s.cfg.ScaleCheckInterval, Log: s.log,
		}
		go func() { _ = controller.Run(s.rootCtx, testRun) }()
	}
}

func (s *server) failRun(runID string, machine *fsm.Machine, cause error) {
	if machine.State() != run.StateFailed && fsm.ValidTransition(machine.State(), run.StateFailed) {
		_ = machine.Transition(s.rootCtx, run.StateFailed, cause.Error())
	}
	s.setRunState(runID, run.StateFailed)
	s.log.Error("submitted run failed", "run_id", runID, "error", cause)
}

func (s *server) setRunState(runID string, state run.State) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item := s.runs[runID]
	item.State = state
	if state == run.StateRunning && item.StartedAt.IsZero() {
		item.StartedAt = time.Now().UTC()
	}
	s.runs[runID] = item
}

func (s *server) StopRun(_ context.Context, req *pb.StopRunRequest) (*pb.StopRunResponse, error) {
	s.mu.RLock()
	testRun, ok := s.runs[req.RunId]
	s.mu.RUnlock()
	if !ok {
		return nil, status.Error(codes.NotFound, "run not found")
	}
	if testRun.State == run.StateDraining || testRun.State == run.StateDone || testRun.State == run.StateFailed {
		return &pb.StopRunResponse{Status: string(testRun.State)}, nil
	}
	machine := fsm.New(testRun.ID, testRun.State, s.store, s.log)
	if err := machine.Transition(s.rootCtx, run.StateDraining, "operator requested stop"); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "stop run: %v", err)
	}
	s.setRunState(testRun.ID, run.StateDraining)
	go func() {
		count, err := s.registry.Count(s.rootCtx, testRun.ID)
		if err == nil {
			err = s.prov.DrainAndRemoveWorkers(s.rootCtx, testRun, count)
		}
		if err != nil {
			s.failRun(testRun.ID, machine, err)
			return
		}
		if err := machine.Transition(s.rootCtx, run.StateDone, "workers drained"); err != nil {
			s.failRun(testRun.ID, machine, err)
			return
		}
		s.setRunState(testRun.ID, run.StateDone)
	}()
	return &pb.StopRunResponse{Status: string(run.StateDraining)}, nil
}

func (s *server) Health(context.Context, *pb.HealthRequest) (*pb.HealthResponse, error) {
	return &pb.HealthResponse{Serving: true}, nil
}

func (s *server) Register(ctx context.Context, req *pb.WorkerInfo) (*pb.TestPlanResponse, error) {
	s.mu.RLock()
	testRun, ok := s.runs[req.RunId]
	planJSON := s.plans[req.RunId]
	s.mu.RUnlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unknown run_id %q", req.RunId)
	}
	if err := s.registry.Register(ctx, run.Worker{RunID: req.RunId, ID: req.WorkerId, PodName: req.WorkerId, LastHeartbeat: time.Now().UTC()}); err != nil {
		return nil, err
	}
	s.watchdog.Watch(s.rootCtx, testRun, req.WorkerId)
	slog.Info("worker registered", "run_id", req.RunId, "worker_id", req.WorkerId, "pod_ip", req.PodIp, "node", req.NodeName)
	count, _ := s.registry.Count(ctx, req.RunId)
	return &pb.TestPlanResponse{RunId: req.RunId, PlanJson: planJSON, WorkerIndex: int32(count - 1), TotalWorkers: int32(max(1, count))}, nil
}

func (s *server) Heartbeat(stream pb.WorkerControl_HeartbeatServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := s.registry.RecordHeartbeat(stream.Context(), req); err != nil {
			return err
		}
		signal, newVUs := s.signals.Take(req.RunId, req.WorkerId)
		if signal == "" {
			signal = "CONTINUE"
		}
		if err := stream.Send(&pb.HeartbeatResponse{Signal: signal, NewVuCount: newVUs}); err != nil {
			return err
		}
	}
}

func (s *server) Stop(ctx context.Context, req *pb.StopRequest) (*pb.StopResponse, error) {
	s.registry.MarkDrained(req.RunId, req.WorkerId)
	s.watchdog.Remove(req.WorkerId)
	slog.Info("worker drain confirmed", "run_id", req.RunId, "worker_id", req.WorkerId, "graceful", req.Graceful)
	return &pb.StopResponse{Acknowledged: true}, nil
}

type memoryFSMStore struct{ mu sync.Mutex }

func (s *memoryFSMStore) PersistTransition(ctx context.Context, t fsm.Transition) error { return nil }

type noopProvisioner struct{ log *slog.Logger }

func (p noopProvisioner) CreateWorkers(ctx context.Context, testRun run.TestRun, count int) error {
	p.log.Info("noop provisioner create workers", "run_id", testRun.ID, "count", count)
	return nil
}
func (p noopProvisioner) DrainAndRemoveWorkers(ctx context.Context, testRun run.TestRun, count int) error {
	p.log.Info("noop provisioner drain workers", "run_id", testRun.ID, "count", count)
	return nil
}

type memoryRegistry struct {
	mu      sync.Mutex
	workers map[string]run.Worker
	drained map[string]chan struct{}
}

func newMemoryRegistry() *memoryRegistry {
	return &memoryRegistry{workers: make(map[string]run.Worker), drained: make(map[string]chan struct{})}
}
func (r *memoryRegistry) Register(ctx context.Context, worker run.Worker) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.workers[worker.ID] = worker
	if _, ok := r.drained[worker.ID]; !ok {
		r.drained[worker.ID] = make(chan struct{})
	}
	return nil
}
func (r *memoryRegistry) Remove(ctx context.Context, runID, workerID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.workers, workerID)
	return nil
}
func (r *memoryRegistry) Count(ctx context.Context, runID string) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	total := 0
	for _, worker := range r.workers {
		if worker.RunID == runID {
			total++
		}
	}
	return total, nil
}
func (r *memoryRegistry) WorkersByLowestDispatchRate(ctx context.Context, runID string, count int) ([]run.Worker, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	workers := make([]run.Worker, 0, len(r.workers))
	for _, worker := range r.workers {
		if worker.RunID == runID {
			workers = append(workers, worker)
		}
	}
	sort.Slice(workers, func(i, j int) bool { return workers[i].DispatchRate < workers[j].DispatchRate })
	if count > len(workers) {
		count = len(workers)
	}
	return workers[:count], nil
}
func (r *memoryRegistry) HeartbeatReceived(ctx context.Context, workerID string, within time.Duration) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	worker, ok := r.workers[workerID]
	return ok && time.Since(worker.LastHeartbeat) <= within, nil
}
func (r *memoryRegistry) RecordHeartbeat(ctx context.Context, req *pb.HeartbeatRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	worker := r.workers[req.WorkerId]
	worker.ID = req.WorkerId
	worker.RunID = req.RunId
	worker.LastHeartbeat = time.Now().UTC()
	if req.Stats != nil {
		worker.DispatchRate = req.Stats.RequestsPerSec
	}
	r.workers[req.WorkerId] = worker
	if _, ok := r.drained[req.WorkerId]; !ok {
		r.drained[req.WorkerId] = make(chan struct{})
	}
	return nil
}
func (r *memoryRegistry) MarkDead(ctx context.Context, runID, workerID string) error {
	slog.Warn("worker marked dead", "run_id", runID, "worker_id", workerID)
	return r.Remove(ctx, runID, workerID)
}
func (r *memoryRegistry) WaitForDrain(ctx context.Context, runID, workerID string) error {
	r.mu.Lock()
	ch, ok := r.drained[workerID]
	r.mu.Unlock()
	if !ok {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ch:
		return nil
	}
}
func (r *memoryRegistry) MarkDrained(runID, workerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ch, ok := r.drained[workerID]
	if !ok {
		return
	}
	select {
	case <-ch:
	default:
		close(ch)
	}
}

type queuedSignal struct {
	signal string
	vus    int32
}

type signalHub struct {
	mu      sync.Mutex
	signals map[string]queuedSignal
}

func newSignalHub() *signalHub { return &signalHub{signals: make(map[string]queuedSignal)} }
func (h *signalHub) Signal(ctx context.Context, runID, workerID, signal string, newVUCount int32) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.signals[runID+"/"+workerID] = queuedSignal{signal: signal, vus: newVUCount}
	return nil
}
func (h *signalHub) Take(runID, workerID string) (string, int32) {
	h.mu.Lock()
	defer h.mu.Unlock()
	key := runID + "/" + workerID
	item := h.signals[key]
	delete(h.signals, key)
	return item.signal, item.vus
}

func startMetricsServer(addr string, logger *slog.Logger) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	go func() {
		logger.Info("orchestrator prometheus metrics server listening", "addr", addr)
		if err := http.ListenAndServe(addr, mux); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Warn("orchestrator metrics server stopped", "error", err)
		}
	}()
}

func loadConfig() run.OrchestratorConfig {
	return run.OrchestratorConfig{
		KubeConfigPath:     envString("KUBE_CONFIG_PATH", ""),
		WorkerNamespace:    envString("WORKER_NAMESPACE", "loadforge-workers"),
		WorkerImage:        envString("WORKER_IMAGE", ""),
		MaxWorkersPerTest:  envInt("MAX_WORKERS_PER_TEST", 100),
		HeartbeatInterval:  envDuration("HEARTBEAT_INTERVAL", 5*time.Second),
		ScaleCheckInterval: envDuration("SCALE_CHECK_INTERVAL", 5*time.Second),
		NATSUrl:            envString("NATS_URL", "nats://localhost:4222"),
		PostgresDSN:        envString("POSTGRES_DSN", ""),
		RedisAddr:          envString("REDIS_ADDR", "localhost:6379"),
	}
}

func envString(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}
func envInt(key string, fallback int) int {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(val)
	if err != nil {
		return fallback
	}
	return parsed
}
func envDuration(key string, fallback time.Duration) time.Duration {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(val)
	if err != nil {
		return fallback
	}
	return parsed
}
