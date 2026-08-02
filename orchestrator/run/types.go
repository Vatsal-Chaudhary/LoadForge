package run

import (
	"context"
	"time"

	"github.com/vatsalchaudhary/loadforge/pkg/testplan"
	pb "github.com/vatsalchaudhary/loadforge/proto"
)

type State string

const (
	StatePending           State = "PENDING"
	StateProvisioning      State = "PROVISIONING"
	StateRunning           State = "RUNNING"
	StateScaling           State = "SCALING"
	StateDraining          State = "DRAINING"
	StateDone              State = "DONE"
	StateFailed            State = "FAILED"
	StateThresholdBreached State = "THRESHOLD_BREACHED"
)

type TestRun struct {
	ID        string
	Plan      testplan.TestPlan
	State     State
	StartedAt time.Time
}

type OrchestratorConfig struct {
	Provisioner          string
	KubeConfigPath       string
	WorkerNamespace      string
	WorkerImage          string
	MaxWorkersPerTest    int
	HeartbeatInterval    time.Duration
	ScaleCheckInterval   time.Duration
	NATSUrl              string
	PostgresDSN          string
	RedisAddr            string
	RedisPassword        string
	WorkerServiceAccount string
	WorkerCPURequest     string
	WorkerCPULimit       string
	WorkerMemoryRequest  string
	WorkerMemoryLimit    string
	LeaderElection       bool
	LeaderLeaseName      string
	LeaderLeaseNamespace string
	LeaderIdentity       string
}

type Worker struct {
	RunID         string
	ID            string
	PodName       string
	DispatchRate  float64
	LastHeartbeat time.Time
}

type WorkerRegistry interface {
	Register(ctx context.Context, worker Worker) error
	Remove(ctx context.Context, runID, workerID string) error
	Count(ctx context.Context, runID string) (int, error)
	WorkersByLowestDispatchRate(ctx context.Context, runID string, count int) ([]Worker, error)
	HeartbeatReceived(ctx context.Context, workerID string, within time.Duration) (bool, error)
	RecordHeartbeat(ctx context.Context, req *pb.HeartbeatRequest) error
	MarkDead(ctx context.Context, runID, workerID string) error
	WaitForDrain(ctx context.Context, runID, workerID string) error
}

type WorkerSignaler interface {
	Signal(ctx context.Context, runID, workerID, signal string, newVUCount int32) error
}
