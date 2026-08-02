package docker

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	"github.com/vatsalchaudhary/loadforge/orchestrator/run"
	pb "github.com/vatsalchaudhary/loadforge/proto"
)

type testRegistry struct {
	workers []run.Worker
}

func (r *testRegistry) Register(ctx context.Context, worker run.Worker) error {
	r.workers = append(r.workers, worker)
	return nil
}
func (r *testRegistry) Remove(ctx context.Context, runID, workerID string) error { return nil }
func (r *testRegistry) Count(ctx context.Context, runID string) (int, error) {
	return len(r.workers), nil
}
func (r *testRegistry) WorkersByLowestDispatchRate(ctx context.Context, runID string, count int) ([]run.Worker, error) {
	if count > len(r.workers) {
		count = len(r.workers)
	}
	return r.workers[:count], nil
}
func (r *testRegistry) HeartbeatReceived(ctx context.Context, workerID string, within time.Duration) (bool, error) {
	return true, nil
}
func (r *testRegistry) RecordHeartbeat(ctx context.Context, req *pb.HeartbeatRequest) error {
	return nil
}
func (r *testRegistry) MarkDead(ctx context.Context, runID, workerID string) error     { return nil }
func (r *testRegistry) WaitForDrain(ctx context.Context, runID, workerID string) error { return nil }

type testSignaler struct {
	signals []string
}

func (s *testSignaler) Signal(ctx context.Context, runID, workerID, signal string, newVUCount int32) error {
	s.signals = append(s.signals, signal)
	return nil
}

func TestDockerProvisionerCreatesAndRemovesContainers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		t.Fatalf("new docker client: %v", err)
	}
	defer cli.Close()
	if _, err := cli.Ping(ctx); err != nil {
		t.Skipf("Docker daemon unavailable; skipping Docker provisioner integration test: %v", err)
	}

	image := os.Getenv("LOADFORGE_DOCKER_TEST_IMAGE")
	if image == "" {
		image = "nats:2.10-alpine"
	}
	if _, _, err := cli.ImageInspectWithRaw(ctx, image); err != nil {
		if errdefs.IsNotFound(err) {
			t.Skipf("Docker image %q unavailable; run `docker compose -f deployments/docker-compose.yaml pull nats` or set LOADFORGE_DOCKER_TEST_IMAGE to a local long-running image", image)
		}
		t.Fatalf("inspect image: %v", err)
	}

	networkName := "loadforge-provisioner-test-" + randomSuffix()
	if _, err := cli.NetworkCreate(ctx, networkName, network.CreateOptions{}); err != nil {
		t.Fatalf("create docker network: %v", err)
	}
	t.Cleanup(func() {
		_ = cli.NetworkRemove(context.Background(), networkName)
	})

	reg := &testRegistry{}
	signals := &testSignaler{}
	p := &Provisioner{
		Client:           cli,
		WorkerImage:      image,
		OrchestratorAddr: "orchestrator:50051",
		NATSURL:          "nats://nats:4222",
		NetworkName:      networkName,
		Registry:         reg,
		Signaler:         signals,
		DrainTimeout:     time.Second,
		StopTimeout:      1,
	}
	testRun := run.TestRun{ID: "run-docker-test-" + randomSuffix()}
	t.Cleanup(func() {
		_ = p.DrainAndRemoveWorkers(context.Background(), testRun, 10)
	})

	if err := p.CreateWorkers(ctx, testRun, 1); err != nil {
		t.Fatalf("CreateWorkers returned error: %v", err)
	}
	if len(reg.workers) != 1 {
		t.Fatalf("registered workers = %d, want 1", len(reg.workers))
	}

	worker := reg.workers[0]
	if err := waitForContainerRunning(ctx, cli, worker.PodName); err != nil {
		t.Fatalf("container did not become running: %v", err)
	}
	inspect, err := cli.ContainerInspect(ctx, worker.PodName)
	if err != nil {
		t.Fatalf("inspect worker container: %v", err)
	}
	env := envMap(inspect.Config.Env)
	expected := map[string]string{
		"LOADFORGE_ORCHESTRATOR_ADDR": "orchestrator:50051",
		"LOADFORGE_TEST_RUN_ID":       testRun.ID,
		"LOADFORGE_WORKER_ID":         worker.ID,
		"LOADFORGE_NATS_URL":          "nats://nats:4222",
	}
	for key, want := range expected {
		if env[key] != want {
			t.Fatalf("env %s = %q, want %q; all env=%v", key, env[key], want, env)
		}
	}
	if inspect.NetworkSettings.Networks[networkName] == nil {
		t.Fatalf("container is not attached to network %q", networkName)
	}

	if err := p.DrainAndRemoveWorkers(ctx, testRun, 1); err != nil {
		t.Fatalf("DrainAndRemoveWorkers returned error: %v", err)
	}
	if len(signals.signals) != 1 || signals.signals[0] != "SCALE_DOWN" {
		t.Fatalf("signals = %v, want [SCALE_DOWN]", signals.signals)
	}
	if _, err := cli.ContainerInspect(ctx, worker.PodName); !errdefs.IsNotFound(err) {
		t.Fatalf("container still exists after drain/remove: %v", err)
	}
}

func waitForContainerRunning(ctx context.Context, cli *client.Client, name string) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		inspect, err := cli.ContainerInspect(ctx, name)
		if err == nil && inspect.State != nil && inspect.State.Running {
			return nil
		}
		if err != nil && !errdefs.IsNotFound(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for running container: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func envMap(items []string) map[string]string {
	out := make(map[string]string, len(items))
	for _, item := range items {
		for i, ch := range item {
			if ch == '=' {
				out[item[:i]] = item[i+1:]
				break
			}
		}
	}
	return out
}
