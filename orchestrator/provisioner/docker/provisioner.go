package docker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	kubeprovisioner "github.com/vatsalchaudhary/loadforge/orchestrator/provisioner"
	"github.com/vatsalchaudhary/loadforge/orchestrator/run"
)

const (
	labelProvisioner = "loadforge.io/provisioner"
	defaultNetwork   = "loadforge"
)

type Provisioner struct {
	Client           *client.Client
	WorkerImage      string
	OrchestratorAddr string
	NATSURL          string
	NetworkName      string
	Registry         run.WorkerRegistry
	Signaler         run.WorkerSignaler
	Log              *slog.Logger
	DrainTimeout     time.Duration
	StopTimeout      int
}

func NewForConfig(ctx context.Context, cfg run.OrchestratorConfig, orchestratorAddr string, logger *slog.Logger) (*Provisioner, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("create docker client: %w", err)
	}
	if _, err := cli.Ping(ctx); err != nil {
		_ = cli.Close()
		return nil, fmt.Errorf("docker provisioner selected but Docker daemon is unreachable: %w", err)
	}
	if strings.TrimSpace(cfg.WorkerImage) == "" {
		_ = cli.Close()
		return nil, errors.New("docker provisioner requires WORKER_IMAGE")
	}
	return &Provisioner{
		Client:           cli,
		WorkerImage:      cfg.WorkerImage,
		OrchestratorAddr: orchestratorAddr,
		NATSURL:          cfg.NATSUrl,
		NetworkName:      defaultNetwork,
		Log:              logger,
		DrainTimeout:     30 * time.Second,
		StopTimeout:      10,
	}, nil
}

func (p *Provisioner) CreateWorkers(ctx context.Context, testRun run.TestRun, count int) error {
	for i := 0; i < count; i++ {
		workerID := fmt.Sprintf("%s-%s", testRun.ID, randomSuffix())
		labels := p.labels(testRun.ID, workerID)
		env := []string{
			"LOADFORGE_ORCHESTRATOR_ADDR=" + p.OrchestratorAddr,
			"LOADFORGE_TEST_RUN_ID=" + testRun.ID,
			"LOADFORGE_WORKER_ID=" + workerID,
			"LOADFORGE_NATS_URL=" + p.NATSURL,
		}
		created, err := p.Client.ContainerCreate(ctx,
			&container.Config{
				Image:  p.WorkerImage,
				Env:    env,
				Labels: labels,
			},
			&container.HostConfig{},
			&network.NetworkingConfig{EndpointsConfig: map[string]*network.EndpointSettings{
				p.networkName(): {},
			}},
			nil,
			workerID,
		)
		if err != nil {
			return err
		}
		if err := p.Client.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
			_ = p.Client.ContainerRemove(ctx, created.ID, container.RemoveOptions{Force: true})
			return err
		}
		if p.Registry != nil {
			_ = p.Registry.Register(ctx, run.Worker{RunID: testRun.ID, ID: workerID, PodName: workerID})
		}
		if p.Log != nil {
			p.Log.Info("worker container created", "run_id", testRun.ID, "worker_id", workerID, "container", created.ID)
		}
	}
	return nil
}

func (p *Provisioner) DrainAndRemoveWorkers(ctx context.Context, testRun run.TestRun, count int) error {
	if count <= 0 {
		return nil
	}
	workers, err := p.workersToRemove(ctx, testRun.ID, count)
	if err != nil {
		return err
	}
	timeout := p.DrainTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	for _, worker := range workers {
		if p.Signaler != nil {
			_ = p.Signaler.Signal(ctx, testRun.ID, worker.ID, "SCALE_DOWN", 0)
		}
		if p.Registry != nil {
			drainCtx, cancel := context.WithTimeout(ctx, timeout)
			_ = p.Registry.WaitForDrain(drainCtx, testRun.ID, worker.ID)
			cancel()
		}
		name := worker.PodName
		if name == "" {
			name = worker.ID
		}
		stopTimeout := p.StopTimeout
		err := p.Client.ContainerStop(ctx, name, container.StopOptions{Timeout: &stopTimeout})
		if err != nil && !errdefs.IsNotFound(err) {
			return err
		}
		err = p.Client.ContainerRemove(ctx, name, container.RemoveOptions{Force: true})
		if err != nil && !errdefs.IsNotFound(err) {
			return err
		}
		if p.Registry != nil {
			_ = p.Registry.Remove(ctx, testRun.ID, worker.ID)
		}
		if p.Log != nil {
			p.Log.Info("worker container removed", "run_id", testRun.ID, "worker_id", worker.ID, "container", name)
		}
	}
	return nil
}

func (p *Provisioner) workersToRemove(ctx context.Context, runID string, count int) ([]run.Worker, error) {
	if p.Registry != nil {
		return p.Registry.WorkersByLowestDispatchRate(ctx, runID, count)
	}
	containers, err := p.Client.ContainerList(ctx, container.ListOptions{
		All:     true,
		Filters: p.listFilters(runID),
	})
	if err != nil {
		return nil, err
	}
	if count > len(containers) {
		count = len(containers)
	}
	workers := make([]run.Worker, 0, count)
	for _, item := range containers[:count] {
		workerID := item.Labels[kubeprovisioner.LabelWorkerID]
		if workerID == "" {
			workerID = strings.TrimPrefix(firstName(item.Names), "/")
		}
		workers = append(workers, run.Worker{RunID: runID, ID: workerID, PodName: item.ID})
	}
	return workers, nil
}

func (p *Provisioner) labels(runID, workerID string) map[string]string {
	return map[string]string{
		kubeprovisioner.LabelRunID:    runID,
		kubeprovisioner.LabelWorkerID: workerID,
		"app":                         "loadforge-worker",
		labelProvisioner:              "docker",
	}
}

func (p *Provisioner) listFilters(runID string) filters.Args {
	args := filters.NewArgs()
	args.Add("label", kubeprovisioner.LabelRunID+"="+runID)
	args.Add("label", labelProvisioner+"=docker")
	return args
}

func (p *Provisioner) networkName() string {
	if p.NetworkName == "" {
		return defaultNetwork
	}
	return p.NetworkName
}

func randomSuffix() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

func firstName(names []string) string {
	if len(names) == 0 {
		return ""
	}
	return names[0]
}
