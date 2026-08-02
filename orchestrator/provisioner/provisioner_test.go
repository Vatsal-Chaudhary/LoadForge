package provisioner

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/vatsalchaudhary/loadforge/orchestrator/run"
	pb "github.com/vatsalchaudhary/loadforge/proto"
)

type fakeRegistry struct {
	workers []run.Worker
}

func (r *fakeRegistry) Register(ctx context.Context, worker run.Worker) error {
	r.workers = append(r.workers, worker)
	return nil
}
func (r *fakeRegistry) Remove(ctx context.Context, runID, workerID string) error { return nil }
func (r *fakeRegistry) Count(ctx context.Context, runID string) (int, error) {
	return len(r.workers), nil
}
func (r *fakeRegistry) WorkersByLowestDispatchRate(ctx context.Context, runID string, count int) ([]run.Worker, error) {
	if count > len(r.workers) {
		count = len(r.workers)
	}
	return r.workers[:count], nil
}
func (r *fakeRegistry) HeartbeatReceived(ctx context.Context, workerID string, within time.Duration) (bool, error) {
	return true, nil
}
func (r *fakeRegistry) RecordHeartbeat(ctx context.Context, req *pb.HeartbeatRequest) error {
	return nil
}
func (r *fakeRegistry) MarkDead(ctx context.Context, runID, workerID string) error     { return nil }
func (r *fakeRegistry) WaitForDrain(ctx context.Context, runID, workerID string) error { return nil }

type fakeSignaler struct{ signals []string }

func (s *fakeSignaler) Signal(ctx context.Context, runID, workerID, signal string, newVUCount int32) error {
	s.signals = append(s.signals, signal)
	return nil
}

func TestCreateWorkersCreatesLabeledPodsWithEnv(t *testing.T) {
	reg := &fakeRegistry{}
	p := &Provisioner{
		Client:           fake.NewSimpleClientset(),
		WorkerNamespace:  "default",
		WorkerImage:      "loadforge-worker:test",
		OrchestratorAddr: "orchestrator:50051",
		NATSURL:          "nats://nats:4222",
		Registry:         reg,
	}
	if err := p.CreateWorkers(context.Background(), run.TestRun{ID: "run-1"}, 2); err != nil {
		t.Fatalf("CreateWorkers returned error: %v", err)
	}
	pods, err := p.Client.CoreV1().Pods("default").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 2 || len(reg.workers) != 2 {
		t.Fatalf("pods/registry = %d/%d, want 2/2", len(pods.Items), len(reg.workers))
	}
	pod := pods.Items[0]
	if pod.Labels[LabelRunID] != "run-1" || pod.Labels[LabelWorkerID] == "" {
		t.Fatalf("missing labels: %v", pod.Labels)
	}
	env := map[string]string{}
	for _, item := range pod.Spec.Containers[0].Env {
		env[item.Name] = item.Value
	}
	if env["LOADFORGE_ORCHESTRATOR_ADDR"] != "orchestrator:50051" ||
		env["LOADFORGE_TEST_RUN_ID"] != "run-1" ||
		env["LOADFORGE_WORKER_ID"] == "" ||
		env["LOADFORGE_NATS_URL"] != "nats://nats:4222" {
		t.Fatalf("unexpected env: %v", env)
	}
	container := pod.Spec.Containers[0]
	if pod.Spec.ServiceAccountName != "loadforge-worker" || pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		t.Fatalf("worker service account/token configuration is not least privilege: %+v", pod.Spec)
	}
	if pod.Spec.SecurityContext == nil || pod.Spec.SecurityContext.RunAsUser == nil || *pod.Spec.SecurityContext.RunAsUser != 1000 ||
		pod.Spec.SecurityContext.RunAsNonRoot == nil || !*pod.Spec.SecurityContext.RunAsNonRoot {
		t.Fatalf("worker pod security context = %+v", pod.Spec.SecurityContext)
	}
	if container.SecurityContext == nil || container.SecurityContext.AllowPrivilegeEscalation == nil || *container.SecurityContext.AllowPrivilegeEscalation ||
		container.SecurityContext.ReadOnlyRootFilesystem == nil || !*container.SecurityContext.ReadOnlyRootFilesystem {
		t.Fatalf("worker container security context = %+v", container.SecurityContext)
	}
	if container.Resources.Limits.Cpu().IsZero() || container.Resources.Limits.Memory().IsZero() ||
		container.Resources.Requests.Cpu().IsZero() || container.Resources.Requests.Memory().IsZero() {
		t.Fatalf("worker resource requirements = %+v", container.Resources)
	}
}

func TestEmitPodEventDetectsCrashReasons(t *testing.T) {
	var got PodEvent
	emitPodEvent(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Labels: map[string]string{LabelRunID: "run-1", LabelWorkerID: "worker-1"}},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff", Message: "back-off"}},
		}}},
	}, func(event PodEvent) { got = event })
	if got.Reason != "CrashLoopBackOff" || got.WorkerID != "worker-1" {
		t.Fatalf("event = %+v", got)
	}
}
