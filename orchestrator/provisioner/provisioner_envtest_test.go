package provisioner

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/vatsalchaudhary/loadforge/orchestrator/run"
)

func TestCreateWorkersWithEnvtestAPIServer(t *testing.T) {
	env := &envtest.Environment{}
	cfg, err := env.Start()
	if err != nil {
		if strings.Contains(err.Error(), "unable to start the controlplane") ||
			strings.Contains(err.Error(), "no such file or directory") {
			t.Skipf("envtest assets unavailable: %v", err)
		}
		t.Fatalf("start envtest: %v", err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Fatalf("stop envtest: %v", err)
		}
	})

	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	ctx := context.Background()
	if _, err := client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "loadforge-test"}}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	reg := &fakeRegistry{}
	p := &Provisioner{
		Client:           client,
		WorkerNamespace:  "loadforge-test",
		WorkerImage:      "loadforge-worker:test",
		OrchestratorAddr: "orchestrator:50051",
		NATSURL:          "nats://nats:4222",
		Registry:         reg,
	}
	if err := p.CreateWorkers(ctx, run.TestRun{ID: "run-envtest"}, 1); err != nil {
		t.Fatalf("CreateWorkers returned error: %v", err)
	}
	pods, err := client.CoreV1().Pods("loadforge-test").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("pods = %d, want 1", len(pods.Items))
	}
}
