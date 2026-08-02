package leader

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

func TestExactlyOneLeaderAndBoundedTakeover(t *testing.T) {
	env := &envtest.Environment{}
	restConfig, err := env.Start()
	if err != nil {
		if strings.Contains(err.Error(), "unable to start the controlplane") ||
			strings.Contains(err.Error(), "unable to start control plane") ||
			strings.Contains(err.Error(), "no such file or directory") ||
			strings.Contains(err.Error(), "operation not permitted") {
			t.Skipf("envtest assets unavailable: %v", err)
		}
		t.Fatalf("start envtest: %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })
	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		t.Fatal(err)
	}
	const namespace = "leader-test"
	if _, err := client.CoreV1().Namespaces().Create(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	type contender struct {
		id       string
		cancel   context.CancelFunc
		acquired chan string
	}
	contenders := make([]contender, 0, 2)
	acquired := make(chan string, 4)
	for _, id := range []string{"orchestrator-a", "orchestrator-b"} {
		ctx, cancel := context.WithCancel(context.Background())
		state := &State{}
		election, err := New(Config{Client: client, Namespace: namespace, LeaseName: "loadforge", Identity: id,
			LeaseDuration: 2 * time.Second, RenewDeadline: time.Second, RetryPeriod: 200 * time.Millisecond, State: state})
		if err != nil {
			t.Fatal(err)
		}
		go func(id string) { _ = election.Run(ctx, func(term context.Context) { acquired <- id; <-term.Done() }) }(id)
		contenders = append(contenders, contender{id: id, cancel: cancel, acquired: acquired})
	}
	t.Cleanup(func() {
		for _, c := range contenders {
			c.cancel()
		}
	})

	var first string
	select {
	case first = <-acquired:
	case <-time.After(10 * time.Second):
		t.Fatal("no contender acquired leadership")
	}
	select {
	case second := <-acquired:
		t.Fatalf("two simultaneous leaders acquired: %s and %s", first, second)
	case <-time.After(750 * time.Millisecond):
	}
	for _, c := range contenders {
		if c.id == first {
			c.cancel()
		}
	}
	select {
	case second := <-acquired:
		if second == first {
			t.Fatalf("released contender reacquired leadership: %s", second)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("follower did not take over within 5 seconds")
	}
}
