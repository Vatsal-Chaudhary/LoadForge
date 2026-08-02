package provisioner

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/vatsalchaudhary/loadforge/orchestrator/run"
)

const (
	LabelRunID    = "loadforge.io/run-id"
	LabelWorkerID = "loadforge.io/worker-id"
)

type Provisioner struct {
	Client           kubernetes.Interface
	WorkerNamespace  string
	WorkerImage      string
	OrchestratorAddr string
	NATSURL          string
	Registry         run.WorkerRegistry
	Signaler         run.WorkerSignaler
	Log              *slog.Logger
	DrainTimeout     time.Duration
}

type Interface interface {
	CreateWorkers(ctx context.Context, testRun run.TestRun, count int) error
	DrainAndRemoveWorkers(ctx context.Context, testRun run.TestRun, count int) error
}

func NewForConfig(cfg run.OrchestratorConfig, orchestratorAddr string, logger *slog.Logger) (*Provisioner, error) {
	restConfig, err := kubeConfig(cfg.KubeConfigPath)
	if err != nil {
		return nil, err
	}
	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, err
	}
	return &Provisioner{
		Client:           client,
		WorkerNamespace:  defaultString(cfg.WorkerNamespace, "loadforge-workers"),
		WorkerImage:      cfg.WorkerImage,
		OrchestratorAddr: orchestratorAddr,
		NATSURL:          cfg.NATSUrl,
		Log:              logger,
		DrainTimeout:     30 * time.Second,
	}, nil
}

func kubeConfig(path string) (*rest.Config, error) {
	if path != "" {
		return clientcmd.BuildConfigFromFlags("", path)
	}
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}
	return clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
}

func (p *Provisioner) CreateWorkers(ctx context.Context, testRun run.TestRun, count int) error {
	for i := 0; i < count; i++ {
		workerID := fmt.Sprintf("%s-%s", testRun.ID, rand.String(8))
		pod := p.workerPod(testRun.ID, workerID)
		if _, err := p.Client.CoreV1().Pods(p.WorkerNamespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
			return err
		}
		if p.Registry != nil {
			_ = p.Registry.Register(ctx, run.Worker{RunID: testRun.ID, ID: workerID, PodName: pod.Name})
		}
		if p.Log != nil {
			p.Log.Info("worker pod created", "run_id", testRun.ID, "worker_id", workerID, "pod", pod.Name)
		}
	}
	return nil
}

func (p *Provisioner) DrainAndRemoveWorkers(ctx context.Context, testRun run.TestRun, count int) error {
	if count <= 0 {
		return nil
	}
	workers, err := p.Registry.WorkersByLowestDispatchRate(ctx, testRun.ID, count)
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
		drainCtx, cancel := context.WithTimeout(ctx, timeout)
		_ = p.Registry.WaitForDrain(drainCtx, testRun.ID, worker.ID)
		cancel()
		if worker.PodName == "" {
			worker.PodName = worker.ID
		}
		err := p.Client.CoreV1().Pods(p.WorkerNamespace).Delete(ctx, worker.PodName, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		_ = p.Registry.Remove(ctx, testRun.ID, worker.ID)
		if p.Log != nil {
			p.Log.Info("worker pod removed", "run_id", testRun.ID, "worker_id", worker.ID, "pod", worker.PodName)
		}
	}
	return nil
}

func (p *Provisioner) WatchPods(ctx context.Context, runID string, onEvent func(event PodEvent)) error {
	factory := informers.NewSharedInformerFactoryWithOptions(
		p.Client,
		0,
		informers.WithNamespace(p.WorkerNamespace),
		informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.LabelSelector = labels.SelectorFromSet(labels.Set{LabelRunID: runID}).String()
		}),
	)
	informer := factory.Core().V1().Pods().Informer()
	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { emitPodEvent(obj, onEvent) },
		UpdateFunc: func(_, obj interface{}) { emitPodEvent(obj, onEvent) },
		DeleteFunc: func(obj interface{}) { emitPodEvent(obj, onEvent) },
	})
	if err != nil {
		return err
	}
	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		return ctx.Err()
	}
	<-ctx.Done()
	return ctx.Err()
}

type PodEvent struct {
	RunID    string
	WorkerID string
	PodName  string
	Phase    corev1.PodPhase
	Reason   string
	Message  string
}

func emitPodEvent(obj interface{}, onEvent func(event PodEvent)) {
	pod, ok := obj.(*corev1.Pod)
	if !ok || onEvent == nil {
		return
	}
	event := PodEvent{
		RunID:    pod.Labels[LabelRunID],
		WorkerID: pod.Labels[LabelWorkerID],
		PodName:  pod.Name,
		Phase:    pod.Status.Phase,
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.State.Waiting != nil {
			event.Reason = status.State.Waiting.Reason
			event.Message = status.State.Waiting.Message
		}
		if status.LastTerminationState.Terminated != nil {
			event.Reason = status.LastTerminationState.Terminated.Reason
			event.Message = status.LastTerminationState.Terminated.Message
		}
	}
	onEvent(event)
}

func (p *Provisioner) workerPod(runID, workerID string) *corev1.Pod {
	labels := map[string]string{LabelRunID: runID, LabelWorkerID: workerID, "app": "loadforge-worker"}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: workerID, Namespace: p.WorkerNamespace, Labels: labels},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:            "worker",
				Image:           p.WorkerImage,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Ports:           []corev1.ContainerPort{{Name: "metrics", ContainerPort: 9102}},
				ReadinessProbe: &corev1.Probe{
					ProbeHandler:  corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/metrics", Port: intstr.FromString("metrics")}},
					PeriodSeconds: 5,
				},
				Env: []corev1.EnvVar{
					{Name: "LOADFORGE_ORCHESTRATOR_ADDR", Value: p.OrchestratorAddr},
					{Name: "LOADFORGE_TEST_RUN_ID", Value: runID},
					{Name: "LOADFORGE_WORKER_ID", Value: workerID},
					{Name: "LOADFORGE_NATS_URL", Value: p.NATSURL},
				},
			}},
		},
	}
}

func defaultString(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}
