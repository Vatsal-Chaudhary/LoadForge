package leader

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	k8sleaderelection "k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

var isLeaderGauge = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "loadforge_orchestrator_is_leader",
	Help: "Whether this orchestrator instance currently holds the Kubernetes leader lease.",
})

type State struct{ leader atomic.Bool }

func (s *State) IsLeader() bool { return s.leader.Load() }

func (s *State) Set(value bool) {
	s.leader.Store(value)
	if value {
		isLeaderGauge.Set(1)
	} else {
		isLeaderGauge.Set(0)
	}
}

type Config struct {
	Client        kubernetes.Interface
	Namespace     string
	LeaseName     string
	Identity      string
	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration
	State         *State
	Log           *slog.Logger
}

type Election struct{ cfg Config }

func New(cfg Config) (*Election, error) {
	if cfg.Client == nil || cfg.Namespace == "" || cfg.LeaseName == "" || cfg.Identity == "" {
		return nil, fmt.Errorf("leader election requires client, namespace, lease name, and identity")
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = 15 * time.Second
	}
	if cfg.RenewDeadline <= 0 {
		cfg.RenewDeadline = 10 * time.Second
	}
	if cfg.RetryPeriod <= 0 {
		cfg.RetryPeriod = 2 * time.Second
	}
	if cfg.State == nil {
		cfg.State = &State{}
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	return &Election{cfg: cfg}, nil
}

func Client(kubeconfig string) (kubernetes.Interface, error) {
	var cfg *rest.Config
	var err error
	if kubeconfig != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else if cfg, err = rest.InClusterConfig(); err != nil {
		cfg, err = clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
	}
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(cfg)
}

func (e *Election) Run(ctx context.Context, runLeader func(context.Context)) error {
	e.cfg.State.Set(false)
	for ctx.Err() == nil {
		lock := &resourcelock.LeaseLock{
			LeaseMeta:  metav1.ObjectMeta{Name: e.cfg.LeaseName, Namespace: e.cfg.Namespace},
			Client:     e.cfg.Client.CoordinationV1(),
			LockConfig: resourcelock.ResourceLockConfig{Identity: e.cfg.Identity},
		}
		elector, err := k8sleaderelection.NewLeaderElector(k8sleaderelection.LeaderElectionConfig{
			Lock: lock, LeaseDuration: e.cfg.LeaseDuration, RenewDeadline: e.cfg.RenewDeadline,
			RetryPeriod: e.cfg.RetryPeriod, ReleaseOnCancel: true, Name: e.cfg.LeaseName,
			Callbacks: k8sleaderelection.LeaderCallbacks{
				OnStartedLeading: func(termCtx context.Context) {
					e.cfg.State.Set(true)
					e.cfg.Log.Info("orchestrator became leader", "identity", e.cfg.Identity)
					runLeader(termCtx)
					e.cfg.State.Set(false)
				},
				OnStoppedLeading: func() {
					e.cfg.State.Set(false)
					e.cfg.Log.Warn("orchestrator leadership term ended", "identity", e.cfg.Identity)
				},
				OnNewLeader: func(identity string) {
					if identity != e.cfg.Identity {
						e.cfg.Log.Info("observed orchestrator leader", "identity", identity)
					}
				},
			},
		})
		if err != nil {
			return err
		}
		elector.Run(ctx)
	}
	e.cfg.State.Set(false)
	return ctx.Err()
}
