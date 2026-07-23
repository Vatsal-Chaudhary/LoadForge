package windower

import (
	"context"
	"log/slog"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/vatsalchaudhary/loadforge/aggregator/histogram"
)

type Key struct {
	RunID    string
	Endpoint string
	Method   string
}

type Sample struct {
	Timestamp  time.Time
	StatusCode int
	LatencyMs  float64
	Failed     bool
}

type Snapshot struct {
	Key         Key
	Timestamp   time.Time
	Window      time.Duration
	RPS         float64
	P50Ms       float64
	P95Ms       float64
	P99Ms       float64
	ErrorRate   float64
	ReqCount    int64
	ErrCount    int64
	StatusCodes map[string]int64

	RunRPS       float64
	RunP95Ms     float64
	RunErrorRate float64
}

type Config struct {
	Window     time.Duration
	Grace      time.Duration
	Logger     *slog.Logger
	Registerer prometheus.Registerer
	OnClose    func(context.Context, []Snapshot) error
	Now        func() time.Time
}

type bucketKey struct {
	Key
	Start time.Time
}

type bucket struct {
	key         bucketKey
	histogram   *histogram.Histogram
	reqCount    int64
	errCount    int64
	statusCodes map[string]int64
}

type Windower struct {
	window  time.Duration
	grace   time.Duration
	log     *slog.Logger
	onClose func(context.Context, []Snapshot) error
	now     func() time.Time
	late    prometheus.Counter

	mu      sync.Mutex
	closeMu sync.Mutex
	buckets map[bucketKey]*bucket
	evicted map[string]time.Time
}

func New(cfg Config) *Windower {
	if cfg.Window <= 0 {
		cfg.Window = 10 * time.Second
	}
	if cfg.Grace < 0 {
		cfg.Grace = 0
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Registerer == nil {
		cfg.Registerer = prometheus.DefaultRegisterer
	}
	late := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "loadforge_aggregator_late_samples_dropped_total",
		Help: "Samples dropped after their window grace period elapsed.",
	})
	cfg.Registerer.MustRegister(late)
	return &Windower{
		window: cfg.Window, grace: cfg.Grace, log: cfg.Logger, onClose: cfg.OnClose,
		now: cfg.Now, late: late, buckets: make(map[bucketKey]*bucket),
		evicted: make(map[string]time.Time),
	}
}

func (w *Windower) Add(key Key, sample Sample) bool {
	start := sample.Timestamp.UTC().Truncate(w.window)
	deadline := start.Add(w.window + w.grace)
	if !w.now().UTC().Before(deadline) {
		w.late.Inc()
		return false
	}

	bk := bucketKey{Key: key, Start: start}
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.now().UTC().Before(deadline) {
		w.late.Inc()
		return false
	}
	if until, ok := w.evicted[key.RunID]; ok && w.now().UTC().Before(until) {
		return false
	}
	b := w.buckets[bk]
	if b == nil {
		b = &bucket{
			key: bk, histogram: histogram.New(),
			statusCodes: make(map[string]int64),
		}
		w.buckets[bk] = b
	}
	b.reqCount++
	if sample.Failed {
		b.errCount++
	}
	b.statusCodes[strconv.Itoa(sample.StatusCode)]++
	b.histogram.RecordMilliseconds(sample.LatencyMs)
	return true
}

func (w *Windower) Run(ctx context.Context) {
	tick := w.window / 10
	if tick <= 0 || tick > time.Second {
		tick = time.Second
	}
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			w.CloseDue(ctx, now)
		}
	}
}

func (w *Windower) CloseDue(ctx context.Context, now time.Time) {
	w.mu.Lock()
	for runID, until := range w.evicted {
		if !now.UTC().Before(until) {
			delete(w.evicted, runID)
		}
	}
	w.mu.Unlock()
	w.closeMatching(ctx, func(key bucketKey) bool {
		return !now.UTC().Before(key.Start.Add(w.window + w.grace))
	})
}

func (w *Windower) Flush(ctx context.Context) {
	w.closeMatching(ctx, func(bucketKey) bool { return true })
}

func (w *Windower) EvictRun(ctx context.Context, runID string) {
	w.mu.Lock()
	w.evicted[runID] = w.now().UTC().Add(w.window + w.grace)
	w.mu.Unlock()
	w.closeMatching(ctx, func(key bucketKey) bool { return key.RunID == runID })
}

func (w *Windower) closeMatching(ctx context.Context, match func(bucketKey) bool) {
	w.closeMu.Lock()
	defer w.closeMu.Unlock()

	w.mu.Lock()
	closed := make([]*bucket, 0)
	for key, b := range w.buckets {
		if match(key) {
			closed = append(closed, b)
			delete(w.buckets, key)
		}
	}
	w.mu.Unlock()
	if len(closed) == 0 {
		return
	}

	sort.Slice(closed, func(i, j int) bool {
		if closed[i].key.Start.Equal(closed[j].key.Start) {
			if closed[i].key.RunID == closed[j].key.RunID {
				if closed[i].key.Endpoint == closed[j].key.Endpoint {
					return closed[i].key.Method < closed[j].key.Method
				}
				return closed[i].key.Endpoint < closed[j].key.Endpoint
			}
			return closed[i].key.RunID < closed[j].key.RunID
		}
		return closed[i].key.Start.Before(closed[j].key.Start)
	})

	snapshots := make([]Snapshot, 0, len(closed))
	type rollup struct {
		req, err int64
		hist     *histogram.Histogram
	}
	rollups := make(map[string]*rollup)
	for _, b := range closed {
		p := b.histogram.Percentiles()
		snapshot := Snapshot{
			Key: b.key.Key, Timestamp: b.key.Start.Add(w.window), Window: w.window,
			RPS:   float64(b.reqCount) / w.window.Seconds(),
			P50Ms: p.P50, P95Ms: p.P95, P99Ms: p.P99,
			ReqCount: b.reqCount, ErrCount: b.errCount,
			StatusCodes: cloneMap(b.statusCodes),
		}
		if b.reqCount > 0 {
			snapshot.ErrorRate = float64(b.errCount) / float64(b.reqCount)
		}
		snapshots = append(snapshots, snapshot)
		id := b.key.Start.Format(time.RFC3339Nano) + "\x00" + b.key.RunID
		r := rollups[id]
		if r == nil {
			r = &rollup{hist: histogram.New()}
			rollups[id] = r
		}
		r.req += b.reqCount
		r.err += b.errCount
		r.hist.Merge(b.histogram)
		w.log.Info("metrics window closed", "run_id", b.key.RunID, "endpoint", b.key.Endpoint,
			"method", b.key.Method, "sample_count", b.reqCount, "duration", w.window)
	}
	for i := range snapshots {
		start := snapshots[i].Timestamp.Add(-w.window)
		r := rollups[start.Format(time.RFC3339Nano)+"\x00"+snapshots[i].Key.RunID]
		snapshots[i].RunRPS = float64(r.req) / w.window.Seconds()
		snapshots[i].RunP95Ms = r.hist.Percentiles().P95
		if r.req > 0 {
			snapshots[i].RunErrorRate = float64(r.err) / float64(r.req)
		}
	}
	if w.onClose != nil {
		if err := w.onClose(ctx, snapshots); err != nil {
			w.log.Error("metrics window callback failed", "error", err, "snapshots", len(snapshots))
		}
	}
}

func cloneMap(src map[string]int64) map[string]int64 {
	dst := make(map[string]int64, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}
