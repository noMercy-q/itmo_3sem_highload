package goldenmetrics

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type Config struct {
	PodNumber  string
	Datacenter string
	Deployment string
	Namespace  string
}

// ID is a stable metric label, not a URL containing resource IDs.
type Endpoint struct {
	ID     string `json:"id"`
	Method string `json:"method"`
	Path   string `json:"path"`
}

type Catalog struct {
	Deployment       string     `json:"deployment"`
	MetricsNamespace string     `json:"metrics_namespace"`
	Endpoints        []Endpoint `json:"endpoints"`
}

func ConfigFromEnv() Config {
	return Config{
		PodNumber:  envOrDefault("POD_NUMBER", "local-1"),
		Datacenter: envOrDefault("DATACENTER", "local"),
		Deployment: envOrDefault("DEPLOYMENT_NAME", "items-service"),
		Namespace:  envOrDefault("METRICS_NAMESPACE", "demo"),
	}
}

func envOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}

	return fallback
}

type Metrics struct {
	config       Config
	latency      *prometheus.HistogramVec
	requests     *prometheus.CounterVec
	errors       *prometheus.CounterVec
	endpointInfo *prometheus.GaugeVec
	endpointsMu  sync.RWMutex
	endpoints    map[string]Endpoint
}

func New(config Config, registerer prometheus.Registerer) (*Metrics, error) {
	if registerer == nil {
		return nil, fmt.Errorf("registerer is required")
	}

	constantLabels := prometheus.Labels{
		"pod_number": config.PodNumber,
		"datacenter": config.Datacenter,
		"deployment": config.Deployment,
	}

	metrics := &Metrics{
		config:    config,
		endpoints: make(map[string]Endpoint),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace:   config.Namespace,
			Subsystem:   "http",
			Name:        "request_latency_seconds",
			Help:        "HTTP request latency in seconds.",
			ConstLabels: constantLabels,
			Buckets: []float64{
				0.000005, 0.00001, 0.00002, 0.00003, 0.00005, 0.000075,
				0.0001, 0.00015, 0.00025, 0.0005, 0.001, 0.0025,
				0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1,
			},
		}, []string{"endpoint", "metric_type"}),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   config.Namespace,
			Subsystem:   "http",
			Name:        "requests_total",
			Help:        "Total HTTP requests.",
			ConstLabels: constantLabels,
		}, []string{"endpoint", "metric_type"}),
		errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   config.Namespace,
			Subsystem:   "http",
			Name:        "request_errors_total",
			Help:        "Total REST responses with an HTTP error status.",
			ConstLabels: constantLabels,
		}, []string{"endpoint", "metric_type", "status_code", "status_class"}),
		endpointInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace:   config.Namespace,
			Subsystem:   "http",
			Name:        "endpoint_info",
			Help:        "Registered HTTP endpoints.",
			ConstLabels: constantLabels,
		}, []string{"endpoint", "metric_type", "method", "route"}),
	}

	collectors := []prometheus.Collector{metrics.latency, metrics.requests, metrics.errors, metrics.endpointInfo}
	for _, collector := range collectors {
		if err := registerer.Register(collector); err != nil {
			return nil, fmt.Errorf("register golden metrics: %w", err)
		}
	}

	return metrics, nil
}

func (m *Metrics) Instrument(endpointID string, next http.Handler) http.Handler {
	return m.instrument(endpointID, next)
}

func (m *Metrics) InstrumentRoute(endpoint Endpoint, next http.Handler) http.Handler {
	endpoint.ID = strings.TrimSpace(endpoint.ID)
	endpoint.Method = strings.ToUpper(strings.TrimSpace(endpoint.Method))
	endpoint.Path = strings.TrimSpace(endpoint.Path)
	if endpoint.Method == "" || endpoint.Path == "" {
		panic("goldenmetrics: endpoint method and path must not be empty")
	}

	m.registerEndpoint(endpoint)

	return m.instrument(endpoint.ID, next)
}

func (m *Metrics) instrument(endpointID string, next http.Handler) http.Handler {
	if strings.TrimSpace(endpointID) == "" {
		panic("goldenmetrics: endpoint ID must not be empty")
	}

	if next == nil {
		panic("goldenmetrics: next handler must not be nil")
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

		defer func() {
			status := recorder.status
			if recovered := recover(); recovered != nil {
				status = http.StatusInternalServerError
				m.observe(endpointID, status, time.Since(started))
				panic(recovered)
			}

			m.observe(endpointID, status, time.Since(started))
		}()

		next.ServeHTTP(recorder, r)
	})
}

func (m *Metrics) registerEndpoint(endpoint Endpoint) {
	m.endpointsMu.Lock()
	defer m.endpointsMu.Unlock()

	if existing, ok := m.endpoints[endpoint.ID]; ok {
		if existing != endpoint {
			panic(fmt.Sprintf("goldenmetrics: endpoint ID %q is already registered for %s %s", endpoint.ID, existing.Method, existing.Path))
		}

		return
	}

	m.endpoints[endpoint.ID] = endpoint
	m.endpointInfo.WithLabelValues(endpoint.ID, "discovery", endpoint.Method, endpoint.Path).Set(1)
}

func (m *Metrics) Catalog() Catalog {
	m.endpointsMu.RLock()
	endpoints := make([]Endpoint, 0, len(m.endpoints))
	for _, endpoint := range m.endpoints {
		endpoints = append(endpoints, endpoint)
	}

	m.endpointsMu.RUnlock()

	sort.Slice(endpoints, func(i, j int) bool {
		return endpoints[i].ID < endpoints[j].ID
	})

	return Catalog{
		Deployment:       m.config.Deployment,
		MetricsNamespace: m.config.Namespace,
		Endpoints:        endpoints,
	}
}

func (m *Metrics) DiscoveryHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(m.Catalog())
	})
}

func (m *Metrics) observe(endpointID string, status int, elapsed time.Duration) {
	m.latency.WithLabelValues(endpointID, "latency").Observe(elapsed.Seconds())
	m.requests.WithLabelValues(endpointID, "throughput").Inc()
	if status >= http.StatusBadRequest {
		m.errors.WithLabelValues(
			endpointID,
			"errors",
			strconv.Itoa(status),
			fmt.Sprintf("%dxx", status/100),
		).Inc()
	}
}

type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (r *statusRecorder) WriteHeader(status int) {
	if r.wroteHeader {
		return
	}

	r.status = status
	r.wroteHeader = true
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(body []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}

	return r.ResponseWriter.Write(body)
}

// Allows http.ResponseController to access the original writer.
func (r *statusRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}
