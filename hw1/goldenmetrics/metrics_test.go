package goldenmetrics

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestInstrumentRecordsGoldenSignals(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := New(Config{
		PodNumber: "7", Datacenter: "dc-1", Deployment: "items", Namespace: "test",
	}, registry)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	handler := metrics.InstrumentRoute(Endpoint{ID: "get_item", Method: http.MethodGet, Path: "/items/{id}"}, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "missing", http.StatusNotFound)
	}))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/items/42", nil))

	if got := testutil.ToFloat64(metrics.requests.WithLabelValues("get_item", "throughput")); got != 1 {
		t.Fatalf("requests = %v, want 1", got)
	}

	if got := testutil.ToFloat64(metrics.errors.WithLabelValues("get_item", "errors", "404", "4xx")); got != 1 {
		t.Fatalf("errors = %v, want 1", got)
	}

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather() error: %v", err)
	}

	foundHistogram := false
	for _, family := range families {
		if family.GetName() == "test_http_request_latency_seconds" && len(family.Metric) == 1 {
			foundHistogram = len(family.Metric[0].GetHistogram().Bucket) == 20
		}
	}

	if !foundHistogram {
		t.Fatal("latency histogram does not expose the configured buckets")
	}

	catalog := metrics.Catalog()
	if len(catalog.Endpoints) != 1 || catalog.Endpoints[0].ID != "get_item" {
		t.Fatalf("unexpected endpoint catalog: %+v", catalog)
	}
}

func TestInstrumentRecordsPanicAsServerError(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := New(Config{Namespace: "panic_test"}, registry)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	handler := metrics.Instrument("panic", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	}))

	func() {
		defer func() { _ = recover() }()
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	}()

	if got := testutil.ToFloat64(metrics.errors.WithLabelValues("panic", "errors", "500", "5xx")); got != 1 {
		t.Fatalf("panic errors = %v, want 1", got)
	}
}
