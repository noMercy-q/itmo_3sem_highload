package main

import (
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"

	"example.com/golden-signals-service/goldenmetrics"
	"example.com/golden-signals-service/internal/api"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	config := goldenmetrics.ConfigFromEnv()
	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))

	metrics, err := goldenmetrics.New(config, registry)
	if err != nil {
		slog.Error("initialize metrics", "error", err)
		os.Exit(1)
	}

	application := api.New(api.NewStore())
	mux := http.NewServeMux()

	mux.Handle("POST /items", metrics.InstrumentRoute(goldenmetrics.Endpoint{
		ID: "create_item", Method: http.MethodPost, Path: "/items",
	}, http.HandlerFunc(application.CreateItem)))

	mux.Handle("GET /items/{id}", metrics.InstrumentRoute(goldenmetrics.Endpoint{
		ID: "get_item", Method: http.MethodGet, Path: "/items/{id}",
	}, http.HandlerFunc(application.GetItem)))

	mux.Handle("PUT /items/{id}", metrics.InstrumentRoute(goldenmetrics.Endpoint{
		ID: "update_item", Method: http.MethodPut, Path: "/items/{id}",
	}, http.HandlerFunc(application.UpdateItem)))

	mux.Handle("GET /metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.Handle("GET /-/endpoints", metrics.DiscoveryHandler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_, _ = w.Write([]byte("{\"status\":\"ok\"}\n"))
	})

	address := envOrDefault("HTTP_ADDR", ":8080")
	server := &http.Server{
		Addr:              address,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	slog.Info("server started", "address", address, "pod", config.PodNumber, "datacenter", config.Datacenter, "deployment", config.Deployment)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}

	return fallback
}
