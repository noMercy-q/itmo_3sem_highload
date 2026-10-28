package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type config struct {
	BaseURL         string
	StartRPS        float64
	TargetRPS       float64
	RampDuration    time.Duration
	MinErrorPercent int
	MaxErrorPercent int
	ErrorCycle      time.Duration
	RequestTimeout  time.Duration
	LogInterval     time.Duration
	MaxInFlight     int
}

func configFromEnv() (config, error) {
	startRPS, err := floatEnv("LOAD_START_RPS", 1)
	if err != nil {
		return config{}, err
	}

	targetRPS, err := floatEnv("LOAD_TARGET_RPS", 10)
	if err != nil {
		return config{}, err
	}

	rampDuration, err := durationEnv("LOAD_RAMP_DURATION", 30*time.Second)
	if err != nil {
		return config{}, err
	}

	minErrorPercent, err := intEnv("LOAD_ERROR_MIN_PERCENT", 5)
	if err != nil {
		return config{}, err
	}

	maxErrorPercent, err := intEnv("LOAD_ERROR_MAX_PERCENT", 35)
	if err != nil {
		return config{}, err
	}

	errorCycle, err := durationEnv("LOAD_ERROR_CYCLE_DURATION", 2*time.Minute)
	if err != nil {
		return config{}, err
	}

	requestTimeout, err := durationEnv("LOAD_REQUEST_TIMEOUT", 3*time.Second)
	if err != nil {
		return config{}, err
	}

	logInterval, err := durationEnv("LOAD_LOG_INTERVAL", 10*time.Second)
	if err != nil {
		return config{}, err
	}

	maxInFlight, err := intEnv("LOAD_MAX_IN_FLIGHT", 32)
	if err != nil {
		return config{}, err
	}

	cfg := config{
		BaseURL:         strings.TrimRight(envOrDefault("TARGET_URL", "http://app:8080"), "/"),
		StartRPS:        startRPS,
		TargetRPS:       targetRPS,
		RampDuration:    rampDuration,
		MinErrorPercent: minErrorPercent,
		MaxErrorPercent: maxErrorPercent,
		ErrorCycle:      errorCycle,
		RequestTimeout:  requestTimeout,
		LogInterval:     logInterval,
		MaxInFlight:     maxInFlight,
	}

	if err := cfg.validate(); err != nil {
		return config{}, err
	}

	return cfg, nil
}

func (c config) validate() error {
	if c.BaseURL == "" {
		return fmt.Errorf("TARGET_URL must not be empty")
	}

	if c.StartRPS <= 0 || c.TargetRPS <= 0 || c.TargetRPS < c.StartRPS {
		return fmt.Errorf("RPS values must satisfy 0 < start <= target")
	}

	if c.RampDuration <= 0 || c.ErrorCycle <= 0 || c.RequestTimeout <= 0 || c.LogInterval <= 0 {
		return fmt.Errorf("durations must be positive")
	}

	if c.MinErrorPercent < 0 || c.MaxErrorPercent > 100 || c.MinErrorPercent > c.MaxErrorPercent {
		return fmt.Errorf("error percentages must satisfy 0 <= min <= max <= 100")
	}

	if c.MaxInFlight <= 0 {
		return fmt.Errorf("LOAD_MAX_IN_FLIGHT must be positive")
	}

	return nil
}

func main() {
	cfg, err := configFromEnv()
	if err != nil {
		slog.Error("load generator configuration", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg); err != nil {
		slog.Error("load generator stopped", "error", err)
		os.Exit(1)
	}
}

type counters struct {
	sent            atomic.Uint64
	success         atomic.Uint64
	expectedErrors  atomic.Uint64
	transportErrors atomic.Uint64
	dropped         atomic.Uint64
}

func run(ctx context.Context, cfg config) error {
	client := &http.Client{Timeout: cfg.RequestTimeout}
	target, err := bootstrap(ctx, client, cfg.BaseURL)
	if err != nil {
		return fmt.Errorf("bootstrap item: %w", err)
	}

	slog.Info(
		"background load started",
		"start_rps", cfg.StartRPS,
		"target_rps", cfg.TargetRPS,
		"ramp_duration", cfg.RampDuration,
		"min_error_percent", cfg.MinErrorPercent,
		"max_error_percent", cfg.MaxErrorPercent,
		"error_cycle", cfg.ErrorCycle,
	)

	started := time.Now()
	scheduler := time.NewTicker(100 * time.Millisecond)
	logger := time.NewTicker(cfg.LogInterval)
	defer scheduler.Stop()
	defer logger.Stop()

	stats := &counters{}
	semaphore := make(chan struct{}, cfg.MaxInFlight)
	var workers sync.WaitGroup
	var sequence uint64
	errors := errorPlanner{
		random: rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())),
	}
	errors.update(cfg, 0)

	var credits float64
	lastTick := time.Now()

	for {
		select {
		case <-ctx.Done():
			workers.Wait()
			slog.Info("background load stopped", "sent", stats.sent.Load())
			return nil
		case now := <-scheduler.C:
			elapsed := now.Sub(started)
			rate := currentRate(cfg, elapsed)
			errors.update(cfg, elapsed)
			credits += rate * now.Sub(lastTick).Seconds()
			lastTick = now
			for credits >= 1 {
				credits--
				request := chooseRequest(sequence, errors.next(), target)
				sequence++
				select {
				case semaphore <- struct{}{}:
					workers.Add(1)
					go func() {
						defer workers.Done()
						defer func() { <-semaphore }()
						send(ctx, client, cfg.BaseURL, request, stats)
					}()
				default:
					stats.dropped.Add(1)
				}
			}
		case now := <-logger.C:
			slog.Info(
				"background load stats",
				"current_rps", fmt.Sprintf("%.2f", currentRate(cfg, now.Sub(started))),
				"current_error_percent", errors.percent,
				"sent", stats.sent.Load(),
				"success", stats.success.Load(),
				"http_errors", stats.expectedErrors.Load(),
				"transport_errors", stats.transportErrors.Load(),
				"dropped", stats.dropped.Load(),
			)
		}
	}
}

func currentRate(cfg config, elapsed time.Duration) float64 {
	if elapsed >= cfg.RampDuration {
		return cfg.TargetRPS
	}

	progress := elapsed.Seconds() / cfg.RampDuration.Seconds()

	return cfg.StartRPS + (cfg.TargetRPS-cfg.StartRPS)*progress
}

func currentErrorPercent(cfg config, elapsed time.Duration) int {
	progress := float64(elapsed%cfg.ErrorCycle) / float64(cfg.ErrorCycle)
	if progress > 0.5 {
		progress = 1 - progress
	}

	value := float64(cfg.MinErrorPercent) + 2*progress*float64(cfg.MaxErrorPercent-cfg.MinErrorPercent)

	return int(value + 0.5)
}

type requestSpec struct {
	method       string
	path         string
	body         string
	expectsError bool
}

type targetItem struct {
	ID   string
	Name string
}

type errorPlanner struct {
	random     *rand.Rand
	percent    int
	jitter     int
	nextJitter time.Duration
}

func (p *errorPlanner) update(cfg config, elapsed time.Duration) {
	if elapsed >= p.nextJitter {
		amplitude := (cfg.MaxErrorPercent - cfg.MinErrorPercent) / 3
		p.jitter = p.random.IntN(2*amplitude+1) - amplitude
		p.nextJitter = elapsed + time.Duration(3+p.random.IntN(8))*time.Second
	}

	p.percent = max(cfg.MinErrorPercent, min(cfg.MaxErrorPercent, currentErrorPercent(cfg, elapsed)+p.jitter))
}

func (p *errorPlanner) next() bool {
	return p.random.IntN(100) < p.percent
}

func chooseRequest(sequence uint64, isError bool, target targetItem) requestSpec {
	itemPath := "/items/" + target.ID
	endpoint := sequence % 3
	endpointSequence := sequence / 3
	if isError {
		variant := endpointSequence % 3
		switch endpoint {
		case 0: // GET: 400 and 404.
			if variant%2 == 0 {
				return requestSpec{method: http.MethodGet, path: "/items/not-a-number", expectsError: true}
			}

			return requestSpec{method: http.MethodGet, path: "/items/999999999", expectsError: true}
		case 1: // POST: 400, 409 and 422.
			switch variant {
			case 0:
				return requestSpec{method: http.MethodPost, path: "/items", body: `{`, expectsError: true}
			case 1:
				return requestSpec{
					method: http.MethodPost, path: "/items",
					body: fmt.Sprintf(`{"name":%q,"quantity":1}`, target.Name), expectsError: true,
				}
			default:
				return requestSpec{method: http.MethodPost, path: "/items", body: `{"name":"","quantity":-1}`, expectsError: true}
			}
		default: // PUT: 400, 404 and 422.
			switch variant {
			case 0:
				return requestSpec{method: http.MethodPut, path: itemPath, body: `{`, expectsError: true}
			case 1:
				return requestSpec{method: http.MethodPut, path: "/items/999999999", body: `{"name":"missing-update","quantity":1}`, expectsError: true}
			default:
				return requestSpec{method: http.MethodPut, path: itemPath, body: `{"name":"","quantity":-1}`, expectsError: true}
			}
		}
	}

	switch endpoint {
	case 0:
		return requestSpec{method: http.MethodGet, path: itemPath}
	case 1:
		return requestSpec{
			method: http.MethodPost, path: "/items",
			body: fmt.Sprintf(`{"name":%q,"quantity":1}`, fmt.Sprintf("%s-created-%d", target.Name, sequence)),
		}
	default:
		return requestSpec{
			method: http.MethodPut, path: itemPath,
			body: fmt.Sprintf(`{"name":%q,"quantity":2}`, target.Name),
		}
	}
}

func bootstrap(ctx context.Context, client *http.Client, baseURL string) (targetItem, error) {
	target := targetItem{Name: fmt.Sprintf("load-bootstrap-%d", time.Now().UnixNano())}
	body := fmt.Sprintf(`{"name":%q,"quantity":1}`, target.Name)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/items", bytes.NewBufferString(body))
	if err != nil {
		return targetItem{}, err
	}

	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return targetItem{}, err
	}

	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		_, _ = io.Copy(io.Discard, response.Body)
		return targetItem{}, fmt.Errorf("POST /items returned %d, want %d", response.StatusCode, http.StatusCreated)
	}

	var created struct {
		ID string `json:"id"`
	}

	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		return targetItem{}, fmt.Errorf("decode bootstrap response: %w", err)
	}

	if created.ID == "" {
		return targetItem{}, errors.New("bootstrap response has empty item id")
	}

	target.ID = created.ID

	return target, nil
}

func send(ctx context.Context, client *http.Client, baseURL string, request requestSpec, stats *counters) {
	stats.sent.Add(1)
	status, err := execute(ctx, client, baseURL, request)
	if err != nil {
		stats.transportErrors.Add(1)
		return
	}

	if status >= http.StatusBadRequest {
		stats.expectedErrors.Add(1)
		return
	}

	stats.success.Add(1)
}

func execute(ctx context.Context, client *http.Client, baseURL string, spec requestSpec) (int, error) {
	request, err := http.NewRequestWithContext(ctx, spec.method, baseURL+spec.path, bytes.NewBufferString(spec.body))
	if err != nil {
		return 0, err
	}

	if spec.body != "" {
		request.Header.Set("Content-Type", "application/json")
	}

	response, err := client.Do(request)
	if err != nil {
		return 0, err
	}

	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)

	return response.StatusCode, nil
}

func envOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}

	return fallback
}

func floatEnv(name string, fallback float64) (float64, error) {
	raw := envOrDefault(name, strconv.FormatFloat(fallback, 'f', -1, 64))
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}

	return value, nil
}

func intEnv(name string, fallback int) (int, error) {
	raw := envOrDefault(name, strconv.Itoa(fallback))
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}

	return value, nil
}

func durationEnv(name string, fallback time.Duration) (time.Duration, error) {
	raw := envOrDefault(name, fallback.String())
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}

	return value, nil
}
