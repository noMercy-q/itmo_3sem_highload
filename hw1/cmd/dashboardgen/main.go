package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"example.com/golden-signals-service/goldenmetrics"
	"github.com/grafana/grafana-foundation-sdk/go/common"
	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/prometheus"
	"github.com/grafana/grafana-foundation-sdk/go/timeseries"
	"github.com/grafana/grafana-foundation-sdk/go/units"
)

type config struct {
	AppURL          string
	GrafanaURL      string
	AdminUser       string
	AdminPassword   string
	ServiceAccount  string
	DashboardUID    string
	DatasourceUID   string
	TokenTTLSeconds int
}

func configFromEnv() config {
	return config{
		AppURL:          envOrDefault("APP_URL", "http://app:8080"),
		GrafanaURL:      strings.TrimRight(envOrDefault("GRAFANA_URL", "http://grafana:3000"), "/"),
		AdminUser:       envOrDefault("GRAFANA_ADMIN_USER", "admin"),
		AdminPassword:   envOrDefault("GRAFANA_ADMIN_PASSWORD", "admin"),
		ServiceAccount:  envOrDefault("GRAFANA_SERVICE_ACCOUNT", "dashboard-generator"),
		DashboardUID:    envOrDefault("GRAFANA_DASHBOARD_UID", "items-golden-signals"),
		DatasourceUID:   envOrDefault("GRAFANA_DATASOURCE_UID", "victoriametrics"),
		TokenTTLSeconds: 300,
	}
}

func envOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}

	return fallback
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if err := run(ctx, configFromEnv()); err != nil {
		slog.Error("generate Grafana dashboard", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg config) error {
	client := &http.Client{Timeout: 10 * time.Second}
	catalog, err := fetchCatalog(ctx, client, strings.TrimRight(cfg.AppURL, "/")+"/-/endpoints")
	if err != nil {
		return fmt.Errorf("discover application endpoints: %w", err)
	}

	if len(catalog.Endpoints) == 0 {
		return errors.New("application returned an empty endpoint catalog")
	}

	serviceAccountID, err := ensureServiceAccount(ctx, client, cfg)
	if err != nil {
		return fmt.Errorf("ensure Grafana service account: %w", err)
	}

	token, err := createServiceAccountToken(ctx, client, cfg, serviceAccountID)
	if err != nil {
		return fmt.Errorf("create Grafana service account token: %w", err)
	}

	defer func() {
		if err := deleteServiceAccountToken(context.Background(), client, cfg, serviceAccountID, token.ID); err != nil {
			slog.Warn("revoke temporary Grafana service account token", "error", err)
		}
	}()

	model, err := buildDashboard(catalog, cfg)
	if err != nil {
		return fmt.Errorf("build dashboard: %w", err)
	}

	if err := publishDashboard(ctx, client, cfg, token.Key, model); err != nil {
		return fmt.Errorf("publish dashboard: %w", err)
	}

	slog.Info("Grafana dashboard generated", "uid", cfg.DashboardUID, "endpoints", len(catalog.Endpoints))

	return nil
}

func fetchCatalog(ctx context.Context, client *http.Client, endpointURL string) (goldenmetrics.Catalog, error) {
	var catalog goldenmetrics.Catalog
	if err := doJSON(ctx, client, http.MethodGet, endpointURL, nil, noAuth, &catalog); err != nil {
		return goldenmetrics.Catalog{}, err
	}

	return catalog, nil
}

type serviceAccount struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

func ensureServiceAccount(ctx context.Context, client *http.Client, cfg config) (int64, error) {
	searchURL := cfg.GrafanaURL + "/api/serviceaccounts/search?query=" + url.QueryEscape(cfg.ServiceAccount)
	var result struct {
		ServiceAccounts []serviceAccount `json:"serviceAccounts"`
	}

	if err := doJSON(ctx, client, http.MethodGet, searchURL, nil, basicAuth(cfg), &result); err != nil {
		return 0, err
	}

	for _, account := range result.ServiceAccounts {
		if account.Name == cfg.ServiceAccount {
			body := map[string]any{"name": cfg.ServiceAccount, "role": "Editor"}
			endpointURL := fmt.Sprintf("%s/api/serviceaccounts/%d", cfg.GrafanaURL, account.ID)
			if err := doJSON(ctx, client, http.MethodPatch, endpointURL, body, basicAuth(cfg), nil); err != nil {
				return 0, err
			}

			return account.ID, nil
		}
	}

	body := map[string]any{"name": cfg.ServiceAccount, "role": "Editor", "isDisabled": false}
	var created serviceAccount
	if err := doJSON(ctx, client, http.MethodPost, cfg.GrafanaURL+"/api/serviceaccounts", body, basicAuth(cfg), &created); err != nil {
		return 0, err
	}

	return created.ID, nil
}

type serviceAccountToken struct {
	ID  int64  `json:"id"`
	Key string `json:"key"`
}

func createServiceAccountToken(ctx context.Context, client *http.Client, cfg config, accountID int64) (serviceAccountToken, error) {
	body := map[string]any{
		"name":          fmt.Sprintf("dashboard-generator-%d", time.Now().UnixNano()),
		"secondsToLive": cfg.TokenTTLSeconds,
	}

	endpointURL := fmt.Sprintf("%s/api/serviceaccounts/%d/tokens", cfg.GrafanaURL, accountID)
	var token serviceAccountToken
	if err := doJSON(ctx, client, http.MethodPost, endpointURL, body, basicAuth(cfg), &token); err != nil {
		return serviceAccountToken{}, err
	}

	if token.Key == "" {
		return serviceAccountToken{}, errors.New("Grafana returned an empty service account token")
	}

	return token, nil
}

func deleteServiceAccountToken(ctx context.Context, client *http.Client, cfg config, accountID, tokenID int64) error {
	endpointURL := fmt.Sprintf("%s/api/serviceaccounts/%d/tokens/%d", cfg.GrafanaURL, accountID, tokenID)

	return doJSON(ctx, client, http.MethodDelete, endpointURL, nil, basicAuth(cfg), nil)
}

func publishDashboard(ctx context.Context, client *http.Client, cfg config, token string, model dashboard.Dashboard) error {
	payload := struct {
		Dashboard dashboard.Dashboard `json:"dashboard"`
		Overwrite bool                `json:"overwrite"`
		Message   string              `json:"message"`
	}{
		Dashboard: model,
		Overwrite: true,
		Message:   "generated from runtime endpoint catalog",
	}

	return doJSON(ctx, client, http.MethodPost, cfg.GrafanaURL+"/api/dashboards/db", payload, bearerAuth(token), nil)
}

func buildDashboard(catalog goldenmetrics.Catalog, cfg config) (dashboard.Dashboard, error) {
	if strings.TrimSpace(catalog.MetricsNamespace) == "" {
		return dashboard.Dashboard{}, errors.New("metrics namespace is empty")
	}

	if strings.TrimSpace(catalog.Deployment) == "" {
		return dashboard.Dashboard{}, errors.New("deployment is empty")
	}

	datasourceType := "prometheus"
	datasourceUID := cfg.DatasourceUID
	datasource := common.DataSourceRef{Type: &datasourceType, Uid: &datasourceUID}
	requestsMetric := catalog.MetricsNamespace + "_http_requests_total"
	errorsMetric := catalog.MetricsNamespace + "_http_request_errors_total"
	latencyBuckets := catalog.MetricsNamespace + "_http_request_latency_seconds_bucket"
	endpointInfoMetric := catalog.MetricsNamespace + "_http_endpoint_info"
	deployment := strconv.Quote(catalog.Deployment)

	builder := dashboard.NewDashboardBuilder(catalog.Deployment+": REST metrics").
		Uid(cfg.DashboardUID).
		Description("Request rate, errors and latency for REST endpoints.").
		Tags([]string{"golden-signals", catalog.Deployment}).
		Readonly().
		Refresh("10s").
		Time("now-30m", "now").
		Timezone(string(common.TimeZoneBrowser))

	datacenterQuery := fmt.Sprintf("label_values(%s{deployment=%s}, datacenter)", endpointInfoMetric, deployment)
	podQuery := fmt.Sprintf("label_values(%s{deployment=%s,datacenter=~\"${datacenter:regex}\"}, pod_number)", endpointInfoMetric, deployment)
	builder.WithVariable(queryVariable("datacenter", "Datacenter", datacenterQuery, datasource))
	builder.WithVariable(queryVariable("pod_number", "Pod", podQuery, datasource))

	for _, endpoint := range catalog.Endpoints {
		endpointValue := strconv.Quote(endpoint.ID)
		selector := fmt.Sprintf(
			"deployment=%s,datacenter=~\"${datacenter:regex}\",pod_number=~\"${pod_number:regex}\",endpoint=%s",
			deployment,
			endpointValue,
		)
		rps := fmt.Sprintf("sum(rate(%s{%s}[$__rate_interval]))", requestsMetric, selector)
		errorRPS := fmt.Sprintf("sum by (status_code) (rate(%s{%s}[$__rate_interval]))", errorsMetric, selector)
		errorRatio := fmt.Sprintf(
			"100 * sum(rate(%s{%s}[$__rate_interval])) / clamp_min(sum(rate(%s{%s}[$__rate_interval])), 1e-9)",
			errorsMetric, selector, requestsMetric, selector,
		)

		builder.WithRow(dashboard.NewRowBuilder(fmt.Sprintf("%s %s (%s)", endpoint.Method, endpoint.Path, endpoint.ID)))
		builder.WithPanel(timeseries.NewPanelBuilder().
			Title("Throughput").
			Description("Requests per second for selected datacenters and pods.").
			Datasource(datasource).
			Unit(units.RequestsPerSecond).
			Min(0).
			Span(12).
			Height(8).
			WithTarget(promQuery("A", rps, "RPS", datasource)))
		builder.WithPanel(timeseries.NewPanelBuilder().
			Title("Errors by HTTP status").
			Description("Errors per second by HTTP status code.").
			Datasource(datasource).
			Unit(units.RequestsPerSecond).
			Min(0).
			Span(12).
			Height(8).
			WithTarget(promQuery("A", errorRPS, "HTTP {{status_code}}", datasource)))
		builder.WithPanel(timeseries.NewPanelBuilder().
			Title("Error ratio").
			Description("HTTP 4xx and 5xx responses as a percentage of all requests.").
			Datasource(datasource).
			Unit(units.Percent).
			Min(0).
			Max(100).
			Span(12).
			Height(8).
			WithTarget(promQuery("A", errorRatio, "Error ratio", datasource)))

		latencyPanel := timeseries.NewPanelBuilder().
			Title("Latency percentiles").
			Description("p50, p75, p90, p95 and p99 across selected datacenters and pods.").
			Datasource(datasource).
			Unit(units.Seconds).
			Min(0).
			Span(12).
			Height(8)
		quantiles := []struct {
			value  string
			legend string
		}{
			{value: "0.50", legend: "p50"},
			{value: "0.75", legend: "p75"},
			{value: "0.90", legend: "p90"},
			{value: "0.95", legend: "p95"},
			{value: "0.99", legend: "p99"},
		}

		for index, quantile := range quantiles {
			expression := fmt.Sprintf(
				"histogram_quantile(%s, sum by (le) (rate(%s{%s}[$__rate_interval])))",
				quantile.value, latencyBuckets, selector,
			)
			latencyPanel.WithTarget(promQuery(string(rune('A'+index)), expression, quantile.legend, datasource))
		}

		builder.WithPanel(latencyPanel)
	}

	return builder.Build()
}

func queryVariable(name, label, query string, datasource common.DataSourceRef) *dashboard.QueryVariableBuilder {
	allText := "All"
	allValue := "$__all"
	selected := true

	return dashboard.NewQueryVariableBuilder(name).
		Label(label).
		Description("Filter by " + label + ".").
		Datasource(datasource).
		Query(dashboard.StringOrMap{String: &query}).
		Definition(query).
		Refresh(dashboard.VariableRefreshOnDashboardLoad).
		Sort(dashboard.VariableSortAlphabeticalAsc).
		Multi(true).
		IncludeAll(true).
		AllValue(".*").
		Current(dashboard.VariableOption{
			Selected: &selected,
			Text:     dashboard.StringOrArrayOfString{String: &allText},
			Value:    dashboard.StringOrArrayOfString{String: &allValue},
		})
}

func promQuery(refID, expression, legend string, datasource common.DataSourceRef) *prometheus.DataqueryBuilder {
	return prometheus.NewDataqueryBuilder().
		RefId(refID).
		Datasource(datasource).
		EditorMode(prometheus.QueryEditorModeCode).
		Format(prometheus.PromQueryFormatTimeSeries).
		Range().
		Expr(expression).
		LegendFormat(legend)
}

type requestAuth func(*http.Request)

func noAuth(*http.Request) {}

func basicAuth(cfg config) requestAuth {
	return func(request *http.Request) {
		request.SetBasicAuth(cfg.AdminUser, cfg.AdminPassword)
	}
}

func bearerAuth(token string) requestAuth {
	return func(request *http.Request) {
		request.Header.Set("Authorization", "Bearer "+token)
	}
}

func doJSON(
	ctx context.Context,
	client *http.Client,
	method string,
	endpointURL string,
	requestBody any,
	auth requestAuth,
	responseBody any,
) error {
	var body io.Reader
	if requestBody != nil {
		encoded, err := json.Marshal(requestBody)
		if err != nil {
			return fmt.Errorf("encode request: %w", err)
		}

		body = bytes.NewReader(encoded)
	}

	request, err := http.NewRequestWithContext(ctx, method, endpointURL, body)
	if err != nil {
		return err
	}

	request.Header.Set("Accept", "application/json")
	if requestBody != nil {
		request.Header.Set("Content-Type", "application/json")
	}

	auth(request)

	response, err := client.Do(request)
	if err != nil {
		return err
	}

	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4<<10))
		return fmt.Errorf("%s %s returned %s: %s", method, endpointURL, response.Status, strings.TrimSpace(string(message)))
	}

	if responseBody == nil {
		_, _ = io.Copy(io.Discard, response.Body)
		return nil
	}

	if err := json.NewDecoder(response.Body).Decode(responseBody); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	return nil
}
