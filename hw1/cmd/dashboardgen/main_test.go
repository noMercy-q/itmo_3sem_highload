package main

import (
	"encoding/json"
	"strings"
	"testing"

	"example.com/golden-signals-service/goldenmetrics"
)

func TestBuildDashboardUsesRuntimeEndpointsAndAggregatedHistograms(t *testing.T) {
	catalog := goldenmetrics.Catalog{
		Deployment:       "items-service",
		MetricsNamespace: "demo",
		Endpoints: []goldenmetrics.Endpoint{
			{ID: "get_item", Method: "GET", Path: "/items/{id}"},
			{ID: "create_item", Method: "POST", Path: "/items"},
		},
	}

	model, err := buildDashboard(catalog, config{DashboardUID: "items-golden-signals", DatasourceUID: "victoriametrics"})
	if err != nil {
		t.Fatalf("buildDashboard() error: %v", err)
	}

	raw, err := json.Marshal(model)
	if err != nil {
		t.Fatalf("marshal dashboard: %v", err)
	}

	generated := string(raw)
	for _, required := range []string{
		`"editable":false`,
		`GET /items/{id} (get_item)`,
		`POST /items (create_item)`,
		`sum(rate(demo_http_requests_total`,
		`sum by (status_code)`,
		`histogram_quantile(0.99, sum by (le)`,
		`${datacenter:regex}`,
		`${pod_number:regex}`,
	} {
		if !strings.Contains(generated, required) {
			t.Errorf("generated dashboard does not contain %q", required)
		}
	}
}
