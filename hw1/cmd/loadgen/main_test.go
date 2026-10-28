package main

import (
	"math/rand/v2"
	"testing"
	"time"
)

func TestCurrentRateRampsAndThenStaysConstant(t *testing.T) {
	cfg := config{StartRPS: 2, TargetRPS: 10, RampDuration: 20 * time.Second}
	tests := []struct {
		elapsed time.Duration
		want    float64
	}{
		{elapsed: 0, want: 2},
		{elapsed: 10 * time.Second, want: 6},
		{elapsed: 20 * time.Second, want: 10},
		{elapsed: time.Minute, want: 10},
	}

	for _, test := range tests {
		if got := currentRate(cfg, test.elapsed); got != test.want {
			t.Errorf("currentRate(%s) = %v, want %v", test.elapsed, got, test.want)
		}
	}
}

func TestChooseRequestProducesConfiguredErrorShare(t *testing.T) {
	const errorPercent = 15
	target := targetItem{ID: "42", Name: "bootstrap-test"}
	planner := &errorPlanner{random: rand.New(rand.NewPCG(1, 2)), percent: errorPercent}
	errors := 0
	methods := make(map[string]bool)
	for sequence := uint64(0); sequence < 30000; sequence++ {
		request := chooseRequest(sequence, planner.next(), target)
		methods[request.method] = true
		if request.expectsError {
			errors++
		}
	}

	if errors < 4200 || errors > 4800 {
		t.Fatalf("error requests = %d, want about 4500 out of 30000", errors)
	}

	for _, method := range []string{"GET", "POST", "PUT"} {
		if !methods[method] {
			t.Errorf("request mix does not include %s", method)
		}
	}
}

func TestCurrentErrorPercentUsesTriangularCycle(t *testing.T) {
	cfg := config{MinErrorPercent: 5, MaxErrorPercent: 35, ErrorCycle: 2 * time.Minute}
	tests := []struct {
		elapsed time.Duration
		want    int
	}{
		{elapsed: 0, want: 5},
		{elapsed: 30 * time.Second, want: 20},
		{elapsed: time.Minute, want: 35},
		{elapsed: 90 * time.Second, want: 20},
		{elapsed: 2 * time.Minute, want: 5},
	}

	for _, test := range tests {
		if got := currentErrorPercent(cfg, test.elapsed); got != test.want {
			t.Errorf("currentErrorPercent(%s) = %d, want %d", test.elapsed, got, test.want)
		}
	}
}

func TestErrorPlannerKeepsJitterUntilNextUpdate(t *testing.T) {
	cfg := config{MinErrorPercent: 5, MaxErrorPercent: 35, ErrorCycle: 2 * time.Minute}
	planner := &errorPlanner{random: rand.New(rand.NewPCG(1, 2))}
	planner.update(cfg, 0)
	jitter, nextUpdate := planner.jitter, planner.nextJitter

	if nextUpdate < 3*time.Second || nextUpdate > 10*time.Second {
		t.Fatalf("next jitter update = %s, want 3s to 10s", nextUpdate)
	}

	planner.update(cfg, nextUpdate-time.Nanosecond)
	if planner.jitter != jitter || planner.nextJitter != nextUpdate {
		t.Fatal("jitter changed before its interval elapsed")
	}

	planner.update(cfg, nextUpdate)
	if planner.nextJitter <= nextUpdate {
		t.Fatal("jitter update did not schedule the next interval")
	}
}

func TestErrorPlannerVariesWithinConfiguredBounds(t *testing.T) {
	cfg := config{MinErrorPercent: 5, MaxErrorPercent: 35, ErrorCycle: 2 * time.Minute}
	planner := &errorPlanner{random: rand.New(rand.NewPCG(1, 2))}
	below, above := false, false
	durations := make(map[time.Duration]bool)

	for elapsed := time.Duration(0); elapsed < 10*time.Minute; elapsed += time.Second {
		previousUpdate := planner.nextJitter
		planner.update(cfg, elapsed)
		if planner.percent < cfg.MinErrorPercent || planner.percent > cfg.MaxErrorPercent {
			t.Fatalf("error percentage = %d outside configured bounds", planner.percent)
		}

		base := currentErrorPercent(cfg, elapsed)
		below = below || planner.percent < base
		above = above || planner.percent > base
		if planner.nextJitter != previousUpdate {
			durations[planner.nextJitter-elapsed] = true
		}
	}

	if !below || !above || len(durations) < 2 {
		t.Fatal("error profile lacks variation in offset or interval")
	}
}

func TestErrorPlannerHandlesZeroAndFullErrorRates(t *testing.T) {
	for _, percent := range []int{0, 100} {
		planner := &errorPlanner{random: rand.New(rand.NewPCG(1, 2)), percent: percent}
		for range 1000 {
			if got := planner.next(); got != (percent == 100) {
				t.Fatalf("error selected = %v with error percentage %d", got, percent)
			}
		}
	}
}
