package sampling

import "testing"

func TestAdaptiveControllerStartsAtMinRateWhenEnabled(t *testing.T) {
	cfg := AdaptiveConfig{
		Enabled:   true,
		TargetTPS: 100,
		MinRate:   0.05,
		MaxRate:   1.0,
		EWMAAlpha: 0.3,
	}

	ctrl := NewAdaptiveController(cfg)

	rate, _ := ctrl.Stats()
	if rate != cfg.MinRate {
		t.Fatalf("initial rate = %v, want %v", rate, cfg.MinRate)
	}
}

func TestAdaptiveControllerStartsAtMaxRateWhenDisabled(t *testing.T) {
	cfg := AdaptiveConfig{
		Enabled: false,
		MinRate: 0.05,
		MaxRate: 0.8,
	}

	ctrl := NewAdaptiveController(cfg)

	rate, _ := ctrl.Stats()
	if rate != cfg.MaxRate {
		t.Fatalf("initial rate = %v, want %v", rate, cfg.MaxRate)
	}
}

func TestAdaptiveControllerUpdateConfigEnablingStartsAtMinRate(t *testing.T) {
	ctrl := NewAdaptiveController(AdaptiveConfig{
		Enabled: false,
		MinRate: 0.01,
		MaxRate: 1.0,
	})

	enabledCfg := AdaptiveConfig{
		Enabled:   true,
		TargetTPS: 100,
		MinRate:   0.02,
		MaxRate:   1.0,
		EWMAAlpha: 0.3,
	}
	ctrl.UpdateConfig(enabledCfg)

	rate, tps := ctrl.Stats()
	if rate != enabledCfg.MinRate {
		t.Fatalf("rate after enabling = %v, want %v", rate, enabledCfg.MinRate)
	}
	if tps != 0 {
		t.Fatalf("tps after enabling = %v, want 0", tps)
	}
}
