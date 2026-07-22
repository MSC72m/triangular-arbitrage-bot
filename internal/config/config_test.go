package config

import (
	"testing"
)

func TestDefaultConfigReturnsValid(t *testing.T) {
	cfg := DefaultConfig()
	if cfg == nil {
		t.Fatal("DefaultConfig returned nil")
	}
	if cfg.APIBaseURL != "https://api.coinex.com/v1" {
		t.Errorf("expected APIBaseURL 'https://api.coinex.com/v1', got '%s'", cfg.APIBaseURL)
	}
	if cfg.ProfitThreshold != 0.0001 {
		t.Errorf("expected ProfitThreshold 0.0001, got %f", cfg.ProfitThreshold)
	}
	if len(cfg.QuoteCurrencies) != 2 {
		t.Errorf("expected 2 QuoteCurrencies, got %d", len(cfg.QuoteCurrencies))
	}
}

func TestValidateMissingAPIKey(t *testing.T) {
	cfg := DefaultConfig()
	cfg.APIKey = ""
	cfg.SecretID = "validsecret12345"
	cfg.SimulationMode = true

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for missing API key")
	}
	if err.Error() != "apiKey is required" {
		t.Errorf("unexpected error message: %s", err.Error())
	}
}

func TestValidateMissingSecretID(t *testing.T) {
	cfg := DefaultConfig()
	cfg.APIKey = "validapikey12345"
	cfg.SecretID = ""
	cfg.SimulationMode = true

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for missing secret ID")
	}
	if err.Error() != "secretID is required" {
		t.Errorf("unexpected error message: %s", err.Error())
	}
}

func TestValidateZeroProfitThreshold(t *testing.T) {
	cfg := DefaultConfig()
	cfg.APIKey = "validapikey12345"
	cfg.SecretID = "validsecret12345"
	cfg.SimulationMode = true
	cfg.ProfitThreshold = 0

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for zero profit threshold")
	}
	if err.Error() != "profitThreshold must be positive" {
		t.Errorf("unexpected error message: %s", err.Error())
	}
}

func TestValidateNegativeProfitThreshold(t *testing.T) {
	cfg := DefaultConfig()
	cfg.APIKey = "validapikey12345"
	cfg.SecretID = "validsecret12345"
	cfg.SimulationMode = true
	cfg.ProfitThreshold = -0.01

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for negative profit threshold")
	}
}

func TestValidateEmptyQuoteCurrencies(t *testing.T) {
	cfg := DefaultConfig()
	cfg.APIKey = "validapikey12345"
	cfg.SecretID = "validsecret12345"
	cfg.SimulationMode = true
	cfg.QuoteCurrencies = []string{}

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for empty quote currencies")
	}
	if err.Error() != "at least one quote currency is required" {
		t.Errorf("unexpected error message: %s", err.Error())
	}
}

func TestValidateNegativeRateLimit(t *testing.T) {
	cfg := DefaultConfig()
	cfg.APIKey = "validapikey12345"
	cfg.SecretID = "validsecret12345"
	cfg.SimulationMode = true
	cfg.RateLimitPerSecond = -1

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for negative rate limit")
	}
	if err.Error() != "rateLimitPerSecond must be positive" {
		t.Errorf("unexpected error message: %s", err.Error())
	}
}

func TestValidateZeroRateLimit(t *testing.T) {
	cfg := DefaultConfig()
	cfg.APIKey = "validapikey12345"
	cfg.SecretID = "validsecret12345"
	cfg.SimulationMode = true
	cfg.RateLimitPerSecond = 0

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for zero rate limit")
	}
}

func TestDefaultValuesReasonable(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.ProfitThreshold <= 0 || cfg.ProfitThreshold > 1 {
		t.Errorf("ProfitThreshold %f not in reasonable range (0, 1)", cfg.ProfitThreshold)
	}
	if cfg.RateLimitPerSecond <= 0 || cfg.RateLimitPerSecond > 1000 {
		t.Errorf("RateLimitPerSecond %d not in reasonable range (0, 1000)", cfg.RateLimitPerSecond)
	}
	if cfg.RateLimitPerMinute <= 0 || cfg.RateLimitPerMinute > 100000 {
		t.Errorf("RateLimitPerMinute %d not in reasonable range (0, 100000)", cfg.RateLimitPerMinute)
	}
	if cfg.DefaultTradingFee < 0 || cfg.DefaultTradingFee > 0.1 {
		t.Errorf("DefaultTradingFee %f not in reasonable range [0, 0.1]", cfg.DefaultTradingFee)
	}
	if cfg.OrderBookDepthLimit <= 0 || cfg.OrderBookDepthLimit > 100 {
		t.Errorf("OrderBookDepthLimit %d not in reasonable range (0, 100]", cfg.OrderBookDepthLimit)
	}
	if cfg.OrderExecutionSettings.StaticOrderAmount <= 0 {
		t.Errorf("StaticOrderAmount %f should be positive", cfg.OrderExecutionSettings.StaticOrderAmount)
	}
	if cfg.OrderExecutionSettings.MaxOrderAmount <= 0 {
		t.Errorf("MaxOrderAmount %f should be positive", cfg.OrderExecutionSettings.MaxOrderAmount)
	}
	if cfg.FOKOrderSettings.PollingFrequencyHz <= 0 || cfg.FOKOrderSettings.PollingFrequencyHz > 100 {
		t.Errorf("FOK PollingFrequencyHz %f not in reasonable range (0, 100]", cfg.FOKOrderSettings.PollingFrequencyHz)
	}
}

func TestGetCriticalMarketsDefault(t *testing.T) {
	cfg := DefaultConfig()
	cfg.CriticalMarkets = []string{}
	markets := cfg.GetCriticalMarkets()
	if len(markets) != 1 || markets[0] != "USDCUSDT" {
		t.Errorf("expected fallback [USDCUSDT], got %v", markets)
	}
}

func TestGetCriticalMarketsConfigured(t *testing.T) {
	cfg := DefaultConfig()
	cfg.CriticalMarkets = []string{"USDCUSDT", "BTCUSDT"}
	markets := cfg.GetCriticalMarkets()
	if len(markets) != 2 {
		t.Errorf("expected 2 critical markets, got %d", len(markets))
	}
}

func TestLoadConfigMissingFile(t *testing.T) {
	cfg, err := LoadConfig("nonexistent_config.json")
	if err != nil {
		t.Fatalf("LoadConfig should return defaults for missing file, got error: %v", err)
	}
	if cfg == nil {
		t.Fatal("LoadConfig returned nil config for missing file")
	}
	if cfg.ProfitThreshold != 0.0001 {
		t.Errorf("expected default ProfitThreshold 0.0001, got %f", cfg.ProfitThreshold)
	}
}
