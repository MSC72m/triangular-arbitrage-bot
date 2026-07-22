package network

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"triangular-arbitrage-bot/internal/config"
	"triangular-arbitrage-bot/pkg/common"
)

func createTestConfig() *config.Config {
	cfg := config.DefaultConfig()
	cfg.APIKey = "testapikey12345678"
	cfg.SecretID = "testsecret12345678"
	cfg.SimulationMode = true
	return cfg
}

func TestGetQueryStringNoParams(t *testing.T) {
	cfg := createTestConfig()
	c := NewHttpClient(cfg)

	result := c.getQueryString("https://api.coinex.com/v1/market/list", map[string]string{})
	if result != "https://api.coinex.com/v1/market/list" {
		t.Errorf("expected URL without query, got '%s'", result)
	}
}

func TestGetQueryStringWithParams(t *testing.T) {
	cfg := createTestConfig()
	c := NewHttpClient(cfg)

	params := map[string]string{
		"url":    "https://api.coinex.com/v1/market/depth",
		"method": "GET",
		"market": "BTCUSDT",
		"limit":  "20",
	}

	result := c.getQueryString("https://api.coinex.com/v1/market/depth", params)
	// Should filter out "url" and "method", keep "market" and "limit"
	// Keys sorted alphabetically: limit, market
	expected := "https://api.coinex.com/v1/market/depth?limit=20&market=BTCUSDT"
	if result != expected {
		t.Errorf("expected '%s', got '%s'", expected, result)
	}
}

func TestGetQueryStringFiltersReservedKeys(t *testing.T) {
	cfg := createTestConfig()
	c := NewHttpClient(cfg)

	params := map[string]string{
		"url":    "https://example.com",
		"method": "POST",
		"body":   `{"key":"value"}`,
		"query":  "existing=query",
		"market": "ETHUSDT",
	}

	result := c.getQueryString("https://example.com", params)
	expected := "https://example.com?market=ETHUSDT"
	if result != expected {
		t.Errorf("expected '%s', got '%s'", expected, result)
	}
}

func TestGetQueryStringPreservesExistingQuery(t *testing.T) {
	cfg := createTestConfig()
	c := NewHttpClient(cfg)

	params := map[string]string{
		"market": "BTCUSDT",
	}

	result := c.getQueryString("https://example.com?existing=param", params)
	expected := "https://example.com?existing=param&market=BTCUSDT"
	if result != expected {
		t.Errorf("expected '%s', got '%s'", expected, result)
	}
}

func TestGetQueryStringSpecialCharacters(t *testing.T) {
	cfg := createTestConfig()
	c := NewHttpClient(cfg)

	params := map[string]string{
		"url": "https://example.com",
		"q":   "hello world",
	}

	result := c.getQueryString("https://example.com", params)
	expected := "https://example.com?q=hello+world"
	if result != expected {
		t.Errorf("expected '%s', got '%s'", expected, result)
	}
}

func TestGetMapKeys(t *testing.T) {
	m := map[string]interface{}{
		"alpha": 1,
		"beta":  2,
		"gamma": 3,
	}

	keys := common.GetMapKeys(m)
	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(keys))
	}

	keySet := make(map[string]bool)
	for _, k := range keys {
		keySet[k] = true
	}
	for _, expected := range []string{"alpha", "beta", "gamma"} {
		if !keySet[expected] {
			t.Errorf("expected key '%s' not found", expected)
		}
	}
}

func TestGetMapKeysEmpty(t *testing.T) {
	m := map[string]interface{}{}
	keys := common.GetMapKeys(m)
	if len(keys) != 0 {
		t.Errorf("expected 0 keys, got %d", len(keys))
	}
}

func TestGetMapKeysSingleElement(t *testing.T) {
	m := map[string]interface{}{
		"only": "value",
	}
	keys := common.GetMapKeys(m)
	if len(keys) != 1 || keys[0] != "only" {
		t.Errorf("expected [only], got %v", keys)
	}
}

func TestHMACSHA256Signature(t *testing.T) {
	// Test that HMAC-SHA256 produces expected output for known inputs
	secret := "my-secret-key"
	message := "GET/v2/spot/order-statusmarket=BTCUSDT&order_id=123451700490703564"

	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(message))
	signature := hex.EncodeToString(h.Sum(nil))

	// Verify it's a valid hex string of proper length (SHA256 = 32 bytes = 64 hex chars)
	if len(signature) != 64 {
		t.Errorf("expected signature length 64, got %d", len(signature))
	}

	// Verify determinism - same inputs produce same output
	h2 := hmac.New(sha256.New, []byte(secret))
	h2.Write([]byte(message))
	signature2 := hex.EncodeToString(h2.Sum(nil))

	if signature != signature2 {
		t.Errorf("HMAC-SHA256 not deterministic: '%s' != '%s'", signature, signature2)
	}
}

func TestHMACSHA256DifferentSecrets(t *testing.T) {
	message := "test message"

	h1 := hmac.New(sha256.New, []byte("secret1"))
	h1.Write([]byte(message))
	sig1 := hex.EncodeToString(h1.Sum(nil))

	h2 := hmac.New(sha256.New, []byte("secret2"))
	h2.Write([]byte(message))
	sig2 := hex.EncodeToString(h2.Sum(nil))

	if sig1 == sig2 {
		t.Error("different secrets should produce different signatures")
	}
}

func TestHMACSHA256DifferentMessages(t *testing.T) {
	secret := "same-secret"

	h1 := hmac.New(sha256.New, []byte(secret))
	h1.Write([]byte("message1"))
	sig1 := hex.EncodeToString(h1.Sum(nil))

	h2 := hmac.New(sha256.New, []byte(secret))
	h2.Write([]byte("message2"))
	sig2 := hex.EncodeToString(h2.Sum(nil))

	if sig1 == sig2 {
		t.Error("different messages should produce different signatures")
	}
}

func TestNewHttpClient(t *testing.T) {
	cfg := createTestConfig()
	c := NewHttpClient(cfg)

	if c == nil {
		t.Fatal("NewHttpClient returned nil")
	}
	if c.config != cfg {
		t.Error("config not set correctly")
	}
	if c.headers == nil {
		t.Error("headers map is nil")
	}
	if c.wsSubscriptions == nil {
		t.Error("wsSubscriptions map is nil")
	}
	if c.marketData == nil {
		t.Error("marketData map is nil")
	}
	if c.rateLimiter == nil {
		t.Error("rateLimiter is nil")
	}
}

func TestHttpClientSetHeaders(t *testing.T) {
	cfg := createTestConfig()
	c := NewHttpClient(cfg)

	headers := map[string]string{
		"Content-Type": "application/json",
		"User-Agent":   "TestBot/1.0",
	}

	c.SetHeaders(headers)

	gotHeaders := c.GetHeaders()
	if gotHeaders.Get("Content-Type") != "application/json" {
		t.Errorf("expected Content-Type 'application/json', got '%s'", gotHeaders.Get("Content-Type"))
	}
	if gotHeaders.Get("User-Agent") != "TestBot/1.0" {
		t.Errorf("expected User-Agent 'TestBot/1.0', got '%s'", gotHeaders.Get("User-Agent"))
	}
}

func TestHttpClientMergeHeaders(t *testing.T) {
	cfg := createTestConfig()
	c := NewHttpClient(cfg)

	c.SetHeaders(map[string]string{
		"Content-Type": "application/json",
	})

	c.MergeHeaders(map[string]string{
		"X-Custom": "value",
		"Accept":   "text/plain",
	})

	gotHeaders := c.GetHeaders()
	if gotHeaders.Get("Content-Type") != "application/json" {
		t.Error("original header lost after merge")
	}
	if gotHeaders.Get("X-Custom") != "value" {
		t.Error("merged header not set")
	}
	if gotHeaders.Get("Accept") != "text/plain" {
		t.Error("merged header not set")
	}
}

func TestHttpClientSetWebSocketUrl(t *testing.T) {
	cfg := createTestConfig()
	c := NewHttpClient(cfg)

	c.SetWebSocketUrl("socket.coinex.com")

	if c.wsUrl != "wss://socket.coinex.com/" {
		t.Errorf("expected wsUrl 'wss://socket.coinex.com/', got '%s'", c.wsUrl)
	}
}

func TestHttpClientIsWebSocketConnected(t *testing.T) {
	cfg := createTestConfig()
	c := NewHttpClient(cfg)

	// Initially not connected
	if c.IsWebSocketConnected() {
		t.Error("should not be connected initially")
	}
}

func TestGetQueryStringSortedKeys(t *testing.T) {
	cfg := createTestConfig()
	c := NewHttpClient(cfg)

	params := map[string]string{
		"z_param": "z",
		"a_param": "a",
		"m_param": "m",
	}

	result := c.getQueryString("https://example.com", params)
	expected := "https://example.com?a_param=a&m_param=m&z_param=z"
	if result != expected {
		t.Errorf("expected '%s', got '%s'", expected, result)
	}
}
