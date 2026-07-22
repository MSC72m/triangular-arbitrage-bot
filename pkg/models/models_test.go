package models

import (
	"encoding/json"
	"sync"
	"testing"
	"time"
)

func TestNewMarketDepths(t *testing.T) {
	md := NewMarketDepths()
	if md == nil {
		t.Fatal("NewMarketDepths returned nil")
	}
	if md.depths == nil {
		t.Fatal("depths map is nil")
	}
}

func TestMarketDepthsStoreAndLoad(t *testing.T) {
	md := NewMarketDepths()

	ob := &OrderBook{
		Asks: []Depth{{Price: "0.1", Amount: "100"}},
		Bids: []Depth{{Price: "0.09", Amount: "100"}},
	}

	md.Store("BTCUSDT", ob)

	loaded, ok := md.Load("BTCUSDT")
	if !ok {
		t.Fatal("Load returned false for stored market")
	}
	if loaded.Market != "BTCUSDT" {
		t.Errorf("expected market 'BTCUSDT', got '%s'", loaded.Market)
	}
	if len(loaded.Asks) != 1 || loaded.Asks[0].Price != "0.1" {
		t.Errorf("ask data mismatch: %+v", loaded.Asks)
	}
	if len(loaded.Bids) != 1 || loaded.Bids[0].Price != "0.09" {
		t.Errorf("bid data mismatch: %+v", loaded.Bids)
	}
}

func TestMarketDepthsLoadNotFound(t *testing.T) {
	md := NewMarketDepths()
	_, ok := md.Load("NONEXISTENT")
	if ok {
		t.Fatal("Load should return false for nonexistent market")
	}
}

func TestMarketDepthsGetAvailableMarkets(t *testing.T) {
	md := NewMarketDepths()

	md.Store("BTCUSDT", &OrderBook{})
	md.Store("ETHUSDT", &OrderBook{})
	md.Store("SOLUSDC", &OrderBook{})

	markets := md.GetAvailableMarkets()
	if len(markets) != 3 {
		t.Errorf("expected 3 markets, got %d", len(markets))
	}

	marketSet := make(map[string]bool)
	for _, m := range markets {
		marketSet[m] = true
	}
	for _, expected := range []string{"BTCUSDT", "ETHUSDT", "SOLUSDC"} {
		if !marketSet[expected] {
			t.Errorf("expected market '%s' not found", expected)
		}
	}
}

func TestMarketDepthsGetAvailableMarketsEmpty(t *testing.T) {
	md := NewMarketDepths()
	markets := md.GetAvailableMarkets()
	if len(markets) != 0 {
		t.Errorf("expected 0 markets, got %d", len(markets))
	}
}

func TestMarketDepthsGetSnapshot(t *testing.T) {
	md := NewMarketDepths()

	ob1 := &OrderBook{
		Asks:   []Depth{{Price: "100", Amount: "1"}},
		Bids:   []Depth{{Price: "99", Amount: "1"}},
		Market: "BTCUSDT",
	}
	ob2 := &OrderBook{
		Asks:   []Depth{{Price: "2000", Amount: "10"}},
		Bids:   []Depth{{Price: "1999", Amount: "10"}},
		Market: "ETHUSDT",
	}

	md.Store("BTCUSDT", ob1)
	md.Store("ETHUSDT", ob2)

	snapshot := md.GetSnapshot()
	if len(snapshot) != 2 {
		t.Errorf("expected 2 markets in snapshot, got %d", len(snapshot))
	}

	// Verify snapshot entries are independent structs (shallow copy)
	// Modifying the snapshot map itself should not affect original
	snapshot["SOLUSDT"] = &OrderBook{Market: "SOLUSDT"}
	_, exists := md.Load("SOLUSDT")
	if exists {
		t.Error("adding to snapshot affected original depths map")
	}

	// Verify snapshot has correct data
	if snapshot["BTCUSDT"].Market != "BTCUSDT" {
		t.Errorf("expected market 'BTCUSDT' in snapshot, got '%s'", snapshot["BTCUSDT"].Market)
	}
}

func TestNewMetrics(t *testing.T) {
	before := time.Now()
	m := NewMetrics()
	after := time.Now()

	if m == nil {
		t.Fatal("NewMetrics returned nil")
	}
	if m.StartTime.Before(before) || m.StartTime.After(after) {
		t.Errorf("StartTime %v not between %v and %v", m.StartTime, before, after)
	}
	if m.CurrentBalance == nil {
		t.Fatal("CurrentBalance map is nil")
	}
}

func TestMetricsIncrementOpportunities(t *testing.T) {
	m := NewMetrics()
	m.IncrementOpportunities()
	m.IncrementOpportunities()
	m.IncrementOpportunities()

	if m.OpportunitiesDetected != 3 {
		t.Errorf("expected 3 opportunities detected, got %d", m.OpportunitiesDetected)
	}
}

func TestMetricsIncrementTrades(t *testing.T) {
	m := NewMetrics()
	m.IncrementTrades()
	m.IncrementTrades()

	if m.TradesExecuted != 2 {
		t.Errorf("expected 2 trades executed, got %d", m.TradesExecuted)
	}
}

func TestMetricsIncrementSuccessfulTrades(t *testing.T) {
	m := NewMetrics()
	m.IncrementSuccessfulTrades()

	if m.TradesSuccessful != 1 {
		t.Errorf("expected 1 successful trade, got %d", m.TradesSuccessful)
	}
}

func TestMetricsIncrementFailedTrades(t *testing.T) {
	m := NewMetrics()
	m.IncrementFailedTrades()
	m.IncrementFailedTrades()

	if m.TradesFailed != 2 {
		t.Errorf("expected 2 failed trades, got %d", m.TradesFailed)
	}
}

func TestMetricsAddFees(t *testing.T) {
	m := NewMetrics()
	m.AddFees(0.5)
	m.AddFees(1.5)

	if m.TotalFees != 2.0 {
		t.Errorf("expected total fees 2.0, got %f", m.TotalFees)
	}
}

func TestMetricsAddPnL(t *testing.T) {
	m := NewMetrics()
	m.AddPnL(10.5)
	m.AddPnL(-2.3)

	if m.TotalPnL != 8.2 {
		t.Errorf("expected total PnL 8.2, got %f", m.TotalPnL)
	}
}

func TestMetricsUpdateBalance(t *testing.T) {
	m := NewMetrics()
	m.UpdateBalance("USDT", 100.5)
	m.UpdateBalance("USDC", 50.25)

	if m.CurrentBalance["USDT"] != 100.5 {
		t.Errorf("expected USDT balance 100.5, got %f", m.CurrentBalance["USDT"])
	}
	if m.CurrentBalance["USDC"] != 50.25 {
		t.Errorf("expected USDC balance 50.25, got %f", m.CurrentBalance["USDC"])
	}
}

func TestMetricsConcurrentSafety(t *testing.T) {
	m := NewMetrics()
	const goroutines = 100
	const iterations = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				m.IncrementOpportunities()
				m.IncrementTrades()
				m.IncrementSuccessfulTrades()
				m.IncrementFailedTrades()
				m.AddFees(0.01)
				m.AddPnL(0.01)
				m.IncrementMessagesProcessed()
				m.IncrementErrorCount()
			}
		}()
	}

	wg.Wait()

	expectedOpportunities := int64(goroutines * iterations)
	if m.OpportunitiesDetected != expectedOpportunities {
		t.Errorf("expected %d opportunities, got %d", expectedOpportunities, m.OpportunitiesDetected)
	}
	if m.TradesExecuted != expectedOpportunities {
		t.Errorf("expected %d trades executed, got %d", expectedOpportunities, m.TradesExecuted)
	}
	if m.TradesSuccessful != expectedOpportunities {
		t.Errorf("expected %d successful trades, got %d", expectedOpportunities, m.TradesSuccessful)
	}
	if m.TradesFailed != expectedOpportunities {
		t.Errorf("expected %d failed trades, got %d", expectedOpportunities, m.TradesFailed)
	}
}

func TestMetricsGetSnapshot(t *testing.T) {
	m := NewMetrics()
	m.IncrementTrades()
	m.UpdateBalance("USDT", 100)

	snapshot := m.GetSnapshot()
	if snapshot.TradesExecuted != 1 {
		t.Errorf("expected snapshot TradesExecuted 1, got %d", snapshot.TradesExecuted)
	}
	if snapshot.CurrentBalance["USDT"] != 100 {
		t.Errorf("expected snapshot balance 100, got %f", snapshot.CurrentBalance["USDT"])
	}

	// Verify snapshot is independent
	snapshot.IncrementTrades()
	if m.TradesExecuted != 1 {
		t.Error("snapshot modification affected original metrics")
	}
}

func TestNewRateLimiter(t *testing.T) {
	rl := NewRateLimiter(10, 5)
	if rl == nil {
		t.Fatal("NewRateLimiter returned nil")
	}
	if rl.maxTokens != 10 {
		t.Errorf("expected maxTokens 10, got %d", rl.maxTokens)
	}
	if rl.refillRate != 5 {
		t.Errorf("expected refillRate 5, got %d", rl.refillRate)
	}
	if rl.tokens != 10 {
		t.Errorf("expected initial tokens 10, got %d", rl.tokens)
	}
}

func TestRateLimiterAllow(t *testing.T) {
	rl := NewRateLimiter(3, 1)

	// Should allow 3 requests (full tokens)
	if !rl.Allow() {
		t.Fatal("expected Allow() to return true (token 1)")
	}
	if !rl.Allow() {
		t.Fatal("expected Allow() to return true (token 2)")
	}
	if !rl.Allow() {
		t.Fatal("expected Allow() to return true (token 3)")
	}

	// Should deny 4th request (no tokens left)
	if rl.Allow() {
		t.Fatal("expected Allow() to return false (no tokens)")
	}
}

func TestRateLimiterDisableEnable(t *testing.T) {
	rl := NewRateLimiter(1, 1)

	// Use the single token
	if !rl.Allow() {
		t.Fatal("expected Allow() to return true")
	}

	// Should deny
	if rl.Allow() {
		t.Fatal("expected Allow() to return false")
	}

	// Disable rate limiting
	rl.Disable()
	if !rl.Allow() {
		t.Fatal("expected Allow() to return true when disabled")
	}
	if !rl.Allow() {
		t.Fatal("expected Allow() to return true when disabled (still)")
	}

	// Re-enable rate limiting
	rl.Enable()
	if rl.Allow() {
		t.Fatal("expected Allow() to return false after re-enabling")
	}
}

func TestDepthUnmarshalJSON(t *testing.T) {
	input := `["0.12345", "100.5"]`
	var d Depth

	err := json.Unmarshal([]byte(input), &d)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d.Price != "0.12345" {
		t.Errorf("expected price '0.12345', got '%s'", d.Price)
	}
	if d.Amount != "100.5" {
		t.Errorf("expected amount '100.5', got '%s'", d.Amount)
	}
}

func TestDepthUnmarshalJSONInvalidLength(t *testing.T) {
	input := `["0.12345"]`
	var d Depth

	err := json.Unmarshal([]byte(input), &d)
	if err == nil {
		t.Fatal("expected error for invalid array length")
	}
}

func TestDepthUnmarshalJSONTooManyElements(t *testing.T) {
	input := `["0.12345", "100", "extra"]`
	var d Depth

	err := json.Unmarshal([]byte(input), &d)
	if err == nil {
		t.Fatal("expected error for too many array elements")
	}
}

func TestDepthUnmarshalJSONInvalidJSON(t *testing.T) {
	input := `not json`
	var d Depth

	err := json.Unmarshal([]byte(input), &d)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestMin(t *testing.T) {
	if Min(3, 5) != 3 {
		t.Errorf("Min(3, 5) should be 3")
	}
	if Min(5, 3) != 3 {
		t.Errorf("Min(5, 3) should be 3")
	}
	if Min(4, 4) != 4 {
		t.Errorf("Min(4, 4) should be 4")
	}
	if Min(-1, 1) != -1 {
		t.Errorf("Min(-1, 1) should be -1")
	}
}

func TestMarketDepthsStoreOverwrite(t *testing.T) {
	md := NewMarketDepths()

	ob1 := &OrderBook{Asks: []Depth{{Price: "100", Amount: "1"}}}
	ob2 := &OrderBook{Asks: []Depth{{Price: "200", Amount: "2"}}}

	md.Store("BTCUSDT", ob1)
	md.Store("BTCUSDT", ob2)

	loaded, ok := md.Load("BTCUSDT")
	if !ok {
		t.Fatal("Load returned false")
	}
	if loaded.Asks[0].Price != "200" {
		t.Errorf("expected overwritten price '200', got '%s'", loaded.Asks[0].Price)
	}
}

func TestMetricsSetActiveConnections(t *testing.T) {
	m := NewMetrics()
	m.SetActiveConnections(5)

	if m.ActiveConnections != 5 {
		t.Errorf("expected 5 active connections, got %d", m.ActiveConnections)
	}
}

func TestMetricsUpdateDetectionTime(t *testing.T) {
	m := NewMetrics()
	m.UpdateDetectionTime(100 * time.Millisecond)

	if m.AvgDetectionTime != 100.0 {
		t.Errorf("expected avg detection time 100ms, got %f", m.AvgDetectionTime)
	}

	m.UpdateDetectionTime(200 * time.Millisecond)
	expected := (100.0 + 200.0) / 2
	if m.AvgDetectionTime != expected {
		t.Errorf("expected avg detection time %fms, got %f", expected, m.AvgDetectionTime)
	}
}

func TestMetricsUpdateExecutionTime(t *testing.T) {
	m := NewMetrics()
	m.UpdateExecutionTime(500 * time.Millisecond)

	if m.AvgExecutionTime != 500.0 {
		t.Errorf("expected avg execution time 500ms, got %f", m.AvgExecutionTime)
	}
}

func TestRateLimiterConcurrent(t *testing.T) {
	rl := NewRateLimiter(100, 10)
	var wg sync.WaitGroup
	const goroutines = 50

	wg.Add(goroutines)
	allowed := make([]bool, goroutines)

	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			allowed[idx] = rl.Allow()
		}(i)
	}

	wg.Wait()

	trueCount := 0
	for _, a := range allowed {
		if a {
			trueCount++
		}
	}
	// With 100 initial tokens and 50 goroutines, all should be allowed
	if trueCount != goroutines {
		t.Errorf("expected %d allowed, got %d", goroutines, trueCount)
	}
}
