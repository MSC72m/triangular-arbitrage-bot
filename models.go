package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// Depth represents a single price level in the order book.
type Depth struct {
	Price  string `json:"price"`
	Amount string `json:"amount"`
}

// UnmarshalJSON custom unmarshaler for Depth to handle array format [price, amount]
func (d *Depth) UnmarshalJSON(data []byte) error {
	var v []string
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	if len(v) != 2 {
		return fmt.Errorf("invalid Depth array length: %d", len(v))
	}
	d.Price = v[0]
	d.Amount = v[1]
	return nil
}

// OrderBook represents the full order book for a market.
type OrderBook struct {
	Asks      []Depth   `json:"asks"`
	Bids      []Depth   `json:"bids"`
	Timestamp time.Time `json:"timestamp"`
	Market    string    `json:"market"`
}

// TriangularPath represents a triangular arbitrage path
type TriangularPath struct {
	BaseAsset  string `json:"baseAsset"`  // e.g., "USDT"
	Asset1     string `json:"asset1"`     // e.g., "HBAR"
	Asset2     string `json:"asset2"`     // e.g., "USDC"
	Market1    string `json:"market1"`    // e.g., "HBARUSDT"
	Market2    string `json:"market2"`    // e.g., "HBARUSDC"
	Market3    string `json:"market3"`    // e.g., "USDCUSDT"
	Direction1 string `json:"direction1"` // "buy" or "sell"
	Direction2 string `json:"direction2"` // "buy" or "sell"
	Direction3 string `json:"direction3"` // "buy" or "sell"
}

// ArbitrageOpportunity represents a profitable triangular arbitrage opportunity
type ArbitrageOpportunity struct {
	Path            TriangularPath `json:"path"`
	EstimatedProfit float64        `json:"estimatedProfit"`
	NetProfit       float64        `json:"netProfit"` // After fees
	Volume          float64        `json:"volume"`
	Timestamp       time.Time      `json:"timestamp"`
	DetectionTime   time.Duration  `json:"detectionTime"`

	// Market data at detection time
	Market1Data OrderBook `json:"market1Data"`
	Market2Data OrderBook `json:"market2Data"`
	Market3Data OrderBook `json:"market3Data"`

	// Prices used for calculation
	Price1 float64 `json:"price1"`
	Price2 float64 `json:"price2"`
	Price3 float64 `json:"price3"`
}

// TradeOrder represents an individual trade order
type TradeOrder struct {
	ID        string    `json:"id"`
	Market    string    `json:"market"`
	Side      string    `json:"side"` // "buy" or "sell"
	Type      string    `json:"type"` // "limit" or "market"
	Amount    float64   `json:"amount"`
	Price     float64   `json:"price"`
	Status    string    `json:"status"` // "pending", "filled", "cancelled", "failed"
	FilledQty float64   `json:"filledQty"`
	Fee       float64   `json:"fee"`
	Timestamp time.Time `json:"timestamp"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// ArbitrageTrade represents a complete triangular arbitrage execution
type ArbitrageTrade struct {
	ID            string               `json:"id"`
	Opportunity   ArbitrageOpportunity `json:"opportunity"`
	Orders        [3]TradeOrder        `json:"orders"`
	Status        string               `json:"status"` // "pending", "executing", "completed", "failed", "partial"
	StartTime     time.Time            `json:"startTime"`
	EndTime       time.Time            `json:"endTime"`
	ExecutionTime time.Duration        `json:"executionTime"`
	RealizedPnL   float64              `json:"realizedPnL"`
	TotalFees     float64              `json:"totalFees"`
	Error         string               `json:"error,omitempty"`
}

// MarketDepths stores the latest order book for each market, protected by a mutex
type MarketDepths struct {
	mu     sync.RWMutex
	depths map[string]*OrderBook
}

// NewMarketDepths creates a new MarketDepths instance
func NewMarketDepths() *MarketDepths {
	return &MarketDepths{
		depths: make(map[string]*OrderBook),
	}
}

// Store updates the order book for a given market
func (md *MarketDepths) Store(market string, ob *OrderBook) {
	md.mu.Lock()
	defer md.mu.Unlock()
	ob.Timestamp = time.Now()
	ob.Market = market
	md.depths[market] = ob
}

// Load retrieves the order book for a given market
func (md *MarketDepths) Load(market string) (*OrderBook, bool) {
	md.mu.RLock()
	defer md.mu.RUnlock()
	ob, ok := md.depths[market]
	return ob, ok
}

// GetAvailableMarkets returns a list of markets that have order book data
func (md *MarketDepths) GetAvailableMarkets() []string {
	md.mu.RLock()
	defer md.mu.RUnlock()
	var markets []string
	for market := range md.depths {
		markets = append(markets, market)
	}
	return markets
}

// GetSnapshot returns a snapshot of all current market depths
func (md *MarketDepths) GetSnapshot() map[string]*OrderBook {
	md.mu.RLock()
	defer md.mu.RUnlock()
	snapshot := make(map[string]*OrderBook)
	for market, ob := range md.depths {
		// Create a copy to avoid race conditions
		obCopy := *ob
		snapshot[market] = &obCopy
	}
	return snapshot
}

// Metrics represents real-time bot metrics
type Metrics struct {
	mu                    sync.RWMutex
	StartTime             time.Time          `json:"startTime"`
	OpportunitiesDetected int64              `json:"opportunitiesDetected"`
	TradesExecuted        int64              `json:"tradesExecuted"`
	TradesSuccessful      int64              `json:"tradesSuccessful"`
	TradesFailed          int64              `json:"tradesFailed"`
	TotalPnL              float64            `json:"totalPnL"`
	TotalFees             float64            `json:"totalFees"`
	AvgDetectionTime      float64            `json:"avgDetectionTimeMs"`
	AvgExecutionTime      float64            `json:"avgExecutionTimeMs"`
	LastUpdateTime        time.Time          `json:"lastUpdateTime"`
	ActiveConnections     int                `json:"activeConnections"`
	MessagesProcessed     int64              `json:"messagesProcessed"`
	ErrorCount            int64              `json:"errorCount"`
	CurrentBalance        map[string]float64 `json:"currentBalance"`
}

// NewMetrics creates a new Metrics instance
func NewMetrics() *Metrics {
	return &Metrics{
		StartTime:      time.Now(),
		CurrentBalance: make(map[string]float64),
		LastUpdateTime: time.Now(),
	}
}

// IncrementOpportunities safely increments opportunities detected counter
func (m *Metrics) IncrementOpportunities() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.OpportunitiesDetected++
	m.LastUpdateTime = time.Now()
}

// IncrementTrades safely increments trades executed counter
func (m *Metrics) IncrementTrades() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.TradesExecuted++
	m.LastUpdateTime = time.Now()
}

// UpdateBalance safely updates balance for a currency
func (m *Metrics) UpdateBalance(currency string, amount float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.CurrentBalance[currency] = amount
	m.LastUpdateTime = time.Now()
}

// AddPnL safely adds to total PnL
func (m *Metrics) AddPnL(pnl float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.TotalPnL += pnl
	m.LastUpdateTime = time.Now()
}

// GetSnapshot returns a snapshot of current metrics
func (m *Metrics) GetSnapshot() Metrics {
	m.mu.RLock()
	defer m.mu.RUnlock()
	snapshot := *m
	// Deep copy the balance map
	snapshot.CurrentBalance = make(map[string]float64)
	for k, v := range m.CurrentBalance {
		snapshot.CurrentBalance[k] = v
	}
	return snapshot
}

// RateLimiter implements a simple rate limiter
type RateLimiter struct {
	mu         sync.Mutex
	tokens     int
	maxTokens  int
	refillRate int
	lastRefill time.Time
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter(maxTokens, refillRate int) *RateLimiter {
	return &RateLimiter{
		tokens:     maxTokens,
		maxTokens:  maxTokens,
		refillRate: refillRate,
		lastRefill: time.Now(),
	}
}

// Allow checks if an operation is allowed under the rate limit
func (rl *RateLimiter) Allow() bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(rl.lastRefill)

	// Refill tokens based on elapsed time
	tokensToAdd := int(elapsed.Seconds()) * rl.refillRate
	if tokensToAdd > 0 {
		rl.tokens = Min(rl.maxTokens, rl.tokens+tokensToAdd)
		rl.lastRefill = now
	}

	if rl.tokens > 0 {
		rl.tokens--
		return true
	}

	return false
}
