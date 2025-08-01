package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// OrderStatus represents the status of an order
type OrderStatus string

const (
	OrderStatusPending   OrderStatus = "pending"
	OrderStatusFilled    OrderStatus = "filled"
	OrderStatusCancelled OrderStatus = "cancelled"
	OrderStatusFailed    OrderStatus = "failed"
	OrderStatusPartial   OrderStatus = "partial"
)

// OrderResult represents the result of an order execution
type OrderResult struct {
	OrderID       string      `json:"order_id"`
	Market        string      `json:"market"`
	Type          string      `json:"type"` // "buy" or "sell"
	Amount        float64     `json:"amount"`
	Price         float64     `json:"price"`
	Status        OrderStatus `json:"status"`
	FilledAmount  float64     `json:"filled_amount"`
	AvgPrice      float64     `json:"avg_price"`
	Fee           float64     `json:"fee"`
	FeeCurrency   string      `json:"fee_currency"`
	ExecutionTime int64       `json:"execution_time_ms"`
	ErrorMessage  string      `json:"error_message,omitempty"`
	Timestamp     time.Time   `json:"timestamp"`
}

// FOKOrderTracker tracks a Fill-or-Kill order with polling and cancellation
type FOKOrderTracker struct {
	OrderID       string
	Market        string
	Type          string
	Amount        float64
	Price         float64
	Status        OrderStatus
	CreatedAt     time.Time
	LastChecked   time.Time
	PollingTicker *time.Ticker
	CancelChan    chan struct{}
	ResultChan    chan *OrderResult
	IsActive      bool
	RetryCount    int
	mu            sync.RWMutex
	// Add flag to track if CancelChan has been closed
	cancelChanClosed bool
	// Add sync.Once to ensure channel is only closed once
	cancelOnce sync.Once
}

// OrderTrackingManager manages multiple FOK orders with polling
type OrderTrackingManager struct {
	trackers map[string]*FOKOrderTracker
	mu       sync.RWMutex
	config   *Config
}

// NewOrderTrackingManager creates a new order tracking manager
func NewOrderTrackingManager(config *Config) *OrderTrackingManager {
	return &OrderTrackingManager{
		trackers: make(map[string]*FOKOrderTracker),
		config:   config,
	}
}

// AddTracker adds a new order tracker
func (otm *OrderTrackingManager) AddTracker(tracker *FOKOrderTracker) {
	otm.mu.Lock()
	defer otm.mu.Unlock()

	// Initialize the cancel channel closed flag
	tracker.cancelChanClosed = false

	otm.trackers[tracker.OrderID] = tracker
	log.Printf("🔍 TRACKER ADDED | ID: %s | Market: %s | Total trackers: %d",
		tracker.OrderID, tracker.Market, len(otm.trackers))
}

// RemoveTracker removes an order tracker
func (otm *OrderTrackingManager) RemoveTracker(orderID string) {
	otm.mu.Lock()
	defer otm.mu.Unlock()

	if tracker, exists := otm.trackers[orderID]; exists {
		tracker.mu.Lock()
		if tracker.PollingTicker != nil {
			tracker.PollingTicker.Stop()
		}
		if tracker.CancelChan != nil {
			// Use sync.Once to ensure channel is only closed once
			tracker.cancelOnce.Do(func() {
				close(tracker.CancelChan)
				tracker.cancelChanClosed = true
				log.Printf("🔍 CANCEL CHANNEL CLOSED | ID: %s", orderID)
			})
		}
		tracker.IsActive = false
		tracker.mu.Unlock()

		delete(otm.trackers, orderID)
		log.Printf("🔍 TRACKER REMOVED | ID: %s | Market: %s | Total trackers: %d",
			orderID, tracker.Market, len(otm.trackers))
	} else {
		log.Printf("🔍 TRACKER NOT FOUND | ID: %s | Total trackers: %d", orderID, len(otm.trackers))
	}
}

// GetTracker gets an order tracker
func (otm *OrderTrackingManager) GetTracker(orderID string) (*FOKOrderTracker, bool) {
	otm.mu.RLock()
	defer otm.mu.RUnlock()
	tracker, exists := otm.trackers[orderID]
	return tracker, exists
}

// GetActiveTrackers returns all active order trackers
func (otm *OrderTrackingManager) GetActiveTrackers() []*FOKOrderTracker {
	otm.mu.RLock()
	defer otm.mu.RUnlock()

	var active []*FOKOrderTracker
	for _, tracker := range otm.trackers {
		tracker.mu.RLock()
		if tracker.IsActive {
			active = append(active, tracker)
		}
		tracker.mu.RUnlock()
	}

	log.Printf("🔍 GET ACTIVE TRACKERS | Found %d active out of %d total trackers", len(active), len(otm.trackers))
	for _, tracker := range active {
		log.Printf("🔍 ACTIVE TRACKER | ID: %s | Market: %s | Status: %s",
			tracker.OrderID, tracker.Market, tracker.Status)
	}

	return active
}

type coinexClient struct {
	httpClient                 *HttpClient
	apiKey                     string
	secretID                   string
	baseUrl                    string
	allowedMarkets             []string
	config                     *Config // Add reference to config
	orderTrackingManager       *OrderTrackingManager
	criticalMarketPriceManager *CriticalMarketPriceManager

	// Order execution tracking
	lastOrderTime    time.Time
	concurrentOrders int
	mu               sync.RWMutex
}

func NewCoinexClient(httpClient *HttpClient, config *Config) *coinexClient {
	fmt.Printf("Loading CoinEx client with quote currencies from config: %v\n", config.QuoteCurrencies)
	client := &coinexClient{
		httpClient:           httpClient,
		apiKey:               config.APIKey,   // Use API key as API key
		secretID:             config.SecretID, // Use secret ID as secret ID
		allowedMarkets:       config.QuoteCurrencies,
		baseUrl:              config.APIBaseURL, // Use v1 API for market operations
		config:               config,            // Store config reference
		orderTrackingManager: NewOrderTrackingManager(config),
	}

	// Initialize critical market price manager after client is created
	client.criticalMarketPriceManager = NewCriticalMarketPriceManager(client, config)

	// Sync concurrent orders counter with actual trackers at startup
	client.syncConcurrentOrders()

	return client
}

func (c *coinexClient) GetApiKey() string {
	return c.apiKey
}

// generateRESTSignature creates HMAC-SHA256 signature for CoinEx API v2 REST endpoints
func (c *coinexClient) generateRESTSignature(method, requestPath, queryString, body string, timestamp int64) string {
	// Create the string to sign for v2 REST API
	// Format: method + request_path + body + timestamp
	// According to CoinEx docs: https://docs.coinex.com/api/v2/authorization

	// Build the full request path including query string if present
	fullRequestPath := requestPath
	if queryString != "" {
		fullRequestPath = requestPath + "?" + queryString
	}

	// Create the string to sign: method + request_path + body + timestamp
	// The body should be included as a string literal (with quotes)
	var stringToSign string
	if body != "" {
		// For POST/PUT requests with body, include the body as a string literal
		stringToSign = method + fullRequestPath + body + strconv.FormatInt(timestamp, 10)
	} else {
		// For GET/DELETE requests without body
		stringToSign = method + fullRequestPath + strconv.FormatInt(timestamp, 10)
	}

	// Create HMAC-SHA256 signature
	h := hmac.New(sha256.New, []byte(c.secretID))
	h.Write([]byte(stringToSign))
	signature := hex.EncodeToString(h.Sum(nil))

	// Debug logging
	log.Printf("🔐 REST SIGNATURE DEBUG | Method: %s | Path: %s | Query: %s | Body: %s | Timestamp: %d",
		method, requestPath, queryString, body, timestamp)
	log.Printf("🔐 REST SIGNATURE DEBUG | Body type: %T | Body length: %d", body, len(body))
	log.Printf("🔐 REST SIGNATURE DEBUG | Full request path: %s", fullRequestPath)
	log.Printf("🔐 REST SIGNATURE DEBUG | Timestamp string: %s", strconv.FormatInt(timestamp, 10))
	log.Printf("🔐 REST SIGNATURE DEBUG | String to sign: %s", stringToSign)
	log.Printf("🔐 REST SIGNATURE DEBUG | Secret key: %s", c.secretID)
	log.Printf("🔐 REST SIGNATURE DEBUG | Signature: %s", signature)
	log.Printf("🔐 REST SIGNATURE DEBUG | Signature length: %d", len(signature))

	return signature
}

// generateWebSocketSignature creates HMAC-SHA256 signature for CoinEx WebSocket authentication
func (c *coinexClient) generateWebSocketSignature(timestamp int64) string {
	// Create the string to sign for WebSocket authentication
	// Format: timestamp (just the timestamp!)
	// According to CoinEx docs: https://docs.coinex.com/api/v2/authorization

	stringToSign := strconv.FormatInt(timestamp, 10)

	// Create HMAC-SHA256 signature
	h := hmac.New(sha256.New, []byte(c.secretID))
	h.Write([]byte(stringToSign))
	signature := hex.EncodeToString(h.Sum(nil))

	// Debug logging
	log.Printf("🔐 WS SIGNATURE DEBUG | Timestamp: %d", timestamp)
	log.Printf("🔐 WS SIGNATURE DEBUG | String to sign: %s", stringToSign)
	log.Printf("🔐 WS SIGNATURE DEBUG | Secret key: %s", c.secretID)
	log.Printf("🔐 WS SIGNATURE DEBUG | Signature: %s", signature)
	log.Printf("🔐 WS SIGNATURE DEBUG | Signature length: %d", len(signature))

	return signature
}

func (c *coinexClient) PlaceOrder() string {
	return ""
}

// PlaceFOKOrder places a Fill-or-Kill order with automatic simulation/real API switching and spending controls
func (c *coinexClient) PlaceFOKOrder(market, orderType string, amount, price float64, orderResultChan chan<- *OrderResult) *FOKOrderTracker {
	log.Printf("🎯 PLACE FOK ORDER START | Market: %s | Type: %s | Amount: %.6f | Price: %.8f",
		market, orderType, amount, price)

	// Check rate limit before placing order
	if !c.checkRateLimit() {
		log.Printf("🚫 RATE LIMIT | Order placement rate limited")
		return nil
	}

	// Check balance before placing order
	if !c.CanPlaceOrder(price * amount) {
		log.Printf("🚫 INSUFFICIENT BALANCE | Cannot place order of value $%.2f", price*amount)
		return nil
	}

	// Step 2: Check spending limits and calculate order amount
	orderAmount, orderValue, err := c.calculateOrderAmount(amount, price)
	if err != nil {
		log.Printf("🚫 ORDER REJECTED | %v | Market: %s", err, market)
		return nil
	}
	log.Printf("✅ ORDER AMOUNT CALCULATED | Amount: %.6f | Value: $%.2f", orderAmount, orderValue)

	// Step 4: Update order tracking AFTER passing all checks
	c.updateOrderTracking(orderValue)

	// Step 5: Generate order ID and create tracker
	orderID := fmt.Sprintf("FOK_%s_%d_%d", market, time.Now().UnixNano(), rand.Intn(10000))
	log.Printf("✅ ORDER ID GENERATED | ID: %s", orderID)

	executionMode := map[bool]string{true: "SIMULATION", false: "REAL API"}[c.config.SimulationMode]
	log.Printf(" PLACING FOK ORDER (%s) | ID: %s | Market: %s | Type: %s | Amount: %.6f | Price: %.8f | Value: $%.2f",
		executionMode, orderID, market, orderType, orderAmount, price, orderValue)

	// Step 6: Create order tracker
	tracker := &FOKOrderTracker{
		OrderID:     orderID,
		Market:      market,
		Type:        orderType,
		Amount:      orderAmount,
		Price:       price,
		Status:      OrderStatusPending,
		CreatedAt:   time.Now(),
		LastChecked: time.Now(),
		CancelChan:  make(chan struct{}),
		ResultChan:  make(chan *OrderResult, 1),
		IsActive:    true,
		RetryCount:  0,
	}
	log.Printf("✅ ORDER TRACKER CREATED")

	// Step 7: Add tracker to manager and start lifecycle management
	c.orderTrackingManager.AddTracker(tracker)
	log.Printf("✅ ORDER TRACKING UPDATED")

	// Step 8: Start order lifecycle (simulation or real API)
	log.Printf("🚀 STARTING ORDER LIFECYCLE | ID: %s", orderID)
	go func() {
		log.Printf("🔄 GOROUTINE STARTED | ID: %s", orderID)
		c.manageOrderLifecycle(tracker, orderResultChan)
		log.Printf("🔄 GOROUTINE COMPLETED | ID: %s", orderID)
	}()

	log.Printf("✅ ORDER LIFECYCLE STARTED | Returning tracker")
	return tracker
}

// checkRateLimit checks if we can place another order based on rate limiting
func (c *coinexClient) checkRateLimit() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	minInterval := time.Duration(1.0/c.config.OrderExecutionSettings.MaxOrdersPerSecond) * time.Second

	if c.lastOrderTime.IsZero() || now.Sub(c.lastOrderTime) >= minInterval {
		c.lastOrderTime = now
		return true
	}

	return false
}

// calculateOrderAmount calculates the appropriate order amount based on configuration
func (c *coinexClient) calculateOrderAmount(requestedAmount, price float64) (float64, float64, error) {
	settings := c.config.OrderExecutionSettings

	var orderAmount float64
	var orderValue float64

	// Calculate based on order amount type
	if settings.OrderAmountType == "static" {
		// Static USD amount
		orderValue = settings.StaticOrderAmount
		orderAmount = orderValue / price
	} else {
		// Dynamic percentage of available balance
		availableBalance := settings.AccountBalance
		orderValue = availableBalance * settings.DynamicOrderPercentage
		orderAmount = orderValue / price

		// In dynamic mode, we ignore requestedAmount and use our configured percentage
		// The requestedAmount is just the arbitrage engine's suggestion, but we want
		// to trade with our configured risk percentage regardless
		log.Printf("💡 DYNAMIC ORDER CALC | Available: $%.2f | Percentage: %.1f%% | Order Value: $%.2f",
			availableBalance, settings.DynamicOrderPercentage*100, orderValue)
	}

	// Apply min/max limits
	if orderValue < settings.MinOrderAmount {
		return 0, 0, fmt.Errorf("order value $%.2f below minimum $%.2f", orderValue, settings.MinOrderAmount)
	}
	if orderValue > settings.MaxOrderAmount {
		orderValue = settings.MaxOrderAmount
		orderAmount = orderValue / price
	}

	return orderAmount, orderValue, nil
}

// updateOrderTracking updates the order tracking state
func (c *coinexClient) updateOrderTracking(orderValue float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.concurrentOrders++
}

// manageOrderLifecycle manages the complete lifecycle of a FOK order
func (c *coinexClient) manageOrderLifecycle(tracker *FOKOrderTracker, orderResultChan chan<- *OrderResult) {
	log.Printf("🎬 ORDER LIFECYCLE STARTED | ID: %s | Market: %s | Simulation Mode: %t",
		tracker.OrderID, tracker.Market, c.config.SimulationMode)

	// Add panic recovery
	defer func() {
		if r := recover(); r != nil {
			log.Printf("🚨 ORDER LIFECYCLE PANIC | ID: %s | Error: %v", tracker.OrderID, r)
		}
		// Always cleanup when lifecycle ends
		c.orderTrackingManager.RemoveTracker(tracker.OrderID)
		c.decrementConcurrentOrders()
		log.Printf("🧹 ORDER LIFECYCLE COMPLETE | ID: %s | Final Status: %s", tracker.OrderID, tracker.Status)
	}()

	// Switch between simulation and real API based on config
	if c.config.SimulationMode {
		log.Printf("🎮 STARTING SIMULATION ORDER | ID: %s", tracker.OrderID)
		c.manageSimulationOrderLifecycle(tracker, orderResultChan)
	} else {
		log.Printf("🚀 STARTING REAL API ORDER | ID: %s", tracker.OrderID)
		c.manageRealFOKOrderLifecycle(tracker, orderResultChan)
	}
}

// decrementConcurrentOrders safely decrements the concurrent order count
func (c *coinexClient) decrementConcurrentOrders() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.concurrentOrders > 0 {
		c.concurrentOrders--
	}
}

// resetConcurrentOrders resets the concurrent order count (for debugging/recovery)
func (c *coinexClient) resetConcurrentOrders() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.concurrentOrders = 0
	log.Printf("🔄 CONCURRENT ORDERS RESET | Counter reset to 0")
}

// getConcurrentOrders returns the current concurrent order count
func (c *coinexClient) getConcurrentOrders() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.concurrentOrders
}

// syncConcurrentOrders synchronizes the concurrent orders counter with actual active trackers
func (c *coinexClient) syncConcurrentOrders() {
	c.mu.Lock()
	defer c.mu.Unlock()

	activeTrackers := c.orderTrackingManager.GetActiveTrackers()
	actualCount := len(activeTrackers)

	if c.concurrentOrders != actualCount {
		log.Printf("🔄 SYNCING CONCURRENT ORDERS | Counter: %d | Actual trackers: %d | Correcting...",
			c.concurrentOrders, actualCount)
		c.concurrentOrders = actualCount
	} else {
		log.Printf("✅ CONCURRENT ORDERS SYNCED | Counter: %d | Actual trackers: %d",
			c.concurrentOrders, actualCount)
	}
}

// manageSimulationOrderLifecycle handles simulation orders
func (c *coinexClient) manageSimulationOrderLifecycle(tracker *FOKOrderTracker, orderResultChan chan<- *OrderResult) {
	// Start order execution simulation
	go c.simulateOrderExecution(tracker)

	// Setup polling
	pollingInterval := time.Duration(1000/c.config.FOKOrderSettings.PollingFrequencyHz) * time.Millisecond
	tracker.PollingTicker = time.NewTicker(pollingInterval)
	defer tracker.PollingTicker.Stop()

	timeout := time.After(time.Duration(c.config.FOKOrderSettings.OrderTimeoutSeconds) * time.Second)

	log.Printf(" SIMULATION POLLING | ID: %s | Interval: %v | Timeout: %ds",
		tracker.OrderID, pollingInterval, c.config.FOKOrderSettings.OrderTimeoutSeconds)

	for {
		select {
		case <-tracker.PollingTicker.C:
			// Poll order status (simulation)
			c.pollOrderStatus(tracker)

			// Check if order is complete
			tracker.mu.RLock()
			status := tracker.Status
			tracker.mu.RUnlock()

			if status == OrderStatusFilled || status == OrderStatusCancelled || status == OrderStatusFailed {
				// Order is complete, send final result
				result := c.buildOrderResult(tracker)

				select {
				case orderResultChan <- result:
					log.Printf(" SIMULATION RESULT SENT | ID: %s | Status: %s", tracker.OrderID, status)
				default:
					log.Printf("  SIMULATION RESULT CHANNEL FULL | ID: %s", tracker.OrderID)
				}
				return
			}

		case <-timeout:
			// Order timeout - attempt cancellation
			log.Printf(" SIMULATION ORDER TIMEOUT | ID: %s", tracker.OrderID)
			c.cancelOrder(tracker)

			result := c.buildOrderResult(tracker)
			result.Status = OrderStatusCancelled
			result.ErrorMessage = "Order timed out and was cancelled"

			select {
			case orderResultChan <- result:
				log.Printf(" SIMULATION TIMEOUT RESULT SENT | ID: %s", tracker.OrderID)
			default:
				log.Printf("  SIMULATION TIMEOUT RESULT CHANNEL FULL | ID: %s", tracker.OrderID)
			}
			return

		case <-tracker.CancelChan:
			// External cancellation request
			log.Printf("🛑 SIMULATION EXTERNAL CANCELLATION | ID: %s", tracker.OrderID)
			c.cancelOrder(tracker)
			return

		case result := <-tracker.ResultChan:
			// Order execution completed (from simulation)
			tracker.mu.Lock()
			tracker.Status = result.Status
			tracker.mu.Unlock()

			log.Printf(" SIMULATION ORDER RESULT | ID: %s | Status: %s", tracker.OrderID, result.Status)

			select {
			case orderResultChan <- result:
				log.Printf("📤 SIMULATION RESULT SENT | ID: %s", tracker.OrderID)
			default:
				log.Printf("  SIMULATION RESULT CHANNEL FULL | ID: %s", tracker.OrderID)
			}
			return
		}
	}
}

// pollOrderStatus polls the order status (simulation - in real implementation would call CoinEx API)
func (c *coinexClient) pollOrderStatus(tracker *FOKOrderTracker) {
	tracker.mu.Lock()
	tracker.LastChecked = time.Now()
	tracker.mu.Unlock()

	// In real implementation, this would call CoinEx order status API
	// For simulation, we just log the polling activity
	log.Printf(" POLLING ORDER STATUS | ID: %s | Status: %s | Age: %v",
		tracker.OrderID, tracker.Status, time.Since(tracker.CreatedAt))
}

// cancelOrder cancels an order (simulation - in real implementation would call CoinEx API)
func (c *coinexClient) cancelOrder(tracker *FOKOrderTracker) {
	log.Printf("🛑 CANCELLING ORDER | ID: %s", tracker.OrderID)

	tracker.mu.Lock()
	if tracker.Status == OrderStatusPending {
		tracker.Status = OrderStatusCancelled
	}
	tracker.IsActive = false
	tracker.mu.Unlock()

	log.Printf(" ORDER CANCELLED | ID: %s", tracker.OrderID)
}

// simulateOrderExecution simulates order execution (in real implementation, this would be actual order placement)
func (c *coinexClient) simulateOrderExecution(tracker *FOKOrderTracker) {
	startTime := time.Now()

	// Simulate order execution time based on configuration
	var executionTimeMs int64
	if c.config != nil && c.config.EnableOrderSimulation {
		executionTimeMs = int64(c.config.OrderExecutionTimeMs)
	} else {
		executionTimeMs = 1500 // Default 1.5 seconds for FOK
	}

	// Add some randomness to execution time (±20%) to simulate real market conditions
	randomFactor := 0.8 + rand.Float64()*0.4 // 0.8 to 1.2
	actualExecutionTime := time.Duration(float64(executionTimeMs)*randomFactor) * time.Millisecond

	log.Printf(" SIMULATING FOK EXECUTION | ID: %s | Est. time: %v", tracker.OrderID, actualExecutionTime)

	// Sleep to simulate order execution
	time.Sleep(actualExecutionTime)

	// Check if order was cancelled during execution
	tracker.mu.RLock()
	if !tracker.IsActive || tracker.Status == OrderStatusCancelled {
		tracker.mu.RUnlock()
		log.Printf("🛑 ORDER CANCELLED DURING EXECUTION | ID: %s", tracker.OrderID)
		return
	}
	tracker.mu.RUnlock()

	// Simulate order outcome (90% success rate for FOK orders)
	var result *OrderResult
	if rand.Float64() < 0.90 {
		// Successful FOK order (fully filled or not at all)
		isFullyFilled := rand.Float64() < 0.85 // 85% chance of full fill

		var filledAmount float64
		var status OrderStatus

		if isFullyFilled {
			filledAmount = tracker.Amount
			status = OrderStatusFilled
		} else {
			filledAmount = 0 // FOK: fully filled or not at all
			status = OrderStatusCancelled
		}

		// Simulate slight price slippage (±0.005% for FOK)
		slippageFactor := 0.99995 + rand.Float64()*0.0001 // ±0.005%
		avgPrice := tracker.Price * slippageFactor

		// Calculate fee
		fee := filledAmount * avgPrice * c.getTradingFee(tracker.Market)
		feeCurrency := "USDT"
		if strings.HasSuffix(tracker.Market, "USDC") {
			feeCurrency = "USDC"
		}

		result = &OrderResult{
			OrderID:       tracker.OrderID,
			Market:        tracker.Market,
			Type:          tracker.Type,
			Amount:        tracker.Amount,
			Price:         tracker.Price,
			Status:        status,
			FilledAmount:  filledAmount,
			AvgPrice:      avgPrice,
			Fee:           fee,
			FeeCurrency:   feeCurrency,
			ExecutionTime: time.Since(startTime).Milliseconds(),
			Timestamp:     time.Now(),
		}

		if status == OrderStatusFilled {
			log.Printf(" FOK ORDER FILLED | ID: %s | Amount: %.6f | Avg Price: %.8f | Fee: %.6f %s | Time: %dms",
				tracker.OrderID, filledAmount, avgPrice, fee, feeCurrency, result.ExecutionTime)
		} else {
			log.Printf(" FOK ORDER NOT FILLED | ID: %s | Insufficient liquidity | Time: %dms",
				tracker.OrderID, result.ExecutionTime)
		}
	} else {
		// Failed order
		errorMessages := []string{
			"FOK order could not be filled immediately",
			"Price moved beyond FOK tolerance",
			"Insufficient liquidity for FOK execution",
			"Market temporarily unavailable",
		}
		errorMsg := errorMessages[rand.Intn(len(errorMessages))]

		result = &OrderResult{
			OrderID:       tracker.OrderID,
			Market:        tracker.Market,
			Type:          tracker.Type,
			Amount:        tracker.Amount,
			Price:         tracker.Price,
			Status:        OrderStatusFailed,
			FilledAmount:  0,
			AvgPrice:      0,
			Fee:           0,
			FeeCurrency:   "",
			ExecutionTime: time.Since(startTime).Milliseconds(),
			ErrorMessage:  errorMsg,
			Timestamp:     time.Now(),
		}

		log.Printf(" FOK ORDER FAILED | ID: %s | Error: %s | Time: %dms",
			tracker.OrderID, errorMsg, result.ExecutionTime)
	}

	// Send result to tracker's result channel
	select {
	case tracker.ResultChan <- result:
		// Successfully sent result
	default:
		log.Printf("  TRACKER RESULT CHANNEL FULL | ID: %s", tracker.OrderID)
	}
}

// buildOrderResult builds a final order result from tracker state
func (c *coinexClient) buildOrderResult(tracker *FOKOrderTracker) *OrderResult {
	tracker.mu.RLock()
	defer tracker.mu.RUnlock()

	return &OrderResult{
		OrderID:       tracker.OrderID,
		Market:        tracker.Market,
		Type:          tracker.Type,
		Amount:        tracker.Amount,
		Price:         tracker.Price,
		Status:        tracker.Status,
		FilledAmount:  0, // Will be updated by execution
		AvgPrice:      tracker.Price,
		Fee:           0,
		FeeCurrency:   "",
		ExecutionTime: time.Since(tracker.CreatedAt).Milliseconds(),
		Timestamp:     time.Now(),
	}
}

// getTradingFee returns the trading fee for a market
func (c *coinexClient) getTradingFee(market string) float64 {
	if fee, exists := c.config.TradingFees[market]; exists {
		return fee
	}
	return c.config.DefaultTradingFee
}

// CancelAllActiveOrders cancels all active orders (real API or simulation)
func (c *coinexClient) CancelAllActiveOrders() {
	activeTrackers := c.orderTrackingManager.GetActiveTrackers()

	log.Printf("🛑 CANCELLING ALL ACTIVE ORDERS | Count: %d | Mode: %s",
		len(activeTrackers), map[bool]string{true: "SIMULATION", false: "REAL API"}[c.config.SimulationMode])

	// In real API mode, also check for any orders on the exchange we might not be tracking
	if !c.config.SimulationMode {
		exchangeOrders, err := c.GetOpenOrders()
		if err != nil {
			log.Printf("⚠️ CANNOT CHECK EXCHANGE ORDERS | Error: %v", err)
		} else if len(exchangeOrders) > 0 {
			log.Printf("📋 FOUND %d ORDERS ON EXCHANGE | Checking for untracked orders", len(exchangeOrders))

			// Create a set of tracked order IDs
			trackedOrderIDs := make(map[string]bool)
			for _, tracker := range activeTrackers {
				trackedOrderIDs[tracker.OrderID] = true
			}

			// Check for untracked orders
			for _, order := range exchangeOrders {
				if orderID, ok := order["order_id"].(string); ok {
					if !trackedOrderIDs[orderID] {
						log.Printf("⚠️ UNTRACKED ORDER FOUND | ID: %s | Market: %v | Side: %v | Amount: %v | Price: %v",
							orderID, order["market"], order["side"], order["amount"], order["price"])

						// Create a temporary tracker to cancel this order
						tempTracker := &FOKOrderTracker{
							OrderID: orderID,
							Market:  fmt.Sprintf("%v", order["market"]),
							Type:    fmt.Sprintf("%v", order["side"]),
							Amount:  0, // We don't know the original amount
							Price:   0, // We don't know the original price
						}

						// Cancel the untracked order
						err := c.cancelRealOrder(tempTracker)
						if err != nil {
							log.Printf("❌ FAILED TO CANCEL UNTRACKED ORDER | ID: %s | Error: %v", orderID, err)
						} else {
							log.Printf("✅ CANCELLED UNTRACKED ORDER | ID: %s", orderID)
						}
					}
				}
			}
		}
	}

	for _, tracker := range activeTrackers {
		tracker.mu.Lock()
		if tracker.IsActive && tracker.CancelChan != nil {
			if c.config.SimulationMode {
				// Simulation mode - use channel cancellation
				select {
				case tracker.CancelChan <- struct{}{}:
					log.Printf("🛑 SIMULATION CANCELLATION SIGNAL SENT | ID: %s", tracker.OrderID)
				default:
					log.Printf("  SIMULATION CANCELLATION CHANNEL FULL | ID: %s", tracker.OrderID)
				}
			} else {
				// Real API mode - cancel via CoinEx API with verification
				tracker.mu.Unlock() // Unlock before API call

				// Step 1: Cancel the order on the exchange
				err := c.cancelRealOrder(tracker)
				if err != nil {
					log.Printf(" REAL API CANCELLATION FAILED | ID: %s | Error: %v", tracker.OrderID, err)
				} else {
					log.Printf(" REAL API CANCELLATION SUCCESS | ID: %s", tracker.OrderID)
				}

				// Step 2: Verify cancellation and check for partial fills
				time.Sleep(500 * time.Millisecond) // Wait for cancellation to process
				isFilled, err := c.pollRealOrderStatus(tracker)
				if err != nil {
					log.Printf("  CANNOT VERIFY CANCELLATION | ID: %s | Error: %v", tracker.OrderID, err)
				} else {
					log.Printf("  CANCELLATION VERIFICATION | ID: %s | Filled: %v", tracker.OrderID, isFilled)
				}

				tracker.mu.Lock() // Re-lock for safe unlock below
			}
		}
		tracker.mu.Unlock()
	}
}

// GetOrderExecutionStats returns current order execution statistics
func (c *coinexClient) GetOrderExecutionStats() (int, float64, float64) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	settings := c.config.OrderExecutionSettings
	availableBalance := settings.AccountBalance

	return c.concurrentOrders, 0, availableBalance
}

// CanPlaceOrder checks if a new order can be placed based on all limits
func (c *coinexClient) CanPlaceOrder(orderValue float64) bool {
	// Check rate limit
	if !c.checkRateLimit() {
		return false
	}

	// REMOVED: Concurrent order limit - now handled at arbitrage level
	// REMOVED: Spending limits - allowing all orders to proceed
	// REMOVED: Balance checks - allowing all orders to proceed

	return true
}

// ResetDailySpending is deprecated - no longer tracking daily spending
func (c *coinexClient) ResetDailySpending() {
	// No longer tracking daily spending
	log.Printf(" RESETTING DAILY SPENDING | Daily spending tracking disabled")
}

// GetOrderStatus gets the current status of an order
func (c *coinexClient) GetOrderStatus(orderID string) (*FOKOrderTracker, bool) {
	return c.orderTrackingManager.GetTracker(orderID)
}

// GetActiveTrackers returns all active order trackers
func (c *coinexClient) GetActiveTrackers() []*FOKOrderTracker {
	return c.orderTrackingManager.GetActiveTrackers()
}

func (c *coinexClient) TestConnection() (string, error) {
	// Test with a public endpoint that doesn't require authentication
	// Use the market list endpoint which is simpler and doesn't need market parameter
	params := map[string]string{
		"url":    c.baseUrl + "/market/list",
		"method": GET,
	}

	response, err := c.httpClient.performRequest(params, GET)
	if err != nil {
		return "", err
	}

	// Convert the response to JSON string for return
	responseJSON, err := json.Marshal(response)
	if err != nil {
		return "", err
	}

	return string(responseJSON), nil
}

func (c *coinexClient) GetBalance() (string, error) {
	// Use v2 API for balance
	timestamp := time.Now().UnixMilli()
	method := GET
	requestPath := "/assets/credit/balance"
	queryString := ""
	body := ""

	// Generate v2 signature
	signature := c.generateRESTSignature(method, requestPath, queryString, body, timestamp)

	// Set v2 authentication headers
	authHeaders := map[string]string{
		"X-COINEX-KEY":       c.apiKey,
		"X-COINEX-SIGN":      signature,
		"X-COINEX-TIMESTAMP": strconv.FormatInt(timestamp, 10),
	}

	// Add debug logging
	fmt.Printf("Debug - V2 Auth headers: %+v\n", authHeaders)

	// Merge auth headers with existing headers
	c.httpClient.mergeHeaders(authHeaders)

	// Add the required parameters to the request
	requestParams := map[string]string{
		"url":    strings.TrimSuffix(c.config.APIBaseURLV2, "/") + requestPath,
		"method": method,
	}

	response, err := c.httpClient.performRequest(requestParams, method)
	if err != nil {
		return "", err
	}

	// Convert the response to JSON string for return
	responseJSON, err := json.Marshal(response)
	if err != nil {
		return "", err
	}

	return string(responseJSON), nil
}

func (c *coinexClient) GetMarketList() (string, error) {
	// Use the v1 market list endpoint
	params := map[string]string{
		"url":    c.baseUrl + "/market/list",
		"method": GET,
	}

	response, err := c.httpClient.performRequest(params, GET)
	if err != nil {
		return "", err
	}

	// Convert the response to JSON string for return
	responseJSON, err := json.Marshal(response)
	if err != nil {
		return "", err
	}

	return string(responseJSON), nil
}

// Add new method to get all market statistics with 24H volume
func (c *coinexClient) GetAllMarketTickers() (map[string]interface{}, error) {
	params := map[string]string{
		"url":    c.baseUrl + "/market/ticker/all",
		"method": GET,
	}

	response, err := c.httpClient.performRequest(params, GET)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (c *coinexClient) GetArbitrageMarkets() ([]string, []string, error) {
	// Get all available markets from CoinEx
	marketListString, err := c.GetMarketList()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get market list: %w", err)
	}

	var marketList map[string]interface{}
	err = json.Unmarshal([]byte(marketListString), &marketList)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal market list: %w", err)
	}

	markets := marketList["data"].([]interface{})
	fmt.Printf(" Total markets from CoinEx: %d\n", len(markets))

	// Step 1: Find theoretical triangular arbitrage opportunities
	fmt.Printf(" Step 1: Finding theoretical triangular markets...\n")
	triangularMarkets, completeAssets := c.findTriangularArbitrageMarkets(markets)

	// Step 2: Get real market activity data (24H volume) for ALL markets
	fmt.Printf(" Step 2: Fetching real market activity data from CoinEx...\n")
	tickerData, err := c.GetAllMarketTickers()
	if err != nil {
		fmt.Printf("  Failed to get market ticker data, using heuristic filtering: %v\n", err)
		// Fallback to heuristic filtering
		activeMarkets, _ := c.validateMarketActivity(triangularMarkets)
		activeCompleteAssets := c.filterCompleteAssetsByActiveMarkets(completeAssets, activeMarkets)
		return activeMarkets, activeCompleteAssets, nil
	}

	// Step 3: Filter markets based on real 24H volume data
	activeMarkets, deadMarkets := c.filterMarketsByRealActivity(triangularMarkets, tickerData)

	fmt.Printf("\n REAL DATA FILTERING RESULTS:\n")
	fmt.Printf("    Theoretical markets: %d\n", len(triangularMarkets))
	fmt.Printf("    Active markets (with volume): %d\n", len(activeMarkets))
	fmt.Printf("   💀 Dead markets (no volume): %d\n", len(deadMarkets))
	fmt.Printf("    Efficiency gain: %.1f%% reduction in subscriptions\n",
		float64(len(deadMarkets))/float64(len(triangularMarkets))*100)

	if len(deadMarkets) > 0 {
		fmt.Printf("   🚫 Dead markets (no 24H volume): %v\n", deadMarkets[:Min(10, len(deadMarkets))])
		if len(deadMarkets) > 10 {
			fmt.Printf("   ... and %d more dead markets\n", len(deadMarkets)-10)
		}
	}

	// Update complete assets based on active markets only
	activeCompleteAssets := c.filterCompleteAssetsByActiveMarkets(completeAssets, activeMarkets)

	fmt.Printf("    Complete assets with active markets: %d (from %d)\n",
		len(activeCompleteAssets), len(completeAssets))

	if len(activeCompleteAssets) > 0 {
		fmt.Printf("    Active triangular assets: %v\n", activeCompleteAssets)
	}

	return activeMarkets, activeCompleteAssets, nil
}

// findTriangularArbitrageMarkets finds markets suitable for triangular arbitrage cycles
func (c *coinexClient) findTriangularArbitrageMarkets(markets []interface{}) ([]string, []string) {
	// Group markets by the configured quote currencies only
	usdtMarkets := make(map[string]string) // asset -> market (e.g., BTC -> BTCUSDT)
	usdcMarkets := make(map[string]string) // asset -> market (e.g., BTC -> BTCUSDC)

	fmt.Printf(" Analyzing markets for triangular arbitrage with quote currencies: %v\n", c.allowedMarkets)

	// Parse all markets and categorize them by configured quote currencies
	for _, market := range markets {
		marketStr := market.(string)

		for _, quote := range c.allowedMarkets {
			if strings.HasSuffix(marketStr, quote) {
				asset := strings.TrimSuffix(marketStr, quote)
				if asset != "" {
					// Store by quote currency
					if quote == "USDT" && asset != "USDC" {
						usdtMarkets[asset] = marketStr
					} else if quote == "USDC" && asset != "USDT" {
						usdcMarkets[asset] = marketStr
					}
				}
				break
			}
		}
	}

	fmt.Printf(" Market Analysis Results:\n")
	fmt.Printf("    USDT pairs: %d\n", len(usdtMarkets))
	fmt.Printf("   💎 USDC pairs: %d\n", len(usdcMarkets))

	// Debug: Show actual USDC markets found
	if len(usdcMarkets) > 0 {
		fmt.Printf("    USDC markets found: ")
		for _, market := range usdcMarkets {
			fmt.Printf("%s ", market)
		}
		fmt.Printf("\n")
	} else {
		fmt.Printf("     NO USDC markets found in CoinEx market list!\n")
	}

	// Find assets that have BOTH quote currency pairs (required for triangular arbitrage)
	completeAssets := []string{}
	triangularMarkets := []string{}

	for asset := range usdtMarkets {
		if _, hasUSDC := usdcMarkets[asset]; hasUSDC {
			completeAssets = append(completeAssets, asset)
			// Add both markets for this asset
			triangularMarkets = append(triangularMarkets, usdtMarkets[asset])
			triangularMarkets = append(triangularMarkets, usdcMarkets[asset])
			fmt.Printf("    Complete asset: %s → Markets: %s, %s\n",
				asset, usdtMarkets[asset], usdcMarkets[asset])
		}
	}

	// NO PRIORITY LOGIC - All tokens treated equally as requested
	fmt.Printf("   💡 All %d assets treated equally (no priorities)\n", len(completeAssets))

	// Add the direct quote pair if it exists (USDC/USDT)
	for _, market := range markets {
		marketStr := market.(string)
		if marketStr == "USDCUSDT" || marketStr == "USDTUSDC" {
			triangularMarkets = append(triangularMarkets, marketStr)
			fmt.Printf("    Added critical quote pair: %s\n", marketStr)
		}
	}

	fmt.Printf("\n TRIANGULAR ARBITRAGE SUMMARY:\n")
	fmt.Printf("    Assets with both quote pairs: %d\n", len(completeAssets))
	fmt.Printf("    Markets to subscribe: %d (vs %d total)\n", len(triangularMarkets), len(markets))
	fmt.Printf("    Efficiency gain: %.1f%% reduction in subscriptions\n",
		float64(len(markets)-len(triangularMarkets))/float64(len(markets))*100)

	if len(completeAssets) > 0 {
		fmt.Printf("    Triangular assets: %v\n", completeAssets)
		fmt.Printf("    Example cycle: USDT → %s → USDC → USDT\n", completeAssets[0])
	} else {
		fmt.Printf("     WARNING: No assets found with both USDT and USDC pairs!\n")
		fmt.Printf("   💡 Suggestion: Check if CoinEx supports USDC markets\n")

		// Show what we do have for debugging
		if len(usdtMarkets) > 0 {
			usdtSample := make([]string, 0, 5)
			for asset := range usdtMarkets {
				if len(usdtSample) < 5 {
					usdtSample = append(usdtSample, asset)
				}
			}
			fmt.Printf("    Sample USDT assets: %v\n", usdtSample)
		}

		if len(usdcMarkets) > 0 {
			usdcSample := make([]string, 0, 5)
			for asset := range usdcMarkets {
				if len(usdcSample) < 5 {
					usdcSample = append(usdcSample, asset)
				}
			}
			fmt.Printf("   💎 Sample USDC assets: %v\n", usdcSample)
		}
	}

	return triangularMarkets, completeAssets
}

// filterMarketsByRealActivity filters markets based on real 24H volume data from CoinEx
func (c *coinexClient) filterMarketsByRealActivity(markets []string, tickerData map[string]interface{}) ([]string, []string) {
	fmt.Printf("    Analyzing real market activity for %d markets...\n", len(markets))

	// Extract ticker data
	data, ok := tickerData["data"].(map[string]interface{})
	if !ok {
		fmt.Printf("     Invalid ticker data format, falling back to heuristic filtering\n")
		return c.validateMarketActivity(markets)
	}

	tickers, ok := data["ticker"].(map[string]interface{})
	if !ok {
		fmt.Printf("     Invalid ticker format, falling back to heuristic filtering\n")
		return c.validateMarketActivity(markets)
	}

	activeMarkets := []string{}
	deadMarkets := []string{}

	// Minimum 24H volume threshold (in USD value)
	minVolumeThreshold := 1000.0 // $1000 minimum 24H volume

	for _, market := range markets {
		marketData, exists := tickers[market]
		if !exists {
			deadMarkets = append(deadMarkets, market)
			if len(deadMarkets) <= 5 {
				fmt.Printf("   💀 Dead: %s (no ticker data)\n", market)
			}
			continue
		}

		marketInfo, ok := marketData.(map[string]interface{})
		if !ok {
			deadMarkets = append(deadMarkets, market)
			continue
		}

		volumeStr, ok := marketInfo["vol"].(string)
		if !ok {
			deadMarkets = append(deadMarkets, market)
			continue
		}

		volume, err := strconv.ParseFloat(volumeStr, 64)
		if err != nil {
			deadMarkets = append(deadMarkets, market)
			continue
		}

		// Get last price to calculate USD volume
		lastPriceStr, ok := marketInfo["last"].(string)
		var usdVolume float64
		if ok {
			lastPrice, err := strconv.ParseFloat(lastPriceStr, 64)
			if err == nil {
				for _, quote := range c.config.QuoteCurrencies {
					if strings.HasSuffix(market, quote) {
						usdVolume = volume * lastPrice
						break
					}
				}
			}
		}

		if usdVolume >= minVolumeThreshold {
			activeMarkets = append(activeMarkets, market)
			if len(activeMarkets) <= 5 {
				fmt.Printf("    Active: %s (24H vol: $%.0f)\n", market, usdVolume)
			}
		} else {
			deadMarkets = append(deadMarkets, market)
			if len(deadMarkets) <= 5 {
				fmt.Printf("   💀 Dead: %s (24H vol: $%.0f < $%.0f threshold)\n", market, usdVolume, minVolumeThreshold)
			}
		}
	}

	if len(activeMarkets) > 5 {
		fmt.Printf("   ... and %d more active markets\n", len(activeMarkets)-5)
	}
	if len(deadMarkets) > 5 {
		fmt.Printf("   ... and %d more dead markets\n", len(deadMarkets)-5)
	}

	fmt.Printf("    Volume Analysis Complete:\n")
	fmt.Printf("     - Active markets (>$%.0f/24h): %d\n", minVolumeThreshold, len(activeMarkets))
	fmt.Printf("     - Dead markets (<$%.0f/24h): %d\n", minVolumeThreshold, len(deadMarkets))

	return activeMarkets, deadMarkets
}

// validateMarketActivity tests markets for actual trading activity before subscription
func (c *coinexClient) validateMarketActivity(markets []string) ([]string, []string) {
	fmt.Printf("    Testing %d markets for trading activity...\n", len(markets))

	activeMarkets := []string{}
	deadMarkets := []string{}

	// For now, we'll use a simple heuristic based on market naming patterns
	// In production, you'd want to call CoinEx API to check 24h volume or recent trades

	for _, market := range markets {
		isActive := c.isMarketActive(market)

		if isActive {
			activeMarkets = append(activeMarkets, market)
			if len(activeMarkets) <= 5 { // Log first few
				fmt.Printf("    Active: %s\n", market)
			}
		} else {
			deadMarkets = append(deadMarkets, market)
			if len(deadMarkets) <= 5 { // Log first few
				fmt.Printf("   💀 Dead: %s (filtered out)\n", market)
			}
		}
	}

	if len(activeMarkets) > 5 {
		fmt.Printf("   ... and %d more active markets\n", len(activeMarkets)-5)
	}
	if len(deadMarkets) > 5 {
		fmt.Printf("   ... and %d more dead markets\n", len(deadMarkets)-5)
	}

	return activeMarkets, deadMarkets
}

// isMarketActive determines if a market is likely to be active based on heuristics
func (c *coinexClient) isMarketActive(market string) bool {
	// Major liquid assets (high priority - likely active)
	majorAssets := []string{"BTC", "ETH", "BNB", "SOL", "ADA", "DOT", "AVAX", "MATIC", "LINK", "UNI", "DOGE", "LTC", "XRP"}

	for _, major := range majorAssets {
		if strings.Contains(market, major) {
			return true // Major assets are almost always active
		}
	}

	// Critical pairs (always include)
	if market == "USDCUSDT" || market == "USDTUSDC" {
		return true
	}

	// USDC markets are often illiquid on CoinEx (be selective)
	if strings.HasSuffix(market, "USDC") {
		// Only include USDC pairs for major assets
		asset := strings.TrimSuffix(market, "USDC")
		for _, major := range majorAssets {
			if asset == major {
				return true
			}
		}
		return false // Most USDC pairs are dead on CoinEx
	}

	// USDT markets are generally more liquid
	if strings.HasSuffix(market, "USDT") {
		return true
	}

	return false // Conservative approach - exclude unknown patterns
}

// filterCompleteAssetsByActiveMarkets filters complete assets to only include those with active markets
func (c *coinexClient) filterCompleteAssetsByActiveMarkets(completeAssets []string, activeMarkets []string) []string {
	activeMarketSet := make(map[string]bool)
	for _, market := range activeMarkets {
		activeMarketSet[market] = true
	}

	filteredAssets := []string{}

	for _, asset := range completeAssets {
		hasActiveUSDT := activeMarketSet[asset+"USDT"]
		hasActiveUSDC := activeMarketSet[asset+"USDC"]

		if hasActiveUSDT && hasActiveUSDC {
			filteredAssets = append(filteredAssets, asset)
			fmt.Printf("    Asset %s: Both %sUSDT and %sUSDC are active\n", asset, asset, asset)
		} else {
			fmt.Printf("    Asset %s excluded: ", asset)
			if !hasActiveUSDT {
				fmt.Printf("%sUSDT inactive ", asset)
			}
			if !hasActiveUSDC {
				fmt.Printf("%sUSDC inactive ", asset)
			}
			fmt.Printf("\n")
		}
	}

	return filteredAssets
}

// manageRealFOKOrderLifecycle implements FOK behavior using limit orders with rapid status checking
func (c *coinexClient) manageRealFOKOrderLifecycle(tracker *FOKOrderTracker, orderResultChan chan<- *OrderResult) {
	log.Printf("🎯 FOK LIFECYCLE START | Mode: %s | Market: %s | Type: %s | Amount: %.6f | Price: %.8f",
		map[bool]string{true: "SIMULATION", false: "REAL API"}[c.config.SimulationMode],
		tracker.Market, tracker.Type, tracker.Amount, tracker.Price)

	// Place limit order instead of FOK
	log.Printf("📤 PLACING FOK ORDER | Market: %s | Type: %s | Amount: %.6f | Price: %.8f",
		tracker.Market, tracker.Type, tracker.Amount, tracker.Price)

	actualOrderID, err := c.placeRealLimitOrder(tracker)
	if err != nil {
		log.Printf("❌ ORDER PLACEMENT FAILED | Market: %s | Error: %v", tracker.Market, err)
		result := &OrderResult{
			OrderID:       tracker.OrderID,
			Market:        tracker.Market,
			Type:          tracker.Type,
			Amount:        tracker.Amount,
			Price:         tracker.Price,
			Status:        OrderStatusFailed,
			FilledAmount:  0,
			AvgPrice:      0,
			Fee:           0,
			FeeCurrency:   "",
			ExecutionTime: time.Since(tracker.CreatedAt).Milliseconds(),
			ErrorMessage:  fmt.Sprintf("Order placement failed: %v", err),
			Timestamp:     time.Now(),
		}
		orderResultChan <- result
		return
	}

	log.Printf("✅ ORDER PLACED | ID: %s | Market: %s", actualOrderID, tracker.Market)

	// Update tracker with real order ID
	tracker.mu.Lock()
	oldID := tracker.OrderID
	tracker.OrderID = actualOrderID
	tracker.mu.Unlock()

	c.orderTrackingManager.RemoveTracker(oldID)
	c.orderTrackingManager.AddTracker(tracker)

	// FOK behavior: Check status rapidly for configurable timeout
	timeoutDuration := time.Duration(c.config.FOKOrderSettings.FOKTimeoutSeconds) * time.Second
	pollingInterval := time.Duration(1000/c.config.FOKOrderSettings.FOKPollingFrequencyHz) * time.Millisecond

	timeout := time.After(timeoutDuration)
	ticker := time.NewTicker(pollingInterval) // Use configurable polling frequency
	defer ticker.Stop()

	log.Printf("⏱️ FOK CONFIG | Timeout: %ds | Polling: %.1fHz (every %dms)",
		c.config.FOKOrderSettings.FOKTimeoutSeconds,
		c.config.FOKOrderSettings.FOKPollingFrequencyHz,
		int(pollingInterval.Milliseconds()))

	pollCount := 0

	for {
		select {
		case <-ticker.C:
			pollCount++
			log.Printf("🔍 POLLING ORDER | ID: %s | Attempt: %d", actualOrderID, pollCount)

			// Poll synchronously (no goroutine)
			isFilled, err := c.pollRealOrderStatus(tracker)
			if err != nil {
				log.Printf("⚠️ POLL ERROR | ID: %s | Error: %v", actualOrderID, err)
				continue // Continue polling on error
			}

			// Update tracker status
			tracker.mu.Lock()
			if isFilled {
				tracker.Status = OrderStatusFilled
			} else {
				tracker.Status = OrderStatusPending
			}
			tracker.LastChecked = time.Now()
			tracker.mu.Unlock()

			log.Printf("📊 ORDER STATUS | ID: %s | Filled: %v", actualOrderID, isFilled)

			// If order is filled, return success immediately
			if isFilled {
				log.Printf("✅ ORDER FILLED | ID: %s", actualOrderID)
				// Create a simple success result
				result := &OrderResult{
					OrderID:       actualOrderID,
					Market:        tracker.Market,
					Type:          tracker.Type,
					Amount:        tracker.Amount,
					Price:         tracker.Price,
					Status:        OrderStatusFilled,
					FilledAmount:  tracker.Amount, // Assume full fill
					AvgPrice:      tracker.Price,
					Fee:           0,
					FeeCurrency:   "",
					ExecutionTime: time.Since(tracker.CreatedAt).Milliseconds(),
					Timestamp:     time.Now(),
				}
				orderResultChan <- result
				return
			}

		case <-timeout:
			// Configurable timeout - cancel order and return failure
			log.Printf("⏰ FOK TIMEOUT | ID: %s | Cancelling order after %d seconds",
				actualOrderID, c.config.FOKOrderSettings.FOKTimeoutSeconds)

			// Cancel the order
			cancelErr := c.cancelRealOrder(tracker)
			if cancelErr != nil {
				log.Printf("⚠️ CANCELLATION FAILED | ID: %s | Error: %v", actualOrderID, cancelErr)
			}

			result := &OrderResult{
				OrderID:       actualOrderID,
				Market:        tracker.Market,
				Type:          tracker.Type,
				Amount:        tracker.Amount,
				Price:         tracker.Price,
				Status:        OrderStatusCancelled,
				FilledAmount:  0,
				AvgPrice:      0,
				Fee:           0,
				FeeCurrency:   "",
				ExecutionTime: time.Since(tracker.CreatedAt).Milliseconds(),
				ErrorMessage:  fmt.Sprintf("FOK timeout - order not filled within %d seconds", c.config.FOKOrderSettings.FOKTimeoutSeconds),
				Timestamp:     time.Now(),
			}
			orderResultChan <- result
			return
		}
	}
}

// placeRealLimitOrder places a real limit order via CoinEx API v2
func (c *coinexClient) placeRealLimitOrder(tracker *FOKOrderTracker) (string, error) {
	log.Printf("📤 PLACING LIMIT ORDER | Market: %s | Type: %s | Amount: %.6f | Price: %.8f",
		tracker.Market, tracker.Type, tracker.Amount, tracker.Price)

	// Use v2 API for spot trading
	timestamp := time.Now().UnixMilli()
	method := "POST"
	requestPath := "/v2/spot/order"

	// Prepare JSON body for v2 API - Use limit order type
	orderData := map[string]interface{}{
		"market":      tracker.Market,
		"market_type": "SPOT",
		"side":        tracker.Type,
		"type":        "limit",
		"amount":      fmt.Sprintf("%.8f", tracker.Amount),
		"price":       fmt.Sprintf("%.8f", tracker.Price),
		"client_id":   fmt.Sprintf("LIMIT_%s_%d", tracker.Market, timestamp),
		"is_hide":     false,
		"stp_mode":    "both",
	}

	// Convert to JSON string
	bodyBytes, err := json.Marshal(orderData)
	if err != nil {
		log.Printf("❌ ORDER MARSHAL ERROR | Market: %s | Error: %v", tracker.Market, err)
		return "", fmt.Errorf("failed to marshal order data: %w", err)
	}
	body := string(bodyBytes)

	log.Printf("📋 ORDER DATA | Market: %s | Body: %s", tracker.Market, body)

	// Generate v2 signature
	signature := c.generateRESTSignature(method, requestPath, "", body, timestamp)

	// Set v2 authentication headers
	authHeaders := map[string]string{
		"X-COINEX-KEY":       c.apiKey,
		"X-COINEX-SIGN":      signature,
		"X-COINEX-TIMESTAMP": strconv.FormatInt(timestamp, 10),
	}

	// Merge auth headers with existing headers
	c.httpClient.mergeHeaders(authHeaders)

	// Prepare request parameters
	requestParams := map[string]string{
		"url":    "https://api.coinex.com" + requestPath,
		"method": method,
		"body":   body,
	}

	log.Printf("📤 SENDING ORDER REQUEST | Market: %s | URL: %s", tracker.Market, requestParams["url"])

	// Make API call
	response, err := c.httpClient.performRequest(requestParams, POST)
	if err != nil {
		log.Printf("❌ ORDER REQUEST FAILED | Market: %s | Error: %v", tracker.Market, err)
		return "", fmt.Errorf("API request failed: %w", err)
	}

	log.Printf("📥 ORDER RESPONSE | Market: %s | Response: %+v", tracker.Market, response)

	// Check response
	if code, ok := response["code"].(float64); !ok || code != 0 {
		message, _ := response["message"].(string)
		log.Printf("❌ ORDER API ERROR | Market: %s | Code: %.0f | Message: %s", tracker.Market, code, message)
		return "", fmt.Errorf("CoinEx API error: code=%.0f, message=%s", code, message)
	}

	// Extract order data
	data, ok := response["data"].(map[string]interface{})
	if !ok {
		log.Printf("❌ ORDER RESPONSE FORMAT ERROR | Market: %s | Missing data field", tracker.Market)
		return "", fmt.Errorf("invalid response format: missing data")
	}

	// For limit orders, we need to get the order_id for polling
	if orderIDValue, exists := data["order_id"]; exists {
		if orderIDStr, ok := orderIDValue.(string); ok && orderIDStr != "" {
			log.Printf("✅ ORDER PLACED SUCCESSFULLY | Market: %s | ID: %s", tracker.Market, orderIDStr)
			return orderIDStr, nil
		} else if orderIDFloat, ok := orderIDValue.(float64); ok {
			orderIDStr := fmt.Sprintf("%.0f", orderIDFloat)
			log.Printf("✅ ORDER PLACED SUCCESSFULLY | Market: %s | ID: %s", tracker.Market, orderIDStr)
			return orderIDStr, nil
		}
	}

	log.Printf("❌ ORDER ID MISSING | Market: %s | Data: %+v", tracker.Market, data)
	return "", fmt.Errorf("invalid limit order response format - no order_id")
}

// pollRealOrderStatus polls the real order status from CoinEx API
func (c *coinexClient) pollRealOrderStatus(tracker *FOKOrderTracker) (bool, error) {
	// Poll up to 4 times
	for i := 0; i < 4; i++ {
		timestamp := time.Now().UnixMilli()
		method := "GET"
		requestPath := "/v2/spot/order-status" // Use v2 API path

		// Build query string for order status API
		queryParams := map[string]string{
			"market":   tracker.Market,
			"order_id": tracker.OrderID,
		}

		// Build query string
		var queryParts []string
		for key, value := range queryParams {
			queryParts = append(queryParts, key+"="+value)
		}
		queryString := strings.Join(queryParts, "&")

		log.Printf("🔍 POLLING ORDER | ID: %s | Attempt: %d/4", tracker.OrderID, i+1)

		// For GET requests: method + request_path + timestamp (no body)
		fullRequestPath := requestPath + "?" + queryString
		signature := c.generateRESTSignature(method, fullRequestPath, "", "", timestamp)

		// Set authentication headers
		authHeaders := map[string]string{
			"X-COINEX-KEY":       c.apiKey,
			"X-COINEX-SIGN":      signature,
			"X-COINEX-TIMESTAMP": strconv.FormatInt(timestamp, 10),
		}

		// Merge auth headers with existing headers
		c.httpClient.mergeHeaders(authHeaders)

		// Prepare request parameters
		requestParams := map[string]string{
			"url":    "https://api.coinex.com" + fullRequestPath,
			"method": method,
		}

		// Make API call
		response, err := c.httpClient.performRequest(requestParams, GET)
		if err != nil {
			log.Printf("⚠️ POLL ERROR | ID: %s | Attempt: %d/4 | Error: %v", tracker.OrderID, i+1, err)
			continue // Try next attempt
		}

		// Check response
		if code, ok := response["code"].(float64); !ok || code != 0 {
			message, _ := response["message"].(string)
			log.Printf("⚠️ POLL API ERROR | ID: %s | Attempt: %d/4 | Code: %.0f | Message: %s", tracker.OrderID, i+1, code, message)
			continue // Try next attempt
		}

		// Extract order data
		data, ok := response["data"].(map[string]interface{})
		if !ok {
			log.Printf("⚠️ POLL RESPONSE FORMAT ERROR | ID: %s | Attempt: %d/4", tracker.OrderID, i+1)
			continue // Try next attempt
		}

		// Check status - only care about "done"
		if statusStr, ok := data["status"].(string); ok {
			if statusStr == "done" {
				log.Printf("✅ ORDER FILLED | ID: %s | Attempt: %d/4", tracker.OrderID, i+1)
				return true, nil // Order is filled
			}
		}

		// If not filled, wait a bit before next attempt
		if i < 3 { // Don't sleep after last attempt
			time.Sleep(200 * time.Millisecond) // 200ms between attempts
		}
	}

	// If we get here, order is not filled after 4 attempts
	log.Printf("❌ ORDER NOT FILLED | ID: %s | After 4 attempts", tracker.OrderID)
	return false, nil
}

// cancelRealOrder cancels a real order via CoinEx API v2
func (c *coinexClient) cancelRealOrder(tracker *FOKOrderTracker) error {
	log.Printf("🛑 CANCELLING ORDER | ID: %s | Market: %s", tracker.OrderID, tracker.Market)

	// Use correct v2 endpoint for order cancellation according to CoinEx docs
	timestamp := time.Now().UnixMilli()
	method := "POST"
	requestPath := "/v2/spot/cancel-order" // Correct v2 endpoint from docs

	// Prepare JSON body for v2 cancel API
	cancelData := map[string]interface{}{
		"market":      tracker.Market,
		"market_type": "SPOT",
		"order_id":    tracker.OrderID,
	}

	// Convert to JSON string
	bodyBytes, err := json.Marshal(cancelData)
	if err != nil {
		log.Printf("❌ CANCELLATION MARSHAL ERROR | ID: %s | Error: %v", tracker.OrderID, err)
		return fmt.Errorf("failed to marshal cancel data: %w", err)
	}
	body := string(bodyBytes)

	// Generate v2 signature
	signature := c.generateRESTSignature(method, requestPath, "", body, timestamp)

	// Set v2 authentication headers
	authHeaders := map[string]string{
		"X-COINEX-KEY":       c.apiKey,
		"X-COINEX-SIGN":      signature,
		"X-COINEX-TIMESTAMP": strconv.FormatInt(timestamp, 10),
		"Content-Type":       "application/json",
	}

	// Merge auth headers with existing headers
	c.httpClient.mergeHeaders(authHeaders)

	// Prepare request parameters
	requestParams := map[string]string{
		"url":    "https://api.coinex.com" + requestPath,
		"method": method,
		"body":   body,
	}

	log.Printf("📤 SENDING CANCELLATION REQUEST | ID: %s | URL: %s", tracker.OrderID, requestParams["url"])

	// Make API call
	response, err := c.httpClient.performRequest(requestParams, POST)
	if err != nil {
		log.Printf("❌ CANCELLATION REQUEST FAILED | ID: %s | Error: %v", tracker.OrderID, err)
		return fmt.Errorf("order cancellation API request failed: %w", err)
	}

	log.Printf("📥 CANCELLATION RESPONSE | ID: %s | Response: %+v", tracker.OrderID, response)

	// Check response
	if code, ok := response["code"].(float64); !ok || code != 0 {
		message, _ := response["message"].(string)
		log.Printf("❌ CANCELLATION API ERROR | ID: %s | Code: %.0f | Message: %s", tracker.OrderID, code, message)
		return fmt.Errorf("CoinEx API error: code=%.0f, message=%s", code, message)
	}

	log.Printf("✅ ORDER CANCELLED | ID: %s | Market: %s", tracker.OrderID, tracker.Market)

	// Update tracker status
	tracker.mu.Lock()
	tracker.Status = OrderStatusCancelled
	tracker.IsActive = false
	tracker.mu.Unlock()

	return nil
}

// Helper methods for building request data

// buildFormData builds form-encoded data from parameters
func (c *coinexClient) buildFormData(params map[string]string) string {
	var parts []string
	for key, value := range params {
		parts = append(parts, key+"="+value)
	}
	return strings.Join(parts, "&")
}

// buildQueryString builds query string from parameters
func (c *coinexClient) buildQueryString(params map[string]string) string {
	// Sort parameters alphabetically (required for signature)
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var parts []string
	for _, key := range keys {
		parts = append(parts, key+"="+params[key])
	}
	return strings.Join(parts, "&")
}

// EnsureCriticalMarketData ensures critical markets have data via WebSocket first, then REST API fallback
func (c *coinexClient) EnsureCriticalMarketData(criticalMarkets []string, marketDepths *MarketDepths) {
	log.Printf(" Ensuring quote currency markets have data: %v", criticalMarkets)

	for _, market := range criticalMarkets {
		// First, check if market already has data in marketDepths (from WebSocket)
		if orderBook, exists := marketDepths.Load(market); exists && orderBook != nil {
			if len(orderBook.Bids) > 0 && len(orderBook.Asks) > 0 {
				// Market already has WebSocket data, skip
				log.Printf(" %s already has WebSocket data, skipping", market)
				continue
			}
		}

		// Try to subscribe to the market via WebSocket first
		log.Printf(" Attempting WebSocket subscription for %s...", market)
		if err := c.httpClient.SubscribeWebSocketWithRetry([]string{market}, 2); err != nil {
			log.Printf(" WebSocket subscription failed for %s: %v", market, err)
			// Add to retry queue for later
			c.httpClient.AddFailedMarket(market)
		} else {
			log.Printf(" WebSocket subscription successful for %s", market)
			// Wait a moment for data to arrive
			time.Sleep(1 * time.Second)

			// Check if we now have WebSocket data
			if orderBook, exists := marketDepths.Load(market); exists && orderBook != nil {
				if len(orderBook.Bids) > 0 && len(orderBook.Asks) > 0 {
					log.Printf(" %s data available via WebSocket | Bids: %d | Asks: %d",
						market, len(orderBook.Bids), len(orderBook.Asks))
					continue
				}
			}
		}

		// If WebSocket failed or no data received, fall back to REST API
		log.Printf(" Fetching %s via REST API (WebSocket data missing)...", market)

		// Use the HttpClient's REST API fallback method
		restOrderBook, err := c.httpClient.GetOrderBookREST(market)
		if err != nil {
			log.Printf(" Failed to get %s via REST: %v", market, err)
			continue
		}

		if len(restOrderBook.Bids) > 0 && len(restOrderBook.Asks) > 0 {
			// Store in marketDepths so arbitrage engine can find it
			marketDepths.Store(market, restOrderBook)

			// Also store in HttpClient's market data cache for consistency
			c.httpClient.marketDataMu.Lock()
			c.httpClient.marketData[market] = restOrderBook
			c.httpClient.marketDataMu.Unlock()

			log.Printf(" Quote currency market %s data restored via REST | Bids: %d | Asks: %d",
				market, len(restOrderBook.Bids), len(restOrderBook.Asks))
		} else {
			log.Printf("  Quote currency market %s has empty order book even via REST", market)
		}
	}
}

// FetchCriticalMarketsViaREST fetches order book data for critical markets via REST API
// This is used as a primary data source for quote currencies and other critical markets
func (c *coinexClient) FetchCriticalMarketsViaREST(markets []string, marketDepths *MarketDepths) {
	log.Printf(" Fetching critical markets via REST API: %v", markets)

	successCount := 0
	failedCount := 0

	for _, market := range markets {
		log.Printf(" Fetching %s via REST API...", market)

		restOrderBook, err := c.httpClient.GetOrderBookREST(market)
		if err != nil {
			log.Printf(" Failed to fetch %s via REST: %v", market, err)
			failedCount++
			continue
		}

		if len(restOrderBook.Bids) > 0 && len(restOrderBook.Asks) > 0 {
			// Store in marketDepths
			marketDepths.Store(market, restOrderBook)

			// Also store in HttpClient's market data cache for consistency
			c.httpClient.marketDataMu.Lock()
			c.httpClient.marketData[market] = restOrderBook
			c.httpClient.marketDataMu.Unlock()

			log.Printf(" %s fetched via REST | Bids: %d | Asks: %d",
				market, len(restOrderBook.Bids), len(restOrderBook.Asks))
			successCount++
		} else {
			log.Printf("  %s has empty order book via REST", market)
			failedCount++
		}

		// Small delay to avoid overwhelming the API
		time.Sleep(100 * time.Millisecond)
	}

	log.Printf(" REST API fetch complete: %d success, %d failed", successCount, failedCount)
}

// GetQuoteCurrencyMarkets returns all markets for the configured quote currencies
func (c *coinexClient) GetQuoteCurrencyMarkets() []string {
	var quoteMarkets []string

	// Get all available markets from CoinEx
	marketListString, err := c.GetMarketList()
	if err != nil {
		log.Printf(" Failed to get market list for quote currencies: %v", err)
		return quoteMarkets
	}

	var marketList map[string]interface{}
	err = json.Unmarshal([]byte(marketListString), &marketList)
	if err != nil {
		log.Printf(" Failed to unmarshal market list: %v", err)
		return quoteMarkets
	}

	markets := marketList["data"].([]interface{})

	// Find markets for configured quote currencies
	for _, market := range markets {
		marketStr := market.(string)
		for _, quote := range c.allowedMarkets {
			if strings.HasSuffix(marketStr, quote) {
				quoteMarkets = append(quoteMarkets, marketStr)
				break
			}
		}
	}

	log.Printf(" Found %d markets for quote currencies %v", len(quoteMarkets), c.allowedMarkets)
	return quoteMarkets
}

// CriticalMarketPriceManager handles critical market data efficiently
type CriticalMarketPriceManager struct {
	config        *Config
	assumedPrices map[string]float64 // Critical market -> assumed price
}

// NewCriticalMarketPriceManager creates a new critical market price manager
func NewCriticalMarketPriceManager(coinexClient *coinexClient, config *Config) *CriticalMarketPriceManager {
	// Initialize assumed prices for critical markets
	assumedPrices := make(map[string]float64)
	for _, market := range config.CriticalMarkets {
		// For stablecoin pairs like USDCUSDT, assume price of 1.0
		if strings.Contains(market, "USDC") && strings.Contains(market, "USDT") {
			assumedPrices[market] = 1.0
		} else {
			// For other critical markets, use a reasonable default
			assumedPrices[market] = 1.0
		}
	}

	// Extract keys for logging
	keys := make([]string, 0, len(assumedPrices))
	for key := range assumedPrices {
		keys = append(keys, key)
	}

	log.Printf("🔧 CRITICAL MARKET PRICE MANAGER | Initialized with %d markets: %v", len(assumedPrices), keys)

	return &CriticalMarketPriceManager{
		config:        config,
		assumedPrices: assumedPrices,
	}
}

// IsCriticalMarket checks if a market is critical (should be assumed to exist)
func (cmm *CriticalMarketPriceManager) IsCriticalMarket(market string) bool {
	for _, critical := range cmm.config.CriticalMarkets {
		if market == critical {
			return true
		}
	}
	return false
}

// AssumeCriticalMarketExists returns true for critical markets (no checks needed)
func (cmm *CriticalMarketPriceManager) AssumeCriticalMarketExists(market string) bool {
	return cmm.IsCriticalMarket(market)
}

// GetAssumedPrice returns the assumed price for a critical market
func (cmm *CriticalMarketPriceManager) GetAssumedPrice(market string) (float64, bool) {
	if price, exists := cmm.assumedPrices[market]; exists {
		return price, true
	}
	return 0.0, false
}

// GetCriticalMarketSnapshot returns empty snapshot for critical markets (we don't need price data)
func (cmm *CriticalMarketPriceManager) GetCriticalMarketSnapshot() map[string]*OrderBook {
	// Return empty snapshot - we don't need price data for critical markets
	// The arbitrage engine will handle critical markets differently
	return make(map[string]*OrderBook)
}

// GetOpenOrders gets all open orders from the exchange
func (c *coinexClient) GetOpenOrders() ([]map[string]interface{}, error) {
	// Use v2 API for getting open orders
	timestamp := time.Now().UnixMilli()
	method := "GET"
	requestPath := "/v2/spot/pending-order" // Get unfilled orders endpoint

	// Generate v2 signature (no body for GET request)
	signature := c.generateRESTSignature(method, requestPath, "", "", timestamp)

	// Set v2 authentication headers
	authHeaders := map[string]string{
		"X-COINEX-KEY":       c.apiKey,
		"X-COINEX-SIGN":      signature,
		"X-COINEX-TIMESTAMP": strconv.FormatInt(timestamp, 10),
		"Content-Type":       "application/json",
	}

	// Merge auth headers with existing headers
	c.httpClient.mergeHeaders(authHeaders)

	// Prepare request parameters
	requestParams := map[string]string{
		"url":    "https://api.coinex.com" + requestPath + "?market_type=SPOT&limit=100", // Get up to 100 orders
		"method": method,
	}

	log.Printf("GETTING OPEN ORDERS | Fetching unfilled orders from exchange")

	// Make API call
	response, err := c.httpClient.performRequest(requestParams, GET)
	if err != nil {
		return nil, fmt.Errorf("get open orders API request failed: %w", err)
	}

	// Check response
	if code, ok := response["code"].(float64); !ok || code != 0 {
		message, _ := response["message"].(string)
		return nil, fmt.Errorf("CoinEx get open orders API error: code=%.0f, message=%s", code, message)
	}

	// Extract orders from response
	data, ok := response["data"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid response format: missing data array")
	}

	var orders []map[string]interface{}
	for _, order := range data {
		if orderMap, ok := order.(map[string]interface{}); ok {
			orders = append(orders, orderMap)
		}
	}

	log.Printf("✅ OPEN ORDERS FETCHED | Found %d unfilled orders", len(orders))
	return orders, nil
}

// CancelAllOrders cancels all open orders for a specific market
func (c *coinexClient) CancelAllOrders(market string) error {
	// Use v2 API for canceling all orders
	timestamp := time.Now().UnixMilli()
	method := "POST"
	requestPath := "/v2/spot/cancel-all-order" // Cancel all orders endpoint

	// Prepare JSON body for v2 API
	cancelData := map[string]interface{}{
		"market":      market,
		"market_type": "SPOT", // Use spot trading
	}

	// Convert to JSON string
	bodyBytes, err := json.Marshal(cancelData)
	if err != nil {
		return fmt.Errorf("failed to marshal cancel all orders data: %w", err)
	}
	body := string(bodyBytes)

	// Generate v2 signature
	signature := c.generateRESTSignature(method, requestPath, "", body, timestamp)

	// Set v2 authentication headers
	authHeaders := map[string]string{
		"X-COINEX-KEY":       c.apiKey,
		"X-COINEX-SIGN":      signature,
		"X-COINEX-TIMESTAMP": strconv.FormatInt(timestamp, 10),
		"Content-Type":       "application/json",
	}

	// Merge auth headers with existing headers
	c.httpClient.mergeHeaders(authHeaders)

	// Prepare request parameters
	requestParams := map[string]string{
		"url":    "https://api.coinex.com" + requestPath,
		"method": method,
		"body":   body,
	}

	log.Printf("CANCELLING ALL ORDERS | Market: %s", market)

	// Make API call
	response, err := c.httpClient.performRequest(requestParams, POST)
	if err != nil {
		return fmt.Errorf("cancel all orders API request failed: %w", err)
	}

	// Check response
	if code, ok := response["code"].(float64); !ok || code != 0 {
		message, _ := response["message"].(string)
		return fmt.Errorf("CoinEx cancel all orders API error: code=%.0f, message=%s", code, message)
	}

	log.Printf("✅ ALL ORDERS CANCELLED | Market: %s", market)
	return nil
}
