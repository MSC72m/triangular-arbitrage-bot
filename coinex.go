package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
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
	FilledValue   float64     `json:"filled_value"`
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

type coinexClient struct {
	httpClient                 *HttpClient
	apiKey                     string
	secretID                   string
	baseUrl                    string
	allowedMarkets             []string
	config                     *Config
	criticalMarketPriceManager *CriticalMarketPriceManager

	// Order execution tracking (simplified)
	lastOrderTime time.Time
	mu            sync.RWMutex
}

func NewCoinexClient(httpClient *HttpClient, config *Config) *coinexClient {
	fmt.Printf("Loading CoinEx client with quote currencies from config: %v\n", config.QuoteCurrencies)
	client := &coinexClient{
		httpClient:     httpClient,
		apiKey:         config.APIKey,   // Use API key as API key
		secretID:       config.SecretID, // Use secret ID as secret ID
		allowedMarkets: config.QuoteCurrencies,
		baseUrl:        config.APIBaseURL, // Use v1 API for market operations
		config:         config,            // Store config reference
	}

	// Initialize critical market price manager after client is created
	client.criticalMarketPriceManager = NewCriticalMarketPriceManager(client, config)

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
	// For GET requests: request_path should include query parameters
	// For POST requests: request_path is just the path, body is separate

	// Create the string to sign: method + request_path + body + timestamp
	var stringToSign string
	if body != "" {
		// For POST/PUT requests with body, include the body as a string literal
		stringToSign = method + requestPath + body + strconv.FormatInt(timestamp, 10)
	} else {
		// For GET/DELETE requests without body
		stringToSign = method + requestPath + strconv.FormatInt(timestamp, 10)
	}

	// Create HMAC-SHA256 signature
	h := hmac.New(sha256.New, []byte(c.secretID))
	h.Write([]byte(stringToSign))
	signature := hex.EncodeToString(h.Sum(nil))

	// Debug logging
	log.Printf("🔐 REST SIGNATURE DEBUG | Method: %s | Path: %s | Query: %s | Body: %s | Timestamp: %d",
		method, requestPath, queryString, body, timestamp)
	log.Printf("🔐 REST SIGNATURE DEBUG | String to sign: %s", stringToSign)
	log.Printf("🔐 REST SIGNATURE DEBUG | Signature: %s", signature)
	log.Printf("🔐 REST SIGNATURE DEBUG | Secret key length: %d", len(c.secretID))

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

// TestSignatureGeneration tests the signature generation for debugging
func (c *coinexClient) TestSignatureGeneration() {
	log.Printf("🧪 TESTING SIGNATURE GENERATION")

	// Test GET request signature (like order status polling)
	timestamp := int64(1700490703564) // Use the example timestamp from docs
	method := "GET"
	requestPath := "/v2/spot/order-status?market=BTCUSDT&order_id=12345"

	signature := c.generateRESTSignature(method, requestPath, "", "", timestamp)
	log.Printf("🧪 GET SIGNATURE TEST | Expected format: GET + path + timestamp")
	log.Printf("🧪 GET SIGNATURE TEST | String to sign: %s%s%d", method, requestPath, timestamp)
	log.Printf("🧪 GET SIGNATURE TEST | Generated signature: %s", signature)

	// Test POST request signature (like order placement)
	postMethod := "POST"
	postPath := "/v2/spot/order"
	postBody := `{"market":"BTCUSDT","type":"buy","amount":"0.001","price":"10000"}`

	postSignature := c.generateRESTSignature(postMethod, postPath, "", postBody, timestamp)
	log.Printf("🧪 POST SIGNATURE TEST | Expected format: POST + path + body + timestamp")
	log.Printf("🧪 POST SIGNATURE TEST | String to sign: %s%s%s%d", postMethod, postPath, postBody, timestamp)
	log.Printf("🧪 POST SIGNATURE TEST | Generated signature: %s", postSignature)
}

// PlaceFOKOrder places a Fill-or-Kill order with automatic simulation/real API switching and spending controls
func (c *coinexClient) PlaceFOKOrder(market, orderType string, amount, price float64, orderResultChan chan<- *OrderResult) *FOKOrderTracker {
	log.Printf("🎯 PLACE FOK ORDER START | Market: %s | Type: %s | Amount: %.6f | Price: %.8f",
		market, orderType, amount, price)
	log.Printf("⚙️ FOK CONFIG | Timeout: %ds | Polling: %.1f Hz | Max Retries: %d",
		c.config.FOKOrderSettings.FOKTimeoutSeconds,
		c.config.FOKOrderSettings.FOKPollingFrequencyHz,
		c.config.FOKOrderSettings.MaxRetryAttempts)

	// Check rate limit before placing order
	if !c.checkRateLimit() {
		log.Printf("🚫 RATE LIMIT | Order placement rate limited")
		return nil
	}

	// Calculate order amount
	orderAmount, _, err := c.calculateOrderAmount(amount, price, orderType)
	if err != nil {
		log.Printf("🚫 ORDER REJECTED | %v | Market: %s", err, market)
		return nil
	}

	// Generate order ID
	orderID := fmt.Sprintf("FOK_%s_%d_%d", market, time.Now().UnixNano(), rand.Intn(10000))

	// Create order tracker
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

	// Execute order lifecycle synchronously (no goroutine)
	if c.config.SimulationMode {
		c.manageSimulationOrderLifecycle(tracker, orderResultChan)
	} else {
		c.manageRealFOKOrderLifecycle(tracker, orderResultChan)
	}

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
func (c *coinexClient) calculateOrderAmount(requestedAmount, price float64, orderType string) (float64, float64, error) {

	settings := c.config.OrderExecutionSettings

	var orderAmount float64
	var orderValue float64

	if orderType == "buy" { // This is for Leg 1
		// For buy orders, requestedAmount is the asset amount to buy
		orderAmount = requestedAmount
		if price > 0 {
			orderValue = orderAmount * price // Calculate USDT value
		} else {
			orderValue = orderAmount // This is an estimation for market orders
		}
	} else { // This is for Leg 2 and 3
		// For sell orders, requestedAmount is the asset amount to sell
		orderAmount = requestedAmount
		if price > 0 {
			orderValue = orderAmount * price // Calculate quote currency value
		} else {
			orderValue = orderAmount // This is an estimation for market orders
		}
	}

	// Apply min/max limits
	if orderValue > 0 && orderValue < settings.MinOrderAmount {
		return 0, 0, fmt.Errorf("order value $%.2f below minimum $%.2f", orderValue, settings.MinOrderAmount)
	}
	if orderValue > 0 && orderValue > settings.MaxOrderAmount {
		orderValue = settings.MaxOrderAmount
		if price > 0 {
			orderAmount = orderValue / price
		} else {
			orderAmount = requestedAmount // Keep original amount for market orders
		}
	}

	return orderAmount, orderValue, nil
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

// manageSimulationOrderLifecycle handles simulation orders
func (c *coinexClient) manageSimulationOrderLifecycle(tracker *FOKOrderTracker, orderResultChan chan<- *OrderResult) {
	// Execute order simulation synchronously (no goroutine)
	c.simulateOrderExecution(tracker)

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
	log.Printf("🛑 CANCELLING ALL ACTIVE ORDERS | Mode: %s",
		map[bool]string{true: "SIMULATION", false: "REAL API"}[c.config.SimulationMode])

	// Simplified: no active order tracking needed with sequential execution
	log.Printf("📋 NO ACTIVE ORDERS TO CANCEL | Using sequential execution")
}

// GetOrderExecutionStats returns current order execution statistics
func (c *coinexClient) GetOrderExecutionStats() (int, float64, float64) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	settings := c.config.OrderExecutionSettings
	availableBalance := settings.AccountBalance

	return 0, 0, availableBalance
}

// GetOrderStatus gets the current status of an order
func (c *coinexClient) GetOrderStatus(orderID string) (*FOKOrderTracker, bool) {
	// Simplified: no order tracking needed with sequential execution
	return nil, false
}

// GetActiveTrackers returns all active order trackers
func (c *coinexClient) GetActiveTrackers() []*FOKOrderTracker {
	// Simplified: no active trackers needed with sequential execution
	return []*FOKOrderTracker{}
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

	// Step 1: Find all USDT and USDC assets separately
	fmt.Printf(" Step 1: Finding all USDT and USDC assets...\n")
	usdtAssets, usdcAssets := c.findUSDTAndUSDCAssets(markets)

	fmt.Printf(" Found %d USDT assets and %d USDC assets\n", len(usdtAssets), len(usdcAssets))

	// Step 2: Find assets that have BOTH USDT and USDC pairs
	fmt.Printf(" Step 2: Finding assets with both USDT and USDC pairs...\n")
	completeAssets, triangularMarkets := c.findCompleteAssets(usdtAssets, usdcAssets)

	fmt.Printf(" Found %d complete assets with both USDT and USDC pairs\n", len(completeAssets))

	// Step 3: Add the critical USDCUSDT pair
	usdcusdtFound := false
	for _, market := range markets {
		marketStr := market.(string)
		if marketStr == "USDCUSDT" {
			triangularMarkets = append(triangularMarkets, marketStr)
			fmt.Printf("    Added critical quote pair: %s\n", marketStr)
			usdcusdtFound = true
			break
		}
	}

	if !usdcusdtFound {
		fmt.Printf("    ⚠️ WARNING: USDCUSDT pair not found in market list!\n")
		fmt.Printf("    💡 This pair is critical for triangular arbitrage\n")
	}

	fmt.Printf(" Final result: %d triangular markets and %d complete assets\n", len(triangularMarkets), len(completeAssets))

	return triangularMarkets, completeAssets, nil
}

// findUSDTAndUSDCAssets finds all USDT and USDC assets separately
func (c *coinexClient) findUSDTAndUSDCAssets(markets []interface{}) ([]string, []string) {
	usdtAssets := []string{}
	usdcAssets := []string{}

	fmt.Printf("    Analyzing %d total markets for USDT and USDC pairs...\n", len(markets))

	for _, market := range markets {
		marketStr := market.(string)
		if strings.HasSuffix(marketStr, "USDT") {
			usdtAssets = append(usdtAssets, marketStr)
		} else if strings.HasSuffix(marketStr, "USDC") {
			usdcAssets = append(usdcAssets, marketStr)
		}
	}

	// Debug: Show some examples of discovered markets
	fmt.Printf("    USDT markets found (first 10): ")
	for i, market := range usdtAssets {
		if i < 10 {
			fmt.Printf("%s ", market)
		}
	}
	if len(usdtAssets) > 10 {
		fmt.Printf("... and %d more", len(usdtAssets)-10)
	}
	fmt.Printf("\n")

	fmt.Printf("    USDC markets found (first 10): ")
	for i, market := range usdcAssets {
		if i < 10 {
			fmt.Printf("%s ", market)
		}
	}
	if len(usdcAssets) > 10 {
		fmt.Printf("... and %d more", len(usdcAssets)-10)
	}
	fmt.Printf("\n")

	return usdtAssets, usdcAssets
}

// findCompleteAssets finds assets that have both USDT and USDC pairs
func (c *coinexClient) findCompleteAssets(usdtAssets, usdcAssets []string) ([]string, []string) {
	completeAssets := []string{}
	triangularMarkets := []string{}

	// Create maps for quick lookup
	usdtAssetMap := make(map[string]string) // asset -> market
	usdcAssetMap := make(map[string]string) // asset -> market

	// Blacklist of USDC markets to ignore
	ignoredUSDCMarkets := map[string]bool{
		"ETCUSDC": true,
		"BTCUSDC": true,
		"BNBUSDC": true,
	}

	// Parse USDT assets
	for _, market := range usdtAssets {
		asset := strings.TrimSuffix(market, "USDT")
		if asset != "" && asset != "USDC" { // Exclude USDCUSDT
			usdtAssetMap[asset] = market
		}
	}

	// Parse USDC assets (excluding blacklisted ones)
	for _, market := range usdcAssets {
		// Skip blacklisted USDC markets
		if ignoredUSDCMarkets[market] {
			fmt.Printf("    ⚠️ Ignoring blacklisted USDC market: %s\n", market)
			continue
		}

		asset := strings.TrimSuffix(market, "USDC")
		if asset != "" && asset != "USDT" { // Exclude USDTUSDC
			usdcAssetMap[asset] = market
		}
	}

	fmt.Printf("    USDT assets found: %d\n", len(usdtAssetMap))
	fmt.Printf("    USDC assets found: %d (excluding blacklisted)\n", len(usdcAssetMap))

	// Find assets that have both USDT and USDC pairs
	for asset := range usdtAssetMap {
		if usdcMarket, hasUSDC := usdcAssetMap[asset]; hasUSDC {
			completeAssets = append(completeAssets, asset)
			// Add both markets for this asset
			triangularMarkets = append(triangularMarkets, usdtAssetMap[asset])
			triangularMarkets = append(triangularMarkets, usdcMarket)
		}
	}

	fmt.Printf("    Complete assets found: %d\n", len(completeAssets))
	fmt.Printf("    Incomplete assets found: %d\n", len(usdtAssetMap)+len(usdcAssetMap)-len(completeAssets))
	fmt.Printf("    USDT assets found: %d\n", len(usdtAssetMap))
	fmt.Printf("    USDC assets found: %d\n", len(usdcAssetMap))
	fmt.Printf("    Triangular markets found: %d\n", len(triangularMarkets))

	// Check for assets that have USDC but no USDT
	for asset := range usdcAssetMap {
		if _, hasUSDT := usdtAssetMap[asset]; !hasUSDT {
			fmt.Printf("    ❌ Incomplete asset: %s (has USDC but no USDT)\n", asset)
		}
	}

	return completeAssets, triangularMarkets
}

// filterMarketsByRealActivity filters markets based on real 24H volume data from CoinEx
func (c *coinexClient) filterMarketsByRealActivity(markets []string, tickerData map[string]interface{}) ([]string, []string) {
	fmt.Printf("    Analyzing real market activity for %d markets...\n", len(markets))

	// Extract ticker data
	data, ok := tickerData["data"].(map[string]interface{})
	if !ok {
		fmt.Printf("     Invalid ticker data format, falling back to heuristic filtering\n")
		return c.validateMarketActivity(markets, nil) // Pass nil for marketDepths as fallback
	}

	tickers, ok := data["ticker"].(map[string]interface{})
	if !ok {
		fmt.Printf("     Invalid ticker format, falling back to heuristic filtering\n")
		return c.validateMarketActivity(markets, nil) // Pass nil for marketDepths as fallback
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
func (c *coinexClient) validateMarketActivity(markets []string, marketDepths *MarketDepths) ([]string, []string) {
	fmt.Printf("    Testing %d markets for trading activity...\n", len(markets))

	activeMarkets := []string{}
	deadMarkets := []string{}

	// For now, we'll use a simple heuristic based on market naming patterns
	// In production, you'd want to call CoinEx API to check 24h volume or recent trades

	for _, market := range markets {
		isActive := c.isMarketActive(market, marketDepths)

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

// isMarketActive determines if a market is likely to be active by checking WebSocket data availability
func (c *coinexClient) isMarketActive(market string, marketDepths *MarketDepths) bool {
	// Check if we have WebSocket data for this market
	if orderBook, exists := marketDepths.Load(market); exists && orderBook != nil {
		if len(orderBook.Bids) > 0 && len(orderBook.Asks) > 0 {
			return true // Market has WebSocket data
		}
	}

	// If no WebSocket data, use heuristic fallback
	// Critical pairs (always include)
	if market == "USDCUSDT" || market == "USDTUSDC" {
		return true
	}

	// USDT markets are generally more liquid
	if strings.HasSuffix(market, "USDT") {
		return true
	}

	// USDC markets - be more selective
	if strings.HasSuffix(market, "USDC") {
		// Only include USDC pairs for major assets that are likely to have both USDT and USDC pairs
		asset := strings.TrimSuffix(market, "USDC")
		majorAssets := []string{"BTC", "ETH", "BNB", "SOL", "ADA", "DOT", "AVAX", "MATIC", "LINK", "UNI", "DOGE", "LTC", "XRP"}
		for _, major := range majorAssets {
			if asset == major {
				return true
			}
		}
		return false // Most USDC pairs are less liquid
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

// manageRealFOKOrderLifecycle implements FOK behavior using limit or market orders based on price
func (c *coinexClient) manageRealFOKOrderLifecycle(tracker *FOKOrderTracker, orderResultChan chan<- *OrderResult) {
	log.Printf("🎯 REAL FOK ORDER | Market: %s | Type: %s | Amount: %.6f | Price: %.8f",
		tracker.Market, tracker.Type, tracker.Amount, tracker.Price)

	// Place the order - use limit order if price is specified, market order if price is 0
	var actualOrderID string
	var err error

	if tracker.Price > 0 {
		// Use limit order when price is specified
		log.Printf("📤 PLACING LIMIT ORDER | Market: %s | Type: %s | Amount: %.6f | Price: %.8f",
			tracker.Market, tracker.Type, tracker.Amount, tracker.Price)
		actualOrderID, err = c.placeRealLimitOrder(tracker)
	} else {
		// Use market order when price is 0 (no price specified)
		log.Printf("📤 PLACING MARKET ORDER | Market: %s | Type: %s | Amount: %.6f",
			tracker.Market, tracker.Type, tracker.Amount)
		actualOrderID, err = c.placeRealMarketOrder(tracker)
	}

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

	log.Printf("✅ ORDER PLACED | ID: %s | Market: %s | Starting FOK polling...", actualOrderID, tracker.Market)

	// Update tracker with real order ID
	tracker.mu.Lock()
	tracker.OrderID = actualOrderID
	tracker.mu.Unlock()

	// Poll until filled or timeout with configurable timeout for FOK
	isFilled, err := c.pollRealOrderStatus(tracker)
	if err != nil {
		log.Printf("❌ POLLING FAILED | ID: %s | Error: %v", actualOrderID, err)

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
			Status:        OrderStatusFailed,
			FilledAmount:  0,
			AvgPrice:      0,
			Fee:           0,
			FeeCurrency:   "",
			ExecutionTime: time.Since(tracker.CreatedAt).Milliseconds(),
			ErrorMessage:  fmt.Sprintf("Polling failed: %v", err),
			Timestamp:     time.Now(),
		}
		orderResultChan <- result
		return
	}

	// Handle the result
	if isFilled {
		// Order was filled - get fill data
		log.Printf("✅ ORDER FILLED - Getting fill data | ID: %s", actualOrderID)
		filledAmount, filledValue, avgPrice, fee := c.getOrderFillData(tracker)

		result := &OrderResult{
			OrderID:       actualOrderID,
			Market:        tracker.Market,
			Type:          tracker.Type,
			Amount:        tracker.Amount,
			Price:         tracker.Price,
			Status:        OrderStatusFilled,
			FilledAmount:  filledAmount,
			FilledValue:   filledValue,
			AvgPrice:      avgPrice,
			Fee:           fee,
			FeeCurrency:   "USDT", // Default fee currency
			ExecutionTime: time.Since(tracker.CreatedAt).Milliseconds(),
			ErrorMessage:  "",
			Timestamp:     time.Now(),
		}

		log.Printf("✅ ORDER FILLED | ID: %s | Filled: %.6f | Filled Value: %.6f | Avg Price: %.8f | Fee: %.6f",
			actualOrderID, filledAmount, filledValue, avgPrice, fee)
		orderResultChan <- result
	} else {
		// Order was not filled - cancel it
		log.Printf("❌ ORDER NOT FILLED - Cancelling | ID: %s", actualOrderID)

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
			ErrorMessage:  "Order not filled within timeout",
			Timestamp:     time.Now(),
		}
		orderResultChan <- result
	}
}

// placeRealMarketOrder places a real market order via CoinEx API v2
func (c *coinexClient) placeRealMarketOrder(tracker *FOKOrderTracker) (string, error) {
	log.Printf("📤 PLACING MARKET ORDER | Market: %s | Type: %s | Amount: %.6f",
		tracker.Market, tracker.Type, tracker.Amount)

	// Use v2 API for spot trading
	timestamp := time.Now().UnixMilli()
	method := "POST"
	requestPath := "/v2/spot/order"

	// Prepare JSON body for v2 API - Market order format according to CoinEx docs
	orderData := map[string]interface{}{
		"market":      tracker.Market,
		"market_type": "SPOT",
		"side":        tracker.Type, // "buy" or "sell"
		"type":        "market",     // Market order type
		"amount":      fmt.Sprintf("%.8f", tracker.Amount),
		// For market orders, we don't specify price - it executes at current market price
		"time_in_force": "FOK", // Fill-or-Kill order type
		"client_id":     fmt.Sprintf("FOK_%s_%d", tracker.Market, timestamp),
		"is_hide":       false,
		"stp_mode":      "both",
	}

	// Convert to JSON string
	bodyBytes, err := json.Marshal(orderData)
	if err != nil {
		log.Printf("❌ ORDER MARSHAL ERROR | Market: %s | Error: %v", tracker.Market, err)
		return "", fmt.Errorf("failed to marshal order data: %w", err)
	}
	body := string(bodyBytes)

	log.Printf("📋 MARKET ORDER DATA | Market: %s | Body: %s", tracker.Market, body)

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

	log.Printf("📤 SENDING MARKET ORDER REQUEST | Market: %s | URL: %s", tracker.Market, requestParams["url"])
	log.Printf("📤 SENDING MARKET ORDER REQUEST | Market: %s | Headers: %+v", tracker.Market, authHeaders)

	// Make API call
	response, err := c.httpClient.performRequest(requestParams, POST)
	if err != nil {
		log.Printf("❌ MARKET ORDER REQUEST FAILED | Market: %s | Error: %v", tracker.Market, err)
		return "", fmt.Errorf("API request failed: %w", err)
	}

	log.Printf("📥 MARKET ORDER RESPONSE | Market: %s | Response: %+v", tracker.Market, response)
	log.Printf("📥 MARKET ORDER RESPONSE | Market: %s | Response Type: %T", tracker.Market, response)

	// Check response
	if code, ok := response["code"].(float64); !ok || code != 0 {
		message, _ := response["message"].(string)
		log.Printf("❌ MARKET ORDER API ERROR | Market: %s | Code: %.0f | Message: %s", tracker.Market, code, message)
		log.Printf("❌ MARKET ORDER API ERROR | Market: %s | Full Response: %+v", tracker.Market, response)

		// Handle specific error codes
		if code == 3606 {
			log.Printf("⚠️ PRICE DEVIATION ERROR | Market: %s | This might be due to stale order book data or high volatility", tracker.Market)
			log.Printf("⚠️ PRICE DEVIATION ERROR | Market: %s | Consider refreshing order book data or using limit orders", tracker.Market)

			// For price deviation errors, we could try a limit order as fallback
			// But for now, let's just return the error and let the caller handle it
			log.Printf("⚠️ PRICE DEVIATION ERROR | Market: %s | Returning error to let caller decide next action", tracker.Market)
		}

		return "", fmt.Errorf("CoinEx API error: code=%.0f, message=%s", code, message)
	}

	// Extract order data
	data, ok := response["data"].(map[string]interface{})
	if !ok {
		log.Printf("❌ MARKET ORDER RESPONSE FORMAT ERROR | Market: %s | Missing data field", tracker.Market)
		log.Printf("❌ MARKET ORDER RESPONSE FORMAT ERROR | Market: %s | Full Response: %+v", tracker.Market, response)
		return "", fmt.Errorf("invalid response format: missing data")
	}

	log.Printf("📥 MARKET ORDER DATA | Market: %s | Data: %+v", tracker.Market, data)

	// For market orders, we need to get the order_id for polling
	if orderIDValue, exists := data["order_id"]; exists {
		log.Printf("📥 MARKET ORDER ID FOUND | Market: %s | OrderID Value: %v (Type: %T)", tracker.Market, orderIDValue, orderIDValue)

		if orderIDStr, ok := orderIDValue.(string); ok && orderIDStr != "" {
			log.Printf("✅ MARKET ORDER PLACED SUCCESSFULLY | Market: %s | ID: %s", tracker.Market, orderIDStr)
			return orderIDStr, nil
		} else if orderIDFloat, ok := orderIDValue.(float64); ok {
			orderIDStr := fmt.Sprintf("%.0f", orderIDFloat)
			log.Printf("✅ MARKET ORDER PLACED SUCCESSFULLY | Market: %s | ID: %s", tracker.Market, orderIDStr)
			return orderIDStr, nil
		}
	}

	log.Printf("❌ MARKET ORDER ID MISSING | Market: %s | Data: %+v", tracker.Market, data)
	log.Printf("❌ MARKET ORDER ID MISSING | Market: %s | Available keys: %v", tracker.Market, getMapKeys(data))
	return "", fmt.Errorf("invalid market order response format - no order_id")
}

// placeRealLimitOrder places a real limit order via CoinEx API v2
func (c *coinexClient) placeRealLimitOrder(tracker *FOKOrderTracker) (string, error) {
	log.Printf("📤 PLACING LIMIT ORDER | Market: %s | Type: %s | Amount: %.6f | Price: %.8f",
		tracker.Market, tracker.Type, tracker.Amount, tracker.Price, tracker.Price)

	// Use v2 API for spot trading
	timestamp := time.Now().UnixMilli()
	method := "POST"
	requestPath := "/v2/spot/order"

	// Prepare JSON body for v2 API - Use limit order type with specified price
	orderData := map[string]interface{}{
		"market":        tracker.Market,
		"market_type":   "SPOT",
		"side":          tracker.Type,
		"type":          "limit", // Limit orders with specified price
		"amount":        fmt.Sprintf("%.8f", tracker.Amount),
		"price":         fmt.Sprintf("%.8f", tracker.Price), // Include price for limit orders
		"time_in_force": "FOK",                              // Fill-or-Kill order type
		"client_id":     fmt.Sprintf("FOK_%s_%d", tracker.Market, timestamp),
		"is_hide":       false,
		"stp_mode":      "both",
	}

	// Convert to JSON string
	bodyBytes, err := json.Marshal(orderData)
	if err != nil {
		log.Printf("❌ ORDER MARSHAL ERROR | Market: %s | Error: %v", tracker.Market, err)
		return "", fmt.Errorf("failed to marshal order data: %w", err)
	}
	body := string(bodyBytes)

	log.Printf("📋 ORDER DATA | Market: %s | Body: %s", tracker.Market, body)

	// Debug: Log exact order details for analysis
	log.Printf("🔍 ORDER ANALYSIS | Market: %s | Type: %s | Amount: %.8f | Price: %.8f | Value: $%.2f | Precision Check: Amount=%.8f, Price=%.8f",
		tracker.Market, tracker.Type, tracker.Amount, tracker.Price, tracker.Amount*tracker.Price, tracker.Amount, tracker.Price)

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
			log.Printf("✅ LIMIT ORDER PLACED SUCCESSFULLY | Market: %s | ID: %s", tracker.Market, orderIDStr)
			return orderIDStr, nil
		} else if orderIDFloat, ok := orderIDValue.(float64); ok {
			orderIDStr := fmt.Sprintf("%.0f", orderIDFloat)
			log.Printf("✅ LIMIT ORDER PLACED SUCCESSFULLY | Market: %s | ID: %s", tracker.Market, orderIDStr)
			return orderIDStr, nil
		}
	}

	log.Printf("❌ ORDER ID MISSING | Market: %s | Data: %+v", tracker.Market, data)
	return "", fmt.Errorf("invalid limit order response format - no order_id")
}

// pollRealOrderStatus polls the real order status from CoinEx API until timeout
func (c *coinexClient) pollRealOrderStatus(tracker *FOKOrderTracker) (bool, error) {
	// Use FOK configuration settings from config
	fokSettings := c.config.FOKOrderSettings
	timeoutDuration := time.Duration(fokSettings.FOKTimeoutSeconds) * time.Second
	pollingInterval := time.Duration(1000/fokSettings.FOKPollingFrequencyHz) * time.Millisecond

	pollCount := 0
	timeout := time.After(timeoutDuration)

	log.Printf("⏱️ FOK POLLING CONFIG | Timeout: %dms | Polling: every %dms | Config: %d seconds, %.1f Hz",
		int(timeoutDuration.Milliseconds()),
		int(pollingInterval.Milliseconds()),
		fokSettings.FOKTimeoutSeconds,
		fokSettings.FOKPollingFrequencyHz)

	// Loop until timeout or filled
	for {
		select {
		case <-timeout:
			// Timeout reached - order not filled (FOK behavior)
			log.Printf("⏰ FOK TIMEOUT | ID: %s | After %d attempts - cancelling order", tracker.OrderID, pollCount)
			return false, nil

		default:
			// Poll the order status
			pollCount++
			if pollCount <= 2 || pollCount%5 == 0 { // Reduced logging
				log.Printf("🔍 POLLING ORDER | ID: %s | Attempt: %d", tracker.OrderID, pollCount)
			}

			// Single poll attempt
			timestamp := time.Now().UnixMilli()
			method := "GET"
			requestPath := "/v2/spot/order-status"

			// Build query string for order status API - MUST be sorted alphabetically
			queryParams := map[string]string{
				"market":   tracker.Market,
				"order_id": tracker.OrderID,
			}

			// Sort parameters alphabetically (required for signature)
			keys := make([]string, 0, len(queryParams))
			for k := range queryParams {
				keys = append(keys, k)
			}
			sort.Strings(keys)

			// Build query string in sorted order (no URL encoding for signature)
			var queryParts []string
			for _, key := range keys {
				queryParts = append(queryParts, key+"="+queryParams[key])
			}
			queryString := strings.Join(queryParts, "&")

			// For GET requests: method + request_path + timestamp (no body)
			// For GET requests, the request_path should include the query string
			requestPathWithQuery := requestPath + "?" + queryString
			signature := c.generateRESTSignature(method, requestPathWithQuery, "", "", timestamp)

			// Set authentication headers
			authHeaders := map[string]string{
				"X-COINEX-KEY":       c.apiKey,
				"X-COINEX-SIGN":      signature,
				"X-COINEX-TIMESTAMP": strconv.FormatInt(timestamp, 10),
			}

			// Prepare request URL
			requestURL := "https://api.coinex.com" + requestPath + "?" + queryString

			// Create HTTP request
			req, err := http.NewRequest(method, requestURL, nil)
			if err != nil {
				if pollCount <= 2 {
					log.Printf("❌ FAILED TO CREATE REQUEST | Error: %v", err)
				}
				return false, fmt.Errorf("failed to create request: %w", err)
			}

			// Add headers
			for key, value := range authHeaders {
				req.Header.Set(key, value)
			}

			// Use the main HttpClient which has rate limiting built in
			if pollCount <= 2 {
				log.Printf("🔍 FOK POLLING | ID: %s | Using rate-limited HttpClient | URL: %s", tracker.OrderID, requestURL)
			}

			// Use the main HttpClient with rate limiting instead of creating a new one
			resp, err := c.httpClient.DoWithRateLimit(req)
			if err != nil {
				if pollCount <= 2 {
					log.Printf("❌ HTTP REQUEST FAILED | Error: %v", err)
				}
				// For rate limit errors, wait and retry
				if strings.Contains(err.Error(), "rate limit exceeded") {
					if pollCount <= 2 {
						log.Printf("⏳ RATE LIMIT | ID: %s | Attempt: %d | Waiting before retry...", tracker.OrderID, pollCount)
					}
					time.Sleep(time.Second) // Wait 1 second for rate limit
					continue
				}
				// For network errors, continue polling instead of failing immediately
				if strings.Contains(err.Error(), "timeout") || strings.Contains(err.Error(), "deadline") {
					if pollCount <= 2 {
						log.Printf("⏳ NETWORK TIMEOUT | ID: %s | Attempt: %d | Continuing...", tracker.OrderID, pollCount)
					}
					time.Sleep(pollingInterval)
					continue
				}
				return false, fmt.Errorf("HTTP request failed: %w", err)
			}
			defer resp.Body.Close()

			// Read response
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				if pollCount <= 2 {
					log.Printf("❌ FAILED TO READ RESPONSE | Error: %v", err)
				}
				return false, fmt.Errorf("failed to read response: %w", err)
			}

			// Parse response
			var response map[string]interface{}
			if err := json.Unmarshal(body, &response); err != nil {
				if pollCount <= 2 {
					log.Printf("❌ FAILED TO PARSE RESPONSE | Error: %v", err)
				}
				return false, fmt.Errorf("failed to parse response: %w", err)
			}

			// Check response
			if code, ok := response["code"].(float64); !ok || code != 0 {
				message, _ := response["message"].(string)
				if pollCount <= 2 {
					log.Printf("⚠️ POLL API ERROR | ID: %s | Code: %.0f | Message: %s", tracker.OrderID, code, message)
				}
				return false, fmt.Errorf("API error: code=%.0f, message=%s", code, message)
			}

			// Extract order data
			data, ok := response["data"].(map[string]interface{})
			if !ok {
				if pollCount <= 2 {
					log.Printf("⚠️ POLL RESPONSE FORMAT ERROR | ID: %s", tracker.OrderID)
				}
				return false, fmt.Errorf("invalid response format: missing data")
			}

			// Check status - for FOK orders, check for filled status
			if statusStr, ok := data["status"].(string); ok {
				// FOK orders should fill immediately, so check for completion statuses
				if statusStr == "done" || statusStr == "filled" || statusStr == "closed" || statusStr == "completed" {
					log.Printf("✅ FOK ORDER FILLED | ID: %s | Status: %s | Attempt: %d", tracker.OrderID, statusStr, pollCount)
					return true, nil // Order is filled
				}

				// For FOK orders, don't check for partial fills on pending orders
				// FOK orders should either fill completely or be rejected
				if statusStr == "pending" || statusStr == "open" {
					// For FOK orders, pending status means the order is still waiting to be filled
					// We should continue polling until timeout or filled
					if pollCount <= 2 {
						log.Printf("⏳ FOK ORDER PENDING | ID: %s | Status: %s | Attempt: %d", tracker.OrderID, statusStr, pollCount)
					}
				}

				// Check for failed/cancelled status
				if statusStr == "cancelled" || statusStr == "failed" || statusStr == "rejected" {
					log.Printf("❌ FOK ORDER FAILED | ID: %s | Status: %s | Attempt: %d", tracker.OrderID, statusStr, pollCount)
					return false, fmt.Errorf("order failed with status: %s", statusStr)
				}
			}

			// Order is not filled yet, wait before next poll
			time.Sleep(pollingInterval)
		}
	}
}

// cancelRealOrder cancels a real order via CoinEx API v2
func (c *coinexClient) cancelRealOrder(tracker *FOKOrderTracker) error {
	log.Printf("🛑 CANCELLING ORDER | ID: %s | Market: %s", tracker.OrderID, tracker.Market)

	// Convert order_id from string to integer as required by CoinEx API
	orderIDInt, err := strconv.ParseInt(tracker.OrderID, 10, 64)
	if err != nil {
		log.Printf("❌ CANCELLATION ORDER ID PARSE ERROR | ID: %s | Error: %v", tracker.OrderID, err)
		return fmt.Errorf("failed to parse order ID: %w", err)
	}

	timestamp := time.Now().UnixMilli()
	method := "POST"
	requestPath := "/v2/spot/cancel-order" // Keep /v2/ as requested

	// Prepare JSON body for cancel order API - order_id must be integer
	cancelData := map[string]interface{}{
		"market":      tracker.Market,
		"market_type": "SPOT",     // Required parameter
		"order_id":    orderIDInt, // Convert to integer as required by API
	}

	// Convert to JSON string
	bodyBytes, err := json.Marshal(cancelData)
	if err != nil {
		log.Printf("❌ CANCELLATION MARSHAL ERROR | ID: %s | Error: %v", tracker.OrderID, err)
		return fmt.Errorf("failed to marshal cancel data: %w", err)
	}
	body := string(bodyBytes)

	log.Printf("📋 CANCELLATION DATA | ID: %s | Body: %s", tracker.OrderID, body)

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

	log.Printf("📤 SENDING CANCELLATION REQUEST | ID: %s | URL: %s", tracker.OrderID, requestParams["url"])

	// Make API call
	response, err := c.httpClient.performRequest(requestParams, POST)
	if err != nil {
		log.Printf("❌ CANCELLATION REQUEST FAILED | ID: %s | Error: %v", tracker.OrderID, err)
		return fmt.Errorf("cancel API request failed: %w", err)
	}

	log.Printf("📥 CANCELLATION RESPONSE | ID: %s | Response: %+v", tracker.OrderID, response)

	// Check response
	if code, ok := response["code"].(float64); !ok || code != 0 {
		message, _ := response["message"].(string)
		log.Printf("❌ CANCELLATION API ERROR | ID: %s | Code: %.0f | Message: %s", tracker.OrderID, code, message)
		return fmt.Errorf("CoinEx API error: code=%.0f, message=%s", code, message)
	}

	// Log successful cancellation details
	if data, ok := response["data"].(map[string]interface{}); ok {
		log.Printf("✅ CANCELLATION SUCCESS | ID: %s | Response data: %+v", tracker.OrderID, data)
	} else {
		log.Printf("✅ ORDER CANCELLED SUCCESSFULLY | ID: %s | No response data", tracker.OrderID)
	}
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

// EnsureCriticalMarketData ensures critical markets have data via WebSocket only
func (c *coinexClient) EnsureCriticalMarketData(criticalMarkets []string, marketDepths *MarketDepths) {
	log.Printf("🔄 Ensuring critical markets have WebSocket data: %v", criticalMarkets)

	failedCount := 0
	for _, market := range criticalMarkets {
		// Check if market already has data in marketDepths (from WebSocket)
		if orderBook, exists := marketDepths.Load(market); exists && orderBook != nil {
			if len(orderBook.Bids) > 0 && len(orderBook.Asks) > 0 {
				// Market already has WebSocket data, skip
				log.Printf("✅ %s already has WebSocket data, skipping", market)
				continue
			}
		}

		// Try to subscribe to the market via WebSocket using full flow
		log.Printf("🔄 Attempting WebSocket subscription for %s with full flow...", market)
		if err := c.httpClient.SubscribeWebSocketWithFullFlow([]string{market}, marketDepths); err != nil {
			log.Printf("❌ WebSocket subscription failed for %s: %v", market, err)
			failedCount++
		} else {
			log.Printf("✅ WebSocket subscription successful for %s", market)

			// Check if we now have WebSocket data
			if orderBook, exists := marketDepths.Load(market); exists && orderBook != nil {
				if len(orderBook.Bids) > 0 && len(orderBook.Asks) > 0 {
					log.Printf("✅ %s data available via WebSocket | Bids: %d | Asks: %d",
						market, len(orderBook.Bids), len(orderBook.Asks))
					continue
				}
			}
		}

		// No REST API fallback - if WebSocket fails, just log it
		log.Printf("⚠️ %s has no WebSocket data", market)
		failedCount++
	}

	log.Printf("📊 Critical markets WebSocket summary: %d failed out of %d total", failedCount, len(criticalMarkets))
}

// FetchCriticalMarketsViaWebSocket fetches order book data for critical markets via WebSocket only
// This is used as a primary data source for quote currencies and other critical markets
func (c *coinexClient) FetchCriticalMarketsViaWebSocket(markets []string, marketDepths *MarketDepths) {
	log.Printf("🔄 Subscribing to critical markets via WebSocket with full flow: %v", markets)

	// Use the new full subscription flow that:
	// 1. Subscribes with full=true for initial snapshots
	// 2. Waits for data to arrive
	// 3. Switches to incremental updates (full=false)
	if err := c.httpClient.SubscribeWebSocketWithFullFlow(markets, marketDepths); err != nil {
		log.Printf("❌ Failed to subscribe to critical markets with full flow: %v", err)
		return
	}

	// Verify that we have data for all critical markets
	successCount := 0
	failedCount := 0

	for _, market := range markets {
		if orderBook, exists := marketDepths.Load(market); exists && orderBook != nil {
			if len(orderBook.Bids) > 0 && len(orderBook.Asks) > 0 {
				log.Printf("✅ %s has WebSocket data | Bids: %d | Asks: %d",
					market, len(orderBook.Bids), len(orderBook.Asks))
				successCount++
			} else {
				log.Printf("⚠️ %s has empty order book via WebSocket", market)
				failedCount++
			}
		} else {
			log.Printf("❌ %s has no WebSocket data", market)
			failedCount++
		}
	}

	log.Printf("📊 Critical markets WebSocket summary: %d success, %d failed out of %d total", successCount, failedCount, len(markets))
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
	config *Config
}

// NewCriticalMarketPriceManager creates a new critical market price manager
func NewCriticalMarketPriceManager(coinexClient *coinexClient, config *Config) *CriticalMarketPriceManager {
	log.Printf("🔧 CRITICAL MARKET PRICE MANAGER | Initialized - all markets use WebSocket data")
	return &CriticalMarketPriceManager{
		config: config,
	}
}

// IsCriticalMarket checks if a market is critical (should be prioritized for WebSocket subscription)
func (cmm *CriticalMarketPriceManager) IsCriticalMarket(market string) bool {
	for _, critical := range cmm.config.CriticalMarkets {
		if market == critical {
			return true
		}
	}
	return false
}

// AssumeCriticalMarketExists returns false - no markets are assumed to exist without WebSocket data
func (cmm *CriticalMarketPriceManager) AssumeCriticalMarketExists(market string) bool {
	return false // All markets must have WebSocket data
}

// GetAssumedPrice returns 0 - no assumed prices, all prices come from WebSocket
func (cmm *CriticalMarketPriceManager) GetAssumedPrice(market string) (float64, bool) {
	return 0.0, false // No assumed prices
}

// GetCriticalMarketSnapshot returns empty snapshot - we don't need price data
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

	// Build query string - MUST be sorted alphabetically
	queryParams := map[string]string{
		"limit":       "100",
		"market_type": "SPOT",
	}

	// Sort parameters alphabetically (required for signature)
	keys := make([]string, 0, len(queryParams))
	for k := range queryParams {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Build query string in sorted order (no URL encoding for signature)
	var queryParts []string
	for _, key := range keys {
		queryParts = append(queryParts, key+"="+queryParams[key])
	}
	queryString := strings.Join(queryParts, "&")

	// Generate v2 signature with query string included in request path
	requestPathWithQuery := requestPath + "?" + queryString
	signature := c.generateRESTSignature(method, requestPathWithQuery, "", "", timestamp)

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
		"url":    "https://api.coinex.com" + requestPath + "?" + queryString,
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

// CancelAllOrders cancels all orders for a specific market using CoinEx API
func (c *coinexClient) CancelAllOrders(market string) error {
	log.Printf("🛑 CANCELLING ALL ORDERS | Market: %s", market)

	timestamp := time.Now().UnixMilli()
	method := "POST"
	requestPath := "/v2/spot/cancel-all-order" // Use the correct endpoint

	// Prepare JSON body for cancel all orders API
	cancelAllData := map[string]interface{}{
		"market":      market,
		"market_type": "SPOT", // Required parameter
		// Note: 'side' is optional, so we don't include it to cancel both buy and sell orders
	}

	// Convert to JSON string
	bodyBytes, err := json.Marshal(cancelAllData)
	if err != nil {
		log.Printf("❌ CANCEL ALL MARSHAL ERROR | Market: %s | Error: %v", market, err)
		return fmt.Errorf("failed to marshal cancel all data: %w", err)
	}
	body := string(bodyBytes)

	log.Printf("📋 CANCEL ALL DATA | Market: %s | Body: %s", market, body)

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

	log.Printf("📤 SENDING CANCEL ALL REQUEST | Market: %s | URL: %s", market, requestParams["url"])

	// Make API call
	response, err := c.httpClient.performRequest(requestParams, POST)
	if err != nil {
		log.Printf("❌ CANCEL ALL REQUEST FAILED | Market: %s | Error: %v", market, err)
		return fmt.Errorf("cancel all API request failed: %w", err)
	}

	log.Printf("📥 CANCEL ALL RESPONSE | Market: %s | Response: %+v", market, response)

	// Check response
	if code, ok := response["code"].(float64); !ok || code != 0 {
		message, _ := response["message"].(string)
		log.Printf("❌ CANCEL ALL API ERROR | Market: %s | Code: %.0f | Message: %s", market, code, message)
		return fmt.Errorf("CoinEx API error: code=%.0f, message=%s", code, message)
	}

	log.Printf("✅ ALL ORDERS CANCELLED SUCCESSFULLY | Market: %s", market)
	return nil
}

// getMapKeysBool returns keys from a map[string]bool
func getMapKeysBool(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// getOrderFillData retrieves actual fill data from the order status API
func (c *coinexClient) getOrderFillData(tracker *FOKOrderTracker) (float64, float64, float64, float64) {
	// Get order status to retrieve fill data
	timestamp := time.Now().UnixMilli()
	method := "GET"
	requestPath := "/v2/spot/order-status"

	// Build query string for order status API
	queryParams := map[string]string{
		"market":   tracker.Market,
		"order_id": tracker.OrderID,
	}

	// Sort parameters alphabetically (required for signature)
	keys := make([]string, 0, len(queryParams))
	for k := range queryParams {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Build query string in sorted order
	var queryParts []string
	for _, key := range keys {
		queryParts = append(queryParts, key+"="+queryParams[key])
	}
	queryString := strings.Join(queryParts, "&")

	// Generate signature with query string included in request path
	requestPathWithQuery := requestPath + "?" + queryString
	signature := c.generateRESTSignature(method, requestPathWithQuery, "", "", timestamp)

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
		"url":    "https://api.coinex.com" + requestPath + "?" + queryString,
		"method": method,
	}

	// Make API call
	response, err := c.httpClient.performRequest(requestParams, GET)
	if err != nil {
		log.Printf("❌ GET FILL DATA FAILED | ID: %s | Error: %v", tracker.OrderID, err)
		return 0, 0, 0, 0
	}

	// Check response
	if code, ok := response["code"].(float64); !ok || code != 0 {
		message, _ := response["message"].(string)
		log.Printf("❌ GET FILL DATA API ERROR | ID: %s | Code: %.0f | Message: %s", tracker.OrderID, code, message)
		return 0, 0, 0, 0
	}

	// Extract order data
	data, ok := response["data"].(map[string]interface{})
	if !ok {
		log.Printf("❌ GET FILL DATA RESPONSE FORMAT ERROR | ID: %s", tracker.OrderID)
		return 0, 0, 0, 0
	}

	log.Printf("🔍 FILL DATA RESPONSE | OrderID: %s | Full data: %+v", tracker.OrderID, data)

	// Extract fill data
	filledAmount := parseFloat(data["deal_amount"])
	filledValue := parseFloat(data["deal_value"])
	avgPrice := parseFloat(data["deal_price"])
	fee := parseFloat(data["quote_fee"])

	log.Printf("🔍 FILL DATA DEBUG | OrderID: %s | deal_amount: %v -> %.6f | deal_value: %v -> %.6f | deal_price: %v -> %.8f | quote_fee: %v -> %.6f",
		tracker.OrderID, data["deal_amount"], filledAmount, data["deal_value"], filledValue, data["deal_price"], avgPrice, data["quote_fee"], fee)

	return filledAmount, filledValue, avgPrice, fee
}

func parseFloat(i interface{}) float64 {
	v, ok := i.(string)
	if !ok {
		return 0
	}
	f, _ := strconv.ParseFloat(v, 64)
	return f
}

// CanPlaceOrder checks if we can place a new order based on spending limits
func (c *coinexClient) CanPlaceOrder(orderValue float64) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	settings := c.config.OrderExecutionSettings
	if settings.EnableSpendingLimits {
		// This is a placeholder for the actual implementation of spending limits
		// For now, we will just log the check
		log.Printf("Spending limit check: value=%.2f, enabled=%t", orderValue, settings.EnableSpendingLimits)
	}
	return true
}

// UpdateSpending updates the current spending amount
func (c *coinexClient) UpdateSpending(orderValue float64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// This is a placeholder for the actual implementation of spending limits
	// For now, we will just log the update
	log.Printf("Update spending: value=%.2f", orderValue)
}
