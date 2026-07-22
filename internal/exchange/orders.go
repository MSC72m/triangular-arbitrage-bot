package exchange

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

// PlaceFOKOrder places a Fill-or-Kill order with automatic simulation/real API switching and spending controls
func (c *CoinexClient) PlaceFOKOrder(market, orderType string, amount, price float64, orderResultChan chan<- OrderResult) *FOKOrderTracker {
	log.Printf(" PLACE FOK ORDER START | Market: %s | Type: %s | Amount: %.6f | Price: %.8f",
		market, orderType, amount, price)
	log.Printf(" FOK CONFIG | Timeout: %ds | Polling: %.1f Hz | Max Retries: %d",
		c.config.FOKOrderSettings.FOKTimeoutSeconds,
		c.config.FOKOrderSettings.FOKPollingFrequencyHz,
		c.config.FOKOrderSettings.MaxRetryAttempts)

	// Check rate limit before placing order
	if !c.checkRateLimit() {
		log.Printf(" RATE LIMIT | Order placement rate limited")
		return nil
	}

	// Calculate order amount
	orderAmount, _, err := c.calculateOrderAmount(amount, price, orderType)
	if err != nil {
		log.Printf(" ORDER REJECTED | %v | Market: %s", err, market)
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
		ResultChan:  make(chan OrderResult, 1),
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

// manageSimulationOrderLifecycle handles simulation orders
func (c *CoinexClient) manageSimulationOrderLifecycle(tracker *FOKOrderTracker, orderResultChan chan<- OrderResult) {
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
			log.Printf(" SIMULATION EXTERNAL CANCELLATION | ID: %s", tracker.OrderID)
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
				log.Printf(" SIMULATION RESULT SENT | ID: %s", tracker.OrderID)
			default:
				log.Printf("  SIMULATION RESULT CHANNEL FULL | ID: %s", tracker.OrderID)
			}
			return
		}
	}
}

// pollOrderStatus polls the order status (simulation - in real implementation would call CoinEx API)
func (c *CoinexClient) pollOrderStatus(tracker *FOKOrderTracker) {
	tracker.mu.Lock()
	tracker.LastChecked = time.Now()
	tracker.mu.Unlock()

	// In real implementation, this would call CoinEx order status API
	// For simulation, we just log the polling activity
	log.Printf(" POLLING ORDER STATUS | ID: %s | Status: %s | Age: %v",
		tracker.OrderID, tracker.Status, time.Since(tracker.CreatedAt))
}

// cancelOrder cancels an order (simulation - in real implementation would call CoinEx API)
func (c *CoinexClient) cancelOrder(tracker *FOKOrderTracker) {
	log.Printf(" CANCELLING ORDER | ID: %s", tracker.OrderID)

	tracker.mu.Lock()
	if tracker.Status == OrderStatusPending {
		tracker.Status = OrderStatusCancelled
	}
	tracker.IsActive = false
	tracker.mu.Unlock()

	log.Printf(" ORDER CANCELLED | ID: %s", tracker.OrderID)
}

// simulateOrderExecution simulates order execution (in real implementation, this would be actual order placement)
func (c *CoinexClient) simulateOrderExecution(tracker *FOKOrderTracker) {
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
		log.Printf(" ORDER CANCELLED DURING EXECUTION | ID: %s", tracker.OrderID)
		return
	}
	tracker.mu.RUnlock()

	// Simulate order outcome (90% success rate for FOK orders)
	var result OrderResult
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

		result = OrderResult{
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

		result = OrderResult{
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
func (c *CoinexClient) buildOrderResult(tracker *FOKOrderTracker) OrderResult {
	tracker.mu.RLock()
	defer tracker.mu.RUnlock()

	return OrderResult{
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

// CancelAllActiveOrders cancels all active orders (real API or simulation)
func (c *CoinexClient) CancelAllActiveOrders() {
	log.Printf(" CANCELLING ALL ACTIVE ORDERS | Mode: %s",
		map[bool]string{true: "SIMULATION", false: "REAL API"}[c.config.SimulationMode])

	// Simplified: no active order tracking needed with sequential execution
	log.Printf(" NO ACTIVE ORDERS TO CANCEL | Using sequential execution")
}

// GetOrderExecutionStats returns current order execution statistics
func (c *CoinexClient) GetOrderExecutionStats() (int, float64, float64) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	settings := c.config.OrderExecutionSettings
	availableBalance := settings.AccountBalance

	return 0, 0, availableBalance
}

// manageRealFOKOrderLifecycle implements FOK behavior using limit or market orders based on price
func (c *CoinexClient) manageRealFOKOrderLifecycle(tracker *FOKOrderTracker, orderResultChan chan<- OrderResult) {
	log.Printf(" REAL FOK ORDER | Market: %s | Type: %s | Amount: %.6f | Price: %.8f",
		tracker.Market, tracker.Type, tracker.Amount, tracker.Price)

	// Place the order - use limit order if price is specified, market order if price is 0
	var actualOrderID string
	var err error

	if tracker.Price > 0 {
		// Use limit order when price is specified
		log.Printf(" PLACING LIMIT ORDER | Market: %s | Type: %s | Amount: %.6f | Price: %.8f",
			tracker.Market, tracker.Type, tracker.Amount, tracker.Price)
		actualOrderID, err = c.placeRealLimitOrder(tracker)
	} else {
		// Use market order when price is 0 (no price specified)
		log.Printf(" PLACING MARKET ORDER | Market: %s | Type: %s | Amount: %.6f",
			tracker.Market, tracker.Type, tracker.Amount)
		actualOrderID, err = c.placeRealMarketOrder(tracker)
	}

	if err != nil {
		log.Printf(" ORDER PLACEMENT FAILED | Market: %s | Error: %v", tracker.Market, err)
		result := OrderResult{
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

	log.Printf(" ORDER PLACED | ID: %s | Market: %s | Starting FOK polling...", actualOrderID, tracker.Market)

	// Update tracker with real order ID
	tracker.mu.Lock()
	tracker.OrderID = actualOrderID
	tracker.mu.Unlock()

	// Immediate status check before starting polling loop
	log.Printf(" IMMEDIATE STATUS CHECK | ID: %s | Market: %s", actualOrderID, tracker.Market)
	if c.checkOrderStatusImmediately(tracker) {
		log.Printf(" ORDER FILLED IMMEDIATELY | ID: %s | No polling needed", actualOrderID)
		// Order filled immediately, get fill data and return success
		filledAmount, filledValue, avgPrice, fee := c.getOrderFillData(tracker)
		result := OrderResult{
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
			FeeCurrency:   "USDT",
			ExecutionTime: time.Since(tracker.CreatedAt).Milliseconds(),
			ErrorMessage:  "",
			Timestamp:     time.Now(),
		}
		orderResultChan <- result
		return
	}

	// If not filled immediately, start aggressive polling with minimal delay
	log.Printf(" ORDER NOT FILLED IMMEDIATELY | ID: %s | Starting aggressive polling", actualOrderID)

	// Poll until filled or timeout with configurable timeout for FOK
	isFilled, err := c.pollRealOrderStatus(tracker)
	if err != nil {
		log.Printf(" POLLING FAILED | ID: %s | Error: %v", actualOrderID, err)

		// Cancel the order
		cancelErr := c.cancelRealOrder(tracker)
		if cancelErr != nil {
			log.Printf(" CANCELLATION FAILED | ID: %s | Error: %v", actualOrderID, cancelErr)
		}

		result := OrderResult{
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
		log.Printf(" ORDER FILLED - Getting fill data | ID: %s", actualOrderID)
		filledAmount, filledValue, avgPrice, fee := c.getOrderFillData(tracker)

		result := OrderResult{
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

		log.Printf(" ORDER FILLED | ID: %s | Filled: %.6f | Filled Value: %.6f | Avg Price: %.8f | Fee: %.6f",
			actualOrderID, filledAmount, filledValue, avgPrice, fee)
		orderResultChan <- result
	} else {
		// Order was not filled - cancel it
		log.Printf(" ORDER NOT FILLED - Cancelling | ID: %s", actualOrderID)

		cancelErr := c.cancelRealOrder(tracker)
		if cancelErr != nil {
			log.Printf(" CANCELLATION FAILED | ID: %s | Error: %v", actualOrderID, cancelErr)
		}

		result := OrderResult{
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
func (c *CoinexClient) placeRealMarketOrder(tracker *FOKOrderTracker) (string, error) {
	log.Printf(" PLACING MARKET ORDER | Market: %s | Type: %s | Amount: %.6f",
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
		log.Printf(" ORDER MARSHAL ERROR | Market: %s | Error: %v", tracker.Market, err)
		return "", fmt.Errorf("failed to marshal order data: %w", err)
	}
	body := string(bodyBytes)

	log.Printf(" MARKET ORDER DATA | Market: %s | Body: %s", tracker.Market, body)

	// Generate v2 signature
	signature := c.generateRESTSignature(method, requestPath, "", body, timestamp)

	// Set v2 authentication headers
	authHeaders := map[string]string{
		"X-COINEX-KEY":       c.apiKey,
		"X-COINEX-SIGN":      signature,
		"X-COINEX-TIMESTAMP": strconv.FormatInt(timestamp, 10),
	}

	// Merge auth headers with existing headers
	c.httpClient.MergeHeaders(authHeaders)

	// Prepare request parameters
	requestParams := map[string]string{
		"url":    c.config.APIBaseURLV2 + requestPath[4:],
		"method": method,
		"body":   body,
	}

	log.Printf(" SENDING MARKET ORDER REQUEST | Market: %s | URL: %s", tracker.Market, requestParams["url"])
	log.Printf(" SENDING MARKET ORDER REQUEST | Market: %s | Headers: %+v", tracker.Market, authHeaders)

	// Make API call
	response, err := c.httpClient.PerformRequest(requestParams, "POST")
	if err != nil {
		log.Printf(" MARKET ORDER REQUEST FAILED | Market: %s | Error: %v", tracker.Market, err)
		return "", fmt.Errorf("API request failed: %w", err)
	}

	log.Printf(" MARKET ORDER RESPONSE | Market: %s | Response: %+v", tracker.Market, response)
	log.Printf(" MARKET ORDER RESPONSE | Market: %s | Response Type: %T", tracker.Market, response)

	// Check response
	if code, ok := response["code"].(float64); !ok || code != 0 {
		message, _ := response["message"].(string)
		log.Printf(" MARKET ORDER API ERROR | Market: %s | Code: %.0f | Message: %s", tracker.Market, code, message)
		log.Printf(" MARKET ORDER API ERROR | Market: %s | Full Response: %+v", tracker.Market, response)

		// Handle specific error codes
		if code == 3606 {
			log.Printf(" PRICE DEVIATION ERROR | Market: %s | This might be due to stale order book data or high volatility", tracker.Market)
			log.Printf(" PRICE DEVIATION ERROR | Market: %s | Consider refreshing order book data or using limit orders", tracker.Market)

			// For price deviation errors, we could try a limit order as fallback
			// But for now, let's just return the error and let the caller handle it
			log.Printf(" PRICE DEVIATION ERROR | Market: %s | Returning error to let caller decide next action", tracker.Market)
		}

		return "", fmt.Errorf("CoinEx API error: code=%.0f, message=%s", code, message)
	}

	// Extract order data
	data, ok := response["data"].(map[string]interface{})
	if !ok {
		log.Printf(" MARKET ORDER RESPONSE FORMAT ERROR | Market: %s | Missing data field", tracker.Market)
		log.Printf(" MARKET ORDER RESPONSE FORMAT ERROR | Market: %s | Full Response: %+v", tracker.Market, response)
		return "", fmt.Errorf("invalid response format: missing data")
	}

	log.Printf(" MARKET ORDER DATA | Market: %s | Data: %+v", tracker.Market, data)

	// For market orders, we need to get the order_id for polling
	if orderIDValue, exists := data["order_id"]; exists {
		log.Printf(" MARKET ORDER ID FOUND | Market: %s | OrderID Value: %v (Type: %T)", tracker.Market, orderIDValue, orderIDValue)

		if orderIDStr, ok := orderIDValue.(string); ok && orderIDStr != "" {
			log.Printf(" MARKET ORDER PLACED SUCCESSFULLY | Market: %s | ID: %s", tracker.Market, orderIDStr)
			return orderIDStr, nil
		} else if orderIDFloat, ok := orderIDValue.(float64); ok {
			orderIDStr := fmt.Sprintf("%.0f", orderIDFloat)
			log.Printf(" MARKET ORDER PLACED SUCCESSFULLY | Market: %s | ID: %s", tracker.Market, orderIDStr)
			return orderIDStr, nil
		}
	}

	log.Printf(" MARKET ORDER ID MISSING | Market: %s | Data: %+v", tracker.Market, data)
	log.Printf(" MARKET ORDER ID MISSING | Market: %s | Available keys: %d", tracker.Market, len(data))
	return "", fmt.Errorf("invalid market order response format - no order_id")
}

// placeRealLimitOrder places a real limit order via CoinEx API v2
func (c *CoinexClient) placeRealLimitOrder(tracker *FOKOrderTracker) (string, error) {
	log.Printf(" PLACING LIMIT ORDER | Market: %s | Type: %s | Amount: %.6f | Price: %.8f",
		tracker.Market, tracker.Type, tracker.Amount, tracker.Price)

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
		log.Printf(" ORDER MARSHAL ERROR | Market: %s | Error: %v", tracker.Market, err)
		return "", fmt.Errorf("failed to marshal order data: %w", err)
	}
	body := string(bodyBytes)

	log.Printf(" ORDER DATA | Market: %s | Body: %s", tracker.Market, body)

	// Debug: Log exact order details for analysis
	log.Printf(" ORDER ANALYSIS | Market: %s | Type: %s | Amount: %.8f | Price: %.8f | Value: $%.2f | Precision Check: Amount=%.8f, Price=%.8f",
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
	c.httpClient.MergeHeaders(authHeaders)

	// Prepare request parameters
	requestParams := map[string]string{
		"url":    c.config.APIBaseURLV2 + requestPath[4:],
		"method": method,
		"body":   body,
	}

	log.Printf(" SENDING ORDER REQUEST | Market: %s | URL: %s", tracker.Market, requestParams["url"])

	// Make API call
	response, err := c.httpClient.PerformRequest(requestParams, "POST")
	if err != nil {
		log.Printf(" ORDER REQUEST FAILED | Market: %s | Error: %v", tracker.Market, err)
		return "", fmt.Errorf("API request failed: %w", err)
	}

	log.Printf(" ORDER RESPONSE | Market: %s | Response: %+v", tracker.Market, response)

	// Check response
	if code, ok := response["code"].(float64); !ok || code != 0 {
		message, _ := response["message"].(string)
		log.Printf(" ORDER API ERROR | Market: %s | Code: %.0f | Message: %s", tracker.Market, code, message)
		return "", fmt.Errorf("CoinEx API error: code=%.0f, message=%s", code, message)
	}

	// Extract order data
	data, ok := response["data"].(map[string]interface{})
	if !ok {
		log.Printf(" ORDER RESPONSE FORMAT ERROR | Market: %s | Missing data field", tracker.Market)
		return "", fmt.Errorf("invalid response format: missing data")
	}

	// For limit orders, we need to get the order_id for polling
	if orderIDValue, exists := data["order_id"]; exists {
		if orderIDStr, ok := orderIDValue.(string); ok && orderIDStr != "" {
			log.Printf(" LIMIT ORDER PLACED SUCCESSFULLY | Market: %s | ID: %s", tracker.Market, orderIDStr)
			return orderIDStr, nil
		} else if orderIDFloat, ok := orderIDValue.(float64); ok {
			orderIDStr := fmt.Sprintf("%.0f", orderIDFloat)
			log.Printf(" LIMIT ORDER PLACED SUCCESSFULLY | Market: %s | ID: %s", tracker.Market, orderIDStr)
			return orderIDStr, nil
		}
	}

	log.Printf(" ORDER ID MISSING | Market: %s | Data: %+v", tracker.Market, data)
	return "", fmt.Errorf("invalid limit order response format - no order_id")
}

// pollRealOrderStatus polls the real order status from CoinEx API until timeout
func (c *CoinexClient) pollRealOrderStatus(tracker *FOKOrderTracker) (bool, error) {
	// Use FOK configuration settings from config
	fokSettings := c.config.FOKOrderSettings
	timeoutDuration := time.Duration(fokSettings.FOKTimeoutSeconds) * time.Second
	pollingInterval := time.Duration(1000/fokSettings.FOKPollingFrequencyHz) * time.Millisecond

	pollCount := 0
	timeout := time.After(timeoutDuration)

	log.Printf(" FOK POLLING CONFIG | Timeout: %dms | Polling: every %dms | Config: %d seconds, %.1f Hz",
		int(timeoutDuration.Milliseconds()),
		int(pollingInterval.Milliseconds()),
		fokSettings.FOKTimeoutSeconds,
		fokSettings.FOKPollingFrequencyHz)

	// Loop until timeout or filled
	for {
		select {
		case <-timeout:
			// Timeout reached - order not filled (FOK behavior)
			log.Printf(" FOK TIMEOUT | ID: %s | After %d attempts - cancelling order", tracker.OrderID, pollCount)
			return false, nil

		default:
			// Poll the order status
			pollCount++

			// For first few polls, check immediately without sleep
			if pollCount <= 5 {
				// Immediate check for first 5 attempts (very aggressive)
			} else if pollCount <= 10 {
				// Very short delay for next 5 attempts
				time.Sleep(10 * time.Millisecond)
			} else {
				// Normal delay for subsequent polls
				time.Sleep(pollingInterval)
			}

			// Single poll attempt
			// Use current timestamp (add debug to check if timestamp is reasonable)
			timestamp := time.Now().UnixMilli()
			method := "GET"
			requestPath := "/v2/spot/order-status"

			// Build query string for order status API - MUST be sorted alphabetically
			queryParams := map[string]string{
				"market":   tracker.Market,
				"order_id": tracker.OrderID,
			}

			// Use encapsulated helper for key extraction and placement
			queryString := c.buildSortedQueryString(queryParams)

			// For GET requests: method + request_path + timestamp (no body)
			// Pass query string separately to signature generation
			signature := c.generateRESTSignature(method, requestPath, queryString, "", timestamp)

			// Set authentication headers (will be handled by performRequest)
			authHeaders := map[string]string{
				"X-COINEX-KEY":       c.apiKey,
				"X-COINEX-SIGN":      signature,
				"X-COINEX-TIMESTAMP": strconv.FormatInt(timestamp, 10),
			}

			// Merge auth headers with existing headers (same as POST requests)
			c.httpClient.MergeHeaders(authHeaders)

			// Prepare request parameters for performRequest (same as POST requests)
			requestParams := map[string]string{
				"url":    c.config.APIBaseURLV2 + requestPath[4:] + "?" + queryString,
				"method": method,
			}

			// Use the main HttpClient which has rate limiting built in
			if pollCount <= 2 {
				log.Printf(" FOK POLLING | ID: %s | Using performRequest | URL: %s", tracker.OrderID, requestParams["url"])
			}

			response, err := c.httpClient.PerformRequest(requestParams, "GET")
			if err != nil {
				if pollCount <= 2 {
					log.Printf(" HTTP REQUEST FAILED | Error: %v", err)
				}
				// For rate limit errors, wait and retry
				if strings.Contains(err.Error(), "rate limit exceeded") {
					if pollCount <= 2 {
						log.Printf(" RATE LIMIT | ID: %s | Attempt: %d | Waiting before retry...", tracker.OrderID, pollCount)
					}
					time.Sleep(time.Second) // Wait 1 second for rate limit
					continue
				}
				// For network errors, continue polling instead of failing immediately
				if strings.Contains(err.Error(), "timeout") || strings.Contains(err.Error(), "deadline") {
					if pollCount <= 2 {
						log.Printf(" NETWORK TIMEOUT | ID: %s | Attempt: %d | Continuing...", tracker.OrderID, pollCount)
					}
					time.Sleep(pollingInterval)
					continue
				}
				return false, fmt.Errorf("HTTP request failed: %w", err)
			}

			// Check response
			if code, ok := response["code"].(float64); !ok || code != 0 {
				message, _ := response["message"].(string)
				if pollCount <= 2 {
					log.Printf(" POLL API ERROR | ID: %s | Code: %.0f | Message: %s", tracker.OrderID, code, message)
				}
				return false, fmt.Errorf("API error: code=%.0f, message=%s", code, message)
			}

			// Extract order data
			data, ok := response["data"].(map[string]interface{})
			if !ok {
				if pollCount <= 2 {
					log.Printf(" POLL RESPONSE FORMAT ERROR | ID: %s", tracker.OrderID)
				}
				return false, fmt.Errorf("invalid response format: missing data")
			}

			// Check status - for FOK orders, check for filled status
			if statusStr, ok := data["status"].(string); ok {
				// FOK orders should fill immediately, so check for completion statuses
				if statusStr == "done" || statusStr == "filled" || statusStr == "closed" || statusStr == "completed" {
					log.Printf(" FOK ORDER FILLED | ID: %s | Status: %s | Attempt: %d", tracker.OrderID, statusStr, pollCount)
					return true, nil // Order is filled
				}

				// For FOK orders, don't check for partial fills on pending orders
				// FOK orders should either fill completely or be rejected
				if statusStr == "pending" || statusStr == "open" {
					// For FOK orders, pending status means the order is still waiting to be filled
					// We should continue polling until timeout or filled
					if pollCount <= 2 {
						log.Printf(" FOK ORDER PENDING | ID: %s | Status: %s | Attempt: %d", tracker.OrderID, statusStr, pollCount)
					}
				}

				// Check for failed/cancelled status
				if statusStr == "cancelled" || statusStr == "failed" || statusStr == "rejected" {
					log.Printf(" FOK ORDER FAILED | ID: %s | Status: %s | Attempt: %d", tracker.OrderID, statusStr, pollCount)
					return false, fmt.Errorf("order failed with status: %s", statusStr)
				}
			}

			// Order is not filled yet, continue to next poll
			// (sleep is handled conditionally above)
		}
	}
}

// cancelRealOrder cancels a real order via CoinEx API v2
func (c *CoinexClient) cancelRealOrder(tracker *FOKOrderTracker) error {
	log.Printf(" CANCELLING ORDER | ID: %s | Market: %s", tracker.OrderID, tracker.Market)

	// Convert order_id from string to integer as required by CoinEx API
	orderIDInt, err := strconv.ParseInt(tracker.OrderID, 10, 64)
	if err != nil {
		log.Printf(" CANCELLATION ORDER ID PARSE ERROR | ID: %s | Error: %v", tracker.OrderID, err)
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
		log.Printf(" CANCELLATION MARSHAL ERROR | ID: %s | Error: %v", tracker.OrderID, err)
		return fmt.Errorf("failed to marshal cancel data: %w", err)
	}
	body := string(bodyBytes)

	log.Printf(" CANCELLATION DATA | ID: %s | Body: %s", tracker.OrderID, body)

	// Generate v2 signature
	signature := c.generateRESTSignature(method, requestPath, "", body, timestamp)

	// Set v2 authentication headers
	authHeaders := map[string]string{
		"X-COINEX-KEY":       c.apiKey,
		"X-COINEX-SIGN":      signature,
		"X-COINEX-TIMESTAMP": strconv.FormatInt(timestamp, 10),
	}

	// Merge auth headers with existing headers
	c.httpClient.MergeHeaders(authHeaders)

	// Prepare request parameters
	requestParams := map[string]string{
		"url":    c.config.APIBaseURLV2 + requestPath[4:],
		"method": method,
		"body":   body,
	}

	log.Printf(" SENDING CANCELLATION REQUEST | ID: %s | URL: %s", tracker.OrderID, requestParams["url"])

	// Make API call
	response, err := c.httpClient.PerformRequest(requestParams, "POST")
	if err != nil {
		log.Printf(" CANCELLATION REQUEST FAILED | ID: %s | Error: %v", tracker.OrderID, err)
		return fmt.Errorf("cancel API request failed: %w", err)
	}

	log.Printf(" CANCELLATION RESPONSE | ID: %s | Response: %+v", tracker.OrderID, response)

	// Check response
	if code, ok := response["code"].(float64); !ok || code != 0 {
		message, _ := response["message"].(string)
		log.Printf(" CANCELLATION API ERROR | ID: %s | Code: %.0f | Message: %s", tracker.OrderID, code, message)
		return fmt.Errorf("CoinEx API error: code=%.0f, message=%s", code, message)
	}

	// Log successful cancellation details
	if data, ok := response["data"].(map[string]interface{}); ok {
		log.Printf(" CANCELLATION SUCCESS | ID: %s | Response data: %+v", tracker.OrderID, data)
	} else {
		log.Printf(" ORDER CANCELLED SUCCESSFULLY | ID: %s | No response data", tracker.OrderID)
	}
	return nil
}

// CancelAllOrders cancels all orders for a specific market using CoinEx API
func (c *CoinexClient) CancelAllOrders(market string) error {
	log.Printf(" CANCELLING ALL ORDERS | Market: %s", market)

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
		log.Printf(" CANCEL ALL MARSHAL ERROR | Market: %s | Error: %v", market, err)
		return fmt.Errorf("failed to marshal cancel all data: %w", err)
	}
	body := string(bodyBytes)

	log.Printf(" CANCEL ALL DATA | Market: %s | Body: %s", market, body)

	// Generate v2 signature
	signature := c.generateRESTSignature(method, requestPath, "", body, timestamp)

	// Set v2 authentication headers
	authHeaders := map[string]string{
		"X-COINEX-KEY":       c.apiKey,
		"X-COINEX-SIGN":      signature,
		"X-COINEX-TIMESTAMP": strconv.FormatInt(timestamp, 10),
	}

	// Merge auth headers with existing headers
	c.httpClient.MergeHeaders(authHeaders)

	// Prepare request parameters
	requestParams := map[string]string{
		"url":    c.config.APIBaseURLV2 + requestPath[4:],
		"method": method,
		"body":   body,
	}

	log.Printf(" SENDING CANCEL ALL REQUEST | Market: %s | URL: %s", market, requestParams["url"])

	// Make API call
	response, err := c.httpClient.PerformRequest(requestParams, "POST")
	if err != nil {
		log.Printf(" CANCEL ALL REQUEST FAILED | Market: %s | Error: %v", market, err)
		return fmt.Errorf("cancel all API request failed: %w", err)
	}

	log.Printf(" CANCEL ALL RESPONSE | Market: %s | Response: %+v", market, response)

	// Check response
	if code, ok := response["code"].(float64); !ok || code != 0 {
		message, _ := response["message"].(string)
		log.Printf(" CANCEL ALL API ERROR | Market: %s | Code: %.0f | Message: %s", market, code, message)
		return fmt.Errorf("CoinEx API error: code=%.0f, message=%s", code, message)
	}

	log.Printf(" ALL ORDERS CANCELLED SUCCESSFULLY | Market: %s", market)
	return nil
}

// checkOrderStatusImmediately checks order status immediately after placement
func (c *CoinexClient) checkOrderStatusImmediately(tracker *FOKOrderTracker) bool {
	// Quick status check without full polling logic
	timestamp := time.Now().UnixMilli()
	method := "GET"
	requestPath := "/v2/spot/order-status"

	// Build query string for order status API
	queryParams := map[string]string{
		"market":   tracker.Market,
		"order_id": tracker.OrderID,
	}

	queryString := c.buildSortedQueryString(queryParams)
	signature := c.generateRESTSignature(method, requestPath, queryString, "", timestamp)

	// Set authentication headers
	authHeaders := map[string]string{
		"X-COINEX-KEY":       c.apiKey,
		"X-COINEX-SIGN":      signature,
		"X-COINEX-TIMESTAMP": strconv.FormatInt(timestamp, 10),
	}

	c.httpClient.MergeHeaders(authHeaders)

	requestParams := map[string]string{
		"url":    c.config.APIBaseURLV2 + requestPath[4:] + "?" + queryString,
		"method": method,
	}

	response, err := c.httpClient.PerformRequest(requestParams, "GET")
	if err != nil {
		log.Printf(" IMMEDIATE STATUS CHECK FAILED | ID: %s | Error: %v", tracker.OrderID, err)
		return false
	}

	// Check response
	if code, ok := response["code"].(float64); !ok || code != 0 {
		log.Printf(" IMMEDIATE STATUS CHECK API ERROR | ID: %s | Code: %.0f", tracker.OrderID, code)
		return false
	}

	// Extract order data
	data, ok := response["data"].(map[string]interface{})
	if !ok {
		log.Printf(" IMMEDIATE STATUS CHECK FORMAT ERROR | ID: %s", tracker.OrderID)
		return false
	}

	// Check status
	if statusStr, ok := data["status"].(string); ok {
		if statusStr == "done" || statusStr == "filled" || statusStr == "closed" || statusStr == "completed" {
			log.Printf(" IMMEDIATE FILL DETECTED | ID: %s | Status: %s", tracker.OrderID, statusStr)
			return true
		}
	}

	return false
}

// getOrderFillData retrieves actual fill data from the order status API
func (c *CoinexClient) getOrderFillData(tracker *FOKOrderTracker) (float64, float64, float64, float64) {
	// Get order status to retrieve fill data
	timestamp := time.Now().UnixMilli()
	method := "GET"
	requestPath := "/v2/spot/order-status"

	// Build query string for order status API
	queryParams := map[string]string{
		"market":   tracker.Market,
		"order_id": tracker.OrderID,
	}

	// Use encapsulated helper for key extraction and placement
	queryString := c.buildSortedQueryString(queryParams)

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
	c.httpClient.MergeHeaders(authHeaders)

	// Prepare request parameters
	requestParams := map[string]string{
		"url":    c.config.APIBaseURLV2 + requestPath[4:] + "?" + queryString,
		"method": method,
	}

	// Make API call
	response, err := c.httpClient.PerformRequest(requestParams, "GET")
	if err != nil {
		log.Printf(" GET FILL DATA FAILED | ID: %s | Error: %v", tracker.OrderID, err)
		return 0, 0, 0, 0
	}

	// Check response
	if code, ok := response["code"].(float64); !ok || code != 0 {
		message, _ := response["message"].(string)
		log.Printf(" GET FILL DATA API ERROR | ID: %s | Code: %.0f | Message: %s", tracker.OrderID, code, message)
		return 0, 0, 0, 0
	}

	// Extract order data
	data, ok := response["data"].(map[string]interface{})
	if !ok {
		log.Printf(" GET FILL DATA RESPONSE FORMAT ERROR | ID: %s", tracker.OrderID)
		return 0, 0, 0, 0
	}

	log.Printf(" FILL DATA RESPONSE | OrderID: %s | Full data: %+v", tracker.OrderID, data)

	// Extract fill data from CoinEx API response
	// CoinEx uses different field names than some other exchanges
	filledAmount := parseFloat(data["filled_amount"])
	filledValue := parseFloat(data["filled_value"])
	avgPrice := parseFloat(data["last_fill_price"])
	fee := parseFloat(data["quote_fee"])

	log.Printf(" FILL DATA DEBUG | OrderID: %s | filled_amount: %v -> %.6f | filled_value: %v -> %.6f | last_fill_price: %v -> %.8f | quote_fee: %v -> %.6f",
		tracker.OrderID, data["filled_amount"], filledAmount, data["filled_value"], filledValue, data["last_fill_price"], avgPrice, data["quote_fee"], fee)

	// Additional debug for raw response values
	log.Printf(" RAW RESPONSE VALUES | filled_amount: %T=%v | filled_value: %T=%v | last_fill_price: %T=%v | quote_fee: %T=%v",
		data["filled_amount"], data["filled_amount"], data["filled_value"], data["filled_value"], data["last_fill_price"], data["last_fill_price"], data["quote_fee"], data["quote_fee"])

	return filledAmount, filledValue, avgPrice, fee
}

// CanPlaceOrder checks if we can place a new order based on spending limits
func (c *CoinexClient) CanPlaceOrder(orderValue float64) bool {
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
func (c *CoinexClient) UpdateSpending(orderValue float64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// This is a placeholder for the actual implementation of spending limits
	// For now, we will just log the update
	log.Printf("Update spending: value=%.2f", orderValue)
}
