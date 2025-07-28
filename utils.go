package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// Global counter for WebSocket debug logging
var wsDataReceivedCounter int

// logBasicMetrics logs basic metrics without prices
func logBasicMetrics(metrics *Metrics, marketDepths *MarketDepths) {
	snapshot := metrics.GetSnapshot()
	availableMarkets := marketDepths.GetAvailableMarkets()

	uptime := time.Since(snapshot.StartTime)

	log.Printf("📊 METRICS | Uptime: %v | Markets: %d | Opportunities: %d | Trades: %d | PnL: $%.4f | Messages: %d",
		uptime.Round(time.Second),
		len(availableMarkets),
		snapshot.OpportunitiesDetected,
		snapshot.TradesExecuted,
		snapshot.TotalPnL,
		snapshot.MessagesProcessed,
	)
}

// logSimpleMetrics logs basic metrics with prices (used for final metrics)
func logSimpleMetrics(metrics *Metrics, marketDepths *MarketDepths) {
	logBasicMetrics(metrics, marketDepths)
	logMarketPrices(marketDepths)
}

// logMarketPrices logs current prices for all connected markets
func logMarketPrices(marketDepths *MarketDepths) {
	snapshot := marketDepths.GetSnapshot()

	if len(snapshot) == 0 {
		log.Printf("💰 PRICES | No market data available")
		return
	}

	log.Printf("💰 MARKET PRICES:")
	for market, orderBook := range snapshot {
		if len(orderBook.Asks) > 0 && len(orderBook.Bids) > 0 {
			askPrice := orderBook.Asks[0].Price
			bidPrice := orderBook.Bids[0].Price
			askAmount := orderBook.Asks[0].Amount
			bidAmount := orderBook.Bids[0].Amount

			log.Printf("   📈 %s | Ask: %s (Vol: %s) | Bid: %s (Vol: %s) | Spread: %.4f%%",
				market, askPrice, askAmount, bidPrice, bidAmount,
				calculateSpread(askPrice, bidPrice))
		} else {
			log.Printf("   📈 %s | No order book data", market)
		}
	}
}

// calculateSpread calculates the bid-ask spread percentage
func calculateSpread(askStr, bidStr string) float64 {
	ask, err1 := strconv.ParseFloat(askStr, 64)
	bid, err2 := strconv.ParseFloat(bidStr, 64)

	if err1 != nil || err2 != nil || ask <= 0 || bid <= 0 {
		return 0
	}

	return ((ask - bid) / bid) * 100
}

// processWebSocketMessage processes incoming WebSocket messages and updates market depths
func processWebSocketMessage(msg []byte, marketDepths *MarketDepths, metrics *Metrics) error {
	var wsResponse map[string]interface{}
	if err := json.Unmarshal(msg, &wsResponse); err != nil {
		return fmt.Errorf("failed to unmarshal WebSocket message: %w", err)
	}

	// Update message counter
	metrics.mu.Lock()
	metrics.MessagesProcessed++
	currentCount := metrics.MessagesProcessed
	metrics.mu.Unlock()

	// Log all message types for debugging (first 20 messages)
	if currentCount <= 20 {
		log.Printf("🔍 WebSocket Message #%d Type: %+v", currentCount, wsResponse)
	}

	// Handle different message types
	method, hasMethod := wsResponse["method"].(string)
	if !hasMethod {
		// Log messages without method field
		if currentCount <= 10 {
			log.Printf("📝 Message without method: %s", string(msg))
		}
		return nil // Ignore messages without method
	}

	// Log method types we receive
	if currentCount <= 20 {
		log.Printf("📋 Message method: %s", method)
	}

	switch method {
	case "depth.update":
		log.Printf("📊 Processing depth.update message")
		return handleDepthUpdate(wsResponse, marketDepths)
	case "server.ping":
		// Ping response - no action needed
		if currentCount <= 5 {
			log.Printf("🏓 Received server.ping")
		}
		return nil
	default:
		// Unknown method - log for debugging
		if currentCount <= 20 {
			log.Printf("❓ Unknown method: %s", method)
		}
		return nil
	}
}

// handleDepthUpdate processes order book depth updates
func handleDepthUpdate(wsResponse map[string]interface{}, marketDepths *MarketDepths) error {
	params, ok := wsResponse["params"].([]interface{})
	if !ok || len(params) < 2 {
		return fmt.Errorf("invalid depth.update params - expected at least 2 parameters")
	}

	// CoinEx depth.update format: [full_flag, order_book_data, market_name]
	// params[0] = full/incremental flag (boolean)
	// params[1] = order book data (object)
	// params[2] = market name (string) - but this might not always be present

	orderBookData, ok := params[1].(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid order book data in depth update - params[1] should be object")
	}

	// Try to get market name from params[2], but have fallback
	var marketName string
	if len(params) >= 3 {
		if marketStr, ok := params[2].(string); ok {
			marketName = marketStr
		}
	}

	// If market name not in params, we might need to track it differently
	// For now, log this case for debugging
	if marketName == "" {
		log.Printf("⚠️  Received depth update without market name in params[2]")
		return fmt.Errorf("missing market name in depth update")
	}

	// Convert to OrderBook struct
	var orderBook OrderBook
	dataBytes, err := json.Marshal(orderBookData)
	if err != nil {
		return fmt.Errorf("failed to marshal order book data: %w", err)
	}

	if err := json.Unmarshal(dataBytes, &orderBook); err != nil {
		return fmt.Errorf("failed to unmarshal order book: %w", err)
	}

	// Store in market depths
	marketDepths.Store(marketName, &orderBook)

	// Debug: Log first few successful data updates to verify WebSocket is working
	wsDataReceivedCounter++
	if wsDataReceivedCounter <= 10 {
		log.Printf("📊 WebSocket Data Received | Market: %s | Bids: %d | Asks: %d",
			marketName, len(orderBook.Bids), len(orderBook.Asks))
	}

	return nil
}

// logMarketDataStatus logs which markets have data vs which are missing
func logMarketDataStatus(marketDepths *MarketDepths, expectedMarkets []string) {
	availableMarkets := marketDepths.GetAvailableMarkets()

	log.Printf("🔍 MARKET DATA STATUS:")
	log.Printf("   📈 Expected markets: %d", len(expectedMarkets))
	log.Printf("   📊 Available markets: %d", len(availableMarkets))

	// Show first few available markets
	log.Printf("   ✅ Available: %v", availableMarkets[:min(10, len(availableMarkets))])

	// Find missing markets
	availableSet := make(map[string]bool)
	for _, market := range availableMarkets {
		availableSet[market] = true
	}

	missing := []string{}
	for _, market := range expectedMarkets {
		if !availableSet[market] {
			missing = append(missing, market)
		}
	}

	if len(missing) > 0 {
		log.Printf("   ❌ Missing: %v", missing[:min(10, len(missing))])
		if len(missing) > 10 {
			log.Printf("   ... and %d more missing markets", len(missing)-10)
		}
	}
}

func loadEnvConfig() map[string]string {
	env := make(map[string]string)

	// First try loading from OS environment
	if osEnvs := os.Environ(); len(osEnvs) > 0 && Includes(osEnvs, "COINEX_API_KEY", "COINEX_SECRET_ID") {
		fmt.Println("Loading env from OS")
		for _, envVar := range osEnvs {
			if key, value := parseEnvVar(envVar); key != "" {
				env[key] = value
			}
		}
		return env
	}

	// Fallback to .env file if no OS environment variables
	envFile, err := os.ReadFile(".env")
	if err != nil {
		fmt.Println("Failed to read .env file: ", err)
		os.Exit(1)
	}

	// Parse .env file contents
	for _, line := range strings.Split(string(envFile), "\n") {
		if key, value := parseEnvVar(line); key != "" {
			env[key] = value
		}
	}

	return env
}

// parseEnvVar splits an environment variable string into key and value
func parseEnvVar(envVar string) (key string, value string) {
	parts := strings.SplitN(strings.TrimSpace(envVar), "=", 2)
	if len(parts) != 2 {
		return "", ""
	}
	return parts[0], parts[1]
}

// Do not remove this function, it is used to load the environment variables to the OS
func loadEnvToOS() error {
	env := loadEnvConfig()
	for key, value := range env {
		if key == "" || value == "" {
			continue
		}
		if os.Getenv(key) != "" {
			continue
		}
		if err := os.Setenv(key, value); err != nil {
			return fmt.Errorf("failed to set env %s: %w", key, err)
		}
	}
	return nil
}

func Includes[T any](arr []string, items ...T) bool {
	for _, item := range items {
		found := false
		itemStr := fmt.Sprintf("%v", item)
		for _, v := range arr {
			// Check if the environment variable starts with the item name followed by =
			if strings.HasPrefix(v, itemStr+"=") {
				found = true
				break
			}
		}
		// If any item is not found, return false
		if !found {
			return false
		}
	}
	// All items were found
	return true
}
