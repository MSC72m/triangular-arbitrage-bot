package main

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
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

	log.Printf(" METRICS | Uptime: %v | Markets: %d | Opportunities: %d | Trades: %d | PnL: $%.4f | Messages: %d",
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
		log.Printf(" PRICES | No market data available")
		return
	}

	log.Printf(" MARKET PRICES:")
	for market, orderBook := range snapshot {
		if len(orderBook.Asks) > 0 && len(orderBook.Bids) > 0 {
			askPrice := orderBook.Asks[0].Price
			bidPrice := orderBook.Bids[0].Price
			askAmount := orderBook.Asks[0].Amount
			bidAmount := orderBook.Bids[0].Amount

			log.Printf("    %s | Ask: %s (Vol: %s) | Bid: %s (Vol: %s) | Spread: %.4f%%",
				market, askPrice, askAmount, bidPrice, bidAmount,
				calculateSpread(askPrice, bidPrice))
		} else {
			log.Printf("    %s | No order book data", market)
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

// decompressGzip decompresses gzip compressed data
func decompressGzip(data []byte) ([]byte, error) {
	reader, err := gzip.NewReader(strings.NewReader(string(data)))
	if err != nil {
		return nil, fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer reader.Close()

	decompressed, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read decompressed data: %w", err)
	}

	return decompressed, nil
}

// processWebSocketMessage processes incoming WebSocket messages and updates market depths
func processWebSocketMessage(msg []byte, marketDepths *MarketDepths, metrics *Metrics, httpClient *HttpClient) error {
	var processedMsg []byte
	if len(msg) > 2 && msg[0] == 0x1f && msg[1] == 0x8b {
		// This is compressed data, decompress it first
		decompressed, err := decompressGzip(msg)
		if err != nil {
			return fmt.Errorf("failed to decompress WebSocket message: %w", err)
		}
		processedMsg = decompressed
	} else {
		// Not compressed, use as is
		processedMsg = msg
	}

	var wsResponse map[string]interface{}
	if err := json.Unmarshal(processedMsg, &wsResponse); err != nil {
		return fmt.Errorf("failed to unmarshal WebSocket message: %w", err)
	}

	// Update message counter
	metrics.mu.Lock()
	metrics.MessagesProcessed++
	metrics.mu.Unlock()

	// Handle different message types
	method, hasMethod := wsResponse["method"].(string)
	if !hasMethod {
		// Ignore messages without method (subscription confirmations, etc.)
		return nil
	}

	switch method {
	case "depth.update":
		return handleDepthUpdate(wsResponse, marketDepths, httpClient)
	case "ticker.update":
		return handleTickerUpdate(wsResponse, marketDepths, httpClient)
	case "server.ping":
		// Ping response - no action needed
		return nil
	case "server.sign":
		// Authentication response - log it
		log.Printf("✅ WebSocket authentication confirmed: %+v", wsResponse)
		return nil
	case "depth.subscribe":
		// Subscription confirmation - log it
		log.Printf("✅ WebSocket depth subscription confirmed: %+v", wsResponse)
		return nil
	case "ticker.subscribe":
		// Subscription confirmation - log it
		log.Printf("✅ WebSocket ticker subscription confirmed: %+v", wsResponse)
		return nil
	default:
		// Unknown method - log it for debugging
		log.Printf("⚠️  Unknown WebSocket method: %s", method)
		return nil
	}
}

// handleDepthUpdate processes depth updates to get order book data
func handleDepthUpdate(wsResponse map[string]interface{}, marketDepths *MarketDepths, httpClient *HttpClient) error {
	// Check if WebSocket processing has been stopped
	if httpClient != nil {
		httpClient.marketDataMu.RLock()
		processingStopped := httpClient.wsProcessingStopped
		httpClient.marketDataMu.RUnlock()

		if processingStopped {
			// Skip processing if shutdown is in progress
			return nil
		}
	}

	// CoinEx API format: {"method": "depth.update", "data": {...}}
	data, ok := wsResponse["data"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid depth.update response - missing data field: %+v", wsResponse)
	}

	// Extract market name
	marketName, ok := data["market"].(string)
	if !ok {
		return fmt.Errorf("invalid market name in depth update: %+v", data)
	}

	// Extract depth data
	depthData, ok := data["depth"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid depth data in depth update: %+v", data)
	}

	// Check if this is a full snapshot or incremental update
	isFull, _ := data["is_full"].(bool)

	// Extract latest price - try multiple locations
	var latestPrice float64 = 0

	// Try to get latest price from data object first
	if lastPriceStr, ok := data["last"].(string); ok {
		if price, err := strconv.ParseFloat(lastPriceStr, 64); err == nil {
			latestPrice = price
		}
	} else if lastPriceStr, ok := data["last"].(float64); ok {
		latestPrice = lastPriceStr
	} else {
		// Try to get from depth data
		if lastPriceStr, ok := depthData["last"].(string); ok {
			if price, err := strconv.ParseFloat(lastPriceStr, 64); err == nil {
				latestPrice = price
			}
		} else if lastPriceStr, ok := depthData["last"].(float64); ok {
			latestPrice = lastPriceStr
		}
	}

	// Get existing order book or create new one
	var orderBook *OrderBook
	if isFull {
		// Full snapshot - create new order book
		orderBook = &OrderBook{
			Asks:   []Depth{},
			Bids:   []Depth{},
			Latest: latestPrice,
			Market: marketName,
		}
	} else {
		// Incremental update - get existing order book
		existingOrderBook, exists := marketDepths.Load(marketName)
		if !exists || existingOrderBook == nil {
			// No existing data, create new order book
			orderBook = &OrderBook{
				Asks:   []Depth{},
				Bids:   []Depth{},
				Latest: latestPrice,
				Market: marketName,
			}
		} else {
			// Use existing order book and update latest price
			orderBook = existingOrderBook
			orderBook.Latest = latestPrice
		}
	}

	// Parse asks (sell orders) - accept data as provided by API
	if asksData, ok := depthData["asks"].([]interface{}); ok {
		if isFull {
			// Full snapshot - replace all asks
			orderBook.Asks = []Depth{}
			for _, ask := range asksData {
				if askArray, ok := ask.([]interface{}); ok && len(askArray) >= 2 {
					if priceStr, ok := askArray[0].(string); ok {
						if amountStr, ok := askArray[1].(string); ok {
							orderBook.Asks = append(orderBook.Asks, Depth{
								Price:  priceStr,
								Amount: amountStr,
							})
						}
					}
				}
			}
		} else {
			// Incremental update - only update existing asks, don't recreate
			for _, ask := range asksData {
				if askArray, ok := ask.([]interface{}); ok && len(askArray) >= 2 {
					if priceStr, ok := askArray[0].(string); ok {
						if amountStr, ok := askArray[1].(string); ok {
							// Find existing ask with same price
							found := false
							for i, existingAsk := range orderBook.Asks {
								if existingAsk.Price == priceStr {
									// Update amount
									if amountStr == "0" {
										// Remove this ask level
										orderBook.Asks = append(orderBook.Asks[:i], orderBook.Asks[i+1:]...)
									} else {
										// Update amount
										orderBook.Asks[i].Amount = amountStr
									}
									found = true
									break
								}
							}
							if !found && amountStr != "0" {
								// Add new ask level - API provides them in correct order, just prepend
								newAsk := Depth{Price: priceStr, Amount: amountStr}
								orderBook.Asks = append([]Depth{newAsk}, orderBook.Asks...)
							}
						}
					}
				}
			}
		}
	}

	// Parse bids (buy orders) - accept data as provided by API
	if bidsData, ok := depthData["bids"].([]interface{}); ok {
		if isFull {
			// Full snapshot - replace all bids
			orderBook.Bids = []Depth{}
			for _, bid := range bidsData {
				if bidArray, ok := bid.([]interface{}); ok && len(bidArray) >= 2 {
					if priceStr, ok := bidArray[0].(string); ok {
						if amountStr, ok := bidArray[1].(string); ok {
							orderBook.Bids = append(orderBook.Bids, Depth{
								Price:  priceStr,
								Amount: amountStr,
							})
						}
					}
				}
			}
		} else {
			// Incremental update - only update existing bids, don't recreate
			for _, bid := range bidsData {
				if bidArray, ok := bid.([]interface{}); ok && len(bidArray) >= 2 {
					if priceStr, ok := bidArray[0].(string); ok {
						if amountStr, ok := bidArray[1].(string); ok {
							// Find existing bid with same price
							found := false
							for i, existingBid := range orderBook.Bids {
								if existingBid.Price == priceStr {
									// Update amount
									if amountStr == "0" {
										// Remove this bid level
										orderBook.Bids = append(orderBook.Bids[:i], orderBook.Bids[i+1:]...)
									} else {
										// Update amount
										orderBook.Bids[i].Amount = amountStr
									}
									found = true
									break
								}
							}
							if !found && amountStr != "0" {
								// Add new bid level - API provides them in correct order, just prepend
								newBid := Depth{Price: priceStr, Amount: amountStr}
								orderBook.Bids = append([]Depth{newBid}, orderBook.Bids...)
							}
						}
					}
				}
			}
		}
	}

	// Store in market depths with the correct market name
	marketDepths.Store(marketName, orderBook)

	// Also store in httpClient's marketData cache for enhanced validation
	if httpClient != nil {
		httpClient.marketDataMu.Lock()
		httpClient.marketData[marketName] = orderBook
		httpClient.marketDataMu.Unlock()
	}

	// Validate data freshness
	if len(orderBook.Bids) > 0 && len(orderBook.Asks) > 0 {
		// Log data freshness for first few updates
		wsDataReceivedCounter++
		if wsDataReceivedCounter <= 10 {
			bidPrice := orderBook.Bids[0].Price
			askPrice := orderBook.Asks[0].Price
			log.Printf(" WebSocket Data Received | Market: %s | Bids: %d | Asks: %d | Best Bid: %s | Best Ask: %s | Latest: %.8f | Full: %v",
				marketName, len(orderBook.Bids), len(orderBook.Asks), bidPrice, askPrice, orderBook.Latest, isFull)
		}
		// Log every 100th update to track continuous data flow
		if wsDataReceivedCounter%100 == 0 {
			bidPrice := orderBook.Bids[0].Price
			askPrice := orderBook.Asks[0].Price
			log.Printf(" WebSocket Update #%d | Market: %s | Bid: %s | Ask: %s | Latest: %.8f | Time: %s | Full: %v",
				wsDataReceivedCounter, marketName, bidPrice, askPrice, orderBook.Latest, time.Now().Format("15:04:05"), isFull)
		}
	} else {
		log.Printf(" WebSocket Data Received | Market: %s | Empty order book (Bids: %d, Asks: %d) | Latest: %.8f | Full: %v",
			marketName, len(orderBook.Bids), len(orderBook.Asks), orderBook.Latest, isFull)
	}

	return nil
}

// handleTickerUpdate processes ticker updates to get latest prices
func handleTickerUpdate(wsResponse map[string]interface{}, marketDepths *MarketDepths, httpClient *HttpClient) error {
	// Check if WebSocket processing has been stopped
	if httpClient != nil {
		httpClient.marketDataMu.RLock()
		processingStopped := httpClient.wsProcessingStopped
		httpClient.marketDataMu.RUnlock()

		if processingStopped {
			// Skip processing if shutdown is in progress
			return nil
		}
	}

	// CoinEx API format: {"method": "ticker.update", "data": {...}}
	data, ok := wsResponse["data"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid ticker.update response - missing data field: %+v", wsResponse)
	}

	// Extract market name
	marketName, ok := data["market"].(string)
	if !ok {
		return fmt.Errorf("invalid market name in ticker update: %+v", data)
	}

	// Extract ticker data
	tickerData, ok := data["ticker"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid ticker data in ticker update: %+v", data)
	}

	// Extract latest price
	var latestPrice float64 = 0
	if lastPriceStr, ok := tickerData["last"].(string); ok {
		if price, err := strconv.ParseFloat(lastPriceStr, 64); err == nil {
			latestPrice = price
		}
	}

	if latestPrice > 0 {
		// Get existing order book or create new one
		orderBook, exists := marketDepths.Load(marketName)
		if !exists || orderBook == nil {
			orderBook = &OrderBook{
				Asks:   []Depth{},
				Bids:   []Depth{},
				Latest: latestPrice,
				Market: marketName,
			}
		} else {
			// Update existing order book with latest price
			orderBook.Latest = latestPrice
		}

		// Store in market depths
		marketDepths.Store(marketName, orderBook)

		// Also store in httpClient's marketData cache
		if httpClient != nil {
			httpClient.marketDataMu.Lock()
			httpClient.marketData[marketName] = orderBook
			httpClient.marketDataMu.Unlock()
		}
	}

	return nil
}

// logMarketDataStatus logs which markets have data vs which are missing
func logMarketDataStatus(marketDepths *MarketDepths, expectedMarkets []string) {
	availableMarkets := marketDepths.GetAvailableMarkets()

	log.Printf(" MARKET DATA STATUS:")
	log.Printf("    Expected markets: %d", len(expectedMarkets))
	log.Printf("    Available markets: %d", len(availableMarkets))

	// Show first few available markets
	log.Printf("    Available: %v", availableMarkets[:Min(10, len(availableMarkets))])

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
		log.Printf("    Missing: %v", missing[:Min(10, len(missing))])
		if len(missing) > 10 {
			log.Printf("    ... and %d more missing markets", len(missing)-10)
		}
	}
}

// logDetailedMarketData logs detailed market data for debugging price issues
func logDetailedMarketData(marketDepths *MarketDepths, markets []string) {
	log.Printf(" DETAILED MARKET DATA DEBUG:")

	for _, market := range markets {
		if orderBook, exists := marketDepths.Load(market); exists && orderBook != nil {
			if len(orderBook.Asks) > 0 && len(orderBook.Bids) > 0 {
				askPrice := orderBook.Asks[0].Price
				bidPrice := orderBook.Bids[0].Price
				askAmount := orderBook.Asks[0].Amount
				bidAmount := orderBook.Bids[0].Amount

				log.Printf("    %s | Ask: %s (Vol: %s) | Bid: %s (Vol: %s) | Spread: %.4f%%",
					market, askPrice, askAmount, bidPrice, bidAmount,
					calculateSpread(askPrice, bidPrice))
			} else {
				log.Printf("    %s | No order book data (Asks: %d, Bids: %d)",
					market, len(orderBook.Asks), len(orderBook.Bids))
			}
		} else {
			log.Printf("    %s | No market data available", market)
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

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
