package utils

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"triangular-arbitrage-bot/pkg/models"
)

// Global counter for WebSocket debug logging
var wsDataReceivedCounter int
var wsDataCounterMutex sync.RWMutex

// Global counters for periodic snapshot tracking
var (
	updateCountsByMarket = make(map[string]int)
	updateCountsMutex    sync.RWMutex
)

// shouldRequestSnapshot determines if we should request a full snapshot for a market
func shouldRequestSnapshot(marketName string) bool {
	updateCountsMutex.Lock()
	defer updateCountsMutex.Unlock()

	count := updateCountsByMarket[marketName]
	count++
	updateCountsByMarket[marketName] = count

	// Request snapshot every 1000 updates to ensure order book integrity
	// This helps recover from any missed zero-quantity updates
	return count%1000 == 0
}

// logBasicMetrics logs basic metrics without prices
func logBasicMetrics(metrics *models.Metrics, marketDepths *models.MarketDepths) {
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
func logSimpleMetrics(metrics *models.Metrics, marketDepths *models.MarketDepths) {
	logBasicMetrics(metrics, marketDepths)
	logMarketPrices(marketDepths)
}

// logMarketPrices logs current prices for all connected markets
func logMarketPrices(marketDepths *models.MarketDepths) {
	snapshot := marketDepths.GetSnapshot()

	if len(snapshot) == 0 {
		log.Printf(" PRICES | No market data available")
		return
	}

	log.Printf(" MARKET PRICES:")
	for market, OrderBook := range snapshot {
		if len(OrderBook.Asks) > 0 && len(OrderBook.Bids) > 0 {
			askPrice := OrderBook.Asks[0].Price
			bidPrice := OrderBook.Bids[0].Price
			askAmount := OrderBook.Asks[0].Amount
			bidAmount := OrderBook.Bids[0].Amount

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
func processWebSocketMessage(msg []byte, marketDepths *models.MarketDepths, metrics *models.Metrics, httpClient *HttpClient) error {
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
		// Try to log a truncated version of the message for debugging
		msgPreview := string(processedMsg)
		if len(msgPreview) > 200 {
			msgPreview = msgPreview[:200] + "..."
		}
		return fmt.Errorf("failed to unmarshal WebSocket message: %w | Message preview: %s", err, msgPreview)
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
		return fmt.Errorf("invalid depth.update response - missing data field")
	}

	// Extract market name
	marketName, ok := data["market"].(string)
	if !ok {
		return fmt.Errorf("invalid market name in depth update")
	}

	// Extract depth data
	depthData, ok := data["depth"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid depth data in depth update")
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

	// Always get existing order book from data store first
	OrderBook, exists := marketDepths.Load(marketName)

	// Only create new order book if it doesn't exist in data store
	if !exists || OrderBook == nil {
		// Create new order book for this market
		OrderBook = &OrderBook{
			Asks:   []Depth{},
			Bids:   []Depth{},
			Latest: latestPrice,
			Market: marketName,
		}
	} else {
		// Use existing order book and update latest price
		OrderBook.Latest = latestPrice
	}

	// Parse bids (CoinEx "bids" are buy orders)
	if rawBidsData, ok := depthData["bids"].([]interface{}); ok {
		if isFull {
			// Full snapshot - replace all bids
			OrderBook.Bids = []Depth{}
			for _, rawBid := range rawBidsData {
				if bidArray, ok := rawBid.([]interface{}); ok && len(bidArray) >= 2 {
					if priceStr, ok := bidArray[0].(string); ok {
						if amountStr, ok := bidArray[1].(string); ok {
							// Validate price and amount
							if priceStr == "" || amountStr == "" {
								continue
							}

							// Validate price is a valid number
							if _, err := strconv.ParseFloat(priceStr, 64); err != nil {
								continue
							}

							// For full snapshots, simply append all bids (they will be sorted later)
							OrderBook.Bids = append(OrderBook.Bids, Depth{
								Price:  priceStr,
								Amount: amountStr,
							})
						}
					}
				}
			}
		} else {
			// Incremental update - simplified: just append or remove
			for _, rawBid := range rawBidsData {
				if bidArray, ok := rawBid.([]interface{}); ok && len(bidArray) >= 2 {
					if priceStr, ok := bidArray[0].(string); ok {
						if amountStr, ok := bidArray[1].(string); ok {
							// Validate price and amount
							if priceStr == "" || amountStr == "" {
								continue
							}

							// Validate price is a valid number
							if _, err := strconv.ParseFloat(priceStr, 64); err != nil {
								continue
							}

							// Find existing bid with same price
							found := false
							for i, existingBid := range OrderBook.Bids {
								if existingBid.Price == priceStr {
									if amountStr == "0" {
										// Remove this bid level
										OrderBook.Bids = append(OrderBook.Bids[:i], OrderBook.Bids[i+1:]...)
									} else {
										// Update amount
										OrderBook.Bids[i].Amount = amountStr
									}
									found = true
									break
								}
							}
							if !found && amountStr != "0" {
								// Add new bid level - just append, will sort later
								OrderBook.Bids = append(OrderBook.Bids, Depth{Price: priceStr, Amount: amountStr})
							}
						}
					}
				}
			}
		}
	}

	// Parse asks (CoinEx "asks" are sell orders)
	if rawAsksData, ok := depthData["asks"].([]interface{}); ok {
		if isFull {
			// Full snapshot - replace all asks
			OrderBook.Asks = []Depth{}
			for _, rawAsk := range rawAsksData {
				if askArray, ok := rawAsk.([]interface{}); ok && len(askArray) >= 2 {
					if priceStr, ok := askArray[0].(string); ok {
						if amountStr, ok := askArray[1].(string); ok {
							// Validate price and amount
							if priceStr == "" || amountStr == "" {
								continue
							}

							// Validate price is a valid number
							if _, err := strconv.ParseFloat(priceStr, 64); err != nil {
								continue
							}

							// For full snapshots, simply append all asks (they will be sorted later)
							OrderBook.Asks = append(OrderBook.Asks, Depth{
								Price:  priceStr,
								Amount: amountStr,
							})
						}
					}
				}
			}
		} else {
			// Incremental update - simplified: just append or remove
			for _, rawAsk := range rawAsksData {
				if askArray, ok := rawAsk.([]interface{}); ok && len(askArray) >= 2 {
					if priceStr, ok := askArray[0].(string); ok {
						if amountStr, ok := askArray[1].(string); ok {
							// Validate price and amount
							if priceStr == "" || amountStr == "" {
								continue
							}

							// Validate price is a valid number
							if _, err := strconv.ParseFloat(priceStr, 64); err != nil {
								continue
							}

							// Find existing ask with same price
							found := false
							for i, existingAsk := range OrderBook.Asks {
								if existingAsk.Price == priceStr {
									if amountStr == "0" {
										// Remove this ask level
										OrderBook.Asks = append(OrderBook.Asks[:i], OrderBook.Asks[i+1:]...)
									} else {
										// Update amount
										OrderBook.Asks[i].Amount = amountStr
									}
									found = true
									break
								}
							}
							if !found && amountStr != "0" {
								// Add new ask level - just append, will sort later
								OrderBook.Asks = append(OrderBook.Asks, Depth{Price: priceStr, Amount: amountStr})
							}
						}
					}
				}
			}
		}
	}

	// Sort asks in ascending order (lowest ask first)
	sort.Slice(OrderBook.Asks, func(i, j int) bool {
		pi, _ := strconv.ParseFloat(OrderBook.Asks[i].Price, 64)
		pj, _ := strconv.ParseFloat(OrderBook.Asks[j].Price, 64)
		return pi < pj // ascending order for asks
	})

	// Sort bids in descending order (highest bid first)
	sort.Slice(OrderBook.Bids, func(i, j int) bool {
		pi, _ := strconv.ParseFloat(OrderBook.Bids[i].Price, 64)
		pj, _ := strconv.ParseFloat(OrderBook.Bids[j].Price, 64)
		return pi > pj // descending order for bids
	})

	// Store in market depths with the correct market name
	marketDepths.Store(marketName, OrderBook)

	// Also store in httpClient's marketData cache for enhanced validation
	if httpClient != nil {
		httpClient.marketDataMu.Lock()
		httpClient.marketData[marketName] = OrderBook
		httpClient.marketDataMu.Unlock()
	}

	// Check if we should request a periodic snapshot for this market (much less frequent)
	if shouldRequestSnapshot(marketName) {
		log.Printf("📸 PERIODIC SNAPSHOT REQUESTED | Market: %s | Update count: %d",
			marketName, updateCountsByMarket[marketName])
	}

	// Minimal logging - only log first few updates or errors
	wsDataCounterMutex.Lock()
	wsDataReceivedCounter++
	counter := wsDataReceivedCounter
	wsDataCounterMutex.Unlock()

	// Only log first 3 updates and every 10000th update
	if counter <= 3 || counter%10000 == 0 {
		if len(OrderBook.Bids) > 0 && len(OrderBook.Asks) > 0 {
			bidPrice := OrderBook.Bids[0].Price
			askPrice := OrderBook.Asks[0].Price
			log.Printf("📊 WS Update #%d | %s | Bid: %s | Ask: %s | Full: %v",
				counter, marketName, bidPrice, askPrice, isFull)
		}
	}

	return nil
}

func OrderBookStructure(OrderBook *OrderBook) {
	// Validate asks are in ascending order (lowest ask first)
	for i := 1; i < len(OrderBook.Asks); i++ {
		prevPrice, err1 := strconv.ParseFloat(OrderBook.Asks[i-1].Price, 64)
		currPrice, err2 := strconv.ParseFloat(OrderBook.Asks[i].Price, 64)
		if err1 != nil || err2 != nil {
			log.Printf("⚠️ ORDER BOOK VALIDATION | Market: %s | Invalid ask price format", OrderBook.Market)
			continue
		}
		if prevPrice > currPrice {
			log.Printf("⚠️ ORDER BOOK VALIDATION | Market: %s | Asks not properly sorted: %.8f > %.8f",
				OrderBook.Market, prevPrice, currPrice)
		}
	}

	// Validate bids are in descending order (highest bid first)
	for i := 1; i < len(OrderBook.Bids); i++ {
		prevPrice, err1 := strconv.ParseFloat(OrderBook.Bids[i-1].Price, 64)
		currPrice, err2 := strconv.ParseFloat(OrderBook.Bids[i].Price, 64)
		if err1 != nil || err2 != nil {
			log.Printf("⚠️ ORDER BOOK VALIDATION | Market: %s | Invalid bid price format", OrderBook.Market)
			continue
		}
		if prevPrice < currPrice {
			log.Printf("⚠️ ORDER BOOK VALIDATION | Market: %s | Bids not properly sorted: %.8f < %.8f",
				OrderBook.Market, prevPrice, currPrice)
		}
	}

	// Validate that asks are not bigger than bids (no crossed or inverted order book)
	if len(OrderBook.Bids) > 0 && len(OrderBook.Asks) > 0 {
		bestBid, errBid := strconv.ParseFloat(OrderBook.Bids[0].Price, 64)
		bestAsk, errAsk := strconv.ParseFloat(OrderBook.Asks[0].Price, 64)
		if errBid != nil || errAsk != nil {
			log.Printf("⚠️ ORDER BOOK VALIDATION | Market: %s | Invalid best bid/ask price format", OrderBook.Market)
		} else if bestAsk > bestBid {
			log.Printf("⚠️ ORDER BOOK VALIDATION | Market: %s | Asks are bigger than bids: Best Ask %.8f > Best Bid %.8f",
				OrderBook.Market, bestAsk, bestBid)
		}
	}
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
		OrderBook, exists := marketDepths.Load(marketName)
		if !exists || OrderBook == nil {
			OrderBook = &OrderBook{
				Asks:   []Depth{},
				Bids:   []Depth{},
				Latest: latestPrice,
				Market: marketName,
			}
		} else {
			// Update existing order book with latest price
			OrderBook.Latest = latestPrice
		}

		// Store in market depths
		marketDepths.Store(marketName, OrderBook)

		// Also store in httpClient's marketData cache
		if httpClient != nil {
			httpClient.marketDataMu.Lock()
			httpClient.marketData[marketName] = OrderBook
			httpClient.marketDataMu.Unlock()
		}
	}

	return nil
}

// logMarketDataStatus logs which markets have data vs which are missing
func logMarketDataStatus(marketDepths *models.MarketDepths, expectedMarkets []string) {
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
func logDetailedMarketData(marketDepths *models.MarketDepths, markets []string) {
	log.Printf(" DETAILED MARKET DATA DEBUG:")

	for _, market := range markets {
		if OrderBook, exists := marketDepths.Load(market); exists && OrderBook != nil {
			if len(OrderBook.Asks) > 0 && len(OrderBook.Bids) > 0 {
				askPrice := OrderBook.Asks[0].Price
				bidPrice := OrderBook.Bids[0].Price
				askAmount := OrderBook.Asks[0].Amount
				bidAmount := OrderBook.Bids[0].Amount

				log.Printf("    %s | Ask: %s (Vol: %s) | Bid: %s (Vol: %s) | Spread: %.4f%%",
					market, askPrice, askAmount, bidPrice, bidAmount,
					calculateSpread(askPrice, bidPrice))
			} else {
				log.Printf("    %s | No order book data (Asks: %d, Bids: %d)",
					market, len(OrderBook.Asks), len(OrderBook.Bids))
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
