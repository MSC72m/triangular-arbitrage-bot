package exchange

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"triangular-arbitrage-bot/internal/config"
	"triangular-arbitrage-bot/pkg/models"
)

func (c *CoinexClient) TestConnection() (string, error) {
	// Test with a public endpoint that doesn't require authentication
	// Use the market list endpoint which is simpler and doesn't need market parameter
	params := map[string]string{
		"url":    c.baseUrl + "/market/list",
		"method": "GET",
	}

	response, err := c.httpClient.PerformRequest(params, "GET")
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

func (c *CoinexClient) GetBalance() (string, error) {
	// Use v2 API for balance
	timestamp := time.Now().UnixMilli()
	method := "GET"
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
	log.Printf("Debug - V2 Auth headers: %+v\n", authHeaders)

	// Merge auth headers with existing headers
	c.httpClient.MergeHeaders(authHeaders)

	// Add the required parameters to the request
	requestParams := map[string]string{
		"url":    strings.TrimSuffix(c.config.APIBaseURLV2, "/") + requestPath,
		"method": method,
	}

	response, err := c.httpClient.PerformRequest(requestParams, method)
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

func (c *CoinexClient) GetMarketList() (string, error) {
	// Use the v1 market list endpoint
	params := map[string]string{
		"url":    c.baseUrl + "/market/list",
		"method": "GET",
	}

	response, err := c.httpClient.PerformRequest(params, "GET")
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
func (c *CoinexClient) GetAllMarketTickers() (map[string]interface{}, error) {
	params := map[string]string{
		"url":    c.baseUrl + "/market/ticker/all",
		"method": "GET",
	}

	response, err := c.httpClient.PerformRequest(params, "GET")
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (c *CoinexClient) GetArbitrageMarkets() ([]string, []string, error) {
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
	log.Printf(" Total markets from CoinEx: %d\n", len(markets))

	// Step 1: Find all USDT and USDC assets separately
	log.Printf(" Step 1: Finding all USDT and USDC assets...\n")
	usdtAssets, usdcAssets := c.findUSDTAndUSDCAssets(markets)

	log.Printf(" Found %d USDT assets and %d USDC assets\n", len(usdtAssets), len(usdcAssets))

	// Step 2: Find assets that have BOTH USDT and USDC pairs
	log.Printf(" Step 2: Finding assets with both USDT and USDC pairs...\n")
	completeAssets, triangularMarkets := c.findCompleteAssets(usdtAssets, usdcAssets)

	log.Printf(" Found %d complete assets with both USDT and USDC pairs\n", len(completeAssets))

	// Step 3: Add the critical USDCUSDT pair
	usdcusdtFound := false
	for _, market := range markets {
		marketStr := market.(string)
		if marketStr == "USDCUSDT" {
			triangularMarkets = append(triangularMarkets, marketStr)
			log.Printf("    Added critical quote pair: %s\n", marketStr)
			usdcusdtFound = true
			break
		}
	}

	if !usdcusdtFound {
		log.Printf("     WARNING: USDCUSDT pair not found in market list!\n")
		log.Printf("     This pair is critical for triangular arbitrage\n")
	}

	log.Printf(" Final result: %d triangular markets and %d complete assets\n", len(triangularMarkets), len(completeAssets))

	return triangularMarkets, completeAssets, nil
}

// findUSDTAndUSDCAssets finds all USDT and USDC assets separately
func (c *CoinexClient) findUSDTAndUSDCAssets(markets []interface{}) ([]string, []string) {
	usdtAssets := []string{}
	usdcAssets := []string{}

	log.Printf("    Analyzing %d total markets for USDT and USDC pairs...\n", len(markets))

	for _, market := range markets {
		marketStr := market.(string)
		if strings.HasSuffix(marketStr, "USDT") {
			usdtAssets = append(usdtAssets, marketStr)
		} else if strings.HasSuffix(marketStr, "USDC") {
			usdcAssets = append(usdcAssets, marketStr)
		}
	}

	// Debug: Show some examples of discovered markets
	log.Printf("    USDT markets found (first 10): ")
	for i, market := range usdtAssets {
		if i < 10 {
			log.Printf("%s ", market)
		}
	}
	if len(usdtAssets) > 10 {
		log.Printf("... and %d more", len(usdtAssets)-10)
	}
	log.Printf("\n")

	log.Printf("    USDC markets found (first 10): ")
	for i, market := range usdcAssets {
		if i < 10 {
			log.Printf("%s ", market)
		}
	}
	if len(usdcAssets) > 10 {
		log.Printf("... and %d more", len(usdcAssets)-10)
	}
	log.Printf("\n")

	return usdtAssets, usdcAssets
}

// findCompleteAssets finds assets that have both USDT and USDC pairs
func (c *CoinexClient) findCompleteAssets(usdtAssets, usdcAssets []string) ([]string, []string) {
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
			log.Printf("     Ignoring blacklisted USDC market: %s\n", market)
			continue
		}

		asset := strings.TrimSuffix(market, "USDC")
		if asset != "" && asset != "USDT" { // Exclude USDTUSDC
			usdcAssetMap[asset] = market
		}
	}

	log.Printf("    USDT assets found: %d\n", len(usdtAssetMap))
	log.Printf("    USDC assets found: %d (excluding blacklisted)\n", len(usdcAssetMap))

	// Find assets that have both USDT and USDC pairs
	for asset := range usdtAssetMap {
		if usdcMarket, hasUSDC := usdcAssetMap[asset]; hasUSDC {
			completeAssets = append(completeAssets, asset)
			// Add both markets for this asset
			triangularMarkets = append(triangularMarkets, usdtAssetMap[asset])
			triangularMarkets = append(triangularMarkets, usdcMarket)
		}
	}

	log.Printf("    Complete assets found: %d\n", len(completeAssets))
	log.Printf("    Incomplete assets found: %d\n", len(usdtAssetMap)+len(usdcAssetMap)-len(completeAssets))
	log.Printf("    USDT assets found: %d\n", len(usdtAssetMap))
	log.Printf("    USDC assets found: %d\n", len(usdcAssetMap))
	log.Printf("    Triangular markets found: %d\n", len(triangularMarkets))

	// Check for assets that have USDC but no USDT
	for asset := range usdcAssetMap {
		if _, hasUSDT := usdtAssetMap[asset]; !hasUSDT {
			log.Printf("     Incomplete asset: %s (has USDC but no USDT)\n", asset)
		}
	}

	return completeAssets, triangularMarkets
}

// filterMarketsByRealActivity filters markets based on real 24H volume data from CoinEx
func (c *CoinexClient) filterMarketsByRealActivity(markets []string, tickerData map[string]interface{}) ([]string, []string) {
	log.Printf("    Analyzing real market activity for %d markets...\n", len(markets))

	// Extract ticker data
	data, ok := tickerData["data"].(map[string]interface{})
	if !ok {
		log.Printf("     Invalid ticker data format, falling back to heuristic filtering\n")
		return c.validateMarketActivity(markets, nil) // Pass nil for marketDepths as fallback
	}

	tickers, ok := data["ticker"].(map[string]interface{})
	if !ok {
		log.Printf("     Invalid ticker format, falling back to heuristic filtering\n")
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
				log.Printf("    Dead: %s (no ticker data)\n", market)
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
				log.Printf("    Active: %s (24H vol: $%.0f)\n", market, usdVolume)
			}
		} else {
			deadMarkets = append(deadMarkets, market)
			if len(deadMarkets) <= 5 {
				log.Printf("    Dead: %s (24H vol: $%.0f < $%.0f threshold)\n", market, usdVolume, minVolumeThreshold)
			}
		}
	}

	if len(activeMarkets) > 5 {
		log.Printf("   ... and %d more active markets\n", len(activeMarkets)-5)
	}
	if len(deadMarkets) > 5 {
		log.Printf("   ... and %d more dead markets\n", len(deadMarkets)-5)
	}

	log.Printf("    Volume Analysis Complete:\n")
	log.Printf("     - Active markets (>$%.0f/24h): %d\n", minVolumeThreshold, len(activeMarkets))
	log.Printf("     - Dead markets (<$%.0f/24h): %d\n", minVolumeThreshold, len(deadMarkets))

	return activeMarkets, deadMarkets
}

// validateMarketActivity tests markets for actual trading activity before subscription
func (c *CoinexClient) validateMarketActivity(markets []string, marketDepths *models.MarketDepths) ([]string, []string) {
	log.Printf("    Testing %d markets for trading activity...\n", len(markets))

	activeMarkets := []string{}
	deadMarkets := []string{}

	// For now, we'll use a simple heuristic based on market naming patterns
	// In production, you'd want to call CoinEx API to check 24h volume or recent trades

	for _, market := range markets {
		isActive := c.isMarketActive(market, marketDepths)

		if isActive {
			activeMarkets = append(activeMarkets, market)
			if len(activeMarkets) <= 5 { // Log first few
				log.Printf("    Active: %s\n", market)
			}
		} else {
			deadMarkets = append(deadMarkets, market)
			if len(deadMarkets) <= 5 { // Log first few
				log.Printf("    Dead: %s (filtered out)\n", market)
			}
		}
	}

	if len(activeMarkets) > 5 {
		log.Printf("   ... and %d more active markets\n", len(activeMarkets)-5)
	}
	if len(deadMarkets) > 5 {
		log.Printf("   ... and %d more dead markets\n", len(deadMarkets)-5)
	}

	return activeMarkets, deadMarkets
}

// isMarketActive determines if a market is likely to be active by checking WebSocket data availability
func (c *CoinexClient) isMarketActive(market string, marketDepths *models.MarketDepths) bool {
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
func (c *CoinexClient) filterCompleteAssetsByActiveMarkets(completeAssets []string, activeMarkets []string) []string {
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
			log.Printf("    Asset %s: Both %sUSDT and %sUSDC are active\n", asset, asset, asset)
		} else {
			log.Printf("    Asset %s excluded: ", asset)
			if !hasActiveUSDT {
				log.Printf("%sUSDT inactive ", asset)
			}
			if !hasActiveUSDC {
				log.Printf("%sUSDC inactive ", asset)
			}
			log.Printf("\n")
		}
	}

	return filteredAssets
}

// CriticalMarketPriceManager handles critical market data efficiently
type CriticalMarketPriceManager struct {
	config *config.Config
}

// NewCriticalMarketPriceManager creates a new critical market price manager
func NewCriticalMarketPriceManager(client *CoinexClient, config *config.Config) *CriticalMarketPriceManager {
	log.Printf(" CRITICAL MARKET PRICE MANAGER | Initialized - all markets use WebSocket data")
	return &CriticalMarketPriceManager{
		config: config,
	}
}

// IsCriticalMarket checks if a market is critical (should be prioritized for WebSocket subscription)
func (cmm CriticalMarketPriceManager) IsCriticalMarket(market string) bool {
	for _, critical := range cmm.config.CriticalMarkets {
		if market == critical {
			return true
		}
	}
	return false
}

// AssumeCriticalMarketExists returns false - no markets are assumed to exist without WebSocket data
func (cmm CriticalMarketPriceManager) AssumeCriticalMarketExists(market string) bool {
	return false // All markets must have WebSocket data
}

// GetAssumedPrice returns 0 - no assumed prices, all prices come from WebSocket
func (cmm CriticalMarketPriceManager) GetAssumedPrice(market string) (float64, bool) {
	return 0.0, false // No assumed prices
}

// GetCriticalMarketSnapshot returns empty snapshot - we don't need price data
func (cmm CriticalMarketPriceManager) GetCriticalMarketSnapshot() map[string]*models.OrderBook {
	// Return empty snapshot - we don't need price data for critical markets
	// The arbitrage engine will handle critical markets differently
	return make(map[string]*models.OrderBook)
}

// EnsureCriticalMarketData ensures critical markets have data via WebSocket only
func (c *CoinexClient) EnsureCriticalMarketData(criticalMarkets []string, marketDepths *models.MarketDepths) {
	log.Printf(" Ensuring critical markets have WebSocket data: %v", criticalMarkets)

	failedCount := 0
	for _, market := range criticalMarkets {
		// Check if market already has data in marketDepths (from WebSocket)
		if orderBook, exists := marketDepths.Load(market); exists && orderBook != nil {
			if len(orderBook.Bids) > 0 && len(orderBook.Asks) > 0 {
				// Market already has WebSocket data, skip
				log.Printf(" %s already has WebSocket data, skipping", market)
				continue
			}
		}

		// Try to subscribe to the market via WebSocket using full flow
		log.Printf(" Attempting WebSocket subscription for %s with full flow...", market)
		if err := c.httpClient.SubscribeWebSocketWithFullFlow([]string{market}, marketDepths); err != nil {
			log.Printf(" WebSocket subscription failed for %s: %v", market, err)
			failedCount++
		} else {
			log.Printf(" WebSocket subscription successful for %s", market)

			// Check if we now have WebSocket data
			if orderBook, exists := marketDepths.Load(market); exists && orderBook != nil {
				if len(orderBook.Bids) > 0 && len(orderBook.Asks) > 0 {
					log.Printf(" %s data available via WebSocket | Bids: %d | Asks: %d",
						market, len(orderBook.Bids), len(orderBook.Asks))
					continue
				}
			}
		}

		// No REST API fallback - if WebSocket fails, just log it
		log.Printf(" %s has no WebSocket data", market)
		failedCount++
	}

	log.Printf(" Critical markets WebSocket summary: %d failed out of %d total", failedCount, len(criticalMarkets))
}

// FetchCriticalMarketsViaWebSocket fetches order book data for critical markets via WebSocket only
// This is used as a primary data source for quote currencies and other critical markets
func (c *CoinexClient) FetchCriticalMarketsViaWebSocket(markets []string, marketDepths *models.MarketDepths) {
	log.Printf(" Subscribing to critical markets via WebSocket with full flow: %v", markets)

	// Use the new full subscription flow that:
	// 1. Subscribes with full=true for initial snapshots
	// 2. Waits for data to arrive
	// 3. Switches to incremental updates (full=false)
	if err := c.httpClient.SubscribeWebSocketWithFullFlow(markets, marketDepths); err != nil {
		log.Printf(" Failed to subscribe to critical markets with full flow: %v", err)
		return
	}

	// Verify that we have data for all critical markets
	successCount := 0
	failedCount := 0

	for _, market := range markets {
		if orderBook, exists := marketDepths.Load(market); exists && orderBook != nil {
			if len(orderBook.Bids) > 0 && len(orderBook.Asks) > 0 {
				log.Printf(" %s has WebSocket data | Bids: %d | Asks: %d",
					market, len(orderBook.Bids), len(orderBook.Asks))
				successCount++
			} else {
				log.Printf(" %s has empty order book via WebSocket", market)
				failedCount++
			}
		} else {
			log.Printf(" %s has no WebSocket data", market)
			failedCount++
		}
	}

	log.Printf(" Critical markets WebSocket summary: %d success, %d failed out of %d total", successCount, failedCount, len(markets))
}

// GetQuoteCurrencyMarkets returns all markets for the configured quote currencies
func (c *CoinexClient) GetQuoteCurrencyMarkets() []string {
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

// GetOpenOrders gets all open orders from the exchange
func (c *CoinexClient) GetOpenOrders() ([]map[string]interface{}, error) {
	// Use v2 API for getting open orders
	timestamp := time.Now().UnixMilli()
	method := "GET"
	requestPath := "/v2/spot/pending-order" // Get unfilled orders endpoint

	// Build query string - MUST be sorted alphabetically
	queryParams := map[string]string{
		"limit":       "100",
		"market_type": "SPOT",
	}

	// Use encapsulated helper for key extraction and placement
	queryString := c.buildSortedQueryString(queryParams)

	// Generate v2 signature with query string
	log.Printf(" SIGNATURE GENERATION DEBUG | Method: %s | Path: %s | Query: %s", method, requestPath, queryString)
	signature := c.generateRESTSignature(method, requestPath, queryString, "", timestamp)

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
		"url":    c.config.APIBaseURLV2 + requestPath[4:] + "?" + queryString,
		"method": method,
	}

	log.Printf("GETTING OPEN ORDERS | Fetching unfilled orders from exchange")

	// Make API call
	response, err := c.httpClient.PerformRequest(requestParams, "GET")
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

	log.Printf(" OPEN ORDERS FETCHED | Found %d unfilled orders", len(orders))
	return orders, nil
}
