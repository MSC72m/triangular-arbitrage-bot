package main

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

type coinexClient struct {
	httpClient     *HttpClient
	apiKey         string
	secretKey      string
	baseUrl        string
	baseUrlV2      string
	allowedMarkets []string
}

func NewCoinexClient(httpClient *HttpClient, config *Config) *coinexClient {
	fmt.Printf("Loading CoinEx client with quote currencies from config: %v\n", config.QuoteCurrencies)
	return &coinexClient{
		httpClient:     httpClient,
		apiKey:         config.APIKey,
		secretKey:      config.SecretKey,
		allowedMarkets: config.QuoteCurrencies,
		baseUrl:        "https://api.coinex.com/v1",
		baseUrlV2:      "https://api.coinex.com/v2",
	}
}

func (c coinexClient) GetApiKey() string {
	return c.apiKey
}

// generateSignature creates MD5 signature according to CoinEx API v1 specification
func (c *coinexClient) generateSignature(params map[string]string) string {
	// Add access_id and tonce to params
	params["access_id"] = c.apiKey
	params["tonce"] = strconv.FormatInt(time.Now().UnixMilli(), 10)

	// Sort parameters alphabetically
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Create query string
	var queryParts []string
	for _, k := range keys {
		queryParts = append(queryParts, k+"="+params[k])
	}
	queryString := strings.Join(queryParts, "&")

	// Add secret key to the end
	signString := queryString + "&secret_key=" + c.secretKey

	// Create MD5 hash
	h := md5.New()
	h.Write([]byte(signString))
	return strings.ToUpper(hex.EncodeToString(h.Sum(nil)))
}

// generateV2Signature creates HMAC-SHA256 signature for CoinEx API v2
func (c *coinexClient) generateV2Signature(method, requestPath, queryString, body string, timestamp int64) string {
	// Create the string to sign for v2 API
	// Format: method + requestPath + queryString + body + timestamp
	var stringToSign string
	if queryString != "" {
		stringToSign = method + requestPath + "?" + queryString + body + strconv.FormatInt(timestamp, 10)
	} else {
		stringToSign = method + requestPath + body + strconv.FormatInt(timestamp, 10)
	}

	// Create HMAC-SHA256 signature
	h := hmac.New(sha256.New, []byte(c.secretKey))
	h.Write([]byte(stringToSign))
	return hex.EncodeToString(h.Sum(nil))
}

func (c coinexClient) PlaceOrder() string {
	return ""
}

func (c *coinexClient) TestConnection() (string, error) {
	// Test with a public endpoint that doesn't require authentication
	// Use the market list endpoint which is simpler and doesn't need market parameter
	params := map[string]string{
		"url":    c.baseUrl + "/market/list",
		"method": "GET",
	}

	response, err := c.httpClient.performRequest(params, "GET")
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
	method := "GET"
	requestPath := "/assets/credit/balance"
	queryString := ""
	body := ""

	// Generate v2 signature
	signature := c.generateV2Signature(method, requestPath, queryString, body, timestamp)

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
		"url":    c.baseUrlV2 + requestPath,
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
		"method": "GET",
	}

	response, err := c.httpClient.performRequest(params, "GET")
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
		"method": "GET",
	}

	response, err := c.httpClient.performRequest(params, "GET")
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
	fmt.Printf("📊 Total markets from CoinEx: %d\n", len(markets))

	// Step 1: Find theoretical triangular arbitrage opportunities
	fmt.Printf("🔍 Step 1: Finding theoretical triangular markets...\n")
	triangularMarkets, completeAssets := c.findTriangularArbitrageMarkets(markets)

	// Step 2: Get real market activity data (24H volume) for ALL markets
	fmt.Printf("🔍 Step 2: Fetching real market activity data from CoinEx...\n")
	tickerData, err := c.GetAllMarketTickers()
	if err != nil {
		fmt.Printf("⚠️  Failed to get market ticker data, using heuristic filtering: %v\n", err)
		// Fallback to heuristic filtering
		activeMarkets, _ := c.validateMarketActivity(triangularMarkets)
		activeCompleteAssets := c.filterCompleteAssetsByActiveMarkets(completeAssets, activeMarkets)
		return activeMarkets, activeCompleteAssets, nil
	}

	// Step 3: Filter markets based on real 24H volume data
	activeMarkets, deadMarkets := c.filterMarketsByRealActivity(triangularMarkets, tickerData)

	fmt.Printf("\n🎯 REAL DATA FILTERING RESULTS:\n")
	fmt.Printf("   📊 Theoretical markets: %d\n", len(triangularMarkets))
	fmt.Printf("   ✅ Active markets (with volume): %d\n", len(activeMarkets))
	fmt.Printf("   💀 Dead markets (no volume): %d\n", len(deadMarkets))
	fmt.Printf("   💰 Efficiency gain: %.1f%% reduction in subscriptions\n",
		float64(len(deadMarkets))/float64(len(triangularMarkets))*100)

	if len(deadMarkets) > 0 {
		fmt.Printf("   🚫 Dead markets (no 24H volume): %v\n", deadMarkets[:min(10, len(deadMarkets))])
		if len(deadMarkets) > 10 {
			fmt.Printf("   ... and %d more dead markets\n", len(deadMarkets)-10)
		}
	}

	// Update complete assets based on active markets only
	activeCompleteAssets := c.filterCompleteAssetsByActiveMarkets(completeAssets, activeMarkets)

	fmt.Printf("   🎯 Complete assets with active markets: %d (from %d)\n",
		len(activeCompleteAssets), len(completeAssets))

	if len(activeCompleteAssets) > 0 {
		fmt.Printf("   📈 Active triangular assets: %v\n", activeCompleteAssets)
	}

	return activeMarkets, activeCompleteAssets, nil
}

// findTriangularArbitrageMarkets finds markets suitable for triangular arbitrage cycles
func (c *coinexClient) findTriangularArbitrageMarkets(markets []interface{}) ([]string, []string) {
	// Group markets by the configured quote currencies only
	usdtMarkets := make(map[string]string) // asset -> market (e.g., BTC -> BTCUSDT)
	usdcMarkets := make(map[string]string) // asset -> market (e.g., BTC -> BTCUSDC)

	fmt.Printf("🔍 Analyzing markets for triangular arbitrage with quote currencies: %v\n", c.allowedMarkets)

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

	fmt.Printf("✅ Market Analysis Results:\n")
	fmt.Printf("   📈 USDT pairs: %d\n", len(usdtMarkets))
	fmt.Printf("   💎 USDC pairs: %d\n", len(usdcMarkets))

	// Debug: Show actual USDC markets found
	if len(usdcMarkets) > 0 {
		fmt.Printf("   🔍 USDC markets found: ")
		for _, market := range usdcMarkets {
			fmt.Printf("%s ", market)
		}
		fmt.Printf("\n")
	} else {
		fmt.Printf("   ⚠️  NO USDC markets found in CoinEx market list!\n")
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
			fmt.Printf("   ✅ Complete asset: %s → Markets: %s, %s\n",
				asset, usdtMarkets[asset], usdcMarkets[asset])
		}
	}

	// Sort complete assets to prioritize major/liquid assets first
	majorAssets := []string{"BTC", "ETH", "BNB", "SOL", "ADA", "DOT", "AVAX", "MATIC", "LINK", "UNI"}
	prioritizedAssets := []string{}
	remainingAssets := []string{}

	// Add major assets first if they exist
	for _, major := range majorAssets {
		for _, asset := range completeAssets {
			if asset == major {
				prioritizedAssets = append(prioritizedAssets, asset)
				break
			}
		}
	}

	// Add remaining assets
	for _, asset := range completeAssets {
		isMajor := false
		for _, major := range majorAssets {
			if asset == major {
				isMajor = true
				break
			}
		}
		if !isMajor {
			remainingAssets = append(remainingAssets, asset)
		}
	}

	// Rebuild the asset list with priorities
	completeAssets = append(prioritizedAssets, remainingAssets...)

	if len(prioritizedAssets) > 0 {
		fmt.Printf("   🎯 Priority assets found: %v\n", prioritizedAssets)
	}

	// Add the direct quote pair if it exists (USDC/USDT)
	for _, market := range markets {
		marketStr := market.(string)
		if marketStr == "USDCUSDT" || marketStr == "USDTUSDC" {
			triangularMarkets = append(triangularMarkets, marketStr)
			fmt.Printf("   🎯 Added critical quote pair: %s\n", marketStr)
		}
	}

	fmt.Printf("\n🎯 TRIANGULAR ARBITRAGE SUMMARY:\n")
	fmt.Printf("   📊 Assets with both quote pairs: %d\n", len(completeAssets))
	fmt.Printf("   📡 Markets to subscribe: %d (vs %d total)\n", len(triangularMarkets), len(markets))
	fmt.Printf("   💰 Efficiency gain: %.1f%% reduction in subscriptions\n",
		float64(len(markets)-len(triangularMarkets))/float64(len(markets))*100)

	if len(completeAssets) > 0 {
		fmt.Printf("   🔄 Triangular assets: %v\n", completeAssets)
		fmt.Printf("   📈 Example cycle: USDT → %s → USDC → USDT\n", completeAssets[0])
	} else {
		fmt.Printf("   ⚠️  WARNING: No assets found with both USDT and USDC pairs!\n")
		fmt.Printf("   💡 Suggestion: Check if CoinEx supports USDC markets\n")

		// Show what we do have for debugging
		if len(usdtMarkets) > 0 {
			usdtSample := make([]string, 0, 5)
			for asset := range usdtMarkets {
				if len(usdtSample) < 5 {
					usdtSample = append(usdtSample, asset)
				}
			}
			fmt.Printf("   📈 Sample USDT assets: %v\n", usdtSample)
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
	fmt.Printf("   🔍 Analyzing real market activity for %d markets...\n", len(markets))

	// Extract ticker data
	data, ok := tickerData["data"].(map[string]interface{})
	if !ok {
		fmt.Printf("   ⚠️  Invalid ticker data format, falling back to heuristic filtering\n")
		return c.validateMarketActivity(markets)
	}

	tickers, ok := data["ticker"].(map[string]interface{})
	if !ok {
		fmt.Printf("   ⚠️  Invalid ticker format, falling back to heuristic filtering\n")
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
				if strings.HasSuffix(market, "USDT") || strings.HasSuffix(market, "USDC") {
					usdVolume = volume * lastPrice // Already in USD equivalent
				} else if strings.HasSuffix(market, "BTC") {
					usdVolume = volume * lastPrice * 45000 // Estimate BTC at $45k
				} else {
					usdVolume = volume * lastPrice // Best guess
				}
			}
		}

		if usdVolume >= minVolumeThreshold {
			activeMarkets = append(activeMarkets, market)
			if len(activeMarkets) <= 5 {
				fmt.Printf("   ✅ Active: %s (24H vol: $%.0f)\n", market, usdVolume)
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

	fmt.Printf("   📊 Volume Analysis Complete:\n")
	fmt.Printf("     - Active markets (>$%.0f/24h): %d\n", minVolumeThreshold, len(activeMarkets))
	fmt.Printf("     - Dead markets (<$%.0f/24h): %d\n", minVolumeThreshold, len(deadMarkets))

	return activeMarkets, deadMarkets
}

// validateMarketActivity tests markets for actual trading activity before subscription
func (c *coinexClient) validateMarketActivity(markets []string) ([]string, []string) {
	fmt.Printf("   🔍 Testing %d markets for trading activity...\n", len(markets))

	activeMarkets := []string{}
	deadMarkets := []string{}

	// For now, we'll use a simple heuristic based on market naming patterns
	// In production, you'd want to call CoinEx API to check 24h volume or recent trades

	for _, market := range markets {
		isActive := c.isMarketActive(market)

		if isActive {
			activeMarkets = append(activeMarkets, market)
			if len(activeMarkets) <= 5 { // Log first few
				fmt.Printf("   ✅ Active: %s\n", market)
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
			fmt.Printf("   ✅ Asset %s: Both %sUSDT and %sUSDC are active\n", asset, asset, asset)
		} else {
			fmt.Printf("   ❌ Asset %s excluded: ", asset)
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
