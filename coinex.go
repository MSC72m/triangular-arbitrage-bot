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

	// Find complete assets and their markets in one pass
	finalMarkets, completeAssets := c.findCompleteTriangularMarkets(markets)
	return finalMarkets, completeAssets, nil
}

// findCompleteTriangularMarkets finds assets that have markets for ALL quote currencies and returns both markets and assets
func (c *coinexClient) findCompleteTriangularMarkets(markets []interface{}) ([]string, []string) {
	// Group markets by asset and quote currency
	assetMarkets := make(map[string]map[string]string) // asset -> quote -> market

	fmt.Printf("🔍 Analyzing markets for quote currencies: %v\n", c.allowedMarkets)

	// Parse all markets and group by asset
	for _, market := range markets {
		marketStr := market.(string)

		for _, quote := range c.allowedMarkets {
			if strings.HasSuffix(marketStr, quote) {
				asset := strings.TrimSuffix(marketStr, quote)
				if asset != "" {
					if assetMarkets[asset] == nil {
						assetMarkets[asset] = make(map[string]string)
					}
					assetMarkets[asset][quote] = marketStr
				}
				break
			}
		}
	}

	// Find assets that have ALL required quote currency pairs
	completeAssets := []string{}
	incompleteAssets := []string{}

	for asset, quotes := range assetMarkets {
		hasAllQuotes := true
		for _, requiredQuote := range c.allowedMarkets {
			if _, exists := quotes[requiredQuote]; !exists {
				hasAllQuotes = false
				break
			}
		}

		if hasAllQuotes {
			completeAssets = append(completeAssets, asset)
		} else {
			incompleteAssets = append(incompleteAssets, asset)
		}
	}

	fmt.Printf("✅ Assets with ALL quote pairs (%d): %v\n", len(completeAssets), completeAssets)
	if len(incompleteAssets) > 0 {
		maxShow := 10
		if len(incompleteAssets) < maxShow {
			maxShow = len(incompleteAssets)
		}
		fmt.Printf("⚠️  Assets with PARTIAL quote pairs (%d): %v\n", len(incompleteAssets), incompleteAssets[:maxShow])
	}

	// Build final market list ONLY for complete assets + direct quote pairs
	finalMarkets := []string{}

	// Add all markets for complete assets only
	for _, asset := range completeAssets {
		for _, quote := range c.allowedMarkets {
			market := asset + quote
			finalMarkets = append(finalMarkets, market)
			fmt.Printf("   📈 Added: %s\n", market)
		}
	}

	// Add direct quote currency pairs if they exist (e.g., USDCUSDT, USDTUSDC)
	directPairs := c.findDirectQuotePairs(markets)
	for _, pair := range directPairs {
		finalMarkets = append(finalMarkets, pair)
		fmt.Printf("   🔗 Added direct pair: %s\n", pair)
	}

	fmt.Printf("🎯 Final market list: %d markets for %d complete assets\n", len(finalMarkets), len(completeAssets))

	return finalMarkets, completeAssets
}

// findDirectQuotePairs finds direct trading pairs between quote currencies
func (c *coinexClient) findDirectQuotePairs(markets []interface{}) []string {
	directPairs := []string{}

	// Check all combinations of quote currencies
	for i, quote1 := range c.allowedMarkets {
		for j, quote2 := range c.allowedMarkets {
			if i >= j { // Avoid duplicates
				continue
			}

			// Try both directions
			pair1 := quote1 + quote2
			pair2 := quote2 + quote1

			for _, market := range markets {
				marketStr := market.(string)
				if marketStr == pair1 || marketStr == pair2 {
					directPairs = append(directPairs, marketStr)
					break
				}
			}
		}
	}

	return directPairs
}
