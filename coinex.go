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

func (c *coinexClient) GetArbitrageMarkets() ([]string, error) {
	// Get all available markets from CoinEx
	marketListString, err := c.GetMarketList()
	fmt.Printf("Market list: %s\n", marketListString)
	if err != nil {
		return nil, fmt.Errorf("failed to get market list: %w", err)
	}

	var marketList map[string]interface{}
	err = json.Unmarshal([]byte(marketListString), &marketList)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal market list: %w", err)
	}

	markets := marketList["data"].([]interface{})

	var arbitrageMarkets []string

	// Categorize markets by quote currency
	for _, market := range markets {
		marketStr := market.(string)
		for _, quoteCurrency := range c.allowedMarkets {
			if strings.HasSuffix(marketStr, quoteCurrency) {
				arbitrageMarkets = append(arbitrageMarkets, marketStr)
				break
			}
		}
	}

	return arbitrageMarkets, nil
}
