package exchange

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"triangular-arbitrage-bot/internal/config"
	"triangular-arbitrage-bot/internal/network"
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
	ResultChan    chan OrderResult
	IsActive      bool
	RetryCount    int
	mu            sync.RWMutex
	// Add flag to track if CancelChan has been closed
	cancelChanClosed bool
	// Add sync.Once to ensure channel is only closed once
	cancelOnce sync.Once
}

type CoinexClient struct {
	httpClient                 *network.HttpClient
	apiKey                     string
	secretID                   string
	baseUrl                    string
	allowedMarkets             []string
	config                     *config.Config
	criticalMarketPriceManager *CriticalMarketPriceManager

	// Order execution tracking (simplified)
	lastOrderTime time.Time
	mu            sync.RWMutex
}

func NewCoinexClient(httpClient *network.HttpClient, config *config.Config) *CoinexClient {
	log.Printf("Loading CoinEx client with quote currencies from config: %v\n", config.QuoteCurrencies)
	client := &CoinexClient{
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

// generateRESTSignature creates HMAC-SHA256 signature for CoinEx API v2 REST endpoints
func (c *CoinexClient) generateRESTSignature(method, requestPath, queryString, body string, timestamp int64) string {
	// Create the string to sign for v2 REST API
	// Format: method + request_path + body + timestamp
	// According to CoinEx docs: https://docs.coinex.com/api/v2/authorization
	// For GET requests: request_path should include query parameters
	// For POST requests: request_path is just the path, body is separate

	var fullPath string
	if queryString != "" {
		fullPath = requestPath + "?" + queryString
	} else {
		fullPath = requestPath
	}

	// Create the string to sign: method + request_path_with_query + body + timestamp
	stringToSign := method + fullPath + body + strconv.FormatInt(timestamp, 10)

	// Create HMAC-SHA256 signature
	h := hmac.New(sha256.New, []byte(c.secretID))
	h.Write([]byte(stringToSign))
	signature := hex.EncodeToString(h.Sum(nil))

	return signature
}

// generateWebSocketSignature creates HMAC-SHA256 signature for CoinEx WebSocket authentication
func (c *CoinexClient) generateWebSocketSignature(timestamp int64) string {
	// Create the string to sign for WebSocket authentication
	// Format: timestamp (just the timestamp!)
	// According to CoinEx docs: https://docs.coinex.com/api/v2/authorization

	stringToSign := strconv.FormatInt(timestamp, 10)

	// Create HMAC-SHA256 signature
	h := hmac.New(sha256.New, []byte(c.secretID))
	h.Write([]byte(stringToSign))
	signature := hex.EncodeToString(h.Sum(nil))

	return signature
}

// TestSignatureGeneration tests the signature generation for debugging
func (c *CoinexClient) TestSignatureGeneration() {
	log.Printf(" TESTING SIGNATURE GENERATION")

	// Test GET request signature (like order status polling)
	timestamp := int64(1700490703564) // Use the example timestamp from docs
	method := "GET"
	requestPath := "/v2/spot/order-status"
	queryString := "market=BTCUSDT&order_id=12345"

	signature := c.generateRESTSignature(method, requestPath, queryString, "", timestamp)
	log.Printf(" GET SIGNATURE TEST | Expected format: GET + path_with_query + timestamp")
	log.Printf(" GET SIGNATURE TEST | String to sign: %s%s?%s%d", method, requestPath, queryString, timestamp)
	log.Printf(" GET SIGNATURE TEST | Generated signature: %s", signature)

	// Test POST request signature (like order placement)
	postMethod := "POST"
	postPath := "/v2/spot/order"
	postBody := `{"market":"BTCUSDT","type":"buy","amount":"0.001","price":"10000"}`

	postSignature := c.generateRESTSignature(postMethod, postPath, "", postBody, timestamp)
	log.Printf(" POST SIGNATURE TEST | Expected format: POST + path + body + timestamp")
	log.Printf(" POST SIGNATURE TEST | String to sign: %s%s%s%d", postMethod, postPath, postBody, timestamp)
	log.Printf(" POST SIGNATURE TEST | Generated signature: %s", postSignature)

	// Test with the exact format from the failing request
	log.Printf(" TESTING EXACT FAILING FORMAT")
	exactTimestamp := int64(1756476851247) // Use the exact timestamp from the failing request
	exactMethod := "GET"
	exactPath := "/v2/spot/order-status"
	exactQueryString := "market=FLOWUSDT&order_id=157152518810"

	exactSignature := c.generateRESTSignature(exactMethod, exactPath, exactQueryString, "", exactTimestamp)
	log.Printf(" EXACT FAILING TEST | String to sign: %s%s?%s%d", exactMethod, exactPath, exactQueryString, exactTimestamp)
	log.Printf(" EXACT FAILING TEST | Generated signature: %s", exactSignature)
}

// checkRateLimit checks if we can place another order based on rate limiting
func (c *CoinexClient) checkRateLimit() bool {
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
func (c *CoinexClient) calculateOrderAmount(requestedAmount, price float64, orderType string) (float64, float64, error) {

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

// getTradingFee returns the trading fee for a market
func (c *CoinexClient) getTradingFee(market string) float64 {
	if fee, exists := c.config.TradingFees[market]; exists {
		return fee
	}
	return c.config.DefaultTradingFee
}

// Helper methods for building request data

// buildFormData builds form-encoded data from parameters
func (c *CoinexClient) buildFormData(params map[string]string) string {
	var parts []string
	for key, value := range params {
		parts = append(parts, key+"="+value)
	}
	return strings.Join(parts, "&")
}

// buildQueryString builds query string from parameters
func (c *CoinexClient) buildQueryString(params map[string]string) string {
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

// buildSortedQueryString builds a sorted query string from key-value pairs
// This encapsulates the key extraction and placement logic
func (c *CoinexClient) buildSortedQueryString(params map[string]string) string {
	// Extract and sort keys
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Build query parts in sorted order
	var queryParts []string
	for _, key := range keys {
		queryParts = append(queryParts, key+"="+params[key])
	}

	return strings.Join(queryParts, "&")
}

func parseFloat(i interface{}) float64 {
	if i == nil {
		return 0
	}

	// Handle string values
	if v, ok := i.(string); ok {
		f, _ := strconv.ParseFloat(v, 64)
		return f
	}

	// Handle numeric values (CoinEx API returns numbers as float64)
	if v, ok := i.(float64); ok {
		return v
	}

	return 0
}
