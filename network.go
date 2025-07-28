package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	GET    = "GET"
	POST   = "POST"
	PUT    = "PUT"
	PATCH  = "PATCH"
	DELETE = "DELETE"
)

type HttpClient struct {
	client  *http.Client
	headers map[string]string
	mu      sync.RWMutex
	config  *Config

	// WebSocket fields
	wsConn          *websocket.Conn
	wsSubscriptions map[string]bool
	wsDataFeed      chan []byte
	wsUrl           string
	wsConnected     bool

	// Market data cache with proper synchronization
	marketData   map[string]*OrderBook
	marketDataMu sync.RWMutex
}

func newHttpClient(config *Config) *HttpClient {
	return &HttpClient{
		config:          config,
		headers:         make(map[string]string),
		wsSubscriptions: make(map[string]bool),
		wsDataFeed:      make(chan []byte, 2000), // Buffered channel
		marketData:      make(map[string]*OrderBook),
	}
}

func (c *HttpClient) setClient(client *http.Client) *HttpClient {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.client = client
	return c
}

func (c *HttpClient) setInternalHeader(req *http.Request) {
	c.mu.RLock()
	headersCopy := make(map[string]string)
	for k, v := range c.headers {
		headersCopy[k] = v
	}
	c.mu.RUnlock()

	for key, value := range headersCopy {
		req.Header.Set(key, value)
	}
}

func (c *HttpClient) setheaders(headers map[string]string) *HttpClient {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.headers = make(map[string]string)
	for k, v := range headers {
		c.headers[k] = v
	}
	return c
}

func (c *HttpClient) GetHeaders() http.Header {
	c.mu.RLock()
	defer c.mu.RUnlock()
	headers := make(http.Header)
	for k, v := range c.headers {
		headers.Add(k, v)
	}
	return headers
}

func (c *HttpClient) mergeHeaders(newHeaders map[string]string) *HttpClient {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.headers == nil {
		c.headers = make(map[string]string)
	}
	for key, value := range newHeaders {
		c.headers[key] = value
	}
	return c
}

// WebSocket Methods

func (c *HttpClient) SetWebSocketUrl(host string) *HttpClient {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.wsUrl = fmt.Sprintf("wss://%s/", host)
	return c
}

func (c *HttpClient) ConnectWebSocket() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.wsConnected {
		return nil // Already connected
	}

	fmt.Printf("🔌 Connecting to WebSocket: %s\n", c.wsUrl)

	// Use existing headers for WebSocket connection
	wsHeaders := make(http.Header)
	for k, v := range c.headers {
		wsHeaders.Set(k, v)
	}

	// Add WebSocket specific headers
	wsHeaders.Set("User-Agent", "triangular-arbitrage-bot/2.0")

	dialer := &websocket.Dialer{
		HandshakeTimeout: 45 * time.Second,
	}

	conn, response, err := dialer.Dial(c.wsUrl, wsHeaders)
	if err != nil {
		fmt.Printf("❌ WebSocket dial failed. Response: %+v\n", response)
		return fmt.Errorf("websocket dial error: %w", err)
	}

	c.wsConn = conn
	c.wsConnected = true
	fmt.Printf("✅ WebSocket connected successfully\n")

	// Start background processes
	go c.readWebSocketMessages()
	go c.pingWebSocketLoop()

	return nil
}

func (c *HttpClient) readWebSocketMessages() {
	defer func() {
		c.mu.Lock()
		if c.wsConn != nil {
			c.wsConn.Close()
			c.wsConn = nil
		}
		c.wsConnected = false
		c.mu.Unlock()
		fmt.Println("📪 WebSocket connection closed")
	}()

	messageCount := 0

	for {
		c.mu.RLock()
		conn := c.wsConn
		connected := c.wsConnected
		c.mu.RUnlock()

		if !connected || conn == nil {
			return
		}

		_, message, err := conn.ReadMessage()
		if err != nil {
			fmt.Printf("❌ WebSocket read error: %v\n", err)
			return
		}

		messageCount++

		// Log only first few messages and periodic summaries
		if messageCount <= 3 {
			fmt.Printf("📨 WebSocket Message #%d: %s\n", messageCount, string(message))
		} else if messageCount%1000 == 0 {
			// Log every 1000th message
			fmt.Printf("📨 WebSocket Message #%d received\n", messageCount)
		}

		// Send to data feed channel (non-blocking)
		select {
		case c.wsDataFeed <- message:
		default:
			fmt.Printf("⚠️  WebSocket data feed channel full, dropping message #%d\n", messageCount)
		}
	}
}

func (c *HttpClient) pingWebSocketLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		c.mu.RLock()
		conn := c.wsConn
		connected := c.wsConnected
		c.mu.RUnlock()

		if !connected || conn == nil {
			return
		}

		// Send ping message in CoinEx format
		pingMsg := map[string]interface{}{
			"method": "server.ping",
			"params": []interface{}{},
			"id":     time.Now().UnixNano(),
		}

		if err := conn.WriteJSON(pingMsg); err != nil {
			fmt.Printf("❌ Failed to send WebSocket ping: %v\n", err)
			return
		}
	}
}

func (c *HttpClient) SubscribeWebSocket(market string) error {
	c.mu.Lock()

	if !c.wsConnected || c.wsConn == nil {
		c.mu.Unlock()
		return fmt.Errorf("websocket not connected")
	}

	if c.wsSubscriptions[market] {
		c.mu.Unlock()
		return nil // Already subscribed
	}

	// CoinEx WebSocket API format: [market, limit, interval, diff]
	subMsg := map[string]interface{}{
		"method": "depth.subscribe",
		"params": []interface{}{market, 5, "0", true}, // market, limit, interval, diff
		"id":     time.Now().UnixNano(),
	}

	fmt.Printf("🔍 DEBUG: Subscribing to %s with ID %d\n", market, subMsg["id"])

	// Send subscription message
	if err := c.wsConn.WriteJSON(subMsg); err != nil {
		c.mu.Unlock()
		return fmt.Errorf("websocket write error for %s: %w", market, err)
	}

	c.mu.Unlock()

	// Mark as subscribed optimistically
	c.wsSubscriptions[market] = true

	return nil
}

// SubscribeWebSocketBatch subscribes to multiple markets with proper rate limiting and error handling
func (c *HttpClient) SubscribeWebSocketBatch(markets []string) ([]string, []string) {
	successful := []string{}
	failed := []string{}

	fmt.Printf("📡 Starting batch subscription to %d markets with rate limiting...\n", len(markets))

	for i, market := range markets {
		// Rate limiting: max 10 subscriptions per second
		if i > 0 && i%10 == 0 {
			fmt.Printf("🔄 Rate limit pause after %d subscriptions...\n", i)
			time.Sleep(1 * time.Second)
		}

		err := c.SubscribeWebSocket(market)
		if err != nil {
			failed = append(failed, market)
			fmt.Printf("❌ Subscription failed: %s - %v\n", market, err)
		} else {
			successful = append(successful, market)
			if len(successful) <= 5 || len(successful)%20 == 0 {
				fmt.Printf("✅ Progress: %d/%d successful (%s)\n", len(successful), len(markets), market)
			}
		}

		// Small delay between subscriptions
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Printf("📊 Batch subscription results: %d successful, %d failed out of %d total\n",
		len(successful), len(failed), len(markets))

	if len(failed) > 0 {
		fmt.Printf("❌ Failed markets (first 10): %v\n", failed[:Min(10, len(failed))])
	}

	return successful, failed
}

// SubscribeWebSocketOptimistic subscribes without waiting for confirmations (faster, more reliable)
func (c *HttpClient) SubscribeWebSocketOptimistic(market string) error {
	c.mu.Lock()

	if !c.wsConnected || c.wsConn == nil {
		c.mu.Unlock()
		return fmt.Errorf("websocket not connected")
	}

	if c.wsSubscriptions[market] {
		c.mu.Unlock()
		return nil // Already subscribed
	}

	// Generate ID but don't track it (just for CoinEx compatibility)
	id := time.Now().UnixNano()

	// CoinEx WebSocket API format: [market, limit, interval, diff]
	subMsg := map[string]interface{}{
		"method": "depth.subscribe",
		"params": []interface{}{market, 5, "0", true}, // market, limit, interval, diff
		"id":     id,                                  // Required by CoinEx but we don't track it
	}

	// Send subscription message
	if err := c.wsConn.WriteJSON(subMsg); err != nil {
		c.mu.Unlock()
		return fmt.Errorf("websocket write error for %s: %w", market, err)
	}

	// Mark as subscribed optimistically
	c.wsSubscriptions[market] = true
	c.mu.Unlock()

	return nil
}

// SubscribeWebSocketBatchOptimistic subscribes to multiple markets quickly without waiting for confirmations
func (c *HttpClient) SubscribeWebSocketBatchOptimistic(markets []string) ([]string, []string) {
	successful := []string{}
	failed := []string{}

	fmt.Printf("📡 Starting optimistic batch subscription to %d markets...\n", len(markets))

	// Send all subscriptions quickly
	for i, market := range markets {
		// Rate limiting: max 20 subscriptions per second
		if i > 0 && i%20 == 0 {
			time.Sleep(1 * time.Second)
		}

		err := c.SubscribeWebSocketOptimistic(market)
		if err != nil {
			failed = append(failed, market)
			fmt.Printf("❌ Subscription send failed: %s - %v\n", market, err)
		} else {
			successful = append(successful, market)
		}

		// Small delay between subscriptions
		time.Sleep(50 * time.Millisecond)
	}

	fmt.Printf("📡 Sent %d subscription requests, waiting for data flow validation...\n", len(successful))

	// Wait for data to start flowing and validate
	fmt.Printf("⏳ Waiting 10 seconds for data to accumulate...\n")

	// Check data every 2 seconds during the wait
	for i := 1; i <= 5; i++ {
		time.Sleep(2 * time.Second)
		c.mu.RLock()
		currentCacheSize := len(c.marketData)
		c.mu.RUnlock()
		fmt.Printf("   📊 After %ds: %d markets in cache\n", i*2, currentCacheSize)
	}

	// Validate which subscriptions are actually providing data
	actuallyWorking := []string{}
	actuallyFailed := []string{}

	fmt.Printf("🔍 DEBUG: Validating %d markets for data...\n", len(successful))

	c.mu.RLock()
	totalCachedMarkets := len(c.marketData)

	// Show first few markets in cache for debugging
	cacheMarkets := make([]string, 0, Min(5, len(c.marketData)))
	for market := range c.marketData {
		if len(cacheMarkets) < 5 {
			cacheMarkets = append(cacheMarkets, market)
		}
	}
	c.mu.RUnlock()

	fmt.Printf("🔍 DEBUG: Total markets in cache: %d\n", totalCachedMarkets)
	if len(cacheMarkets) > 0 {
		fmt.Printf("🔍 DEBUG: Sample cached markets: %v\n", cacheMarkets)
	}

	for i, market := range successful {
		if orderBook, exists := c.marketData[market]; exists && orderBook != nil {
			if len(orderBook.Bids) > 0 || len(orderBook.Asks) > 0 {
				actuallyWorking = append(actuallyWorking, market)
				if i < 5 { // Debug first 5 working markets
					fmt.Printf("✅ DEBUG: %s working - Bids: %d, Asks: %d\n",
						market, len(orderBook.Bids), len(orderBook.Asks))
				}
			} else {
				actuallyFailed = append(actuallyFailed, market)
				if i < 5 { // Debug first 5 empty markets
					fmt.Printf("❌ DEBUG: %s empty - Bids: %d, Asks: %d\n",
						market, len(orderBook.Bids), len(orderBook.Asks))
				}
			}
		} else {
			actuallyFailed = append(actuallyFailed, market)
			if i < 5 { // Debug first 5 missing markets
				fmt.Printf("❌ DEBUG: %s not in cache (exists: %t)\n", market, exists)
			}
		}
	}

	// Add original failures
	actuallyFailed = append(actuallyFailed, failed...)

	fmt.Printf("📊 Optimistic subscription results: %d working, %d failed out of %d total\n",
		len(actuallyWorking), len(actuallyFailed), len(markets))

	if len(actuallyFailed) > 0 {
		fmt.Printf("❌ Failed/dead markets (first 10): %v\n", actuallyFailed[:Min(10, len(actuallyFailed))])
	}

	return actuallyWorking, actuallyFailed
}

// SubscribeWebSocketProgressive subscribes to markets in progressive batches to avoid overwhelming the connection
func (c *HttpClient) SubscribeWebSocketProgressive(markets []string, batchSize int) ([]string, []string) {
	successful := []string{}
	failed := []string{}

	if batchSize <= 0 {
		batchSize = 10 // Reduced from 15 to 10 for better stability
	}

	fmt.Printf("📡 Starting progressive batch subscription: %d markets in batches of %d...\n",
		len(markets), batchSize)

	// Process markets in batches
	for batchStart := 0; batchStart < len(markets); batchStart += batchSize {
		batchEnd := batchStart + batchSize
		if batchEnd > len(markets) {
			batchEnd = len(markets)
		}

		currentBatch := markets[batchStart:batchEnd]
		batchNum := (batchStart / batchSize) + 1
		totalBatches := (len(markets) + batchSize - 1) / batchSize

		fmt.Printf("\n🔄 Batch %d/%d: Subscribing to %d markets...\n",
			batchNum, totalBatches, len(currentBatch))

		// Subscribe to current batch (fast within batch)
		batchSuccessful := []string{}
		batchFailed := []string{}

		for i, market := range currentBatch {
			// Small delay between subscriptions within batch (increased for stability)
			if i > 0 {
				time.Sleep(75 * time.Millisecond) // Increased from 50ms to 75ms
			}

			err := c.SubscribeWebSocketOptimistic(market)
			if err != nil {
				batchFailed = append(batchFailed, market)
				fmt.Printf("   ❌ %s: %v\n", market, err)
			} else {
				batchSuccessful = append(batchSuccessful, market)
				if len(batchSuccessful) <= 3 || len(batchSuccessful)%5 == 0 {
					fmt.Printf("   📡 %s subscribed\n", market)
				}
			}
		}

		fmt.Printf("   📊 Batch %d sent: %d subscriptions\n", batchNum, len(batchSuccessful))

		// Wait for data to start flowing from this batch
		fmt.Printf("   ⏳ Waiting for batch %d data validation...\n", batchNum)

		batchWorking := []string{}
		batchDead := []string{}

		// Give time for data to accumulate from this batch
		for waitSeconds := 1; waitSeconds <= 4; waitSeconds++ {
			time.Sleep(1 * time.Second)

			// Check how many from this batch are working
			workingCount := 0
			c.marketDataMu.RLock()
			for _, market := range batchSuccessful {
				if orderBook, exists := c.marketData[market]; exists && orderBook != nil {
					if len(orderBook.Bids) > 0 || len(orderBook.Asks) > 0 {
						workingCount++
					}
				}
			}
			c.marketDataMu.RUnlock()

			fmt.Printf("     📊 After %ds: %d/%d markets from batch %d have data\n",
				waitSeconds, workingCount, len(batchSuccessful), batchNum)

			// If most of the batch is working, we can move on
			if float64(workingCount)/float64(len(batchSuccessful)) >= 0.6 && waitSeconds >= 2 {
				fmt.Printf("     ✅ Batch %d ready (%.1f%% working)\n",
					batchNum, float64(workingCount)/float64(len(batchSuccessful))*100)
				break
			}
		}

		// Final validation for this batch
		c.marketDataMu.RLock()
		for _, market := range batchSuccessful {
			if orderBook, exists := c.marketData[market]; exists && orderBook != nil {
				if len(orderBook.Bids) > 0 || len(orderBook.Asks) > 0 {
					batchWorking = append(batchWorking, market)
				} else {
					batchDead = append(batchDead, market)
				}
			} else {
				batchDead = append(batchDead, market)
			}
		}
		c.marketDataMu.RUnlock()

		// Add batch results to totals
		successful = append(successful, batchWorking...)
		failed = append(failed, batchFailed...)
		failed = append(failed, batchDead...)

		fmt.Printf("   📊 Batch %d results: %d working, %d failed\n",
			batchNum, len(batchWorking), len(batchFailed)+len(batchDead))

		if len(batchWorking) > 0 {
			fmt.Printf("   ✅ Batch %d working markets: %v\n",
				batchNum, batchWorking[:Min(3, len(batchWorking))])
			if len(batchWorking) > 3 {
				fmt.Printf("       ... and %d more\n", len(batchWorking)-3)
			}
		}

		// Check if connection is still healthy before next batch
		if !c.IsWebSocketConnected() {
			fmt.Printf("   ⚠️  WebSocket disconnected during batch %d, stopping\n", batchNum)
			break
		}

		// Small pause before next batch (unless it's the last batch)
		if batchEnd < len(markets) {
			fmt.Printf("   💤 Cooling down 3s before next batch...\n") // Increased from 2s to 3s
			time.Sleep(3 * time.Second)
		}
	}

	fmt.Printf("\n📊 Progressive subscription completed!\n")
	fmt.Printf("   ✅ Total working: %d\n", len(successful))
	fmt.Printf("   ❌ Total failed: %d\n", len(failed))
	fmt.Printf("   📈 Success rate: %.1f%%\n", float64(len(successful))/float64(len(markets))*100)

	if len(failed) > 0 {
		fmt.Printf("   🚫 Failed markets (first 10): %v\n", failed[:Min(10, len(failed))])
	}

	return successful, failed
}

func (c *HttpClient) GetWebSocketDataFeed() <-chan []byte {
	return c.wsDataFeed
}

func (c *HttpClient) IsWebSocketConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.wsConnected
}

func (c *HttpClient) CloseWebSocket() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.wsConn != nil {
		err := c.wsConn.Close()
		c.wsConn = nil
		c.wsConnected = false
		return err
	}
	return nil
}

// ValidateWebSocketDataHealth validates which markets have actual order book data
func (c *HttpClient) ValidateWebSocketDataHealth(expectedMarkets []string) ([]string, []string) {
	c.marketDataMu.RLock()
	defer c.marketDataMu.RUnlock()

	activeMarkets := []string{}
	deadMarkets := []string{}

	for _, market := range expectedMarkets {
		if orderBook, exists := c.marketData[market]; exists && orderBook != nil {
			if len(orderBook.Bids) > 0 && len(orderBook.Asks) > 0 {
				activeMarkets = append(activeMarkets, market)
			} else {
				deadMarkets = append(deadMarkets, market)
			}
		} else {
			deadMarkets = append(deadMarkets, market)
		}
	}

	return activeMarkets, deadMarkets
}

// Add method to get order book via REST API as fallback
func (c *HttpClient) GetOrderBookREST(market string) (*OrderBook, error) {
	params := map[string]string{
		"url":    fmt.Sprintf("https://api.coinex.com/v1/market/depth?market=%s&merge=0&limit=5", market),
		"method": "GET",
	}

	response, err := c.performRequest(params, "GET")
	if err != nil {
		return nil, err
	}

	// Parse CoinEx depth response
	data, ok := response["data"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid response format")
	}

	asks, ok := data["asks"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid asks format")
	}

	bids, ok := data["bids"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid bids format")
	}

	orderBook := &OrderBook{
		Asks: []Depth{},
		Bids: []Depth{},
	}

	// Convert asks
	for _, ask := range asks {
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

	// Convert bids
	for _, bid := range bids {
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

	return orderBook, nil
}

// EnsureCriticalMarketData ensures critical markets have data via REST API fallback
func (c *HttpClient) EnsureCriticalMarketData(criticalMarkets []string, marketDepths *MarketDepths) {
	for _, market := range criticalMarkets {
		c.marketDataMu.RLock()
		orderBook, exists := c.marketData[market]
		hasData := exists && orderBook != nil && len(orderBook.Bids) > 0 && len(orderBook.Asks) > 0
		c.marketDataMu.RUnlock()

		if !hasData {
			fmt.Printf("🔄 Critical market %s missing data, fetching via REST API...\n", market)

			restOrderBook, err := c.GetOrderBookREST(market)
			if err != nil {
				fmt.Printf("❌ Failed to get %s via REST: %v\n", market, err)
				continue
			}

			if len(restOrderBook.Bids) > 0 && len(restOrderBook.Asks) > 0 {
				// Store in both caches with proper synchronization
				c.marketDataMu.Lock()
				c.marketData[market] = restOrderBook
				c.marketDataMu.Unlock()

				// Also store in marketDepths so arbitrage engine can find it
				if marketDepths != nil {
					marketDepths.Store(market, restOrderBook)
				}

				fmt.Printf("✅ Critical market %s data restored via REST | Bids: %d | Asks: %d\n",
					market, len(restOrderBook.Bids), len(restOrderBook.Asks))
			} else {
				fmt.Printf("⚠️  Critical market %s has empty order book even via REST\n", market)
			}
		}
	}
}

// HTTP Methods (existing functionality)

func (c HttpClient) getQueryString(urlStr string, params map[string]string) string {
	var sb strings.Builder
	sb.WriteString(urlStr)
	if len(params) == 0 {
		return sb.String()
	}
	sb.WriteString("?")
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for i, k := range keys {
		sb.WriteString(k)
		sb.WriteString("=")
		sb.WriteString(url.QueryEscape(params[k]))
		if i < len(keys)-1 {
			sb.WriteString("&")
		}
	}
	return sb.String()
}

func (c *HttpClient) performRequest(params map[string]string, method string) (map[string]interface{}, error) {
	fmt.Println("Performing request", params, method)
	var reqBody io.Reader
	url := params["url"]

	if method == GET {
		reqBody = nil
		url = c.getQueryString(url, params)
	} else {
		if body, exists := params["body"]; exists {
			reqBody = strings.NewReader(body)
		}
	}

	if queryParams, exists := params["query"]; exists {
		if queryParams != "" {
			url += "?" + queryParams
		}
	}

	req, err := http.NewRequest(method, url, reqBody)
	if err != nil {
		return nil, err
	}

	// Set proper content type for form data
	if method == POST || method == PUT || method == PATCH || method == DELETE {
		if _, hasBody := params["body"]; hasBody {
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		}
	}

	c.setInternalHeader(req)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	bodyString, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var body map[string]interface{}
	err = json.Unmarshal(bodyString, &body)
	if err != nil {
		fmt.Printf("Failed to unmarshal JSON response: %v\n", err)
		fmt.Printf("Response body: %s\n", string(bodyString))
		return nil, err
	}

	fmt.Println("Response length", len(bodyString))

	return body, nil
}
