package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
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

	// Rate limiting
	rateLimiter *RateLimiter

	// WebSocket connection management
	wsReconnectChan chan bool
	wsStopChan      chan bool
	wsReconnecting  bool
	wsLastPong      time.Time
	wsPongTimeout   time.Duration

	// WebSocket background processes
	wsReadDone   chan bool
	wsPingDone   chan bool
	wsHealthDone chan bool
}

func newHttpClient(config *Config) *HttpClient {
	return &HttpClient{
		config:          config,
		headers:         make(map[string]string),
		wsSubscriptions: make(map[string]bool),
		wsDataFeed:      make(chan []byte, 2000), // Buffered channel
		marketData:      make(map[string]*OrderBook),
		rateLimiter:     NewRateLimiter(config.RateLimitPerSecond, config.RateLimitPerSecond),
		wsUrl:           "wss://socket.coinex.com/v2/spot", // Original CoinEx spot WebSocket URL
		wsReconnectChan: make(chan bool, 1),
		wsStopChan:      make(chan bool, 1),
		wsPongTimeout:   60 * time.Second, // 60 second pong timeout
		wsReadDone:      make(chan bool, 1),
		wsPingDone:      make(chan bool, 1),
		wsHealthDone:    make(chan bool, 1),
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

	fmt.Printf("🔗 Connecting to WebSocket: %s\n", c.wsUrl)

	// Use existing headers for WebSocket connection
	wsHeaders := make(http.Header)
	for k, v := range c.headers {
		wsHeaders.Set(k, v)
	}

	// Add WebSocket specific headers
	wsHeaders.Set("User-Agent", "triangular-arbitrage-bot/2.0")

	fmt.Printf("🔗 WebSocket headers: %+v\n", wsHeaders)

	dialer := &websocket.Dialer{
		HandshakeTimeout: 45 * time.Second,
	}

	conn, response, err := dialer.Dial(c.wsUrl, wsHeaders)
	if err != nil {
		fmt.Printf("❌ WebSocket dial failed. Response: %+v\n", response)
		return fmt.Errorf("websocket dial error: %w", err)
	}

	fmt.Printf("🔗 WebSocket dial successful, setting up handlers...\n")

	// Set up connection handlers
	conn.SetPongHandler(func(string) error {
		c.wsLastPong = time.Now()
		fmt.Printf("🏓 Pong received at %v\n", c.wsLastPong)
		return nil
	})

	conn.SetCloseHandler(func(code int, text string) error {
		fmt.Printf("🔌 WebSocket close handler called: code=%d, text=%s\n", code, text)
		select {
		case c.wsReconnectChan <- true:
		default:
			// Channel full, ignore
		}
		return nil
	})

	c.wsConn = conn
	c.wsConnected = true
	c.wsLastPong = time.Now()
	fmt.Printf("✅ WebSocket connected successfully to %s\n", c.wsUrl)

	// Clear any pending done signals before starting new goroutines
	select {
	case <-c.wsReadDone:
	default:
	}
	select {
	case <-c.wsPingDone:
	default:
	}
	select {
	case <-c.wsHealthDone:
	default:
	}

	// Start background processes
	go c.readWebSocketMessages()
	go c.pingWebSocketLoop()
	go c.connectionHealthMonitor()

	// Wait a moment for connection to stabilize before allowing subscriptions
	fmt.Printf("⏳ Waiting for connection to stabilize...\n")
	time.Sleep(5 * time.Second)

	return nil
}

// ReconnectWebSocket attempts to reconnect to the WebSocket
func (c *HttpClient) ReconnectWebSocket() error {
	c.mu.Lock()

	if c.wsReconnecting {
		c.mu.Unlock()
		return fmt.Errorf("reconnection already in progress")
	}

	c.wsReconnecting = true
	c.mu.Unlock()

	// Stop existing goroutines
	select {
	case c.wsReadDone <- true:
	default:
	}
	select {
	case c.wsPingDone <- true:
	default:
	}
	select {
	case c.wsHealthDone <- true:
	default:
	}

	// Close existing connection if any
	c.mu.Lock()
	if c.wsConn != nil {
		c.wsConn.Close()
		c.wsConn = nil
	}
	c.wsConnected = false
	c.mu.Unlock()

	fmt.Printf("🔄 Attempting WebSocket reconnection to %s\n", c.wsUrl)

	// Use existing headers for WebSocket connection
	wsHeaders := make(http.Header)
	c.mu.RLock()
	for k, v := range c.headers {
		wsHeaders.Set(k, v)
	}
	c.mu.RUnlock()

	// Add WebSocket specific headers
	wsHeaders.Set("User-Agent", "triangular-arbitrage-bot/2.0")

	dialer := &websocket.Dialer{
		HandshakeTimeout: 45 * time.Second,
	}

	conn, response, err := dialer.Dial(c.wsUrl, wsHeaders)
	if err != nil {
		c.mu.Lock()
		c.wsReconnecting = false
		c.mu.Unlock()
		fmt.Printf("❌ WebSocket reconnection failed. Response: %+v\n", response)
		return fmt.Errorf("websocket reconnection error: %w", err)
	}

	// Set up connection handlers
	conn.SetPongHandler(func(string) error {
		c.wsLastPong = time.Now()
		return nil
	})

	conn.SetCloseHandler(func(code int, text string) error {
		fmt.Printf("🔌 WebSocket close handler called: code=%d, text=%s\n", code, text)
		select {
		case c.wsReconnectChan <- true:
		default:
			// Channel full, ignore
		}
		return nil
	})

	c.mu.Lock()
	c.wsConn = conn
	c.wsConnected = true
	c.wsLastPong = time.Now()
	c.wsReconnecting = false
	c.mu.Unlock()

	fmt.Printf("✅ WebSocket reconnected successfully to %s\n", c.wsUrl)

	// Clear subscription cache since we need to resubscribe
	c.mu.Lock()
	c.wsSubscriptions = make(map[string]bool)
	c.mu.Unlock()

	// Clear any pending done signals before starting new goroutines
	select {
	case <-c.wsReadDone:
	default:
	}
	select {
	case <-c.wsPingDone:
	default:
	}
	select {
	case <-c.wsHealthDone:
	default:
	}

	// Start background processes
	go c.readWebSocketMessages()
	go c.pingWebSocketLoop()
	go c.connectionHealthMonitor()

	return nil
}

func (c *HttpClient) readWebSocketMessages() {
	defer func() {
		// Only close connection if there was an actual error
		if r := recover(); r != nil {
			fmt.Printf("❌ WebSocket read panic: %v\n", r)
		}
	}()

	messageCount := 0

	for {
		select {
		case <-c.wsReadDone:
			fmt.Printf("📨 WebSocket read loop stopped by done signal\n")
			return
		default:
		}

		c.mu.RLock()
		conn := c.wsConn
		connected := c.wsConnected
		c.mu.RUnlock()

		if !connected || conn == nil {
			fmt.Printf("📨 WebSocket read loop exiting - connection not available\n")
			return
		}

		// Set read deadline
		if err := conn.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
			fmt.Printf("❌ Failed to set read deadline: %v\n", err)
			return
		}

		_, message, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				fmt.Printf("❌ WebSocket read error: %v\n", err)
			} else {
				fmt.Printf("📨 WebSocket connection closed normally\n")
			}

			// Update connection state when read fails
			c.mu.Lock()
			c.wsConnected = false
			c.mu.Unlock()

            // Attempt to reconnect
            c.handleReconnection()
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

		// Also log any error messages or subscription responses
		if messageCount <= 10 {
			fmt.Printf("📨 WebSocket Message #%d (first 10): %s\n", messageCount, string(message))
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

	for {
		select {
		case <-ticker.C:
			// Continue with ping
		case <-c.wsPingDone:
			return
		}

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

// connectionHealthMonitor monitors the WebSocket connection health
func (c *HttpClient) connectionHealthMonitor() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.checkConnectionHealth()
		case <-c.wsReconnectChan:
			c.handleReconnection()
		case <-c.wsStopChan:
			return
		case <-c.wsHealthDone:
			return
		}
	}
}

// GetWebSocketStats returns connection statistics
func (c *HttpClient) GetWebSocketStats() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	stats := map[string]interface{}{
		"connected":     c.wsConnected,
		"reconnecting":  c.wsReconnecting,
		"subscriptions": len(c.wsSubscriptions),
		"last_pong":     c.wsLastPong,
		"pong_timeout":  c.wsPongTimeout,
	}

	if c.wsConn != nil {
		stats["remote_addr"] = c.wsConn.RemoteAddr().String()
	}

	return stats
}

func (c *HttpClient) checkConnectionHealth() {
	c.mu.RLock()
	connected := c.wsConnected
	lastPong := c.wsLastPong
	reconnecting := c.wsReconnecting
	c.mu.RUnlock()

	if !connected || reconnecting {
		return
	}

	// Check if we haven't received a pong in too long
	if time.Since(lastPong) > c.wsPongTimeout {
		fmt.Printf("⚠️  WebSocket connection appears stale (no pong for %v), triggering reconnection\n", time.Since(lastPong))
		select {
		case c.wsReconnectChan <- true:
		default:
			// Channel full, ignore
		}
		return
	}

	// Check if connection is still alive by trying to write a ping
	c.mu.RLock()
	conn := c.wsConn
	c.mu.RUnlock()

	if conn != nil {
		// Try to write a ping using direct method
		pingMsg := map[string]interface{}{
			"method": "server.ping",
			"params": []interface{}{},
			"id":     time.Now().UnixNano(),
		}

		if err := conn.WriteJSON(pingMsg); err != nil {
			fmt.Printf("⚠️  Failed to write ping, connection may be dead: %v\n", err)
			select {
			case c.wsReconnectChan <- true:
			default:
				// Channel full, ignore
			}
			return
		}
	}
}

// isConnectionHealthy checks if the WebSocket connection is healthy
func (c *HttpClient) isConnectionHealthy() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.wsConnected && c.wsConn != nil && !c.wsReconnecting
}

func (c *HttpClient) handleReconnection() {
	c.mu.RLock()
	reconnecting := c.wsReconnecting
	c.mu.RUnlock()

	if reconnecting {
		fmt.Printf("🔄 Reconnection already in progress, skipping...\n")
		return // Already reconnecting
	}

	fmt.Printf("🔄 Handling WebSocket reconnection...\n")

	// Stop health monitor to prevent concurrent operations
	select {
	case c.wsHealthDone <- true:
	default:
	}

	// Try to reconnect with exponential backoff
	maxRetries := 5
	for attempt := 1; attempt <= maxRetries; attempt++ {
		fmt.Printf("🔄 Reconnection attempt %d/%d\n", attempt, maxRetries)

		if err := c.ReconnectWebSocket(); err != nil {
			fmt.Printf("❌ Reconnection attempt %d failed: %v\n", attempt, err)

			if attempt < maxRetries {
				// Exponential backoff: 1s, 2s, 4s, 8s, 16s
				backoff := time.Duration(1<<uint(attempt-1)) * time.Second
				fmt.Printf("⏳ Waiting %v before next attempt...\n", backoff)
				time.Sleep(backoff)
			} else {
				fmt.Printf("❌ All reconnection attempts failed\n")
				return
			}
		} else {
			fmt.Printf("✅ WebSocket reconnection successful on attempt %d\n", attempt)

			// Wait for connection to stabilize
			time.Sleep(2 * time.Second)

			// Resubscribe to previously subscribed markets
			if err := c.ResubscribeToMarkets(); err != nil {
				fmt.Printf("⚠️  Failed to resubscribe to markets: %v\n", err)
			}

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

	// Try a simple subscription format
	subMsg := map[string]interface{}{
		"method": "depth.subscribe",
		"params": []interface{}{market}, // Just the market name
		"id":     time.Now().UnixNano(),
	}

	fmt.Printf("🔍 DEBUG: Subscribing to %s with ID %d\n", market, subMsg["id"])

	// Send subscription message using direct WriteJSON
	if err := c.wsConn.WriteJSON(subMsg); err != nil {
		c.mu.Unlock()
		fmt.Printf("❌ DEBUG: Failed to send subscription for %s: %v\n", market, err)
		return fmt.Errorf("websocket write error for %s: %w", market, err)
	}

	fmt.Printf("✅ DEBUG: Subscription sent successfully for %s\n", market)

	c.mu.Unlock()

	// Mark as subscribed optimistically
	c.wsSubscriptions[market] = true

	return nil
}

// SubscribeWebSocketWithRetry attempts to subscribe with retry logic
func (c *HttpClient) SubscribeWebSocketWithRetry(market string, maxRetries int) error {
	if maxRetries <= 0 {
		maxRetries = 3
	}

	for attempt := 1; attempt <= maxRetries; attempt++ {
		err := c.SubscribeWebSocket(market)
		if err == nil {
			return nil
		}

		fmt.Printf("❌ Subscription attempt %d/%d failed for %s: %v\n", attempt, maxRetries, market, err)

		if attempt < maxRetries {
			// Wait before retry with exponential backoff
			backoff := time.Duration(attempt) * time.Second
			fmt.Printf("⏳ Waiting %v before retry...\n", backoff)
			time.Sleep(backoff)

			// Check if we need to reconnect
			if !c.IsWebSocketConnected() {
				fmt.Printf("🔄 WebSocket disconnected, attempting reconnection...\n")

				// Use a channel to signal reconnection completion
				reconnectDone := make(chan error, 1)
				go func() {
					reconnectDone <- c.ReconnectWebSocket()
				}()

				// Wait for reconnection with timeout
				select {
				case err := <-reconnectDone:
					if err != nil {
						fmt.Printf("❌ Reconnection failed: %v\n", err)
						return fmt.Errorf("failed to reconnect: %w", err)
					}
				case <-time.After(30 * time.Second):
					fmt.Printf("❌ Reconnection timeout\n")
					return fmt.Errorf("reconnection timeout")
				}

				// Wait for connection to stabilize
				time.Sleep(2 * time.Second)
			}
		}
	}

	return fmt.Errorf("failed to subscribe to %s after %d attempts", market, maxRetries)
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

	// Try a simple subscription format
	subMsg := map[string]interface{}{
		"method": "depth.subscribe",
		"params": []interface{}{market}, // Just the market name
		"id":     id,                    // Required by CoinEx but we don't track it
	}

	// Send subscription message using direct WriteJSON
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
		batchSize = 10 // Increased from 5 to 10 for larger batches
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

		// Check connection health before starting batch
		if !c.IsWebSocketConnected() {
			fmt.Printf("   ⚠️  WebSocket disconnected, attempting reconnection...\n")
			if err := c.ReconnectWebSocket(); err != nil {
				fmt.Printf("   ❌ Failed to reconnect: %v\n", err)
				// Add remaining markets to failed list
				for _, market := range markets[batchStart:] {
					failed = append(failed, market)
				}
				break
			}
			// Wait for connection to stabilize
			time.Sleep(2 * time.Second)
		}

		// Subscribe to current batch with retry logic
		batchSuccessful := []string{}
		batchFailed := []string{}

		for i, market := range currentBatch {
			// Shorter delay between subscriptions for faster processing
			if i > 0 {
				time.Sleep(300 * time.Millisecond) // Reduced from 500ms to 300ms
			}

			// Use retry logic for each subscription
			err := c.SubscribeWebSocketWithRetry(market, 3)
			if err != nil {
				batchFailed = append(batchFailed, market)
				fmt.Printf("   ❌ %s: %v\n", market, err)
			} else {
				batchSuccessful = append(batchSuccessful, market)
				if len(batchSuccessful) <= 3 || len(batchSuccessful)%5 == 0 {
					fmt.Printf("   📡 %s subscribed\n", market)
				}
			}

			// Check connection health after each subscription
			if !c.IsWebSocketConnected() {
				fmt.Printf("   ⚠️  WebSocket disconnected during batch %d, stopping\n", batchNum)
				// Add remaining markets in this batch to failed list
				for _, remainingMarket := range currentBatch[i+1:] {
					batchFailed = append(batchFailed, remainingMarket)
				}
				break
			}
		}

		fmt.Printf("   📊 Batch %d sent: %d subscriptions\n", batchNum, len(batchSuccessful))

		// Wait for data to start flowing from this batch
		fmt.Printf("   ⏳ Waiting for batch %d data validation...\n", batchNum)

		batchWorking := []string{}
		batchDead := []string{}

		// Give more time for data to accumulate from this batch
		for waitSeconds := 1; waitSeconds <= 5; waitSeconds++ {
			time.Sleep(1 * time.Second)

			// Check connection health during wait
			if !c.IsWebSocketConnected() {
				fmt.Printf("     ❌ WebSocket disconnected during batch %d wait, stopping\n", batchNum)
				// Add remaining markets to failed list
				for _, market := range markets[batchStart:] {
					failed = append(failed, market)
				}
				return successful, failed
			}

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
			if float64(workingCount)/float64(len(batchSuccessful)) >= 0.6 && waitSeconds >= 3 {
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

		// Shorter pause before next batch (unless it's the last batch)
		if batchEnd < len(markets) {
			fmt.Printf("   💤 Cooling down 2s before next batch...\n") // Reduced from 5s to 2s
			time.Sleep(2 * time.Second)
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

func (c *HttpClient) SubscribeWebSocketSequentially(markets []string) ([]string, []string) {
    successful := []string{}
    failed := []string{}

    log.Printf("Subscribing to %d markets sequentially...\n", len(markets))

    for _, market := range markets {
        if err := c.SubscribeWebSocketWithRetry(market, 3); err != nil {
            log.Printf("Failed to subscribe to %s: %v\n", market, err)
            failed = append(failed, market)
        } else {
            log.Printf("Successfully subscribed to %s\n", market)
            successful = append(successful, market)
        }
        // Add a small delay between subscriptions to avoid overwhelming the server
        time.Sleep(250 * time.Millisecond)
    }

    log.Printf("Sequential subscription complete: %d successful, %d failed\n", len(successful), len(failed))
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

	// Stop all goroutines
	select {
	case c.wsReadDone <- true:
	default:
	}
	select {
	case c.wsPingDone <- true:
	default:
	}
	select {
	case c.wsHealthDone <- true:
	default:
	}
	select {
	case c.wsStopChan <- true:
	default:
	}

	if c.wsConn != nil {
		err := c.wsConn.Close()
		c.wsConn = nil
		c.wsConnected = false
		return err
	}
	return nil
}

// IsWebSocketReconnecting returns true if a reconnection is in progress
func (c *HttpClient) IsWebSocketReconnecting() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.wsReconnecting
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

// ResubscribeToMarkets resubscribes to all previously subscribed markets
func (c *HttpClient) ResubscribeToMarkets() error {
	c.mu.RLock()
	subscribedMarkets := make([]string, 0, len(c.wsSubscriptions))
	for market := range c.wsSubscriptions {
		subscribedMarkets = append(subscribedMarkets, market)
	}
	c.mu.RUnlock()

	if len(subscribedMarkets) == 0 {
		return nil // No markets to resubscribe
	}

	fmt.Printf("🔄 Resubscribing to %d markets after reconnection...\n", len(subscribedMarkets))

	successful := []string{}
	failed := []string{}

	// Resubscribe in larger batches for faster processing
	batchSize := 10 // Increased from 5 to 10
	for i := 0; i < len(subscribedMarkets); i += batchSize {
		end := i + batchSize
		if end > len(subscribedMarkets) {
			end = len(subscribedMarkets)
		}

		batch := subscribedMarkets[i:end]
		fmt.Printf("   📡 Resubscribing batch %d-%d (%d markets)...\n", i+1, end, len(batch))

		for _, market := range batch {
			if err := c.SubscribeWebSocketWithRetry(market, 2); err != nil {
				failed = append(failed, market)
				fmt.Printf("   ❌ Failed to resubscribe to %s: %v\n", market, err)
			} else {
				successful = append(successful, market)
			}

			// Shorter delay between resubscriptions
			time.Sleep(150 * time.Millisecond) // Reduced from 200ms to 150ms
		}

		// Shorter wait between batches
		if end < len(subscribedMarkets) {
			time.Sleep(500 * time.Millisecond) // Reduced from 1s to 500ms
		}
	}

	fmt.Printf("🔄 Resubscription complete: %d successful, %d failed\n", len(successful), len(failed))

	if len(failed) > 0 {
		// Remove failed markets from subscription cache
		c.mu.Lock()
		for _, market := range failed {
			delete(c.wsSubscriptions, market)
		}
		c.mu.Unlock()
	}

	return nil
}

// Add method to get order book via REST API as fallback
func (c *HttpClient) GetOrderBookREST(market string) (*OrderBook, error) {
	log.Printf("🔗 Making REST API call for %s order book...", market)

	// Add timestamp to avoid cached responses
	timestamp := time.Now().Unix()
	params := map[string]string{
		"url":    fmt.Sprintf("%s/market/depth?market=%s&merge=0&limit=%d&_t=%d", c.config.APIBaseURL, market, c.config.OrderBookDepthLimit, timestamp),
		"method": GET,
	}

	response, err := c.performRequest(params, GET)
	if err != nil {
		log.Printf("❌ REST API request failed for %s: %v", market, err)
		return nil, err
	}

	// Parse CoinEx depth response
	data, ok := response["data"].(map[string]interface{})
	if !ok {
		log.Printf("❌ Invalid response format for %s: missing data", market)
		log.Printf("🔍 DEBUG: Response structure: %+v", response)
		return nil, fmt.Errorf("invalid response format")
	}

	log.Printf("🔍 DEBUG: Data keys: %v", getMapKeys(data))

	asks, ok := data["asks"].([]interface{})
	if !ok {
		log.Printf("❌ Invalid asks format for %s", market)
		return nil, fmt.Errorf("invalid asks format")
	}

	bids, ok := data["bids"].([]interface{})
	if !ok {
		log.Printf("❌ Invalid bids format for %s", market)
		log.Printf("🔍 DEBUG: Bids type: %T, value: %+v", data["bids"], data["bids"])
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

	// Fetch latest price for this market
	if latestPrice, err := c.GetMarketTicker(market); err == nil {
		orderBook.Latest = latestPrice
		log.Printf("✅ REST API order book parsed for %s: %d bids, %d asks, latest: %.8f", market, len(orderBook.Bids), len(orderBook.Asks), orderBook.Latest)
	} else {
		log.Printf("✅ REST API order book parsed for %s: %d bids, %d asks, no latest price available", market, len(orderBook.Bids), len(orderBook.Asks))
	}

	return orderBook, nil
}

// GetMarketTicker fetches the latest ticker data for a market
func (c *HttpClient) GetMarketTicker(market string) (float64, error) {
	params := map[string]string{
		"url":    fmt.Sprintf("%s/market/ticker?market=%s", c.config.APIBaseURL, market),
		"method": GET,
	}

	response, err := c.performRequest(params, GET)
	if err != nil {
		return 0, fmt.Errorf("HTTP request failed: %v", err)
	}

	// Parse response
	data, ok := response["data"].(map[string]interface{})
	if !ok {
		return 0, fmt.Errorf("invalid response format")
	}

	ticker, ok := data["ticker"].(map[string]interface{})
	if !ok {
		return 0, fmt.Errorf("invalid ticker format")
	}

	lastPriceStr, ok := ticker["last"].(string)
	if !ok {
		return 0, fmt.Errorf("missing last price")
	}

	price, err := strconv.ParseFloat(lastPriceStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid price format: %v", err)
	}

	return price, nil
}

// Helper function to get map keys for debugging
func getMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// HTTP Methods (existing functionality)

func (c *HttpClient) getQueryString(urlStr string, params map[string]string) string {
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
	// Apply rate limiting
	if !c.rateLimiter.Allow() {
		return nil, fmt.Errorf("rate limit exceeded")
	}

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

// TestWebSocketConnection tests the WebSocket connection with a single subscription
func (c *HttpClient) TestWebSocketConnection() error {
	fmt.Printf("🧪 Testing WebSocket connection...\n")

	// Connect to WebSocket
	if err := c.ConnectWebSocket(); err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	// Wait a moment for connection to stabilize
	time.Sleep(2 * time.Second)

	// Check if connection is still alive
	if !c.IsWebSocketConnected() {
		return fmt.Errorf("websocket disconnected after initial connection")
	}

	fmt.Printf("🧪 Connection test successful - WebSocket is connected and stable\n")

	// Try a single subscription with a valid CoinEx market
	testMarket := "SUIUSDT" // Back to SUIUSDT which should be available based on the codebase
	fmt.Printf("🧪 Testing subscription to %s...\n", testMarket)

	// Use simple subscription format
	testMsg := map[string]interface{}{
		"method": "depth.subscribe",
		"params": []interface{}{testMarket}, // Just the market name
		"id":     time.Now().UnixNano(),
	}

	fmt.Printf("🧪 Sending subscription message...\n")

	// Check connection health before sending
	if !c.isConnectionHealthy() {
		return fmt.Errorf("websocket not healthy before sending subscription")
	}

	// Use direct write method
	if err := c.wsConn.WriteJSON(testMsg); err != nil {
		return fmt.Errorf("failed to send test subscription: %w", err)
	}

	fmt.Printf("🧪 Test subscription sent successfully, waiting for response...\n")

	// Wait for response and monitor for messages
	timeout := time.After(10 * time.Second)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			fmt.Printf("🧪 Test timeout - no response received\n")
			return nil
		case <-ticker.C:
			// Check if we received any messages
			select {
			case msg := <-c.wsDataFeed:
				fmt.Printf("🧪 Received message: %s\n", string(msg)[:Min(200, len(msg))])
				return nil
			default:
				// No message received yet
			}
		}
	}
}

// ValidateWebSocketDataAccuracy validates WebSocket data accuracy by comparing with REST API
func (c *HttpClient) ValidateWebSocketDataAccuracy(markets []string) {
	log.Printf(" Validating WebSocket data accuracy for %d markets...", len(markets))

	validationCount := 0
	for _, market := range markets {
		if validationCount >= 5 { // Only validate first 5 markets to avoid spam
			break
		}

		// Get WebSocket data
		c.marketDataMu.RLock()
		wsOrderBook, wsExists := c.marketData[market]
		c.marketDataMu.RUnlock()

		if !wsExists || wsOrderBook == nil || len(wsOrderBook.Bids) == 0 || len(wsOrderBook.Asks) == 0 {
			continue
		}

		// Get REST API data for comparison
		restOrderBook, err := c.GetOrderBookREST(market)
		if err != nil {
			continue
		}

		if len(restOrderBook.Bids) > 0 && len(restOrderBook.Asks) > 0 {
			wsBid := wsOrderBook.Bids[0].Price
			wsAsk := wsOrderBook.Asks[0].Price
			restBid := restOrderBook.Bids[0].Price
			restAsk := restOrderBook.Asks[0].Price

			log.Printf(" Data Validation | %s | WS: Bid=%s Ask=%s | REST: Bid=%s Ask=%s",
				market, wsBid, wsAsk, restBid, restAsk)

			validationCount++
		}
	}
}
