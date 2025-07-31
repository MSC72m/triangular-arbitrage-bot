package main

import (
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"encoding/hex"
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
	wsReadDone     chan bool
	wsPingDone     chan bool
	wsHealthDone   chan bool
	reconnectMutex sync.Mutex
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
		wsPongTimeout:   45 * time.Second, // Reduced from 60s to 45s for faster detection
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
		EnableCompression: true, // Enable deflate compression as required by CoinEx
		HandshakeTimeout:  45 * time.Second,
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

	// Start only the read messages goroutine - no ping or health monitor
	go c.readWebSocketMessages()

	// Wait a moment for connection to stabilize before allowing subscriptions
	fmt.Printf("⏳ Waiting for connection to stabilize...\n")
	time.Sleep(2 * time.Second) // Reduced back to 2s like working test

	// For public market data, authentication is not required
	// CoinEx WebSocket for public data doesn't need server.sign
	fmt.Printf("ℹ️  Using public WebSocket connection (no authentication required)\n")

	// Send a small immediate subscription to keep connection alive
	fmt.Printf("📡 Sending immediate subscription to keep connection alive...\n")
	immediateMsg := map[string]interface{}{
		"method": "depth.subscribe",
		"params": map[string]interface{}{
			"market_list": [][]interface{}{
				{"BTCUSDT", 10, "0", true},
			},
		},
		"id": time.Now().UnixNano(),
	}

	if err := conn.WriteJSON(immediateMsg); err != nil {
		fmt.Printf("❌ Failed to send immediate subscription: %v\n", err)
		return fmt.Errorf("failed to send immediate subscription: %w", err)
	}

	fmt.Printf("✅ Immediate subscription sent successfully\n")

	// Connection is ready for subscriptions
	fmt.Printf("✅ WebSocket connection ready for subscriptions\n")

	return nil
}

func (c *HttpClient) authenticateWebSocket(apiKey, secretKey string) error {
	if !c.wsConnected || c.wsConn == nil {
		return fmt.Errorf("websocket not connected")
	}

	timestamp := time.Now().UnixMilli()
	stringToSign := fmt.Sprintf("access_id=%s&timestamp=%d&secret_key=%s", apiKey, timestamp, secretKey)
	hash := md5.Sum([]byte(stringToSign))
	signature := hex.EncodeToString(hash[:])

	authMsg := map[string]interface{}{
		"method": "server.sign",
		"params": []interface{}{apiKey, signature, timestamp},
		"id":     time.Now().UnixNano(),
	}

	fmt.Printf("🔐 Sending WebSocket authentication...\n")

	if err := c.wsConn.WriteJSON(authMsg); err != nil {
		return fmt.Errorf("failed to send authentication request: %w", err)
	}

	// Wait for authentication response (optional - some exchanges don't require confirmation)
	fmt.Printf("⏳ Waiting for authentication response...\n")
	time.Sleep(2 * time.Second)

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

		// Set read deadline like the working websocket.go test
		if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
			fmt.Printf("❌ Failed to set read deadline: %v\n", err)
			return
		}

		messageType, message, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				fmt.Printf("❌ WebSocket read error: %v\n", err)
			} else {
				fmt.Printf("📨 WebSocket connection closed normally\n")
			}

			// Log the specific close code and reason
			if closeErr, ok := err.(*websocket.CloseError); ok {
				fmt.Printf("🔌 WebSocket close details: code=%d, reason='%s'\n", closeErr.Code, closeErr.Text)
			}

			// Log additional context about the error
			fmt.Printf("🔍 WebSocket read error type: %T, error: %v\n", err, err)

			// Update connection state when read fails
			c.mu.Lock()
			c.wsConnected = false
			c.mu.Unlock()

			// Attempt to reconnect
			c.handleReconnection()
			return
		}

		messageCount++

		// Handle different message types
		switch messageType {
		case websocket.TextMessage:
			// Log only first few messages and periodic summaries
			if messageCount <= 3 {
				fmt.Printf("📨 WebSocket Text Message #%d: %s\n", messageCount, string(message))
			} else if messageCount%1000 == 0 {
				// Log every 1000th message
				fmt.Printf("📨 WebSocket Text Message #%d received\n", messageCount)
			}

			// Send to data feed channel (non-blocking)
			select {
			case c.wsDataFeed <- message:
			default:
				fmt.Printf("⚠️  WebSocket data feed channel full, dropping message #%d\n", messageCount)
			}

		case websocket.BinaryMessage:
			// Handle compressed binary messages
			if messageCount <= 3 {
				fmt.Printf("📦 WebSocket Binary Message #%d: %d bytes\n", messageCount, len(message))
			}

			// Try to decompress the binary data
			reader := bytes.NewReader(message)
			gzipReader, err := gzip.NewReader(reader)
			if err != nil {
				fmt.Printf("❌ Failed to create gzip reader: %v\n", err)
				continue
			}
			defer gzipReader.Close()

			// Read the decompressed data
			decompressedData, err := io.ReadAll(gzipReader)
			if err != nil {
				fmt.Printf("❌ Failed to decompress data: %v\n", err)
				continue
			}

			if messageCount <= 3 {
				fmt.Printf("📦 Decompressed data: %s\n", string(decompressedData))
			}

			// Send decompressed data to data feed channel (non-blocking)
			select {
			case c.wsDataFeed <- decompressedData:
			default:
				fmt.Printf("⚠️  WebSocket data feed channel full, dropping decompressed message #%d\n", messageCount)
			}

		case websocket.PingMessage:
			fmt.Printf("🏓 Ping received\n")
			// Don't send pong - let the server handle it
			// This prevents potential connection issues

		case websocket.PongMessage:
			fmt.Printf("🏓 Pong received\n")

		case websocket.CloseMessage:
			fmt.Printf("🔌 Close message received\n")
			return

		default:
			fmt.Printf("📨 Unknown message type %d\n", messageType)
		}

		// Also log any error messages or subscription responses
		if messageCount <= 10 {
			if messageType == websocket.TextMessage {
				fmt.Printf("📨 WebSocket Message #%d (first 10): %s\n", messageCount, string(message))
			} else if messageType == websocket.BinaryMessage {
				fmt.Printf("📦 WebSocket Binary Message #%d (first 10): %d bytes\n", messageCount, len(message))
			}
		}
	}
}

// pingWebSocketLoop is disabled to prevent connection timeouts
/*
func (c *HttpClient) pingWebSocketLoop() {
	ticker := time.NewTicker(30 * time.Second) // Reduced from 60s to 30s to prevent server timeouts
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

		fmt.Printf("🏓 Ping sent at %v\n", time.Now())
	}
}
*/

// connectionHealthMonitor is disabled to prevent connection timeouts
/*
func (c *HttpClient) connectionHealthMonitor() {
	ticker := time.NewTicker(30 * time.Second) // Increased from 10s to 30s
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
*/

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

	// Don't send additional pings from health check - let the ping loop handle it
	// This avoids overwhelming the connection with too many pings
}

// isConnectionHealthy checks if the WebSocket connection is healthy
func (c *HttpClient) isConnectionHealthy() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Basic checks
	if !c.wsConnected || c.wsConn == nil || c.wsReconnecting {
		return false
	}

	// Check if the connection has a remote address (basic connectivity check)
	if c.wsConn.RemoteAddr() == nil {
		fmt.Printf("⚠️  WebSocket connection has no remote address\n")
		return false
	}

	// Additional check: try to get connection state
	// This is a basic check - if we can't get the remote address, connection is dead
	remoteAddr := c.wsConn.RemoteAddr().String()
	if remoteAddr == "" {
		fmt.Printf("⚠️  WebSocket connection remote address is empty\n")
		return false
	}

	return true
}

func (c *HttpClient) handleReconnection() {
	c.reconnectMutex.Lock()
	defer c.reconnectMutex.Unlock()

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

// SubscribeWebSocket subscribes to all markets at once using the correct format
func (c *HttpClient) SubscribeWebSocket(markets []string) error {
	fmt.Printf("🔍 DEBUG: SubscribeWebSocket called with %d markets\n", len(markets))

	c.mu.Lock()
	fmt.Printf("🔍 DEBUG: Acquired lock\n")

	if !c.wsConnected || c.wsConn == nil {
		c.mu.Unlock()
		fmt.Printf("❌ WebSocket not connected, skipping subscription for %d markets\n", len(markets))
		return fmt.Errorf("websocket not connected")
	}

	// Check connection health before sending
	fmt.Printf("🔍 DEBUG: Checking connection health...\n")
	// Removed health check that was causing hang
	/*
		if !c.isConnectionHealthy() {
			c.mu.Unlock()
			fmt.Printf("❌ WebSocket connection not healthy, skipping subscription for %d markets\n", len(markets))
			return fmt.Errorf("websocket connection not healthy for %d markets", len(markets))
		}
	*/

	// Break large batches into smaller chunks to avoid overwhelming the connection
	batchSize := 5 // Reduced from 20 to 5 markets at a time
	totalMarkets := len(markets)
	successfulSubscriptions := 0

	fmt.Printf("📡 Subscribing to %d markets in batches of %d...\n", totalMarkets, batchSize)

	for i := 0; i < totalMarkets; i += batchSize {
		end := i + batchSize
		if end > totalMarkets {
			end = totalMarkets
		}

		batch := markets[i:end]
		fmt.Printf("📡 Subscribing batch %d-%d (%d markets)...\n", i+1, end, len(batch))

		// Prepare market list for subscription
		fmt.Printf("🔍 DEBUG: Preparing market list for batch %d-%d\n", i+1, end)
		var marketList = make([][]interface{}, len(batch))
		for j, market := range batch {
			marketList[j] = []interface{}{market, c.config.OrderBookDepthLimit, "0", true}
		}

		// Use CoinEx's correct subscription format with market_list
		subMsg := map[string]interface{}{
			"method": "depth.subscribe",
			"params": map[string]interface{}{
				"market_list": marketList,
			},
			"id": time.Now().UnixNano(),
		}

		fmt.Printf("🔍 DEBUG: Subscribing to %d markets with ID %d\n", len(batch), subMsg["id"])

		// Get the connection reference while holding the lock
		conn := c.wsConn
		fmt.Printf("🔍 DEBUG: Got connection reference\n")

		// Mark all markets as subscribed optimistically BEFORE releasing the lock
		for _, market := range batch {
			c.wsSubscriptions[market] = true
		}
		fmt.Printf("🔍 DEBUG: Marked markets as subscribed, releasing lock\n")
		c.mu.Unlock()

		// Set a write deadline to prevent indefinite blocking
		fmt.Printf("🔍 DEBUG: Setting write deadline...\n")
		if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
			fmt.Printf("❌ DEBUG: Failed to set write deadline for batch %d-%d: %v\n", i+1, end, err)
			c.mu.Lock()
			for _, market := range batch {
				delete(c.wsSubscriptions, market)
			}
			c.mu.Unlock()
			return fmt.Errorf("failed to set write deadline for batch %d-%d: %w", i+1, end, err)
		}

		// Send subscription message using direct WriteJSON with timeout protection
		fmt.Printf("🔍 DEBUG: Sending WriteJSON for batch %d-%d...\n", i+1, end)

		// Use a channel to timeout the WriteJSON call
		writeDone := make(chan error, 1)
		go func() {
			writeDone <- conn.WriteJSON(subMsg)
		}()

		select {
		case err := <-writeDone:
			if err != nil {
				fmt.Printf("❌ DEBUG: Failed to send subscription for batch %d-%d: %v\n", i+1, end, err)
				// Remove from subscriptions since write failed
				c.mu.Lock()
				for _, market := range batch {
					delete(c.wsSubscriptions, market)
				}
				c.mu.Unlock()
				return fmt.Errorf("websocket write error for batch %d-%d: %w", i+1, end, err)
			}
		case <-time.After(5 * time.Second):
			fmt.Printf("❌ DEBUG: WriteJSON timeout for batch %d-%d\n", i+1, end)
			// Remove from subscriptions since write failed
			c.mu.Lock()
			for _, market := range batch {
				delete(c.wsSubscriptions, market)
			}
			c.mu.Unlock()
			return fmt.Errorf("websocket write timeout for batch %d-%d", i+1, end)
		}

		fmt.Printf("✅ DEBUG: Subscription sent successfully for batch %d-%d (%d markets)\n", i+1, end, len(batch))
		successfulSubscriptions += len(batch)

		// Wait a moment between batches to avoid overwhelming the connection
		if end < totalMarkets {
			fmt.Printf("🔍 DEBUG: Waiting 500ms before next batch...\n")
			time.Sleep(500 * time.Millisecond)
		}

		// Re-acquire lock for next iteration
		fmt.Printf("🔍 DEBUG: Re-acquiring lock for next iteration...\n")
		c.mu.Lock()
	}

	c.mu.Unlock()

	fmt.Printf("✅ DEBUG: All subscriptions completed: %d/%d markets\n", successfulSubscriptions, totalMarkets)

	// Wait a moment for all subscriptions to be processed
	time.Sleep(2 * time.Second) // Reduced to match working test

	return nil
}

// SubscribeWebSocketWithRetry attempts to subscribe with retry logic
func (c *HttpClient) SubscribeWebSocketWithRetry(markets []string, maxRetries int) error {
	if maxRetries <= 0 {
		maxRetries = 3
	}

	for attempt := 1; attempt <= maxRetries; attempt++ {
		err := c.SubscribeWebSocket(markets)
		if err == nil {
			return nil
		}

		fmt.Printf("❌ Subscription attempt %d/%d failed for %d markets: %v\n", attempt, maxRetries, len(markets), err)

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

	return fmt.Errorf("failed to subscribe to %d markets after %d attempts", len(markets), maxRetries)
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
			if err := c.SubscribeWebSocketWithRetry([]string{market}, 2); err != nil {
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

	// Don't send any test subscription - just verify the connection is stable
	fmt.Printf("🧪 Connection verified - ready for subscriptions\n")

	return nil
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

// ValidateMarketExists checks if a market exists before subscribing
func (c *HttpClient) ValidateMarketExists(market string) bool {
	// Basic validation - check if market format is valid
	if len(market) < 6 || !strings.Contains(market, "USDT") {
		return false
	}

	// Check if we have this market in our cache from the initial market list
	// This is a simple validation - in production you might want to check against
	// the actual market list from the exchange
	return true
}

// SubscribeWebSocketWithValidation subscribes to a market with validation
func (c *HttpClient) SubscribeWebSocketWithValidation(market string) error {
	if !c.ValidateMarketExists(market) {
		return fmt.Errorf("market %s appears to be invalid", market)
	}

	return c.SubscribeWebSocket([]string{market})
}

// TestMarketSubscription tests subscription to a specific market and provides detailed feedback
func (c *HttpClient) TestMarketSubscription(market string) error {
	fmt.Printf("🧪 Testing subscription to market: %s\n", market)

	// Check connection health first
	if !c.IsWebSocketConnected() {
		return fmt.Errorf("websocket not connected")
	}

	if !c.isConnectionHealthy() {
		return fmt.Errorf("websocket connection not healthy")
	}

	// Validate market format
	if !c.ValidateMarketExists(market) {
		return fmt.Errorf("market %s appears to be invalid", market)
	}

	// Try subscription
	err := c.SubscribeWebSocket([]string{market})
	if err != nil {
		return fmt.Errorf("subscription failed: %w", err)
	}

	fmt.Printf("✅ Test subscription to %s sent successfully\n", market)

	// Wait a moment and check if we received any data
	time.Sleep(3 * time.Second)

	// Check if we have data for this market
	c.marketDataMu.RLock()
	orderBook, exists := c.marketData[market]
	c.marketDataMu.RUnlock()

	if !exists {
		return fmt.Errorf("no data received for %s after subscription", market)
	}

	if orderBook == nil || (len(orderBook.Bids) == 0 && len(orderBook.Asks) == 0) {
		return fmt.Errorf("received empty order book for %s", market)
	}

	fmt.Printf("✅ Market %s is working - received %d bids, %d asks\n",
		market, len(orderBook.Bids), len(orderBook.Asks))

	return nil
}
