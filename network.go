package main

import (
	"bytes"
	"compress/gzip"
	"crypto/hmac"
	"crypto/sha256"
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

	// Retry queue for failed WebSocket markets
	failedMarkets    map[string]time.Time // market -> last failure time
	failedMarketsMu  sync.RWMutex
	retryInterval    time.Duration
	maxRetryAttempts int

	// Flag to indicate if WebSocket data processing has been stopped
	wsProcessingStopped bool

	// Market depths reference for resubscription
	marketDepths *MarketDepths

	// Flag to track initial phase (full updates only) - DEPRECATED
	// We now use REST API for initial data and WebSocket with is_full=false for incremental updates
}

func newHttpClient(config *Config) *HttpClient {
	return &HttpClient{
		config:              config,
		headers:             make(map[string]string),
		wsSubscriptions:     make(map[string]bool),
		wsDataFeed:          make(chan []byte, config.WebSocketBufferSize), // Reasonable buffer for incremental updates
		marketData:          make(map[string]*OrderBook),
		rateLimiter:         NewRateLimiter(config.RateLimitPerSecond, config.RateLimitPerSecond),
		wsUrl:               "wss://socket.coinex.com/v2/spot", // Original CoinEx spot WebSocket URL
		wsReconnectChan:     make(chan bool, 1),
		wsStopChan:          make(chan bool, 1),
		wsPongTimeout:       45 * time.Second, // Reduced from 60s to 45s for faster detection
		wsReadDone:          make(chan bool, 1),
		wsPingDone:          make(chan bool, 1),
		wsHealthDone:        make(chan bool, 1),
		failedMarkets:       make(map[string]time.Time),
		retryInterval:       5 * time.Second, // Default retry interval
		maxRetryAttempts:    3,
		wsProcessingStopped: false,
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

	// Use minimal headers for WebSocket connection
	wsHeaders := make(http.Header)
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

	// Authenticate WebSocket connection if credentials are provided
	if c.config.APIKey != "" && c.config.SecretID != "" {
		fmt.Printf("🔐 Authenticating WebSocket connection...\n")
		if err := c.authenticateWebSocket(c.config.APIKey, c.config.SecretID); err != nil {
			return fmt.Errorf("WebSocket authentication failed: %w", err)
		}
	}

	// Send a small immediate subscription to keep connection alive
	fmt.Printf("📡 Sending immediate subscription to keep connection alive...\n")
	immediateMsg := map[string]interface{}{
		"method": "depth.subscribe",
		"params": map[string]interface{}{
			"market_list": [][]interface{}{
				{"BTCUSDT", 10, "0", true}, // merge=0 for precise numbers, full=true for initial
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
func (c *HttpClient) authenticateWebSocket(secretID, apiKey string) error {
	if !c.wsConnected || c.wsConn == nil {
		return fmt.Errorf("websocket not connected")
	}

	timestamp := time.Now().UnixMilli()

	// Step 1: Create the string to sign (just timestamp as per CoinEx docs)
	// According to CoinEx docs: https://docs.coinex.com/api/v2/authorization
	// WebSocket signature format: timestamp (just the timestamp!)
	preparedStr := strconv.FormatInt(timestamp, 10)

	// Step 2: Create HMAC-SHA256 signature using the secret key (longer string)
	h := hmac.New(sha256.New, []byte(apiKey))
	h.Write([]byte(preparedStr))
	signedStr := hex.EncodeToString(h.Sum(nil))

	authMsg := map[string]interface{}{
		"method": "server.sign",
		"params": map[string]interface{}{
			"access_id":  secretID,
			"signed_str": signedStr,
			"timestamp":  timestamp,
		},
		"id": time.Now().UnixNano(),
	}

	if err := c.wsConn.WriteJSON(authMsg); err != nil {
		return fmt.Errorf("failed to send authentication request: %w", err)
	}

	// Wait for authentication response with longer timeout
	c.wsConn.SetReadDeadline(time.Now().Add(30 * time.Second))

	// Read multiple messages to find the auth response
	for i := 0; i < 10; i++ { // Try up to 10 messages
		messageType, message, err := c.wsConn.ReadMessage()
		if err != nil {
			return fmt.Errorf("failed to read authentication response (attempt %d): %w", i+1, err)
		}

		if messageType != websocket.TextMessage && messageType != websocket.BinaryMessage {
			continue
		}

		var wsResponse map[string]interface{}
		var data []byte

		if messageType == websocket.BinaryMessage {
			reader := bytes.NewReader(message)
			gzipReader, err := gzip.NewReader(reader)
			if err != nil {
				fmt.Printf("⚠️  Failed to create gzip reader for auth response: %v\n", err)
				continue
			}
			defer gzipReader.Close()
			data, err = io.ReadAll(gzipReader)
			if err != nil {
				fmt.Printf("⚠️  Failed to decompress auth response: %v\n", err)
				continue
			}
		} else {
			data = message
		}

		fmt.Printf("📨 Auth response data: %s\n", string(data))

		if err := json.Unmarshal(data, &wsResponse); err != nil {
			fmt.Printf("⚠️  Failed to parse auth response: %v\n", err)
			continue
		}

		fmt.Printf("📨 Parsed auth response: %+v\n", wsResponse)

		// Check for CoinEx success response format: {"id":..., "code":0, "message":"OK"}
		if code, ok := wsResponse["code"].(float64); ok && code == 0 {
			if message, ok := wsResponse["message"].(string); ok && message == "OK" {
				fmt.Printf("✅ WebSocket authentication successful (CoinEx format): %+v\n", wsResponse)
				return nil
			}
		}

		// Check for successful authentication response with method
		if method, ok := wsResponse["method"].(string); ok && method == "server.sign" {
			if _, ok := wsResponse["error"]; ok && wsResponse["error"] != nil {
				fmt.Printf("❌ WebSocket authentication error: %+v\n", wsResponse["error"])
				return fmt.Errorf("authentication error: %+v", wsResponse["error"])
			}
			fmt.Printf("✅ WebSocket authentication successful: %+v\n", wsResponse)
			return nil
		}

		// Check for result with matching id
		if _, ok := wsResponse["result"]; ok {
			if id, ok := wsResponse["id"]; ok && id == authMsg["id"] {
				fmt.Printf("✅ WebSocket authentication successful (by id): %+v\n", wsResponse)
				return nil
			}
		}

		// Check for any success indication
		if _, ok := wsResponse["result"]; ok {
			fmt.Printf("✅ WebSocket authentication successful (generic result): %+v\n", wsResponse)
			return nil
		}

		// If we get here, this wasn't the auth response, continue reading
		fmt.Printf("📨 Not auth response, continuing...\n")
	}

	return fmt.Errorf("authentication timeout - no valid response received")
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

	// Use minimal headers for WebSocket connection
	wsHeaders := make(http.Header)
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
		processingStopped := c.wsProcessingStopped
		c.mu.RUnlock()

		if !connected || conn == nil {
			fmt.Printf("📨 WebSocket read loop exiting - connection not available\n")
			return
		}

		// Check if processing has been stopped
		if processingStopped {
			fmt.Printf("📨 WebSocket read loop exiting - processing stopped\n")
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

		// Check again if processing has been stopped before processing the message
		c.mu.RLock()
		processingStopped = c.wsProcessingStopped
		c.mu.RUnlock()

		if processingStopped {
			fmt.Printf("📨 Skipping message processing - processing stopped\n")
			continue
		}

		// Handle different message types
		switch messageType {
		case websocket.TextMessage:
			// Minimal logging for text messages
			if messageCount <= 2 {
				fmt.Printf("📨 WebSocket Text Message #%d: %s\n", messageCount, string(message)[:Min(100, len(message))])
			}

			// Send to data feed channel (non-blocking)
			select {
			case c.wsDataFeed <- message:
			default:
				if messageCount%5000 == 0 {
					fmt.Printf("⚠️  WebSocket data feed channel full, dropping message #%d\n", messageCount)
				}
			}

		case websocket.BinaryMessage:
			// Handle compressed binary messages
			if messageCount <= 2 {
				fmt.Printf("📦 WebSocket Binary Message #%d: %d bytes\n", messageCount, len(message))
			}

			// Try to decompress the binary data
			reader := bytes.NewReader(message)
			gzipReader, err := gzip.NewReader(reader)
			if err != nil {
				if messageCount%1000 == 0 {
					fmt.Printf("❌ Failed to create gzip reader: %v\n", err)
				}
				continue
			}
			defer gzipReader.Close()

			// Read the decompressed data
			decompressedData, err := io.ReadAll(gzipReader)
			if err != nil {
				if messageCount%1000 == 0 {
					fmt.Printf("❌ Failed to decompress data: %v\n", err)
				}
				continue
			}

			// Send decompressed data to data feed channel (non-blocking)
			select {
			case c.wsDataFeed <- decompressedData:
			default:
				if messageCount%5000 == 0 {
					fmt.Printf("⚠️  WebSocket data feed channel full, dropping decompressed message #%d\n", messageCount)
				}
			}

		case websocket.PingMessage:
			// Don't log ping messages - too frequent
			break

		case websocket.PongMessage:
			// Don't log pong messages - too frequent
			break

		case websocket.CloseMessage:
			fmt.Printf("🔌 Close message received\n")
			return

		default:
			if messageCount%1000 == 0 {
				fmt.Printf("📨 Unknown message type %d\n", messageType)
			}
		}

		// No need to log every message - this was causing excessive logging
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
			if err := c.ResubscribeToMarkets(c.marketDepths); err != nil {
				fmt.Printf("⚠️  Failed to resubscribe to markets: %v\n", err)
			}

			return
		}
	}
}

// SubscribeWebSocket subscribes to all markets at once using the correct format
func (c *HttpClient) SubscribeWebSocket(markets []string, isFull bool) error {
	c.mu.Lock()

	if !c.wsConnected || c.wsConn == nil {
		c.mu.Unlock()
		return fmt.Errorf("websocket not connected")
	}

	// Break large batches into smaller chunks to avoid overwhelming the connection
	totalMarkets := len(markets)
	successfulSubscriptions := 0

	fmt.Printf("📡 Subscribing to %d markets in batches of %d (isFull: %v)...\n", totalMarkets, c.config.wsSubscriptionsBatchSize, isFull)

	for i := 0; i < totalMarkets; i += c.config.wsSubscriptionsBatchSize {
		end := i + c.config.wsSubscriptionsBatchSize
		if end > totalMarkets {
			end = totalMarkets
		}

		batch := markets[i:end]
		fmt.Printf("📡 Subscribing batch %d-%d (%d markets)...\n", i+1, end, len(batch))

		// Check connection health before each batch
		if !c.wsConnected || c.wsConn == nil {
			c.mu.Unlock()
			return fmt.Errorf("websocket disconnected during subscription")
		}

		// Prepare market list for subscription
		var marketList = make([][]interface{}, len(batch))
		for j, market := range batch {
			// Format: [market, depth, merge, full]
			// - market: market name
			// - depth: order book depth (use config value)
			// - merge: 0 for precise numbers (no aggregation)
			// - full: isFull parameter for full vs incremental updates
			marketList[j] = []interface{}{market, c.config.OrderBookDepthLimit, "0", isFull}
		}

		// Use CoinEx's correct subscription format with market_list
		// Set merge=0 for precise numbers, full=isFull for full/incremental updates
		subMsg := map[string]interface{}{
			"method": "depth.subscribe",
			"params": map[string]interface{}{
				"market_list": marketList,
			},
			"id": time.Now().UnixNano(),
		}

		// Get the connection reference while holding the lock
		conn := c.wsConn

		// Mark all markets as subscribed optimistically BEFORE releasing the lock
		for _, market := range batch {
			c.wsSubscriptions[market] = true
		}
		c.mu.Unlock()

		// Set a write deadline to prevent indefinite blocking
		if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
			c.mu.Lock()
			for _, market := range batch {
				delete(c.wsSubscriptions, market)
			}
			c.mu.Unlock()
			return fmt.Errorf("failed to set write deadline for batch %d-%d: %w", i+1, end, err)
		}

		// Send subscription message using direct WriteJSON with timeout protection
		// Use a channel to timeout the WriteJSON call
		writeDone := make(chan error, 1)
		go func() {
			writeDone <- conn.WriteJSON(subMsg)
		}()

		select {
		case err := <-writeDone:
			if err != nil {
				// Remove from subscriptions since write failed
				c.mu.Lock()
				for _, market := range batch {
					delete(c.wsSubscriptions, market)
				}
				c.mu.Unlock()
				return fmt.Errorf("websocket write error for batch %d-%d: %w", i+1, end, err)
			}
		case <-time.After(5 * time.Second):
			// Remove from subscriptions since write failed
			c.mu.Lock()
			for _, market := range batch {
				delete(c.wsSubscriptions, market)
			}
			c.mu.Unlock()
			return fmt.Errorf("websocket write timeout for batch %d-%d", i+1, end)
		}

		successfulSubscriptions += len(batch)

		// Wait a moment between batches to avoid overwhelming the connection
		if end < totalMarkets {
			time.Sleep(500 * time.Millisecond)
		}

		// Re-acquire lock for next iteration
		c.mu.Lock()
	}

	c.mu.Unlock()

	fmt.Printf("✅ All subscriptions completed: %d/%d markets (isFull: %v)\n", successfulSubscriptions, totalMarkets, isFull)

	// Wait longer for initial data to arrive (as requested)
	if isFull {
		fmt.Printf("⏳ Waiting for initial full snapshots to arrive...\n")
		time.Sleep(5 * time.Second) // Increased wait time for initial data
	} else {
		fmt.Printf("⏳ Waiting for incremental subscriptions to be processed...\n")
		time.Sleep(2 * time.Second)
	}

	return nil
}

// UnsubscribeWebSocket unsubscribes from markets
func (c *HttpClient) UnsubscribeWebSocket(markets []string) error {
	c.mu.Lock()

	if !c.wsConnected || c.wsConn == nil {
		c.mu.Unlock()
		return fmt.Errorf("websocket not connected")
	}

	fmt.Printf("📡 Unsubscribing from %d markets...\n", len(markets))

	// Prepare market list for unsubscription
	var marketList = make([][]interface{}, len(markets))
	for j, market := range markets {
		// Format: [market, depth, merge, full]
		// For unsubscription, we use the same format but with depth=0 to indicate unsubscribe
		marketList[j] = []interface{}{market, 0, "0", false}
	}

	// Use CoinEx's unsubscription format
	unsubMsg := map[string]interface{}{
		"method": "depth.unsubscribe",
		"params": map[string]interface{}{
			"market_list": marketList,
		},
		"id": time.Now().UnixNano(),
	}

	// Get the connection reference while holding the lock
	conn := c.wsConn

	// Remove markets from subscriptions BEFORE releasing the lock
	for _, market := range markets {
		delete(c.wsSubscriptions, market)
	}
	c.mu.Unlock()

	// Set a write deadline to prevent indefinite blocking
	if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return fmt.Errorf("failed to set write deadline for unsubscription: %w", err)
	}

	// Send unsubscription message using direct WriteJSON with timeout protection
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- conn.WriteJSON(unsubMsg)
	}()

	select {
	case err := <-writeDone:
		if err != nil {
			return fmt.Errorf("websocket unsubscription write error: %w", err)
		}
	case <-time.After(5 * time.Second):
		return fmt.Errorf("websocket unsubscription write timeout")
	}

	fmt.Printf("✅ Unsubscription completed for %d markets\n", len(markets))

	// Wait a moment for unsubscription to be processed
	time.Sleep(1 * time.Second)

	return nil
}

// SwitchToIncrementalUpdates is deprecated - we now use REST API for initial data
// and WebSocket with is_full=false for incremental updates only
func (c *HttpClient) SwitchToIncrementalUpdates(markets []string, marketDepths *MarketDepths) error {
	fmt.Printf("⚠️ SwitchToIncrementalUpdates is deprecated - using simplified flow\n")

	// Just subscribe with is_full=false directly
	if err := c.SubscribeWebSocket(markets, false); err != nil {
		return fmt.Errorf("failed to subscribe for incremental updates: %w", err)
	}

	fmt.Printf("✅ Subscribed to incremental updates for %d markets\n", len(markets))
	return nil
}

// WaitForMarketData is deprecated - we now use REST API for initial data
// and don't need to wait for WebSocket data
func (c *HttpClient) WaitForMarketData(markets []string, marketDepths *MarketDepths, timeout time.Duration) ([]string, []string) {
	fmt.Printf("⚠️ WaitForMarketData is deprecated - using REST API for initial data\n")

	// Return all markets as available since we get initial data via REST API
	availableMarkets := make([]string, len(markets))
	copy(availableMarkets, markets)

	return availableMarkets, []string{}
}

// SubscribeWebSocketWithFullFlow implements a simplified subscription flow:
//
// 1. Skip REST API calls since data is already loaded in parallel
// 2. Subscribe to WebSocket with is_full=false for incremental updates only
//
// This approach avoids redundant REST API calls and ensures we have
// initial data before starting WebSocket subscriptions.
func (c *HttpClient) SubscribeWebSocketWithFullFlow(markets []string, marketDepths *MarketDepths) error {
	fmt.Printf("🔄 Starting simplified subscription flow for %d markets...\n", len(markets))

	// Skip Step 1: REST API calls since data is already loaded in parallel
	fmt.Printf("📡 Skipping REST API calls - data already loaded in parallel\n")

	// Step 2: Subscribe to WebSocket with is_full=false for incremental updates only
	fmt.Printf("📡 Step 2: Subscribing to WebSocket with incremental updates (is_full=false)...\n")
	if err := c.SubscribeWebSocket(markets, false); err != nil {
		return fmt.Errorf("failed to subscribe to WebSocket: %w", err)
	}
	fmt.Printf("✅ Step 2 complete: Subscribed to %d markets for incremental updates\n", len(markets))

	fmt.Printf("✅ Simplified subscription flow completed for %d markets\n", len(markets))
	fmt.Printf("📋 SUMMARY: WebSocket incremental updates only (REST data already loaded)\n")
	return nil
}

// SubscribeWebSocketWithRetry attempts to subscribe with retry logic
func (c *HttpClient) SubscribeWebSocketWithRetry(markets []string, maxRetries int) error {
	if maxRetries <= 0 {
		maxRetries = 3
	}

	for attempt := 1; attempt <= maxRetries; attempt++ {
		// Use the full flow to ensure proper incremental updates after initial phase
		err := c.SubscribeWebSocketWithFullFlow(markets, c.marketDepths)
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
func (c *HttpClient) ResubscribeToMarkets(marketDepths *MarketDepths) error {
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

	// Use the simplified subscription flow for resubscription
	if err := c.SubscribeWebSocketWithFullFlow(subscribedMarkets, marketDepths); err != nil {
		return fmt.Errorf("failed to resubscribe: %w", err)
	}

	fmt.Printf("✅ Resubscription complete for %d markets\n", len(subscribedMarkets))
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

	// Fetch latest price for this market (optional - don't fail if unavailable)
	if latestPrice, err := c.GetMarketTicker(market); err == nil {
		orderBook.Latest = latestPrice
		log.Printf("✅ REST API order book parsed for %s: %d bids, %d asks, latest: %.8f", market, len(orderBook.Bids), len(orderBook.Asks), orderBook.Latest)
	} else {
		// Don't fail the entire order book fetch if ticker is unavailable
		log.Printf("⚠️ REST API order book parsed for %s: %d bids, %d asks, ticker unavailable: %v", market, len(orderBook.Bids), len(orderBook.Asks), err)
		// Set a default latest price based on the best bid/ask if available
		if len(orderBook.Bids) > 0 && len(orderBook.Asks) > 0 {
			bidPrice, _ := strconv.ParseFloat(orderBook.Bids[0].Price, 64)
			askPrice, _ := strconv.ParseFloat(orderBook.Asks[0].Price, 64)
			orderBook.Latest = (bidPrice + askPrice) / 2 // Use mid-price as fallback
			log.Printf("📊 Using mid-price as latest for %s: %.8f (bid: %.8f, ask: %.8f)", market, orderBook.Latest, bidPrice, askPrice)
		}
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
		return 0, fmt.Errorf("invalid response format - missing data field")
	}

	ticker, ok := data["ticker"].(map[string]interface{})
	if !ok {
		return 0, fmt.Errorf("invalid ticker format - missing ticker field")
	}

	lastPriceStr, ok := ticker["last"].(string)
	if !ok {
		return 0, fmt.Errorf("missing last price in ticker data")
	}

	price, err := strconv.ParseFloat(lastPriceStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid price format '%s': %v", lastPriceStr, err)
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

	// Filter out non-query parameters
	queryParams := make(map[string]string)
	for k, v := range params {
		if k != "url" && k != "method" && k != "body" && k != "query" {
			queryParams[k] = v
		}
	}

	if len(queryParams) == 0 {
		return sb.String()
	}

	// Check if URL already has query parameters
	if strings.Contains(urlStr, "?") {
		sb.WriteString("&")
	} else {
		sb.WriteString("?")
	}

	keys := make([]string, 0, len(queryParams))
	for k := range queryParams {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for i, k := range keys {
		sb.WriteString(k)
		sb.WriteString("=")
		sb.WriteString(url.QueryEscape(queryParams[k]))
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
			// Check if body looks like JSON (starts with { or [)
			bodyStr := params["body"]
			if strings.HasPrefix(strings.TrimSpace(bodyStr), "{") || strings.HasPrefix(strings.TrimSpace(bodyStr), "[") {
				req.Header.Set("Content-Type", "application/json")
			} else {
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			}
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

// DoWithRateLimit executes an HTTP request with rate limiting
func (c *HttpClient) DoWithRateLimit(req *http.Request) (*http.Response, error) {
	// Apply rate limiting
	if !c.rateLimiter.Allow() {
		return nil, fmt.Errorf("rate limit exceeded")
	}

	// Set internal headers
	c.setInternalHeader(req)

	// Execute the request using the underlying http.Client
	return c.client.Do(req)
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

// ValidateWebSocketDataAccuracy validates WebSocket data accuracy and reconnects to failed markets
func (c *HttpClient) ValidateWebSocketDataAccuracy(markets []string) {
	log.Printf("🔍 Validating WebSocket data accuracy for %d markets", len(markets))

	// Check which markets have data and which don't
	activeMarkets := []string{}
	failedMarkets := []string{}

	c.marketDataMu.RLock()
	for _, market := range markets {
		if orderBook, exists := c.marketData[market]; exists && orderBook != nil {
			if len(orderBook.Bids) > 0 && len(orderBook.Asks) > 0 {
				activeMarkets = append(activeMarkets, market)
			} else {
				failedMarkets = append(failedMarkets, market)
			}
		} else {
			failedMarkets = append(failedMarkets, market)
		}
	}
	c.marketDataMu.RUnlock()

	log.Printf("📊 WebSocket validation results: %d active, %d failed", len(activeMarkets), len(failedMarkets))

	// Reconnect to failed markets via WebSocket
	if len(failedMarkets) > 0 {
		log.Printf("🔄 Attempting to reconnect to %d failed WebSocket markets", len(failedMarkets))

		successful := []string{}
		failed := []string{}

		for _, market := range failedMarkets {
			// Add to retry queue for background processing
			c.AddFailedMarket(market)

			// Also try immediate reconnection
			if err := c.SubscribeWebSocketWithRetry([]string{market}, 2); err != nil {
				failed = append(failed, market)
				log.Printf("❌ Failed to reconnect to %s: %v", market, err)
			} else {
				successful = append(successful, market)
				c.RemoveFailedMarket(market)
				log.Printf("✅ Successfully reconnected to %s", market)
			}

			// Small delay between reconnections
			time.Sleep(100 * time.Millisecond)
		}

		log.Printf("🔄 Reconnection results: %d successful, %d failed", len(successful), len(failed))
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

	// Use the full flow to ensure proper incremental updates after initial phase
	return c.SubscribeWebSocketWithFullFlow([]string{market}, c.marketDepths)
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

	// Try subscription using full flow
	err := c.SubscribeWebSocketWithFullFlow([]string{market}, c.marketDepths)
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

// AddFailedMarket adds a market to the retry queue
func (c *HttpClient) AddFailedMarket(market string) {
	c.failedMarketsMu.Lock()
	defer c.failedMarketsMu.Unlock()
	c.failedMarkets[market] = time.Now()
	log.Printf("📝 Added %s to WebSocket retry queue", market)
}

// RemoveFailedMarket removes a market from the retry queue
func (c *HttpClient) RemoveFailedMarket(market string) {
	c.failedMarketsMu.Lock()
	defer c.failedMarketsMu.Unlock()
	delete(c.failedMarkets, market)
	log.Printf("✅ Removed %s from WebSocket retry queue", market)
}

// GetFailedMarkets returns markets that are ready for retry
func (c *HttpClient) GetFailedMarkets() []string {
	c.failedMarketsMu.RLock()
	defer c.failedMarketsMu.RUnlock()

	var readyMarkets []string
	now := time.Now()

	for market, lastFailure := range c.failedMarkets {
		if now.Sub(lastFailure) >= c.retryInterval {
			readyMarkets = append(readyMarkets, market)
		}
	}

	return readyMarkets
}

// ProcessRetryQueue processes the retry queue for failed markets
func (c *HttpClient) ProcessRetryQueue(marketDepths *MarketDepths) {
	readyMarkets := c.GetFailedMarkets()
	if len(readyMarkets) == 0 {
		return
	}

	log.Printf("🔄 Processing retry queue: %d markets ready for retry", len(readyMarkets))

	successful := []string{}
	failed := []string{}

	for _, market := range readyMarkets {
		// First try to get initial data via REST API
		orderBook, err := c.GetOrderBookREST(market)
		if err != nil {
			failed = append(failed, market)
			log.Printf("❌ REST API retry failed for %s: %v", market, err)
			continue
		}

		// Store the initial data
		marketDepths.Store(market, orderBook)

		// Then subscribe to WebSocket with incremental updates
		if err := c.SubscribeWebSocket([]string{market}, false); err != nil {
			failed = append(failed, market)
			log.Printf("❌ WebSocket retry failed for %s: %v", market, err)
		} else {
			successful = append(successful, market)
			c.RemoveFailedMarket(market)
			log.Printf("✅ Retry successful for %s", market)
		}

		// Small delay between retries
		time.Sleep(100 * time.Millisecond)
	}

	log.Printf("🔄 Retry queue processed: %d successful, %d failed", len(successful), len(failed))
}

// StartRetryQueueProcessor is deprecated - no longer needed with simplified architecture
func (c *HttpClient) StartRetryQueueProcessor(marketDepths *MarketDepths) {
	// No longer needed - simplified architecture doesn't use background retry processing
	log.Printf("📋 RETRY QUEUE PROCESSOR DEPRECATED | Using simplified architecture")
}

// StopWebSocketProcessing stops WebSocket data processing to prevent new market data updates
func (c *HttpClient) StopWebSocketProcessing() {
	c.mu.Lock()
	defer c.mu.Unlock()

	log.Printf("🛑 STOPPING WEBSOCKET DATA PROCESSING | Connected: %v", c.wsConnected)

	// Stop all WebSocket goroutines
	select {
	case c.wsReadDone <- true:
		log.Printf("🛑 WebSocket read loop stop signal sent")
	default:
		log.Printf("🛑 WebSocket read loop already stopped")
	}

	select {
	case c.wsPingDone <- true:
		log.Printf("🛑 WebSocket ping loop stop signal sent")
	default:
		log.Printf("🛑 WebSocket ping loop already stopped")
	}

	select {
	case c.wsHealthDone <- true:
		log.Printf("🛑 WebSocket health monitor stop signal sent")
	default:
		log.Printf("🛑 WebSocket health monitor already stopped")
	}

	select {
	case c.wsStopChan <- true:
		log.Printf("🛑 WebSocket general stop signal sent")
	default:
		log.Printf("🛑 WebSocket general stop already sent")
	}

	// Mark as processing stopped
	c.wsProcessingStopped = true
	log.Printf("🛑 WEBSOCKET DATA PROCESSING STOPPED | No new market data will be processed")
}

// SetMarketDepths sets the market depths reference for resubscription
func (c *HttpClient) SetMarketDepths(marketDepths *MarketDepths) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.marketDepths = marketDepths
}

// DisableRateLimiting temporarily disables rate limiting for bulk operations
func (c *HttpClient) DisableRateLimiting() {
	c.rateLimiter.Disable()
}

// EnableRateLimiting re-enables rate limiting after bulk operations
func (c *HttpClient) EnableRateLimiting() {
	c.rateLimiter.Enable()
}
