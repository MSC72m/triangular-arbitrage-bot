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

	// WebSocket fields
	wsConn          *websocket.Conn
	wsSubscriptions map[string]bool
	wsDataFeed      chan []byte
	wsUrl           string
	wsConnected     bool
}

func newHttpClient() *HttpClient {
	return &HttpClient{
		headers:         make(map[string]string),
		wsSubscriptions: make(map[string]bool),
		wsDataFeed:      make(chan []byte, 1000), // Buffered channel
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

		// Log first few messages for debugging
		if messageCount <= 10 {
			fmt.Printf("📨 WebSocket Message #%d: %s\n", messageCount, string(message))
		} else if messageCount%100 == 0 {
			// Log every 100th message after first 10
			fmt.Printf("📨 WebSocket Message #%d received\n", messageCount)
		}

		// Send to data feed channel (non-blocking)
		select {
		case c.wsDataFeed <- message:
		default:
			fmt.Printf("⚠️  WebSocket data feed channel full, dropping message\n")
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
	defer c.mu.Unlock()

	if !c.wsConnected || c.wsConn == nil {
		return fmt.Errorf("websocket not connected")
	}

	if c.wsSubscriptions[market] {
		return nil // Already subscribed
	}

	// CoinEx WebSocket API format: [market, limit, interval, diff]
	subMsg := map[string]interface{}{
		"method": "depth.subscribe",
		"params": []interface{}{market, 5, "0", true}, // market, limit, interval, diff
		"id":     time.Now().UnixNano(),
	}

	// Log the exact message we're sending
	if msgBytes, err := json.Marshal(subMsg); err == nil {
		fmt.Printf("🔗 Subscribing to %s with message: %s\n", market, string(msgBytes))
	}

	if err := c.wsConn.WriteJSON(subMsg); err != nil {
		return fmt.Errorf("websocket write error for %s: %w", market, err)
	}

	fmt.Printf("✅ Subscription sent for %s\n", market)
	c.wsSubscriptions[market] = true

	// Add small delay to avoid overwhelming the server
	time.Sleep(2 * time.Millisecond)

	return nil
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
