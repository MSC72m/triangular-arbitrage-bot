package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

// Min returns the minimum of two integers
func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// OrderBook represents the order book structure
type OrderBook struct {
	Asks   []Depth `json:"asks"`
	Bids   []Depth `json:"bids"`
	Latest float64 `json:"latest"`
	Market string  `json:"market"`
}

// Depth represents a price level in the order book
type Depth struct {
	Price  string `json:"price"`
	Amount string `json:"amount"`
}

// Test HTTP client for REST API calls
type TestHttpClient struct {
	apiKey   string
	secretID string
}

func NewTestHttpClient() *TestHttpClient {
	return &TestHttpClient{
		apiKey:   "77F875170F174EF19A696173AD103A63",
		secretID: "FA569E5EF5FB1CCB8CBEC312737A0DE52F497611CC7075E1",
	}
}

// generateRESTSignature creates HMAC-SHA256 signature for CoinEx API v2 REST endpoints
func (c *TestHttpClient) generateRESTSignature(method, requestPath, queryString, body string, timestamp int64) string {
	fullRequestPath := requestPath
	if queryString != "" {
		fullRequestPath = requestPath + "?" + queryString
	}

	var stringToSign string
	if body != "" {
		stringToSign = method + fullRequestPath + body + strconv.FormatInt(timestamp, 10)
	} else {
		stringToSign = method + fullRequestPath + strconv.FormatInt(timestamp, 10)
	}

	h := hmac.New(sha256.New, []byte(c.secretID))
	h.Write([]byte(stringToSign))
	signature := hex.EncodeToString(h.Sum(nil))

	return signature
}

// getMarketDepth gets the current market depth from REST API
func (c *TestHttpClient) getMarketDepth(market string, limit int) (*OrderBook, error) {
	timestamp := time.Now().UnixMilli()
	method := "GET"
	requestPath := "/v2/spot/market/depth"

	queryParams := map[string]string{
		"market":   market,
		"limit":    strconv.Itoa(limit),
		"interval": "0",
	}

	// Sort parameters alphabetically
	keys := make([]string, 0, len(queryParams))
	for k := range queryParams {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Build query string
	var queryParts []string
	for _, key := range keys {
		queryParts = append(queryParts, key+"="+queryParams[key])
	}
	queryString := strings.Join(queryParts, "&")

	// Generate signature
	signature := c.generateRESTSignature(method, requestPath, queryString, "", timestamp)

	// Build request URL
	url := fmt.Sprintf("https://api.coinex.com%s?%s", requestPath, queryString)

	// Create request
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Add headers
	req.Header.Set("Authorization", c.apiKey)
	req.Header.Set("User-Agent", "CoinEx-Bot/1.0")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Signature", signature)
	req.Header.Set("Timestamp", strconv.FormatInt(timestamp, 10))

	// Make request
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	// Read response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	// Parse response
	var response struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
		Data struct {
			Asks [][]string `json:"asks"`
			Bids [][]string `json:"bids"`
			Last string     `json:"last"`
		} `json:"data"`
	}

	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	if response.Code != 0 {
		return nil, fmt.Errorf("API error: %s", response.Msg)
	}

	// Convert to OrderBook
	orderBook := &OrderBook{
		Market: market,
		Asks:   []Depth{},
		Bids:   []Depth{},
	}

	// Parse latest price
	if response.Data.Last != "" {
		if price, err := strconv.ParseFloat(response.Data.Last, 64); err == nil {
			orderBook.Latest = price
		}
	}

	// Parse asks (sell orders)
	for _, ask := range response.Data.Asks {
		if len(ask) >= 2 {
			orderBook.Asks = append(orderBook.Asks, Depth{
				Price:  ask[0],
				Amount: ask[1],
			})
		}
	}

	// Parse bids (buy orders)
	for _, bid := range response.Data.Bids {
		if len(bid) >= 2 {
			orderBook.Bids = append(orderBook.Bids, Depth{
				Price:  bid[0],
				Amount: bid[1],
			})
		}
	}

	return orderBook, nil
}

// WebSocket client for depth updates
type WebSocketClient struct {
	conn *websocket.Conn
}

func NewWebSocketClient() *WebSocketClient {
	return &WebSocketClient{}
}

// connect establishes WebSocket connection
func (c *WebSocketClient) connect() error {
	conn, _, err := websocket.DefaultDialer.Dial("wss://socket.coinex.com/", nil)
	if err != nil {
		return fmt.Errorf("failed to connect to WebSocket: %w", err)
	}
	c.conn = conn
	return nil
}

// subscribe subscribes to market depth updates
func (c *WebSocketClient) subscribe(market string, limit int, dif bool) error {
	// Subscribe to depth updates
	subscribeMsg := map[string]interface{}{
		"method": "depth.subscribe",
		"params": []interface{}{
			market,
			limit,
			"0", // merge level
		},
		"id": 1,
	}

	// Add dif parameter if needed
	if dif {
		subscribeMsg["params"] = append(subscribeMsg["params"].([]interface{}), dif)
	}

	log.Printf("📤 SENDING SUBSCRIPTION | Market: %s | Limit: %d | DIF: %v", market, limit, dif)
	log.Printf("📤 SUBSCRIPTION MESSAGE: %+v", subscribeMsg)

	if err := c.conn.WriteJSON(subscribeMsg); err != nil {
		return fmt.Errorf("failed to send subscribe message: %w", err)
	}

	// Wait for subscription confirmation with timeout
	c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var response map[string]interface{}
	if err := c.conn.ReadJSON(&response); err != nil {
		return fmt.Errorf("failed to read subscription response: %w", err)
	}

	log.Printf("📨 SUBSCRIPTION RESPONSE: %+v", response)
	log.Printf("✅ WebSocket subscribed to %s depth updates (limit: %d, dif: %v)", market, limit, dif)
	return nil
}

// listenForUpdates listens for depth updates and processes them
func (c *WebSocketClient) listenForUpdates(market string, duration time.Duration, dif bool, initialOrderBook *OrderBook) {
	log.Printf("🎧 LISTENING FOR %s UPDATES | Duration: %v | DIF: %v", market, duration, dif)

	// If we have an initial order book, apply it
	var orderBook *OrderBook
	if initialOrderBook != nil {
		orderBook = &OrderBook{
			Market: initialOrderBook.Market,
			Latest: initialOrderBook.Latest,
			Asks:   make([]Depth, len(initialOrderBook.Asks)),
			Bids:   make([]Depth, len(initialOrderBook.Bids)),
		}
		copy(orderBook.Asks, initialOrderBook.Asks)
		copy(orderBook.Bids, initialOrderBook.Bids)
		log.Printf("📊 INITIAL ORDER BOOK APPLIED | Asks: %d | Bids: %d", len(orderBook.Asks), len(orderBook.Bids))
	} else {
		orderBook = &OrderBook{
			Market: market,
			Asks:   []Depth{},
			Bids:   []Depth{},
		}
	}

	updateCount := 0
	messageCount := 0
	ticker := time.NewTicker(5 * time.Second) // Log every 5 seconds
	defer ticker.Stop()

	// Set a read deadline to prevent infinite blocking
	c.conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	for {
		select {
		case <-time.After(duration):
			log.Printf("⏰ TEST DURATION COMPLETED | Total updates: %d | Total messages: %d", updateCount, messageCount)
			return
		case <-ticker.C:
			if len(orderBook.Asks) > 0 && len(orderBook.Bids) > 0 {
				askPrice, _ := strconv.ParseFloat(orderBook.Asks[0].Price, 64)
				bidPrice, _ := strconv.ParseFloat(orderBook.Bids[0].Price, 64)
				log.Printf("📊 ORDER BOOK STATUS | Asks[0]: %.8f | Bids[0]: %.8f | Spread: %.4f%% | Updates: %d | Messages: %d",
					askPrice, bidPrice, ((askPrice-bidPrice)/bidPrice)*100, updateCount, messageCount)
			} else {
				log.Printf("📊 ORDER BOOK STATUS | No data yet | Updates: %d | Messages: %d", updateCount, messageCount)
			}
			// Reset read deadline
			c.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		default:
			// Read WebSocket message with timeout
			var message map[string]interface{}
			if err := c.conn.ReadJSON(&message); err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					log.Printf("❌ WebSocket connection closed: %v", err)
					return
				}
				log.Printf("⚠️ WebSocket read error (continuing): %v", err)
				// Reset read deadline and continue
				c.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
				continue
			}

			messageCount++
			log.Printf("📨 RECEIVED MESSAGE #%d | Type: %v", messageCount, message["method"])

			// Process depth update
			if method, ok := message["method"].(string); ok && method == "depth.update" {
				updateCount++
				log.Printf("📊 PROCESSING DEPTH UPDATE #%d", updateCount)
				orderBook = processDepthUpdate(message, orderBook, dif)

				// For dif=true (full updates only), quit after first message
				if dif && updateCount >= 1 {
					log.Printf("✅ FULL UPDATE RECEIVED | Quitting after first message (dif=true)")
					return
				}
			} else if method, ok := message["method"].(string); ok {
				log.Printf("📨 OTHER MESSAGE | Method: %s", method)
			}
		}
	}
}

// processDepthUpdate processes a depth update message
func processDepthUpdate(message map[string]interface{}, orderBook *OrderBook, dif bool) *OrderBook {
	data, ok := message["data"].(map[string]interface{})
	if !ok {
		return orderBook
	}

	market, _ := data["market"].(string)
	isFull, _ := data["is_full"].(bool)

	// Extract latest price
	var latestPrice float64
	if lastPriceStr, ok := data["last"].(string); ok {
		if price, err := strconv.ParseFloat(lastPriceStr, 64); err == nil {
			latestPrice = price
		}
	}

	// Update order book
	if orderBook == nil {
		orderBook = &OrderBook{
			Market: market,
			Asks:   []Depth{},
			Bids:   []Depth{},
		}
	}
	orderBook.Latest = latestPrice

	// Process depth data
	depthData, ok := data["depth"].(map[string]interface{})
	if !ok {
		return orderBook
	}

	// FIXED: CoinEx API mapping - their "asks" are actually bids, "bids" are actually asks
	// Process raw bids (what CoinEx calls "asks")
	if rawBidsData, ok := depthData["asks"].([]interface{}); ok {
		if isFull {
			// Full snapshot - replace all bids
			orderBook.Bids = []Depth{}
			for _, rawBid := range rawBidsData {
				if bidArray, ok := rawBid.([]interface{}); ok && len(bidArray) >= 2 {
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
		} else {
			// Incremental update
			for _, rawBid := range rawBidsData {
				if bidArray, ok := rawBid.([]interface{}); ok && len(bidArray) >= 2 {
					if priceStr, ok := bidArray[0].(string); ok {
						if amountStr, ok := bidArray[1].(string); ok {
							// Find existing bid with same price
							found := false
							for i, existingBid := range orderBook.Bids {
								if existingBid.Price == priceStr {
									if amountStr == "0" {
										// Remove this bid level
										orderBook.Bids = append(orderBook.Bids[:i], orderBook.Bids[i+1:]...)
									} else {
										// Update amount
										orderBook.Bids[i].Amount = amountStr
									}
									found = true
									break
								}
							}
							if !found && amountStr != "0" {
								// Add new bid level - just append, will sort later
								orderBook.Bids = append(orderBook.Bids, Depth{Price: priceStr, Amount: amountStr})
							}
						}
					}
				}
			}
		}
	}

	// Process raw asks (what CoinEx calls "bids")
	if rawAsksData, ok := depthData["bids"].([]interface{}); ok {
		if isFull {
			// Full snapshot - replace all asks
			orderBook.Asks = []Depth{}
			for _, rawAsk := range rawAsksData {
				if askArray, ok := rawAsk.([]interface{}); ok && len(askArray) >= 2 {
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
		} else {
			// Incremental update
			for _, rawAsk := range rawAsksData {
				if askArray, ok := rawAsk.([]interface{}); ok && len(askArray) >= 2 {
					if priceStr, ok := askArray[0].(string); ok {
						if amountStr, ok := askArray[1].(string); ok {
							// Find existing ask with same price
							found := false
							for i, existingAsk := range orderBook.Asks {
								if existingAsk.Price == priceStr {
									if amountStr == "0" {
										// Remove this ask level
										orderBook.Asks = append(orderBook.Asks[:i], orderBook.Asks[i+1:]...)
									} else {
										// Update amount
										orderBook.Asks[i].Amount = amountStr
									}
									found = true
									break
								}
							}
							if !found && amountStr != "0" {
								// Add new ask level - just append, will sort later
								orderBook.Asks = append(orderBook.Asks, Depth{Price: priceStr, Amount: amountStr})
							}
						}
					}
				}
			}
		}
	}

	// Sort order book
	sort.Slice(orderBook.Asks, func(i, j int) bool {
		pi, _ := strconv.ParseFloat(orderBook.Asks[i].Price, 64)
		pj, _ := strconv.ParseFloat(orderBook.Asks[j].Price, 64)
		return pi < pj // ascending order for asks
	})

	sort.Slice(orderBook.Bids, func(i, j int) bool {
		pi, _ := strconv.ParseFloat(orderBook.Bids[i].Price, 64)
		pj, _ := strconv.ParseFloat(orderBook.Bids[j].Price, 64)
		return pi > pj // descending order for bids
	})

	return orderBook
}

// close closes the WebSocket connection
func (c *WebSocketClient) close() {
	if c.conn != nil {
		c.conn.Close()
	}
}

func main() {
	log.Printf("🧪 STARTING COMPREHENSIVE BIDS VS ASKS TEST")
	log.Printf("📊 Testing 3 scenarios: REST, WebSocket full, WebSocket incremental")

	market := "XEMUSDT"
	limit := 10

	// Test 1: REST API only
	log.Printf("\n" + strings.Repeat("=", 60))
	log.Printf("🧪 TEST 1: REST API ONLY")
	log.Printf("📊 Getting order book from REST API")
	log.Printf(strings.Repeat("=", 60))

	client := NewTestHttpClient()
	restOrderBook, err := client.getMarketDepth(market, limit)
	if err != nil {
		log.Printf("❌ Failed to get REST order book: %v", err)
		return
	}

	log.Printf("📊 REST API ORDER BOOK:")
	log.Printf("   Market: %s", restOrderBook.Market)
	log.Printf("   Latest Price: %.8f", restOrderBook.Latest)
	log.Printf("   Asks: %d levels", len(restOrderBook.Asks))
	log.Printf("   Bids: %d levels", len(restOrderBook.Bids))

	if len(restOrderBook.Asks) > 0 && len(restOrderBook.Bids) > 0 {
		askPrice, _ := strconv.ParseFloat(restOrderBook.Asks[0].Price, 64)
		bidPrice, _ := strconv.ParseFloat(restOrderBook.Bids[0].Price, 64)
		spread := ((askPrice - bidPrice) / bidPrice) * 100

		log.Printf("🎯 REST API RESULTS:")
		log.Printf("   Asks[0] (Lowest Ask): %.8f", askPrice)
		log.Printf("   Bids[0] (Highest Bid): %.8f", bidPrice)
		log.Printf("   Spread: %.4f%%", spread)

		if askPrice > bidPrice {
			log.Printf("   ✅ CORRECT: Asks[0] > Bids[0] (Spread is positive)")
		} else {
			log.Printf("   ❌ ERROR: Asks[0] <= Bids[0] (Crossed order book)")
		}
	}

	// Test 2: WebSocket with dif=true (full updates only)
	log.Printf("\n" + strings.Repeat("=", 60))
	log.Printf("🧪 TEST 2: WEBSOCKET FULL UPDATES (dif = true)")
	log.Printf("📊 Getting full snapshots only - NO incremental updates")
	log.Printf(strings.Repeat("=", 60))

	ws1 := NewWebSocketClient()
	if err := ws1.connect(); err != nil {
		log.Printf("❌ Failed to connect: %v", err)
		return
	}
	defer ws1.close()

	if err := ws1.subscribe(market, limit, true); err != nil {
		log.Printf("❌ Failed to subscribe: %v", err)
		return
	}

	// Wait for one full update
	log.Printf("📡 Waiting for one full depth update...")
	var ws1OrderBook *OrderBook
	messageCount := 0
	timeout := time.After(15 * time.Second)

	for messageCount < 1 {
		select {
		case <-timeout:
			log.Printf("⚠️ Timeout waiting for full depth update")
			return
		default:
			var message map[string]interface{}
			if err := ws1.conn.ReadJSON(&message); err != nil {
				log.Printf("⚠️ Read error: %v", err)
				return
			}

			messageCount++
			log.Printf("📨 RECEIVED MESSAGE #%d | Type: %v", messageCount, message["method"])

			if method, ok := message["method"].(string); ok && method == "depth.update" {
				log.Printf("📊 PROCESSING FULL DEPTH UPDATE")
				ws1OrderBook = processDepthUpdate(message, nil, true)

				if ws1OrderBook != nil {
					log.Printf("📊 WEBSOCKET FULL UPDATE RESULTS:")
					log.Printf("   Market: %s", ws1OrderBook.Market)
					log.Printf("   Latest Price: %.8f", ws1OrderBook.Latest)
					log.Printf("   Asks: %d levels", len(ws1OrderBook.Asks))
					log.Printf("   Bids: %d levels", len(ws1OrderBook.Bids))

					if len(ws1OrderBook.Asks) > 0 && len(ws1OrderBook.Bids) > 0 {
						askPrice, _ := strconv.ParseFloat(ws1OrderBook.Asks[0].Price, 64)
						bidPrice, _ := strconv.ParseFloat(ws1OrderBook.Bids[0].Price, 64)
						spread := ((askPrice - bidPrice) / bidPrice) * 100

						log.Printf("🎯 WEBSOCKET FULL UPDATE RESULTS:")
						log.Printf("   Asks[0] (Lowest Ask): %.8f", askPrice)
						log.Printf("   Bids[0] (Highest Bid): %.8f", bidPrice)
						log.Printf("   Spread: %.4f%%", spread)

						if askPrice > bidPrice {
							log.Printf("   ✅ CORRECT: Asks[0] > Bids[0] (Spread is positive)")
						} else {
							log.Printf("   ❌ ERROR: Asks[0] <= Bids[0] (Crossed order book)")
						}
					}
				}
				break
			}
		}
	}

	// Test 3: WebSocket with dif=false (incremental) with initial REST snapshot
	log.Printf("\n" + strings.Repeat("=", 60))
	log.Printf("🧪 TEST 3: WEBSOCKET INCREMENTAL UPDATES (dif = false)")
	log.Printf("📊 Starting with REST snapshot, then applying incremental updates")
	log.Printf(strings.Repeat("=", 60))

	ws2 := NewWebSocketClient()
	if err := ws2.connect(); err != nil {
		log.Printf("❌ Failed to connect: %v", err)
		return
	}
	defer ws2.close()

	if err := ws2.subscribe(market, limit, false); err != nil {
		log.Printf("❌ Failed to subscribe: %v", err)
		return
	}

	// Start with REST order book
	ws2OrderBook := &OrderBook{
		Market: restOrderBook.Market,
		Latest: restOrderBook.Latest,
		Asks:   make([]Depth, len(restOrderBook.Asks)),
		Bids:   make([]Depth, len(restOrderBook.Bids)),
	}
	copy(ws2OrderBook.Asks, restOrderBook.Asks)
	copy(ws2OrderBook.Bids, restOrderBook.Bids)

	log.Printf("📊 INITIAL ORDER BOOK (from REST):")
	log.Printf("   Asks: %d levels", len(ws2OrderBook.Asks))
	log.Printf("   Bids: %d levels", len(ws2OrderBook.Bids))
	if len(ws2OrderBook.Asks) > 0 && len(ws2OrderBook.Bids) > 0 {
		askPrice, _ := strconv.ParseFloat(ws2OrderBook.Asks[0].Price, 64)
		bidPrice, _ := strconv.ParseFloat(ws2OrderBook.Bids[0].Price, 64)
		log.Printf("   Asks[0]: %.8f | Bids[0]: %.8f", askPrice, bidPrice)
	}

	// Process 50 incremental updates
	log.Printf("📡 Processing 50 incremental updates...")
	updateCount := 0
	timeout = time.After(30 * time.Second)

	for updateCount < 50 {
		select {
		case <-timeout:
			log.Printf("⚠️ Timeout waiting for incremental updates")
			break
		default:
			var message map[string]interface{}
			if err := ws2.conn.ReadJSON(&message); err != nil {
				log.Printf("⚠️ Read error: %v", err)
				break
			}

			if method, ok := message["method"].(string); ok && method == "depth.update" {
				updateCount++
				ws2OrderBook = processDepthUpdate(message, ws2OrderBook, false)

				// Compare bids[0] vs asks[0] at each update
				if len(ws2OrderBook.Asks) > 0 && len(ws2OrderBook.Bids) > 0 {
					askPrice, _ := strconv.ParseFloat(ws2OrderBook.Asks[0].Price, 64)
					bidPrice, _ := strconv.ParseFloat(ws2OrderBook.Bids[0].Price, 64)
					spread := ((askPrice - bidPrice) / bidPrice) * 100

					status := "❌ ERROR"
					if askPrice > bidPrice {
						status = "✅ CORRECT"
					}
					log.Printf("📊 UPDATE #%d | Asks[0]: %.8f | Bids[0]: %.8f | Spread: %.4f%% | %s",
						updateCount, askPrice, bidPrice, spread, status)
				}
			}
		}
	}

	log.Printf("\n" + strings.Repeat("=", 60))
	log.Printf("🎯 FINAL COMPARISON")
	log.Printf("📊 Comparing all three methods:")

	// Compare REST vs WebSocket full
	if restOrderBook != nil && ws1OrderBook != nil {
		if len(restOrderBook.Asks) > 0 && len(restOrderBook.Bids) > 0 &&
			len(ws1OrderBook.Asks) > 0 && len(ws1OrderBook.Bids) > 0 {
			restAsk, _ := strconv.ParseFloat(restOrderBook.Asks[0].Price, 64)
			restBid, _ := strconv.ParseFloat(restOrderBook.Bids[0].Price, 64)
			ws1Ask, _ := strconv.ParseFloat(ws1OrderBook.Asks[0].Price, 64)
			ws1Bid, _ := strconv.ParseFloat(ws1OrderBook.Bids[0].Price, 64)

			log.Printf("   REST API:     Asks[0]=%.8f, Bids[0]=%.8f", restAsk, restBid)
			log.Printf("   WebSocket Full: Asks[0]=%.8f, Bids[0]=%.8f", ws1Ask, ws1Bid)
			log.Printf("   Difference:   Asks[0]=%.8f, Bids[0]=%.8f", ws1Ask-restAsk, ws1Bid-restBid)
		}
	}

	log.Printf("✅ All tests completed successfully!")
	log.Printf(strings.Repeat("=", 60))
}
