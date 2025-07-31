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
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// authenticateWebSocket authenticates the WebSocket connection with CoinEx using MD5 signature as per CoinEx docs.
// Returns true if authentication is successful, false otherwise.
func authenticateWebSocket(conn *websocket.Conn, apiKey, secretKey string) (bool, error) {
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

	if err := conn.WriteJSON(authMsg); err != nil {
		return false, fmt.Errorf("failed to send authentication request: %w", err)
	}

	// Wait for authentication response (CoinEx will reply with "server.sign" result)
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	for {
		messageType, message, err := conn.ReadMessage()
		if err != nil {
			return false, fmt.Errorf("failed to read authentication response: %w", err)
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
				return false, fmt.Errorf("failed to create gzip reader for auth response: %w", err)
			}
			defer gzipReader.Close()
			data, err = io.ReadAll(gzipReader)
			if err != nil {
				return false, fmt.Errorf("failed to decompress auth response: %w", err)
			}
		} else {
			data = message
		}

		if err := json.Unmarshal(data, &wsResponse); err != nil {
			return false, fmt.Errorf("failed to parse auth response: %w", err)
		}

		// Look for "result" in response to "server.sign"
		if method, ok := wsResponse["method"].(string); ok && method == "server.sign" {
			if _, ok := wsResponse["error"]; ok && wsResponse["error"] != nil {
				fmt.Printf("❌ WebSocket authentication error: %+v\n", wsResponse["error"])
				return false, fmt.Errorf("authentication error: %+v", wsResponse["error"])
			}
			fmt.Printf("✅ WebSocket authentication successful: %+v\n", wsResponse)
			return true, nil
		}
		// Some responses may not have "method", but have "result" and "id"
		if _, ok := wsResponse["result"]; ok && wsResponse["id"] == authMsg["id"] {
			fmt.Printf("✅ WebSocket authentication successful (by id): %+v\n", wsResponse)
			return true, nil
		}
		// Otherwise, keep reading until we get the auth response
	}
}

func testWebSocket() {
	fmt.Println("🧪 Testing WebSocket connection for all markets at once...")

	// Get API credentials from environment variables
	apiKey := os.Getenv("COINEX_API_KEY")
	secretKey := os.Getenv("COINEX_API_SECRET")
	if apiKey == "" || secretKey == "" {
		log.Fatalf("❌ Please set COINEX_API_KEY and COINEX_API_SECRET environment variables for authentication test.")
	}

	// Create dialer with compression support
	dialer := websocket.Dialer{
		EnableCompression: true, // Enable deflate compression as required by CoinEx
		HandshakeTimeout:  45 * time.Second,
	}

	// Set up headers for WebSocket connection
	wsHeaders := make(http.Header)
	wsHeaders.Set("User-Agent", "triangular-arbitrage-bot/2.0")

	fmt.Printf("🔗 WebSocket headers: %+v\n", wsHeaders)

	// Connect to CoinEx WebSocket
	url := "wss://socket.coinex.com/v2/spot"
	conn, response, err := dialer.Dial(url, wsHeaders)
	if err != nil {
		log.Printf("❌ Failed to connect to WebSocket: %v", err)
		if response != nil {
			log.Printf("❌ Response: %+v", response)
		}
		return
	}
	defer conn.Close()

	fmt.Printf("✅ Connected to WebSocket successfully\n")
	fmt.Printf("🔗 WebSocket URL: %s\n", url)

	// Set up ping/pong handlers
	conn.SetPongHandler(func(string) error {
		fmt.Printf("🏓 Pong received\n")
		return nil
	})

	conn.SetCloseHandler(func(code int, text string) error {
		fmt.Printf("🔌 WebSocket close: code=%d, text=%s\n", code, text)
		return nil
	})

	// Wait a moment for connection to stabilize
	time.Sleep(2 * time.Second)

	// Authenticate before any subscription
	ok, err := authenticateWebSocket(conn, apiKey, secretKey)
	if err != nil || !ok {
		log.Printf("❌ WebSocket authentication failed: %v", err)
		return
	}
	fmt.Printf("ℹ️  Authenticated WebSocket connection (private access enabled)\n")

	// Try a simpler subscription first - just one market
	fmt.Printf("📡 Testing with single market subscription first...\n")
	singleMarketMsg := map[string]interface{}{
		"method": "depth.subscribe",
		"params": map[string]interface{}{
			"market_list": [][]interface{}{
				{"BTCUSDT", 10, "0", true},
			},
		},
		"id": time.Now().UnixNano(),
	}

	fmt.Printf("📡 Single market subscription message: %+v\n", singleMarketMsg)

	if err := conn.WriteJSON(singleMarketMsg); err != nil {
		log.Printf("❌ Failed to send single market subscription: %v", err)
		return
	}

	fmt.Printf("✅ Single market subscription sent successfully\n")

	// Track price updates for each market
	priceCounts := make(map[string]int)
	var priceMutex sync.Mutex
	maxPricesPerMarket := 3 // Reduced for faster testing

	// Create a map to track which markets we're subscribed to
	subscribedMarkets := make(map[string]bool)
	subscribedMarkets["BTCUSDT"] = true

	// Listen for messages and extract price data
	messageCount := 0
	totalPriceUpdates := 0

	for {
		// Set read deadline
		if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
			log.Printf("❌ Failed to set read deadline: %v", err)
			break
		}

		// Read message
		messageType, message, err := conn.ReadMessage()
		if err != nil {
			log.Printf("❌ Failed to read message: %v", err)
			break
		}

		messageCount++

		// Handle different message types
		switch messageType {
		case websocket.TextMessage:
			fmt.Printf("📨 Text message: %s\n", string(message))

			// Parse message
			var wsResponse map[string]interface{}
			if err := json.Unmarshal(message, &wsResponse); err != nil {
				log.Printf("❌ Failed to parse message: %v", err)
				continue
			}

			// Check if it's a depth update
			if method, ok := wsResponse["method"].(string); ok && method == "depth.update" {
				if data, ok := wsResponse["data"].(map[string]interface{}); ok {
					if marketName, ok := data["market"].(string); ok {
						if subscribedMarkets[marketName] {
							if depth, ok := data["depth"].(map[string]interface{}); ok {
								if lastPrice, ok := depth["last"].(string); ok {
									priceMutex.Lock()
									priceCounts[marketName]++
									currentCount := priceCounts[marketName]
									priceMutex.Unlock()

									currentTime := time.Now().Format("15:04:05.000")
									fmt.Printf("💰 [%s] %s Price: %s (Message #%d)\n", currentTime, marketName, lastPrice, currentCount)
									totalPriceUpdates++
								}
							}
						}
					}
				}
			}

			// Also check for ticker updates
			if method, ok := wsResponse["method"].(string); ok && method == "ticker.update" {
				if data, ok := wsResponse["data"].(map[string]interface{}); ok {
					if marketName, ok := data["market"].(string); ok {
						if subscribedMarkets[marketName] {
							if ticker, ok := data["ticker"].(map[string]interface{}); ok {
								if lastPrice, ok := ticker["last"].(string); ok {
									priceMutex.Lock()
									priceCounts[marketName]++
									currentCount := priceCounts[marketName]
									priceMutex.Unlock()

									currentTime := time.Now().Format("15:04:05.000")
									fmt.Printf("💰 [%s] %s Price: %s (Message #%d)\n", currentTime, marketName, lastPrice, currentCount)
									totalPriceUpdates++
								}
							}
						}
					}
				}
			}

			// Log other message types for debugging
			if method, ok := wsResponse["method"].(string); ok {
				if method != "depth.update" && method != "ticker.update" {
					fmt.Printf("📋 Other message type: %s\n", method)
				}
			}

		case websocket.BinaryMessage:
			fmt.Printf("📦 Binary message received (likely compressed data)\n")
			fmt.Printf("📦 Binary message length: %d bytes\n", len(message))

			// Try to decompress the binary data
			reader := bytes.NewReader(message)
			gzipReader, err := gzip.NewReader(reader)
			if err != nil {
				log.Printf("❌ Failed to create gzip reader: %v", err)
				continue
			}
			defer gzipReader.Close()

			// Read the decompressed data
			decompressedData, err := io.ReadAll(gzipReader)
			if err != nil {
				log.Printf("❌ Failed to decompress data: %v", err)
				continue
			}

			fmt.Printf("📦 Decompressed data: %s\n", string(decompressedData))

			// Parse the decompressed JSON message
			var wsResponse map[string]interface{}
			if err := json.Unmarshal(decompressedData, &wsResponse); err != nil {
				log.Printf("❌ Failed to parse decompressed message: %v", err)
				continue
			}

			// Handle the decompressed message the same way as text messages
			// Check if it's a depth update
			if method, ok := wsResponse["method"].(string); ok && method == "depth.update" {
				if data, ok := wsResponse["data"].(map[string]interface{}); ok {
					if marketName, ok := data["market"].(string); ok {
						if subscribedMarkets[marketName] {
							if depth, ok := data["depth"].(map[string]interface{}); ok {
								if lastPrice, ok := depth["last"].(string); ok {
									priceMutex.Lock()
									priceCounts[marketName]++
									currentCount := priceCounts[marketName]
									priceMutex.Unlock()

									currentTime := time.Now().Format("15:04:05.000")
									fmt.Printf("💰 [%s] %s Price: %s (Message #%d)\n", currentTime, marketName, lastPrice, currentCount)
									totalPriceUpdates++
								}
							}
						}
					}
				}
			}

			// Also check for ticker updates
			if method, ok := wsResponse["method"].(string); ok && method == "ticker.update" {
				if data, ok := wsResponse["data"].(map[string]interface{}); ok {
					if marketName, ok := data["market"].(string); ok {
						if subscribedMarkets[marketName] {
							if ticker, ok := data["ticker"].(map[string]interface{}); ok {
								if lastPrice, ok := ticker["last"].(string); ok {
									priceMutex.Lock()
									priceCounts[marketName]++
									currentCount := priceCounts[marketName]
									priceMutex.Unlock()

									currentTime := time.Now().Format("15:04:05.000")
									fmt.Printf("💰 [%s] %s Price: %s (Message #%d)\n", currentTime, marketName, lastPrice, currentCount)
									totalPriceUpdates++
								}
							}
						}
					}
				}
			}

			// Log other message types for debugging
			if method, ok := wsResponse["method"].(string); ok {
				if method != "depth.update" && method != "ticker.update" {
					fmt.Printf("📋 Other message type: %s\n", method)
				}
			}

		case websocket.PingMessage:
			fmt.Printf("🏓 Ping received, sending pong\n")
			if err := conn.WriteMessage(websocket.PongMessage, nil); err != nil {
				log.Printf("❌ Failed to send pong: %v", err)
				break
			}

		case websocket.PongMessage:
			fmt.Printf("🏓 Pong received\n")

		case websocket.CloseMessage:
			fmt.Printf("🔌 Close message received\n")
			break

		default:
			fmt.Printf("📨 Unknown message type %d\n", messageType)
		}

		// Send periodic ping to keep connection alive
		if messageCount%5 == 0 {
			pingMsg := map[string]interface{}{
				"method": "server.ping",
				"params": []interface{}{},
				"id":     time.Now().UnixNano(),
			}
			if err := conn.WriteJSON(pingMsg); err != nil {
				log.Printf("❌ Failed to send periodic ping: %v", err)
				break
			}
		}

		// Check if we've received enough price updates
		priceMutex.Lock()
		if priceCounts["BTCUSDT"] >= maxPricesPerMarket {
			fmt.Printf("✅ BTCUSDT has received sufficient price updates\n")
			break
		}
		priceMutex.Unlock()
	}

	// Print summary
	fmt.Printf("\n📊 Test Summary:\n")
	priceMutex.Lock()
	for market, count := range priceCounts {
		fmt.Printf("  %s: %d price updates\n", market, count)
	}
	priceMutex.Unlock()
	fmt.Printf("  Total price updates: %d\n", totalPriceUpdates)
	fmt.Printf("  Total messages processed: %d\n", messageCount)

	fmt.Printf("\n🎉 Test completed!\n")
}

func main() {
	testWebSocket()
}
