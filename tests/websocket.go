package main

import (
	"bufio"
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
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// authenticateWebSocket authenticates the WebSocket connection with CoinEx using HMAC-SHA256 signature as per CoinEx docs.
// Returns true if authentication is successful, false otherwise.
func authenticateWebSocket(conn *websocket.Conn, apiKey, secretKey string) (bool, error) {
	timestamp := time.Now().UnixMilli()

	// Step 1: Create the string to sign (just timestamp as per CoinEx docs)
	preparedStr := fmt.Sprintf("%d", timestamp)

	// Step 2: Create HMAC-SHA256 signature
	h := hmac.New(sha256.New, []byte(secretKey))
	h.Write([]byte(preparedStr))
	signedStr := hex.EncodeToString(h.Sum(nil))

	// Try the correct format as per CoinEx docs
	authMsg := map[string]interface{}{
		"method": "server.sign",
		"params": map[string]interface{}{
			"access_id":  apiKey,
			"signed_str": signedStr,
			"timestamp":  timestamp,
		},
		"id": time.Now().UnixNano(),
	}

	fmt.Printf("🔐 Sending WebSocket authentication...\n")
	fmt.Printf("🔐 Timestamp: %d\n", timestamp)
	fmt.Printf("🔐 Prepared string: %s\n", preparedStr)
	fmt.Printf("🔐 Signed string: %s\n", signedStr)
	fmt.Printf("🔐 Auth message: %+v\n", authMsg)

	if err := conn.WriteJSON(authMsg); err != nil {
		return false, fmt.Errorf("failed to send authentication request: %w", err)
	}

	// Wait for authentication response with longer timeout
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))

	// Read multiple messages to find the auth response
	for i := 0; i < 10; i++ { // Try up to 10 messages
		messageType, message, err := conn.ReadMessage()
		if err != nil {
			return false, fmt.Errorf("failed to read authentication response (attempt %d): %w", i+1, err)
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
				return true, nil
			}
		}

		// Check for successful authentication response with method
		if method, ok := wsResponse["method"].(string); ok && method == "server.sign" {
			if _, ok := wsResponse["error"]; ok && wsResponse["error"] != nil {
				fmt.Printf("❌ WebSocket authentication error: %+v\n", wsResponse["error"])
				return false, fmt.Errorf("authentication error: %+v", wsResponse["error"])
			}
			fmt.Printf("✅ WebSocket authentication successful: %+v\n", wsResponse)
			return true, nil
		}

		// Check for result with matching id
		if _, ok := wsResponse["result"]; ok {
			if id, ok := wsResponse["id"]; ok && id == authMsg["id"] {
				fmt.Printf("✅ WebSocket authentication successful (by id): %+v\n", wsResponse)
				return true, nil
			}
		}

		// Check for any success indication
		if _, ok := wsResponse["result"]; ok {
			fmt.Printf("✅ WebSocket authentication successful (generic result): %+v\n", wsResponse)
			return true, nil
		}

		// If we get here, this wasn't the auth response, continue reading
		fmt.Printf("📨 Not auth response, continuing...\n")
	}

	return false, fmt.Errorf("authentication timeout - no valid response received")
}

// readEnv returns the CoinEx API key and secret from the environment or .env file.
// It will try the following, in order:
//  1. Environment variables COINEX_API_KEY and COINEX_API_SECRET
//  2. .env file, looking for COINEX_API_KEY and COINEX_API_SECRET (not COINEX_SECRET_ID)
func readEnv() (string, string) {
	// 1. Try environment variables first
	apiKey := "AC2D8447877241C0A2BD6F827C178B05"
	secretKey := "F260070459F8FDEAC1E5BF27C3FF8F1B3B9EE362B3BB3789"
	if apiKey != "" && secretKey != "" {
		fmt.Printf("🔑 Found API key and secret in environment variables\n")
		return apiKey, secretKey
	}

	// 2. Try .env file
	env, err := os.Open("../.env")
	if err != nil {
		log.Printf("❌ Failed to open .env file: %v", err)
		return "", ""
	}
	defer env.Close()

	scanner := bufio.NewScanner(env)
	for scanner.Scan() {
		line := scanner.Text()
		// Only print lines that are relevant for debugging
		if strings.HasPrefix(line, "COINEX_API_KEY=") {
			apiKey = strings.TrimPrefix(line, "COINEX_API_KEY=")
			fmt.Printf("🔑 Found API key: %s\n", apiKey)
		}
		if strings.HasPrefix(line, "COINEX_SECRET_ID=") {
			secretKey = strings.TrimPrefix(line, "COINEX_SECRET_ID=")
			fmt.Printf("🔑 Found secret key: %s\n", secretKey)
		}
	}
	return apiKey, secretKey
}

func testWebSocket() {
	fmt.Println("🧪 Testing WebSocket connection for all markets at once...")

	// Get API credentials from environment variables or .env file
	apiKey, secretKey := readEnv()

	if apiKey == "" || secretKey == "" {
		log.Fatalf("❌ Please set COINEX_API_KEY and COINEX_API_SECRET environment variables or .env file for authentication test.")
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
			priceMutex.Unlock()
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
