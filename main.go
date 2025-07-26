package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	config, err := LoadConfig("config.json")
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		return
	}

	httpClient := newHttpClient()
	httpClient.setheaders(map[string]string{
		"Content-Type": "application/json",
		"User-Agent":   "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
	}).setClient(http.DefaultClient)

	coinexClient := NewCoinexClient(httpClient, config)

	// Test connection first
	fmt.Println("Testing connection...")
	testResponse, err := coinexClient.TestConnection()
	if err != nil {
		fmt.Printf("Connection test failed: %v\n", err)
	} else {
		fmt.Printf("Connection test response: %s\n", testResponse)
	}

	// Initialize WebSocket client
	wsc := NewWebSocketClient("socket.coinex.com")
	if err := wsc.Connect(); err != nil {
		fmt.Printf("Failed to connect to WebSocket: %v\n", err)
		return
	}

	marketDepths := NewMarketDepths()

	// Get markets suitable for triangular arbitrage from CoinEx
	arbitrageMarkets, err := coinexClient.GetArbitrageMarkets()
	if err != nil {
		fmt.Printf("Failed to get arbitrage markets: %v\n", err)
		return
	}

	fmt.Printf("Found %d markets for triangular arbitrage: %v\n", len(arbitrageMarkets), arbitrageMarkets)

	if len(arbitrageMarkets) == 0 {
		fmt.Println("No suitable markets found for arbitrage")
		return
	}

	fmt.Printf("Subscribing to %d markets for arbitrage detection...\n", len(arbitrageMarkets))
	for _, marketStr := range arbitrageMarkets {
		go func() {
			if err := wsc.Subscribe(marketStr); err != nil {
				fmt.Printf("Failed to subscribe to %s: %v\n", marketStr, err)
			}
			time.Sleep(10 * time.Millisecond)
		}()
	}

	fmt.Println("Successfully subscribed to markets. Waiting for market data...")
	time.Sleep(3 * time.Second) // Give time for initial market data to arrive

	// Get initial market list for the arbitrage detection function
	marketListString, err := coinexClient.GetMarketList()
	if err != nil {
		fmt.Printf("GetMarketList failed: %v\n", err)
		return
	}

	var marketList map[string]interface{}
	err = json.Unmarshal([]byte(marketListString), &marketList)
	if err != nil {
		fmt.Printf("Failed to unmarshal market list: %v\n", err)
		return
	}

	// Goroutine to process WebSocket messages
	var wsWg sync.WaitGroup
	wsWg.Add(1)
	messageCounter := atomic.Uint64{}

	go func() {
		defer wsWg.Done()
		for msg := range wsc.DataFeed() {
			messageCounter.Add(1)

			var wsResponse map[string]interface{}
			if err := json.Unmarshal(msg, &wsResponse); err != nil {
				fmt.Printf("Failed to unmarshal WebSocket message: %v\n", err)
				continue
			}

			// Handle market depth updates concurrently
			if method, ok := wsResponse["method"].(string); ok && (method == "depth.update" || method == "market.update") {
				if params, ok := wsResponse["params"].([]interface{}); ok && len(params) >= 3 {
					// Process updates concurrently
					go func(method string, params []interface{}) {
						if method == "depth.update" {
							// params[0] is full/incremental flag, params[1] is orderbook data, params[2] is market name
							marketName, ok1 := params[2].(string)
							orderBookData, ok2 := params[1].(map[string]interface{})

							if ok1 && ok2 {
								var orderBook OrderBook
								dataBytes, err := json.Marshal(orderBookData)
								if err != nil {
									fmt.Printf("Failed to marshal market data: %v\n", err)
									return
								}
								if err := json.Unmarshal(dataBytes, &orderBook); err != nil {
									fmt.Printf("Failed to unmarshal order book: %v\n", err)
									fmt.Printf("DEBUG: Order book data: %+v\n", orderBookData)
									return
								}
								marketDepths.Store(marketName, &orderBook)
							}
						} else if method == "market.update" {
							// Old API format (fallback)
							if marketData, ok := params[0].(map[string]interface{}); ok {
								if marketName, ok := marketData["market"].(string); ok {
									var orderBook OrderBook
									dataBytes, err := json.Marshal(marketData["data"])
									if err != nil {
										fmt.Printf("Failed to marshal market data: %v\n", err)
										return
									}
									if err := json.Unmarshal(dataBytes, &orderBook); err != nil {
										fmt.Printf("Failed to unmarshal order book: %v\n", err)
										return
									}
									marketDepths.Store(marketName, &orderBook)
								}
							}
						}
					}(method, params)
				}
			}
		}
	}()

	// Wait for initial market data to be processed
	wsWg.Wait()
	fmt.Printf("Processed %d initial websocket messages\n", messageCounter.Load())
	var wg sync.WaitGroup
	stopTime := time.Now().Add(5 * time.Minute)
	for time.Now().Before(stopTime) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			findArbitrageOpportunities(marketList, config, marketDepths)
		}()
	}
	wg.Wait()
}
