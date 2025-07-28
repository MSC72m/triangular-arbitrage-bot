package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	// Set up structured logging
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix("[ARBITRAGE] ")

	log.Println("🔺 Starting Triangular Arbitrage Bot...")

	// Load configuration
	config, err := LoadConfig("config.json")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Printf("Loaded configuration: API Key length=%d, Quote currencies=%v, Profit threshold=%.6f%%",
		len(config.APIKey), config.QuoteCurrencies, config.ProfitThreshold*100)

	if config.SimulationMode {
		log.Println("⚠️  SIMULATION MODE ENABLED - No real trades will be executed")
	}

	// Initialize HTTP client with WebSocket support
	httpClient := newHttpClient()
	httpClient.setheaders(map[string]string{
		"Content-Type": "application/json",
		"User-Agent":   "TriangularArbitrageBot/2.0",
	}).setClient(&http.Client{
		Timeout: 30 * time.Second,
	}).SetWebSocketUrl("socket.coinex.com")

	// Initialize exchange client
	coinexClient := NewCoinexClient(httpClient, config)

	// Test connection
	log.Println("🔗 Testing exchange connection...")
	testResponse, err := coinexClient.TestConnection()
	if err != nil {
		log.Fatalf("Connection test failed: %v", err)
	}
	log.Printf("✅ Connection test successful: %s", testResponse[:min(100, len(testResponse))])

	// Initialize core components
	marketDepths := NewMarketDepths()
	metrics := NewMetrics()
	arbitrageEngine := NewArbitrageEngine(config, marketDepths, metrics, coinexClient)

	// Connect to WebSocket using integrated HttpClient
	log.Println("🔌 Connecting to WebSocket...")
	if err := httpClient.ConnectWebSocket(); err != nil {
		log.Fatalf("Failed to connect to WebSocket: %v", err)
	}
	log.Println("✅ WebSocket connected successfully")

	// Get markets suitable for triangular arbitrage
	log.Println("🔍 Discovering arbitrage markets...")
	arbitrageMarkets, completeAssets, err := coinexClient.GetArbitrageMarkets()
	if err != nil {
		log.Fatalf("Failed to get arbitrage markets: %v", err)
	}

	if len(arbitrageMarkets) == 0 {
		log.Fatal("❌ No suitable markets found for triangular arbitrage")
	}

	log.Printf("✅ Found %d markets for arbitrage (%d complete assets)", len(arbitrageMarkets), len(completeAssets))

	// Log all discovered markets for debugging
	log.Printf("🔍 DISCOVERED MARKETS:")
	for i, market := range arbitrageMarkets {
		if i < 20 { // Show first 20 markets
			log.Printf("   📊 %s", market)
		}
	}
	if len(arbitrageMarkets) > 20 {
		log.Printf("   ... and %d more markets", len(arbitrageMarkets)-20)
	}

	// Log complete assets
	log.Printf("🎯 COMPLETE ASSETS: %v", completeAssets)

	// Subscribe to markets
	log.Printf("📡 Subscribing to %d markets...", len(arbitrageMarkets))
	subscriptionErrors := 0
	successfulSubscriptions := 0

	for i, market := range arbitrageMarkets {
		if err := httpClient.SubscribeWebSocket(market); err != nil {
			subscriptionErrors++
			log.Printf("❌ Failed to subscribe to %s: %v", market, err)
		} else {
			successfulSubscriptions++
			if i < 5 { // Log first few subscriptions
				log.Printf("✅ Subscribed to %s", market)
			}
		}

		// Add small delay to avoid overwhelming the WebSocket
		if i%10 == 0 && i > 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	log.Printf("📊 Subscription Results: %d successful, %d failed out of %d total",
		successfulSubscriptions, subscriptionErrors, len(arbitrageMarkets))
	log.Println("✅ Market subscriptions completed")

	// Start market data processor
	messageCounter := uint64(0)
	go func() {
		log.Println("🔄 Starting market data processor...")
		for msg := range httpClient.GetWebSocketDataFeed() {
			messageCounter++

			// Process WebSocket message
			if err := processWebSocketMessage(msg, marketDepths, metrics); err != nil {
				if messageCounter%100 == 0 { // Log errors more frequently for debugging
					log.Printf("⚠️  WebSocket message processing error: %v | Message preview: %s",
						err, string(msg)[:min(200, len(msg))])
				}
				continue
			}

			// Log processing stats periodically
			if messageCounter%10000 == 0 {
				log.Printf("📈 Processed %d WebSocket messages", messageCounter)
			}
		}
	}()

	// Wait for initial market data and verify it's flowing with proper synchronization
	log.Println("⏳ Waiting for initial market data...")

	var wg sync.WaitGroup
	marketDataReady := make(chan bool, 1)

	// Start goroutine to monitor market data availability
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		timeout := time.After(30 * time.Second) // 30 second timeout

		for {
			select {
			case <-timeout:
				log.Printf("⚠️  Timeout waiting for initial market data")
				marketDataReady <- false
				return
			case <-ticker.C:
				initialMarkets := marketDepths.GetAvailableMarkets()
				log.Printf("🔍 MARKET DATA CHECK: %d markets available", len(initialMarkets))

				// Check if we have the critical USDCUSDT pair and at least 2 other markets
				hasUSDCUSDT := false
				for _, market := range initialMarkets {
					if market == "USDCUSDT" || market == "USDTUSDC" {
						hasUSDCUSDT = true
						break
					}
				}

				if len(initialMarkets) >= 3 && hasUSDCUSDT { // Reduced from 5 to 3, but require USDC pair
					log.Printf("✅ Initial market data ready with %d markets (including USDC/USDT pair)", len(initialMarkets))
					log.Printf("✅ Market data flowing: %v", initialMarkets[:min(10, len(initialMarkets))])
					marketDataReady <- true
					return
				} else if len(initialMarkets) >= 3 {
					log.Printf("⚠️  Have %d markets but missing critical USDC/USDT pair", len(initialMarkets))
				}
			}
		}
	}()

	// Wait for market data to be ready
	wg.Wait()
	ready := <-marketDataReady
	if !ready {
		log.Printf("❌ WARNING: Proceeding without sufficient market data...")
		retryMarkets := marketDepths.GetAvailableMarkets()
		log.Printf("   📊 Available markets: %d", len(retryMarkets))
		if len(retryMarkets) > 0 {
			log.Printf("   📈 Available: %v", retryMarkets[:min(5, len(retryMarkets))])
		}
	}

	// Update triangular paths in the arbitrage engine
	arbitrageEngine.UpdateTriangularPaths(arbitrageMarkets, completeAssets)

	// Start arbitrage engine
	log.Println("🚀 Starting arbitrage engine...")
	arbitrageEngine.Start()

	// Start simple periodic metrics logging
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		priceTicker := time.NewTicker(2 * time.Minute)  // Log prices less frequently
		debugTicker := time.NewTicker(15 * time.Second) // Debug market data status frequently
		defer ticker.Stop()
		defer priceTicker.Stop()
		defer debugTicker.Stop()

		for {
			select {
			case <-ticker.C:
				logBasicMetrics(metrics, marketDepths)
			case <-priceTicker.C:
				logMarketPrices(marketDepths)
			case <-debugTicker.C:
				logMarketDataStatus(marketDepths, arbitrageMarkets)

				// Log WebSocket connection status
				if httpClient.IsWebSocketConnected() {
					log.Printf("🔗 WebSocket Status: Connected ✅")
				} else {
					log.Printf("🔗 WebSocket Status: Disconnected ❌")
				}
			}
		}
	}()

	// Set up graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	log.Printf("✅ Triangular Arbitrage Bot is running!")
	log.Printf("🛑 Press Ctrl+C to stop")

	// Wait for shutdown signal
	<-quit
	log.Println("🛑 Shutdown signal received, stopping bot...")

	// Graceful shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Stop arbitrage engine
	log.Println("⏹️  Stopping arbitrage engine...")
	arbitrageEngine.Stop()

	// Close WebSocket connection
	log.Println("📪 Closing WebSocket connection...")
	if err := httpClient.CloseWebSocket(); err != nil {
		log.Printf("⚠️  Error closing WebSocket: %v", err)
	}

	// Final metrics log
	log.Println("📊 Final metrics:")
	logSimpleMetrics(metrics, marketDepths)

	// Wait for shutdown or timeout
	done := make(chan struct{})
	go func() {
		// Simulate cleanup completion
		time.Sleep(1 * time.Second)
		close(done)
	}()

	select {
	case <-done:
		log.Println("✅ Graceful shutdown completed")
	case <-shutdownCtx.Done():
		log.Println("⚠️  Shutdown timeout exceeded")
	}

	log.Println("👋 Triangular Arbitrage Bot stopped")
}
