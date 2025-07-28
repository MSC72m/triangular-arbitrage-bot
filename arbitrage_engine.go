package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ArbitrageEngine handles triangular arbitrage detection and execution
type ArbitrageEngine struct {
	config       *Config
	marketDepths *MarketDepths
	metrics      *Metrics
	coinexClient *coinexClient
	rateLimiter  *RateLimiter

	// Triangular paths cache
	triangularPaths []TriangularPath
	pathsLock       sync.RWMutex

	// Active trades tracking
	activeTrades map[string]*ArbitrageTrade
	tradesLock   sync.RWMutex

	// Execution channel
	executionChan chan ArbitrageOpportunity
	stopChan      chan struct{}
}

// NewArbitrageEngine creates a new arbitrage engine
func NewArbitrageEngine(config *Config, marketDepths *MarketDepths, metrics *Metrics, coinexClient *coinexClient) *ArbitrageEngine {
	return &ArbitrageEngine{
		config:        config,
		marketDepths:  marketDepths,
		metrics:       metrics,
		coinexClient:  coinexClient,
		rateLimiter:   NewRateLimiter(config.RateLimitPerSecond, config.RateLimitPerSecond),
		activeTrades:  make(map[string]*ArbitrageTrade),
		executionChan: make(chan ArbitrageOpportunity, 100),
		stopChan:      make(chan struct{}),
	}
}

// Start begins the arbitrage detection and execution
func (ae *ArbitrageEngine) Start() {
	log.Printf("Starting arbitrage engine with %d concurrent scans", ae.config.ConcurrentScans)

	// Start execution worker
	go ae.executionWorker()

	// Start concurrent arbitrage scanners
	for i := 0; i < ae.config.ConcurrentScans; i++ {
		go ae.arbitrageScanner(i)
	}
}

// Stop stops the arbitrage engine
func (ae *ArbitrageEngine) Stop() {
	close(ae.stopChan)
}

// UpdateTriangularPaths updates the cache of triangular arbitrage paths
func (ae *ArbitrageEngine) UpdateTriangularPaths(markets []string, completeAssets []string) {
	// Use WaitGroup to wait for market data to become available
	log.Printf("⏳ Waiting for market data before creating triangular paths...")

	var wg sync.WaitGroup
	marketDataReady := make(chan bool, 1)

	// Start goroutine to monitor market data availability
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		timeout := time.After(30 * time.Second) // 30 second timeout

		for {
			select {
			case <-timeout:
				log.Printf("⚠️  Timeout waiting for market data")
				marketDataReady <- false
				return
			case <-ticker.C:
				availableMarkets := ae.marketDepths.GetAvailableMarkets()
				if len(availableMarkets) >= 3 { // Reduced from 10 to 3 - many markets are illiquid
					log.Printf("✅ Market data ready with %d markets", len(availableMarkets))
					marketDataReady <- true
					return
				}
			}
		}
	}()

	// Wait for market data to be ready
	wg.Wait()
	ready := <-marketDataReady
	if !ready {
		log.Printf("❌ Failed to get sufficient market data, proceeding anyway...")
	}

	// Get available markets from market depths (markets that actually have data)
	availableMarkets := ae.marketDepths.GetAvailableMarkets()
	availableMarketSet := make(map[string]bool)
	for _, market := range availableMarkets {
		availableMarketSet[market] = true
	}

	log.Printf("🔍 MARKET VALIDATION:")
	log.Printf("   📊 Expected markets: %d", len(markets))
	log.Printf("   ✅ Available markets with data: %d", len(availableMarkets))

	// Show some examples of available vs expected
	log.Printf("   📈 Available markets (first 10): %v", availableMarkets[:Min(10, len(availableMarkets))])

	// Filter markets to only include those with actual data AND liquidity
	validMarkets := []string{}
	liquidMarkets := []string{}
	missingMarkets := []string{}

	for _, market := range markets {
		if availableMarketSet[market] {
			// Check if market has actual liquidity (order book data from WebSocket)
			if orderBook, exists := ae.marketDepths.Load(market); exists && orderBook != nil {
				if len(orderBook.Bids) > 0 && len(orderBook.Asks) > 0 {
					liquidMarkets = append(liquidMarkets, market)
				}
			}
			validMarkets = append(validMarkets, market)
		} else {
			missingMarkets = append(missingMarkets, market)
		}
	}

	log.Printf("   ✅ Valid markets: %d", len(validMarkets))
	log.Printf("   💧 Liquid markets: %d", len(liquidMarkets))
	log.Printf("   ❌ Missing markets: %d", len(missingMarkets))

	if len(missingMarkets) > 0 {
		log.Printf("   🚫 Missing markets (first 10): %v", missingMarkets[:Min(10, len(missingMarkets))])
	}

	// Create triangular arbitrage paths using liquid markets only
	// Look for cycles like USDT → Asset → USDC → USDT
	paths := ae.discoverTriangularArbitrageCycles(liquidMarkets)

	ae.pathsLock.Lock()
	ae.triangularPaths = paths
	ae.pathsLock.Unlock()

	log.Printf("🔄 Updated triangular paths: %d arbitrage cycles discovered", len(paths))

	// Log first few paths for debugging
	for i, path := range paths {
		if i < 5 { // Show first 5 paths
			log.Printf("   📊 Cycle %d: %s (%s) → %s (%s) → %s (%s) → %s",
				i+1,
				path.BaseAsset,
				path.Direction1,
				path.Asset1,
				path.Direction2,
				path.Asset2,
				path.Direction3,
				path.BaseAsset)
		}
	}

	if len(paths) > 5 {
		log.Printf("   ... and %d more cycles", len(paths)-5)
	}

	if len(paths) == 0 {
		log.Printf("⚠️  WARNING: No valid triangular arbitrage cycles found!")
		log.Printf("   This might be due to:")
		log.Printf("   1. Insufficient market data")
		log.Printf("   2. No suitable asset combinations for triangular arbitrage")
		log.Printf("   3. WebSocket data not fully synchronized")
	}
}

// discoverTriangularArbitrageCycles discovers triangular arbitrage cycles like USDT → Asset → USDC → USDT
func (ae *ArbitrageEngine) discoverTriangularArbitrageCycles(availableMarkets []string) []TriangularPath {
	var paths []TriangularPath

	log.Printf("🔍 Discovering triangular arbitrage cycles from %d available markets", len(availableMarkets))

	// Create market lookup sets for configured quote currencies only
	marketSet := make(map[string]bool)
	usdtMarkets := make(map[string]bool) // Asset/USDT pairs
	usdcMarkets := make(map[string]bool) // Asset/USDC pairs

	for _, market := range availableMarkets {
		marketSet[market] = true

		// Only process markets for configured quote currencies
		for _, quote := range ae.config.QuoteCurrencies {
			if strings.HasSuffix(market, quote) {
				asset := strings.TrimSuffix(market, quote)
				if asset != "" {
					if quote == "USDT" && asset != "USDC" {
						usdtMarkets[asset] = true
					} else if quote == "USDC" && asset != "USDT" {
						usdcMarkets[asset] = true
					}
				}
				break
			}
		}
	}

	log.Printf("   📊 USDT pairs: %d, USDC pairs: %d", len(usdtMarkets), len(usdcMarkets))

	// Find triangular cycles: USDT → Asset → USDC → USDT
	cycleCount := 0

	// Only create cycles if we have both quote currencies configured
	if len(ae.config.QuoteCurrencies) >= 2 {
		// USDT → Asset → USDC → USDT (requires USDCUSDT pair)
		if marketSet["USDCUSDT"] || marketSet["USDTUSDC"] {
			quotePair := "USDCUSDT"
			if marketSet["USDTUSDC"] {
				quotePair = "USDTUSDC"
			}

			for asset := range usdtMarkets {
				if usdcMarkets[asset] { // Asset has both USDT and USDC pairs
					// Forward cycle: USDT → Asset → USDC → USDT
					path1 := TriangularPath{
						BaseAsset:  "USDT",
						Asset1:     asset,
						Asset2:     "USDC",
						Market1:    asset + "USDT", // Buy asset with USDT
						Market2:    asset + "USDC", // Sell asset for USDC
						Market3:    quotePair,      // Sell USDC for USDT
						Direction1: "buy",
						Direction2: "sell",
						Direction3: "sell",
					}

					// Reverse cycle: USDT → USDC → Asset → USDT
					path2 := TriangularPath{
						BaseAsset:  "USDT",
						Asset1:     "USDC",
						Asset2:     asset,
						Market1:    quotePair,      // Buy USDC with USDT
						Market2:    asset + "USDC", // Buy asset with USDC
						Market3:    asset + "USDT", // Sell asset for USDT
						Direction1: "buy",
						Direction2: "buy",
						Direction3: "sell",
					}

					paths = append(paths, path1, path2)
					cycleCount += 2

					log.Printf("   ✅ Created cycles for %s: USDT→%s→USDC→USDT and reverse", asset, asset)
				}
			}
		} else {
			log.Printf("   ⚠️  Missing USDC/USDT pair - cannot create triangular cycles")
		}
	} else {
		log.Printf("   ⚠️  Need at least 2 quote currencies for triangular arbitrage (configured: %v)", ae.config.QuoteCurrencies)
	}

	log.Printf("✅ Created %d triangular arbitrage cycles", cycleCount)

	// Log discovered assets for debugging
	if len(usdtMarkets) > 0 && len(usdcMarkets) > 0 {
		completeAssets := []string{}
		for asset := range usdtMarkets {
			if usdcMarkets[asset] {
				completeAssets = append(completeAssets, asset)
			}
		}

		log.Printf("   💰 Assets with both USDT and USDC pairs: %v", completeAssets)

		if len(completeAssets) == 0 {
			log.Printf("   ⚠️  No assets found with both USDT and USDC pairs")

			// Show what we do have
			usdtAssets := make([]string, 0, len(usdtMarkets))
			for asset := range usdtMarkets {
				usdtAssets = append(usdtAssets, asset)
			}
			usdcAssets := make([]string, 0, len(usdcMarkets))
			for asset := range usdcMarkets {
				usdcAssets = append(usdcAssets, asset)
			}

			log.Printf("   📈 USDT-only assets: %v", usdtAssets[:Min(10, len(usdtAssets))])
			log.Printf("   💎 USDC-only assets: %v", usdcAssets[:Min(10, len(usdcAssets))])
		}
	}

	return paths
}

// arbitrageScanner continuously scans for arbitrage opportunities
func (ae *ArbitrageEngine) arbitrageScanner(scannerID int) {
	ticker := time.NewTicker(100 * time.Millisecond) // Reduced frequency from 10ms to 100ms for illiquid markets
	defer ticker.Stop()

	scanCount := 0

	for {
		select {
		case <-ae.stopChan:
			return
		case <-ticker.C:
			scanCount++
			ae.scanForOpportunities(scannerID, scanCount)
		}
	}
}

// scanForOpportunities scans for profitable arbitrage opportunities
func (ae *ArbitrageEngine) scanForOpportunities(scannerID int, scanCount int) {
	startTime := time.Now()
	defer func() {
		detectionTime := time.Since(startTime)
		if detectionTime > time.Duration(ae.config.MaxLatencyMs)*time.Millisecond {
			log.Printf("⚠️  SLOW SCAN | Scanner %d | Latency: %v (limit: %dms)",
				scannerID, detectionTime, ae.config.MaxLatencyMs)
		}
	}()

	ae.pathsLock.RLock()
	paths := make([]TriangularPath, len(ae.triangularPaths))
	copy(paths, ae.triangularPaths)
	ae.pathsLock.RUnlock()

	if len(paths) == 0 {
		if scanCount%1000 == 0 && scannerID == 0 { // Log occasionally from scanner 0 only
			log.Printf("⚠️  NO PATHS | Scanner %d | No valid triangular paths available for scanning", scannerID)

			// Get available markets for debugging
			availableMarkets := ae.marketDepths.GetAvailableMarkets()
			log.Printf("   📊 Available markets with data: %d", len(availableMarkets))
			if len(availableMarkets) > 0 {
				log.Printf("   📈 Sample available markets: %v", availableMarkets[:Min(5, len(availableMarkets))])
			} else {
				log.Printf("   ❌ No market data available at all - WebSocket may be disconnected")
			}
		}
		return
	}

	// Get snapshot of market depths
	snapshot := ae.marketDepths.GetSnapshot()

	// Debug: Show available markets vs required markets
	if scanCount%1000 == 0 && scannerID == 0 { // Log every 1000 scans from scanner 0 (every 10 seconds)
		availableMarkets := make([]string, 0, len(snapshot))
		for market := range snapshot {
			availableMarkets = append(availableMarkets, market)
		}

		requiredMarkets := make(map[string]bool)
		for _, path := range paths[:Min(3, len(paths))] { // Check first 3 paths only
			requiredMarkets[path.Market1] = true
			requiredMarkets[path.Market2] = true
			requiredMarkets[path.Market3] = true
		}

		log.Printf("🔍 MARKET DEBUG | Available: %d markets | Required (first 3 paths): %v",
			len(availableMarkets), getKeys(requiredMarkets))
	}

	opportunitiesFound := 0
	pathsChecked := 0
	pathsWithData := 0

	for _, path := range paths {
		pathsChecked++
		opportunity := ae.calculateOpportunity(path, snapshot, startTime)

		if opportunity != nil {
			pathsWithData++
			profitPercent := opportunity.NetProfit * 100

			if opportunity.NetProfit >= ae.config.ProfitThreshold {
				opportunitiesFound++
				ae.metrics.IncrementOpportunities()

				log.Printf("🚨 OPPORTUNITY FOUND | Scanner %d | Path: %s→%s→%s | Profit: +%.6f%% | Volume: $%.2f | Prices: %.8f, %.8f, %.8f",
					scannerID,
					path.Market1, path.Market2, path.Market3,
					profitPercent,
					opportunity.Volume,
					opportunity.Price1, opportunity.Price2, opportunity.Price3)

				// Send to execution channel (non-blocking)
				select {
				case ae.executionChan <- *opportunity:
					log.Printf("✅ QUEUED | Scanner %d | Opportunity sent to execution", scannerID)
				default:
					log.Printf("⚠️  DROPPED | Scanner %d | Execution channel full", scannerID)
				}
			}
			// Note: Below threshold and losing paths are now logged within calculateOpportunity()
		}
	}

	// Log scan summary every 1000 scans and only from scanner 0 to avoid spam
	if scanCount%1000 == 0 && scannerID == 0 {
		log.Printf("🔍 SCAN SUMMARY | Paths: %d | With data: %d | Opportunities: %d | Detection time: %v",
			pathsChecked, pathsWithData, opportunitiesFound, time.Since(startTime))
	}
}

// calculateOpportunity calculates the profitability of a triangular arbitrage path
func (ae *ArbitrageEngine) calculateOpportunity(path TriangularPath, snapshot map[string]*OrderBook, detectionStart time.Time) *ArbitrageOpportunity {
	market1Data, ok1 := snapshot[path.Market1]
	market2Data, ok2 := snapshot[path.Market2]
	market3Data, ok3 := snapshot[path.Market3]

	// This should no longer happen since we now filter to only valid markets
	if !ok1 || !ok2 || !ok3 {
		// Only log this as a warning since it could be a temporary issue
		missing := []string{}
		if !ok1 {
			missing = append(missing, path.Market1)
		}
		if !ok2 {
			missing = append(missing, path.Market2)
		}
		if !ok3 {
			missing = append(missing, path.Market3)
		}

		// Reduced logging frequency to avoid spam
		if detectionStart.UnixNano()%10000 == 0 { // Log ~0.01% of these
			log.Printf("⚠️  TEMP MISSING | Path: %s→%s→%s | Missing: %v (temporary data gap)",
				path.Market1, path.Market2, path.Market3, missing)
		}
		return nil
	}

	if len(market1Data.Asks) == 0 || len(market1Data.Bids) == 0 ||
		len(market2Data.Asks) == 0 || len(market2Data.Bids) == 0 ||
		len(market3Data.Asks) == 0 || len(market3Data.Bids) == 0 {

		// Reduce logging frequency for empty orderbooks to avoid spam
		if detectionStart.UnixNano()%50000 == 0 { // Log only ~0.002% of these
			log.Printf("⚠️  EMPTY ORDERBOOK | Path: %s→%s→%s | Some order books are empty",
				path.Market1, path.Market2, path.Market3)
		}
		return nil
	}

	// Get relevant prices based on direction
	var price1, price2, price3 float64
	var volume1, volume2, volume3 float64
	var err error

	if path.Direction1 == "buy" {
		price1, err = strconv.ParseFloat(market1Data.Asks[0].Price, 64)
		volume1, _ = strconv.ParseFloat(market1Data.Asks[0].Amount, 64)
	} else {
		price1, err = strconv.ParseFloat(market1Data.Bids[0].Price, 64)
		volume1, _ = strconv.ParseFloat(market1Data.Bids[0].Amount, 64)
	}
	if err != nil || price1 <= 0 {
		log.Printf("⚠️  INVALID PRICE | Path: %s→%s→%s | Invalid price1: %s (error: %v)",
			path.Market1, path.Market2, path.Market3, market1Data.Asks[0].Price, err)
		return nil
	}

	if path.Direction2 == "buy" {
		price2, err = strconv.ParseFloat(market2Data.Asks[0].Price, 64)
		volume2, _ = strconv.ParseFloat(market2Data.Asks[0].Amount, 64)
	} else {
		price2, err = strconv.ParseFloat(market2Data.Bids[0].Price, 64)
		volume2, _ = strconv.ParseFloat(market2Data.Bids[0].Amount, 64)
	}
	if err != nil || price2 <= 0 {
		log.Printf("⚠️  INVALID PRICE | Path: %s→%s→%s | Invalid price2: %s (error: %v)",
			path.Market1, path.Market2, path.Market3, market2Data.Asks[0].Price, err)
		return nil
	}

	if path.Direction3 == "buy" {
		price3, err = strconv.ParseFloat(market3Data.Asks[0].Price, 64)
		volume3, _ = strconv.ParseFloat(market3Data.Asks[0].Amount, 64)
	} else {
		price3, err = strconv.ParseFloat(market3Data.Bids[0].Price, 64)
		volume3, _ = strconv.ParseFloat(market3Data.Bids[0].Amount, 64)
	}
	if err != nil || price3 <= 0 {
		log.Printf("⚠️  INVALID PRICE | Path: %s→%s→%s | Invalid price3: %s (error: %v)",
			path.Market1, path.Market2, path.Market3, market3Data.Asks[0].Price, err)
		return nil
	}

	// Calculate round-trip rate based on direction
	var roundTripRate float64
	if path.Direction1 == "buy" && path.Direction2 == "sell" && path.Direction3 == "sell" {
		// USDT → Asset → USDC → USDT
		roundTripRate = (1.0 / price1) * price2 * price3
	} else if path.Direction1 == "sell" && path.Direction2 == "buy" && path.Direction3 == "buy" {
		// USDT → USDC → Asset → USDT (reverse)
		roundTripRate = (1.0 / price3) * (1.0 / price2) * price1
	} else if path.Direction1 == "buy" && path.Direction2 == "buy" && path.Direction3 == "sell" {
		// USDT → USDC → Asset → USDT
		roundTripRate = (1.0 / price1) * (1.0 / price2) * price3
	} else if path.Direction1 == "sell" && path.Direction2 == "sell" && path.Direction3 == "buy" {
		// USDT → Asset → USDC → USDT (reverse)
		roundTripRate = price1 * price2 * (1.0 / price3)
	} else {
		log.Printf("⚠️  INVALID DIRECTION | Path: %s→%s→%s | Invalid direction combination: %s, %s, %s",
			path.Market1, path.Market2, path.Market3,
			path.Direction1, path.Direction2, path.Direction3)
		return nil // Invalid direction combination
	}

	estimatedProfit := roundTripRate - 1.0

	// Calculate fees
	fee1 := ae.getTradingFee(path.Market1)
	fee2 := ae.getTradingFee(path.Market2)
	fee3 := ae.getTradingFee(path.Market3)
	totalFees := fee1 + fee2 + fee3

	netProfit := estimatedProfit - totalFees

	// Determine volume (use fixed volume or calculate from order book depth)
	var volume float64
	if ae.config.FixedVolume > 0 {
		volume = ae.config.FixedVolume
	} else {
		// Calculate max volume based on order book depth and risk limits
		volume = ae.calculateMaxVolume(volume1, volume2, volume3, price1, price2, price3)
	}

	if volume <= 0 {
		log.Printf("⚠️  INVALID VOLUME | Path: %s→%s→%s | Invalid volume: %.6f",
			path.Market1, path.Market2, path.Market3, volume)
		return nil
	}

	// Log calculation results based on profitability
	if netProfit >= ae.config.ProfitThreshold {
		// This is a profitable opportunity
		log.Printf("💰 PROFITABLE PATH | %s→%s→%s | Profit: +%.8f%% | Volume: $%.2f | Prices: %.8f, %.8f, %.8f",
			path.Market1, path.Market2, path.Market3,
			netProfit*100, volume, price1, price2, price3)
	} else if netProfit > 0 && netProfit < ae.config.ProfitThreshold {
		// Profitable but below threshold - log occasionally to avoid spam
		if detectionStart.UnixNano()%1000 == 0 { // Log ~0.1% of these
			log.Printf("💡 BELOW THRESHOLD | %s→%s→%s | Profit: +%.8f%% (need: %.8f%%) | Volume: $%.2f",
				path.Market1, path.Market2, path.Market3,
				netProfit*100, ae.config.ProfitThreshold*100, volume)
		}
	} else {
		// This is a losing trade - log occasionally for debugging
		if detectionStart.UnixNano()%10000 == 0 { // Log ~0.01% of these
			log.Printf("📉 LOSING PATH | %s→%s→%s | Loss: %.8f%% | Volume: $%.2f | Prices: %.8f, %.8f, %.8f",
				path.Market1, path.Market2, path.Market3,
				netProfit*100, volume, price1, price2, price3)
		}
	}

	return &ArbitrageOpportunity{
		Path:            path,
		EstimatedProfit: estimatedProfit,
		NetProfit:       netProfit,
		Volume:          volume,
		Timestamp:       time.Now(),
		DetectionTime:   time.Since(detectionStart),
		Market1Data:     *market1Data,
		Market2Data:     *market2Data,
		Market3Data:     *market3Data,
		Price1:          price1,
		Price2:          price2,
		Price3:          price3,
	}
}

// getTradingFee returns the trading fee for a specific market
func (ae *ArbitrageEngine) getTradingFee(market string) float64 {
	if fee, exists := ae.config.TradingFees[market]; exists {
		return fee
	}
	return ae.config.DefaultTradingFee
}

// calculateMaxVolume calculates the maximum safe volume for the arbitrage
func (ae *ArbitrageEngine) calculateMaxVolume(vol1, vol2, vol3, price1, price2, price3 float64) float64 {
	// Apply volume fraction limit
	maxVol1 := vol1 * ae.config.MaxVolumeFraction
	maxVol2 := vol2 * ae.config.MaxVolumeFraction
	maxVol3 := vol3 * ae.config.MaxVolumeFraction

	// Convert all volumes to base currency equivalent
	baseVol1 := maxVol1 * price1
	baseVol2 := maxVol2 * price2
	baseVol3 := maxVol3 * price3

	// Take minimum of all three
	minVolume := baseVol1
	if baseVol2 < minVolume {
		minVolume = baseVol2
	}
	if baseVol3 < minVolume {
		minVolume = baseVol3
	}

	// Apply position size limit
	if minVolume > ae.config.MaxPositionSize {
		minVolume = ae.config.MaxPositionSize
	}

	return minVolume
}

// executionWorker handles the execution of arbitrage opportunities
func (ae *ArbitrageEngine) executionWorker() {
	for {
		select {
		case <-ae.stopChan:
			return
		case opportunity := <-ae.executionChan:
			if ae.config.SimulationMode {
				ae.simulateExecution(opportunity)
			} else {
				ae.executeArbitrage(opportunity)
			}
		}
	}
}

// simulateExecution simulates the execution for testing purposes
func (ae *ArbitrageEngine) simulateExecution(opportunity ArbitrageOpportunity) {
	log.Printf("💰 SIMULATION TRADE | Path: %s→%s→%s | Expected Profit: %.6f%% | Volume: $%.2f | Est. PnL: $%.4f",
		opportunity.Path.Market1, opportunity.Path.Market2, opportunity.Path.Market3,
		opportunity.NetProfit*100,
		opportunity.Volume,
		opportunity.NetProfit*opportunity.Volume)

	// Log the three legs of the trade
	log.Printf("   🔄 Leg 1: %s %s at price %.8f",
		opportunity.Path.Direction1, opportunity.Path.Market1, opportunity.Price1)
	log.Printf("   🔄 Leg 2: %s %s at price %.8f",
		opportunity.Path.Direction2, opportunity.Path.Market2, opportunity.Price2)
	log.Printf("   🔄 Leg 3: %s %s at price %.8f",
		opportunity.Path.Direction3, opportunity.Path.Market3, opportunity.Price3)

	ae.metrics.IncrementTrades()
	ae.metrics.AddPnL(opportunity.NetProfit * opportunity.Volume)

	log.Printf("✅ SIMULATION COMPLETE | Total trades: %d | Total PnL: $%.4f",
		ae.metrics.GetSnapshot().TradesExecuted,
		ae.metrics.GetSnapshot().TotalPnL)
}

// executeArbitrage executes the actual arbitrage trade
func (ae *ArbitrageEngine) executeArbitrage(opportunity ArbitrageOpportunity) {
	if !ae.rateLimiter.Allow() {
		log.Printf("Rate limit exceeded, skipping trade")
		return
	}

	log.Printf("EXECUTING: Arbitrage trade: %s->%s->%s, Expected profit: %.6f%%",
		opportunity.Path.Market1, opportunity.Path.Market2, opportunity.Path.Market3,
		opportunity.NetProfit*100)

	trade := &ArbitrageTrade{
		ID:          fmt.Sprintf("arb_%d", time.Now().UnixNano()),
		Opportunity: opportunity,
		Status:      "pending",
		StartTime:   time.Now(),
	}

	// Track active trade
	ae.tradesLock.Lock()
	ae.activeTrades[trade.ID] = trade
	ae.tradesLock.Unlock()

	// Execute the three legs in parallel
	go ae.executeThreeLegs(trade)
}

// executeThreeLegs executes the three legs of the arbitrage trade
func (ae *ArbitrageEngine) executeThreeLegs(trade *ArbitrageTrade) {
	defer func() {
		ae.tradesLock.Lock()
		delete(ae.activeTrades, trade.ID)
		ae.tradesLock.Unlock()
	}()

	trade.Status = "executing"

	// For now, this is a placeholder - actual order execution would be implemented here
	// The CoinEx API integration would place actual orders
	log.Printf("Trade %s: Executing three legs (placeholder implementation)", trade.ID)

	// Simulate execution time
	time.Sleep(100 * time.Millisecond)

	trade.Status = "completed"
	trade.EndTime = time.Now()
	trade.ExecutionTime = trade.EndTime.Sub(trade.StartTime)

	ae.metrics.IncrementTrades()
	ae.metrics.AddPnL(trade.Opportunity.NetProfit * trade.Opportunity.Volume)

	log.Printf("Trade %s completed in %v", trade.ID, trade.ExecutionTime)
}

// Helper function to get map keys
func getKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
