package execution

import (
	"context"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"triangular-arbitrage-bot/internal/config"
	"triangular-arbitrage-bot/internal/exchange"
	"triangular-arbitrage-bot/pkg/models"
)

// Simplified ArbitrageEngine with minimal concurrency
type ArbitrageEngine struct {
	config       *config.Config
	marketDepths *models.MarketDepths
	metrics      *models.Metrics
	client       *exchange.CoinexClient

	// Triangular paths cache
	triangularPaths []models.TriangularPath
	pathsLock       sync.RWMutex

	// Simple execution state
	isExecuting    bool
	executionMutex sync.Mutex
	lastExecution  time.Time

	// Context for graceful shutdown
	ctx    context.Context
	cancel context.CancelFunc

	// Stop flag
	stopped   bool
	stopMutex sync.Mutex

	// Track executed opportunities to prevent duplicates
	executedOpportunities map[string]time.Time
	executedMutex         sync.RWMutex
}

// NewArbitrageEngine creates a new simplified arbitrage engine
func NewArbitrageEngine(config *config.Config, marketDepths *models.MarketDepths, metrics *models.Metrics, client *exchange.CoinexClient) *ArbitrageEngine {
	ctx, cancel := context.WithCancel(context.Background())
	return &ArbitrageEngine{
		config:                config,
		marketDepths:          marketDepths,
		metrics:               metrics,
		client:                client,
		triangularPaths:       []models.TriangularPath{},
		executedOpportunities: make(map[string]time.Time),
		ctx:                   ctx,
		cancel:                cancel,
	}
}

// Start begins the simplified arbitrage detection and execution
func (ae *ArbitrageEngine) Start() {
	log.Printf(" Starting simplified arbitrage engine - sequential execution only")

	// Start the main arbitrage cycle in a single goroutine
	go ae.arbitrageCycle()
}

// Stop stops the arbitrage engine
func (ae *ArbitrageEngine) Stop() {
	ae.stopMutex.Lock()
	defer ae.stopMutex.Unlock()

	if !ae.stopped {
		ae.stopped = true
		ae.cancel() // Cancel context to stop all goroutines
		log.Printf(" ARBITRAGE ENGINE STOPPED")
	}
}

// arbitrageCycle is the main sequential cycle that processes one arbitrage opportunity at a time
func (ae *ArbitrageEngine) arbitrageCycle() {
	ticker := time.NewTicker(50 * time.Millisecond) // Check every 50ms for faster detection
	defer ticker.Stop()

	cycleCount := 0

	for {
		select {
		case <-ae.ctx.Done():
			log.Printf(" Arbitrage cycle stopped by context cancellation")
			return
		case <-ticker.C:
			cycleCount++
			ae.processArbitrageCycle(cycleCount)
		}
	}
}

// processArbitrageCycle processes one complete arbitrage cycle
func (ae *ArbitrageEngine) processArbitrageCycle(cycleCount int) {
	// Check if we're already executing an arbitrage - FULL LOCK
	ae.executionMutex.Lock()
	if ae.isExecuting {
		ae.executionMutex.Unlock()
		return // Skip this cycle if already executing
	}
	ae.isExecuting = true
	ae.executionMutex.Unlock()

	// Ensure we mark as not executing when done
	defer func() {
		ae.executionMutex.Lock()
		ae.isExecuting = false
		ae.executionMutex.Unlock()
	}()

	// Check cooldown period - reduced for faster execution
	if time.Since(ae.lastExecution) < 2*time.Second {
		return // Skip if we executed recently
	}

	// Log cycle start with path count (only every 1000 cycles)
	ae.pathsLock.RLock()
	pathCount := len(ae.triangularPaths)
	ae.pathsLock.RUnlock()

	// Get market data snapshot to check availability
	snapshot := ae.marketDepths.GetSnapshot()
	availableMarkets := len(snapshot)

	// Only log detailed cycle information every 1000 cycles
	if cycleCount%1000 == 0 {
		log.Printf(" ARBITRAGECYCLE #%d | Scanning %d paths | %d markets available", cycleCount, pathCount, availableMarkets)

		// Show order book data for first 5 available markets
		marketCount := 0
		for market, orderBook := range snapshot {
			if marketCount >= 5 { // Show first 5 markets only
				break
			}
			if orderBook != nil && len(orderBook.Bids) > 0 && len(orderBook.Asks) > 0 {
				latest := orderBook.Latest
				if latest == 0 && len(orderBook.Bids) > 0 && len(orderBook.Asks) > 0 {
					// Calculate mid-price if latest is not available
					bidPrice, _ := strconv.ParseFloat(orderBook.Bids[0].Price, 64)
					askPrice, _ := strconv.ParseFloat(orderBook.Asks[0].Price, 64)
					latest = (bidPrice + askPrice) / 2
				}
				log.Printf("   %s: %s @ %s (bid) | %s @ %s (ask) | latest: %.8f",
					market, orderBook.Bids[0].Price, orderBook.Bids[0].Amount,
					orderBook.Asks[0].Price, orderBook.Asks[0].Amount, latest)
				marketCount++
			}
		}
		if availableMarkets > 5 {
			log.Printf("   ... and %d more markets", availableMarkets-5)
		}
	}

	// Find the best arbitrage opportunity
	opportunity := ae.findBestArbitrageOpportunity(cycleCount)
	if opportunity == nil {
		// Only log "no opportunities" every 1000 cycles
		if cycleCount%1000 == 0 {
			log.Printf("CYCLE #%d | No profitable opportunities found", cycleCount)
		}
		return // No profitable opportunity found
	}

	// Check if this opportunity was already executed recently
	if ae.isOpportunityAlreadyExecuted(*opportunity) {
		// Only log "already executed" every 1000 cycles
		if cycleCount%1000 == 0 {
			log.Printf("CYCLE #%d | Opportunity already executed recently", cycleCount)
		}
		return
	}

	// Mark opportunity as executed
	ae.markOpportunityAsExecuted(*opportunity)

	// Execute the arbitrage opportunity (ALWAYS log successful opportunities)
	log.Printf("EXECUTING ARBITRAGE | Cycle %d | Path: %s→%s→%s | Profit: %.6f%% | Volume: $%.2f",
		cycleCount, opportunity.Path.Market1, opportunity.Path.Market2, opportunity.Path.Market3,
		opportunity.NetProfit*100, opportunity.Volume)

	// Log initial prices and order book data
	log.Printf(" INITIAL OPPORTUNITY PRICES | Leg1: %.8f | Leg2: %.8f | Leg3: %.8f",
		opportunity.Price1, opportunity.Price2, opportunity.Price3)

	// Set execution start time
	opportunity.ExecutionStartTime = time.Now()
	ae.lastExecution = time.Now()

	// FULLY SYNCHRONOUS EXECUTION - Wait for complete triangular trade
	// The engine is now locked and nothing else can interfere
	log.Printf(" ARBITRAGE ENGINE LOCKED - No other operations allowed during execution")

	if ae.config.SimulationMode {
		ae.simulateExecution(*opportunity)
	} else {
		ae.executeRealArbitrage(*opportunity)
	}

	// Wait for execution to complete before continuing to next cycle
	log.Printf(" ARBITRAGE ENGINE UNLOCKED - Ready for next cycle")
	log.Printf(" ARBITRAGECYCLE #%d COMPLETED - Ready for next cycle", cycleCount)
}

// findBestArbitrageOpportunity finds the most profitable arbitrage opportunity
func (ae *ArbitrageEngine) findBestArbitrageOpportunity(cycleCount int) *models.ArbitrageOpportunity {
	ae.pathsLock.RLock()
	paths := make([]models.TriangularPath, len(ae.triangularPaths))
	copy(paths, ae.triangularPaths)
	ae.pathsLock.RUnlock()

	if len(paths) == 0 {
		log.Printf(" No triangular paths available for arbitrage scanning")
		return nil
	}

	// Get snapshot of market depths
	snapshot := ae.marketDepths.GetSnapshot()
	availableMarkets := len(snapshot)

	if availableMarkets == 0 {
		log.Printf(" No market data available for arbitrage scanning")
		return nil
	}

	var bestOpportunity *models.ArbitrageOpportunity
	bestProfit := ae.config.ProfitThreshold
	opportunitiesChecked := 0
	validOpportunities := 0

	// Only log scanning start every 1000 cycles
	if cycleCount%1000 == 0 {
		log.Printf(" Scanning %d triangular paths for opportunities...", len(paths))
	}

	// Check each path for opportunities
	for _, path := range paths {
		opportunitiesChecked++

		opportunity := ae.calculateOpportunity(path, snapshot, time.Now(), cycleCount)
		if opportunity == nil {
			continue
		}

		validOpportunities++

		// Only consider profitable opportunities
		if opportunity.NetProfit >= ae.config.ProfitThreshold && opportunity.NetProfit > bestProfit {
			bestProfit = opportunity.NetProfit
			bestOpportunity = opportunity
			// ALWAYS log new best opportunities immediately
			log.Printf("NEW BEST OPPORTUNITY | Path: %s→%s→%s | Profit: %.6f%% | Volume: $%.2f",
				path.Market1, path.Market2, path.Market3, opportunity.NetProfit*100, opportunity.Volume)
		}
	}

	// Only log scan completion every 1000 cycles
	if cycleCount%1000 == 0 {
		log.Printf(" OPPORTUNITY SCAN COMPLETE | Checked: %d/%d paths | Valid: %d | Best profit: %.6f%%",
			opportunitiesChecked, len(paths), validOpportunities, bestProfit*100)
	}

	return bestOpportunity
}

// executeRealArbitrage executes a real arbitrage trade
func (ae *ArbitrageEngine) executeRealArbitrage(opportunity models.ArbitrageOpportunity) {
	log.Printf("STARTING REAL ARBITRAGE EXECUTION | Path: %s→%s→%s",
		opportunity.Path.Market1, opportunity.Path.Market2, opportunity.Path.Market3)

	// Log the prices we're using for verification
	log.Printf(" ARBITRAGE PRICES | %s: %.8f (%s) | %s: %.8f (%s) | %s: %.8f (%s)",
		opportunity.Path.Market1, opportunity.Price1, opportunity.Path.Direction1,
		opportunity.Path.Market2, opportunity.Price2, opportunity.Path.Direction2,
		opportunity.Path.Market3, opportunity.Price3, opportunity.Path.Direction3)

	// Use static order amount from config
	initialUSDT := ae.config.OrderExecutionSettings.StaticOrderAmount
	log.Printf(" USING STATIC ORDER AMOUNT | Initial USDT: %.6f", initialUSDT)

	// Execute legs sequentially with proper synchronization
	results := make([]*exchange.OrderResult, 0, 3)

	// Leg 1: Buy Asset with USDT (LIMIT ORDER with calculated price)
	// Calculate the asset amount to buy: USDT amount / price
	assetAmountToBuy := initialUSDT / opportunity.Price1
	opportunity.Leg1ExecutionTime = time.Now()

	log.Printf(" LEG 1: Buying %.6f %s with %.6f USDT at limit price %.8f", assetAmountToBuy, opportunity.Path.Asset1, initialUSDT, opportunity.Price1)
	log.Printf(" LEG 1 DEBUG: initialUSDT=%.6f, Price1=%.8f, assetAmountToBuy=%.8f", initialUSDT, opportunity.Price1, assetAmountToBuy)
	log.Printf("TIMING: LEG 1 TIMING: Detection to execution delay: %v", time.Since(opportunity.Timestamp))

	// Use the same price from opportunity calculation for order placement
	result1 := ae.placeAndWaitForOrder(opportunity.Path.Market1, "buy", assetAmountToBuy, opportunity.Price1, "Leg 1")
	if result1 == nil || result1.Status != exchange.OrderStatusFilled {
		log.Printf(" LEG 1 FAILED | Status: %v | Error: %s", result1.Status, result1.ErrorMessage)
		ae.logCurrentOrderBookData(opportunity.Path.Market1, "Leg 1 Failure")
		return // No recovery needed if leg 1 fails
	}
	results = append(results, result1)
	log.Printf(" LEG 1 COMPLETED | Received %.6f %s", result1.FilledAmount, opportunity.Path.Asset1)

	// Leg 2: Sell Asset for USDC (LIMIT ORDER with calculated price) - IMMEDIATE START
	actualAssetReceived := result1.FilledAmount
	if actualAssetReceived <= 0 {
		log.Printf(" INSUFFICIENT ASSET | Cannot continue")
		return
	}

	// Start Leg 2 immediately after Leg 1 completion
	opportunity.Leg2ExecutionTime = time.Now()
	leg1ToLeg2Delay := time.Since(opportunity.Leg1ExecutionTime)
	log.Printf(" LEG 2: Selling %.6f %s for USDC at limit price %.8f", actualAssetReceived, opportunity.Path.Asset1, opportunity.Price2)
	log.Printf(" LEG 2 DEBUG: result1.FilledAmount=%.6f, result1.FilledValue=%.6f, result1.AvgPrice=%.8f",
		result1.FilledAmount, result1.FilledValue, result1.AvgPrice)
	log.Printf("TIMING: LEG 2 TIMING: Leg1 completion to Leg2 start: %v", leg1ToLeg2Delay)

	// Use the same price from opportunity calculation for order placement
	result2 := ae.placeAndWaitForOrder(opportunity.Path.Market2, "sell", actualAssetReceived, opportunity.Price2, "Leg 2")
	if result2 == nil || result2.Status != exchange.OrderStatusFilled {
		log.Printf(" LEG 2 FAILED | Status: %v | Error: %s", result2.Status, result2.ErrorMessage)
		log.Printf(" LEG 2 FAILURE ANALYSIS | Initial Price: %.8f | Current Order Book:", opportunity.Price2)
		ae.logCurrentOrderBookData(opportunity.Path.Market2, "Leg 2 Failure")
		// Attempt to reverse the trade by selling the asset back to USDT
		ae.placeReversalOrder(opportunity.Path.Market1, "sell", actualAssetReceived, "Leg 2 Reversal")
		return
	}
	results = append(results, result2)
	log.Printf(" LEG 2 COMPLETED | Received %.6f USDC", result2.FilledValue)

	// Leg 3: Sell USDC for USDT (MARKET ORDER - no price specified) - IMMEDIATE START
	// Use the actual USDC amount received from the 2nd leg
	actualUSDCReceived := result2.FilledValue
	if actualUSDCReceived <= 0 {
		log.Printf(" INSUFFICIENT USDC | Cannot continue")
		return
	}

	// Start Leg 3 immediately after Leg 2 completion
	opportunity.Leg3ExecutionTime = time.Now()
	leg2ToLeg3Delay := time.Since(opportunity.Leg2ExecutionTime)
	log.Printf(" LEG 3: Selling %.6f USDC for USDT (MARKET ORDER)", actualUSDCReceived)
	log.Printf(" LEG 3 DEBUG: result2.FilledAmount=%.6f, result2.FilledValue=%.6f, result2.AvgPrice=%.8f",
		result2.FilledAmount, result2.FilledValue, result2.AvgPrice)
	log.Printf("TIMING: LEG 3 TIMING: Leg2 completion to Leg3 start: %v", leg2ToLeg3Delay)

	var result3 *exchange.OrderResult
	for i := 0; i < ae.config.FOKOrderSettings.MaxRetryAttempts; i++ {
		log.Printf(" LEG 3 DEBUG: Market=%s, Type=sell, Amount=%.6f, Price=0 (market order), Attempt %d/%d",
			opportunity.Path.Market3, actualUSDCReceived, i+1, ae.config.FOKOrderSettings.MaxRetryAttempts)

		result3 = ae.placeAndWaitForOrder(opportunity.Path.Market3, "sell", actualUSDCReceived, 0, fmt.Sprintf("Leg 3 (Attempt %d)", i+1))
		if result3 != nil && result3.Status == exchange.OrderStatusFilled {
			break // Success
		}
		// Minimal retry delay for faster execution
		retryDelay := time.Duration(1000/ae.config.FOKOrderSettings.FOKPollingFrequencyHz) * time.Millisecond
		if retryDelay > 10*time.Millisecond {
			retryDelay = 10 * time.Millisecond // Cap at 10ms for faster retries
		}
		time.Sleep(retryDelay)
	}

	if result3 == nil || result3.Status != exchange.OrderStatusFilled {
		log.Printf(" LEG 3 FAILED | Status: %v | Error: %s", result3.Status, result3.ErrorMessage)
		log.Printf(" LEG 3 FAILURE ANALYSIS | Initial Price: %.8f | Current Order Book:", opportunity.Price3)
		ae.logCurrentOrderBookData(opportunity.Path.Market3, "Leg 3 Failure")
		// Attempt to reverse the trade by selling the USDC back to USDT
		// Minimal balance update delay for faster execution
		balanceUpdateDelay := time.Duration(1000/ae.config.FOKOrderSettings.FOKPollingFrequencyHz) * time.Millisecond
		if balanceUpdateDelay > 5*time.Millisecond {
			balanceUpdateDelay = 5 * time.Millisecond // Cap at 5ms for faster execution
		}
		time.Sleep(balanceUpdateDelay)
		ae.placeReversalOrder(opportunity.Path.Market3, "sell", actualUSDCReceived, "Leg 3 Reversal")
		return
	}

	results = append(results, result3)
	log.Printf(" LEG 3 COMPLETED | Received %.6f USDT at avg price %.8f", result3.FilledAmount, result3.AvgPrice)

	// Calculate final profit
	actualUSDTReceived := result3.FilledAmount
	netProfit := actualUSDTReceived - initialUSDT
	profitPercentage := (netProfit / initialUSDT) * 100

	log.Printf(" ARBITRAGE COMPLETE | Initial: $%.2f | Final: $%.2f | Net: $%.2f (%.2f%%)",
		initialUSDT, actualUSDTReceived, netProfit, profitPercentage)
	log.Printf(" PROFIT DEBUG: result3.FilledAmount=%.6f, result3.FilledValue=%.6f, result3.AvgPrice=%.8f",
		result3.FilledAmount, result3.FilledValue, result3.AvgPrice)
	log.Printf(" ARBITRAGE SUMMARY: Leg1=%s(%.6f), Leg2=%s(%.6f), Leg3=%s(%.6f)",
		opportunity.Path.Market1, result1.FilledAmount,
		opportunity.Path.Market2, result2.FilledAmount,
		opportunity.Path.Market3, result3.FilledAmount)

	// Update metrics
	ae.metrics.IncrementTrades()
	ae.metrics.AddPnL(netProfit)
}

// placeReversalOrder places a market order to reverse a failed trade
func (ae *ArbitrageEngine) placeReversalOrder(market, orderType string, amount float64, legName string) {
	log.Printf(" REVERSAL TRADE | %s | Market: %s | Type: %s | Amount: %.6f",
		legName, market, orderType, amount)

	// Place a market order to reverse the trade
	result := ae.placeAndWaitForOrder(market, orderType, amount, 0, legName)
	if result == nil {
		log.Printf(" REVERSAL FAILED | %s | Market: %s | Order placement failed", legName, market)
		return
	}
	if result.Status != exchange.OrderStatusFilled {
		log.Printf(" REVERSAL FAILED | %s | Market: %s | Status: %s | Error: %s",
			legName, market, result.Status, result.ErrorMessage)
	} else {
		log.Printf(" REVERSAL COMPLETED | %s | Market: %s | Filled: %.6f",
			legName, market, result.FilledAmount)
	}
}

// placeAndWaitForOrder places an order and waits for completion with optimized timing
func (ae *ArbitrageEngine) placeAndWaitForOrder(market, orderType string, amount, price float64, legName string) *exchange.OrderResult {
	// Create result channel with buffer for immediate response
	orderResultChan := make(chan exchange.OrderResult, 1)

	startTime := time.Now()
	log.Printf(" PLACING ORDER | %s | Market: %s | Type: %s | Amount: %.6f | Price: %.8f",
		legName, market, orderType, amount, price)

	// Place the order
	tracker := ae.client.PlaceFOKOrder(market, orderType, amount, price, orderResultChan)
	if tracker == nil {
		log.Printf(" ORDER PLACEMENT FAILED | %s | Market: %s | Tracker is nil", legName, market)
		return nil
	}

	log.Printf(" ORDER PLACED | %s | Market: %s | OrderID: %s | Placement time: %v",
		legName, market, tracker.OrderID, time.Since(startTime))

	// Wait for order result with optimized timeout
	timeout := time.Duration(ae.config.FOKOrderSettings.FOKTimeoutSeconds) * time.Second

	select {
	case result := <-orderResultChan:
		totalTime := time.Since(startTime)
		log.Printf(" ORDER RESULT | %s | Market: %s | Status: %s | Filled: %.6f | Avg Price: %.8f | Total Time: %v | Error: %s",
			legName, market, result.Status, result.FilledAmount, result.AvgPrice, totalTime, result.ErrorMessage)
		return &result
	case <-time.After(timeout):
		totalTime := time.Since(startTime)
		log.Printf("ORDER TIMEOUT | %s | Market: %s | OrderID: %s | Timeout: %v | Total Time: %v",
			legName, market, tracker.OrderID, timeout, totalTime)
		return &exchange.OrderResult{
			OrderID:       tracker.OrderID,
			Market:        market,
			Type:          orderType,
			Amount:        amount,
			Price:         price,
			Status:        exchange.OrderStatusFailed,
			FilledAmount:  0,
			AvgPrice:      0,
			Fee:           0,
			FeeCurrency:   "",
			ExecutionTime: time.Since(tracker.CreatedAt).Milliseconds(),
			ErrorMessage:  fmt.Sprintf("Order timeout after %v", timeout),
			Timestamp:     time.Now(),
		}
	case <-ae.ctx.Done():
		totalTime := time.Since(startTime)
		log.Printf(" ORDER CANCELLED | %s | Market: %s | Context cancelled | Total Time: %v", legName, market, totalTime)
		return &exchange.OrderResult{
			OrderID:       tracker.OrderID,
			Market:        market,
			Type:          orderType,
			Amount:        amount,
			Price:         price,
			Status:        exchange.OrderStatusCancelled,
			FilledAmount:  0,
			AvgPrice:      0,
			Fee:           0,
			FeeCurrency:   "",
			ExecutionTime: time.Since(tracker.CreatedAt).Milliseconds(),
			ErrorMessage:  "Order cancelled by context",
			Timestamp:     time.Now(),
		}
	}
}

// simulateExecution simulates the execution for testing purposes
func (ae *ArbitrageEngine) simulateExecution(opportunity models.ArbitrageOpportunity) {
	log.Printf("SIMULATION MODE | Path: %s→%s→%s | Expected Profit: %.6f%% | Volume: $%.2f",
		opportunity.Path.Market1, opportunity.Path.Market2, opportunity.Path.Market3,
		opportunity.NetProfit*100, opportunity.Volume)

	// Simulate execution time
	if ae.config.EnableOrderSimulation {
		executionTime := time.Duration(ae.config.OrderExecutionTimeMs) * time.Millisecond
		log.Printf("SIMULATING EXECUTION | Duration: %v", executionTime)
		time.Sleep(executionTime)
	}

	// Update metrics
	ae.metrics.IncrementTrades()
	ae.metrics.AddPnL(opportunity.NetProfit * opportunity.Volume)

	log.Printf("SIMULATION COMPLETE | Total trades: %d | Total PnL: $%.4f",
		ae.metrics.GetSnapshot().TradesExecuted, ae.metrics.GetSnapshot().TotalPnL)
}

// UpdateTriangularPaths updates the cache of triangular arbitrage paths
func (ae *ArbitrageEngine) UpdateTriangularPaths(markets []string, completeAssets []string) {
	log.Printf(" Updating triangular paths from %d markets", len(markets))

	// Get available markets from market depths
	availableMarkets := ae.marketDepths.GetAvailableMarkets()
	availableMarketSet := make(map[string]bool)
	for _, market := range availableMarkets {
		availableMarketSet[market] = true
	}

	log.Printf(" Market validation: %d expected, %d available", len(markets), len(availableMarkets))

	// Filter markets to only include those with actual data
	validMarkets := []string{}
	for _, market := range markets {
		if availableMarketSet[market] {
			validMarkets = append(validMarkets, market)
		}
	}

	log.Printf(" Valid markets: %d", len(validMarkets))

	// Create triangular arbitrage paths
	paths := ae.discoverTriangularArbitrageCycles(validMarkets)

	ae.pathsLock.Lock()
	ae.triangularPaths = paths
	ae.pathsLock.Unlock()

	log.Printf(" Updated triangular paths: %d arbitrage cycles discovered", len(paths))

	// Log first few paths for debugging
	for i, path := range paths {
		if i < 5 {
			log.Printf("   Cycle %d: %s (%s) → %s (%s) → %s (%s) → %s",
				i+1, path.BaseAsset, path.Direction1, path.Asset1, path.Direction2, path.Asset2, path.Direction3, path.BaseAsset)
		}
	}

	if len(paths) > 5 {
		log.Printf("   ... and %d more cycles", len(paths)-5)
	}
}

// discoverTriangularArbitrageCycles discovers triangular arbitrage cycles
func (ae *ArbitrageEngine) discoverTriangularArbitrageCycles(availableMarkets []string) []models.TriangularPath {
	var paths []models.TriangularPath

	log.Printf(" Discovering triangular arbitrage cycles from %d markets", len(availableMarkets))

	// Create market lookup sets
	marketSet := make(map[string]bool)
	usdtMarkets := make(map[string]bool)
	usdcMarkets := make(map[string]bool)

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

	log.Printf(" USDT pairs: %d, USDC pairs: %d", len(usdtMarkets), len(usdcMarkets))

	// Find triangular cycles: USDT → Asset → USDC → USDT
	if len(ae.config.QuoteCurrencies) >= 2 {
		// Check if we have a direct USDC/USDT pair
		hasDirectPair := false
		for _, critical := range ae.config.CriticalMarkets {
			if marketSet[critical] {
				hasDirectPair = true
				break
			}
		}

		if hasDirectPair {
			// Use direct USDC/USDT pair
			var quotePair string
			for _, critical := range ae.config.CriticalMarkets {
				if marketSet[critical] {
					quotePair = critical
					log.Printf(" Using direct quote pair: %s", critical)
					break
				}
			}

			if quotePair != "" {
				for asset := range usdtMarkets {
					if usdcMarkets[asset] {
						// Triangular cycle: USDT → Asset → USDC → USDT
						path := models.TriangularPath{
							BaseAsset:  "USDT",
							Asset1:     asset,
							Asset2:     "USDC",
							Market1:    asset + "USDT",
							Market2:    asset + "USDC",
							Market3:    quotePair,
							Direction1: "buy",
							Direction2: "sell",
							Direction3: "sell",
						}
						paths = append(paths, path)
					}
				}
			}
		}
	}

	log.Printf(" Created %d triangular arbitrage cycles", len(paths))
	return paths
}

// calculateOpportunity calculates the profitability of a triangular arbitrage path
func (ae *ArbitrageEngine) calculateOpportunity(path models.TriangularPath, snapshot map[string]*models.OrderBook, detectionStart time.Time, cycleCount int) *models.ArbitrageOpportunity {
	// Validate market data availability
	market1Data, market2Data, market3Data, isValid := ae.validateMarketData(path, snapshot, detectionStart)
	if !isValid {
		return nil
	}

	// Calculate prices for each market
	price1, price2, price3, err := ae.calculatePrices(path, market1Data, market2Data, market3Data, detectionStart, cycleCount)
	if err != nil {
		return nil
	}

	// Calculate profit
	roundTripRate, netProfit := ae.calculateProfit(path, price1, price2, price3, detectionStart, snapshot)

	// Calculate volume for opportunity calculation (use static amount for consistency)
	volume := ae.config.OrderExecutionSettings.StaticOrderAmount
	if volume <= 0 {
		volume = 3.0 // Default to $3 if not configured
	}

	opportunity := &models.ArbitrageOpportunity{
		Path:            path,
		EstimatedProfit: roundTripRate - 1.0,
		NetProfit:       netProfit,
		Volume:          volume,
		Price1:          price1,
		Price2:          price2,
		Price3:          price3,
		Timestamp:       time.Now(),
		DetectionTime:   time.Since(detectionStart),
	}

	// Set market data
	if market1Data != nil {
		opportunity.Market1Data = *market1Data
	}
	if market2Data != nil {
		opportunity.Market2Data = *market2Data
	}
	if market3Data != nil {
		opportunity.Market3Data = *market3Data
	}

	// Only log detailed information for profitable opportunities (not every 1000 cycles)
	if netProfit > ae.config.ProfitThreshold {
		log.Printf("PROFITABLE OPPORTUNITY | Path: %s→%s→%s | Profit: %.6f%% | Volume: $%.2f",
			path.Market1, path.Market2, path.Market3, netProfit*100, volume)
		log.Printf("   Calculated Prices: %.8f → %.8f → %.8f", price1, price2, price3)
		log.Printf("    VOLUME CHECK PASSED: All markets have sufficient liquidity for StaticOrderAmount: %.6f USDT", ae.config.OrderExecutionSettings.StaticOrderAmount)

		// Show order book details in clear format: Price @ Volume (full precision)
		if market1Data != nil && len(market1Data.Bids) > 0 && len(market1Data.Asks) > 0 {
			log.Printf("   %s (%s): %s @ %s (bid) | %s @ %s (ask)",
				path.Market1, path.Direction1,
				market1Data.Bids[0].Price, market1Data.Bids[0].Amount,
				market1Data.Asks[0].Price, market1Data.Asks[0].Amount)
		}
		if market2Data != nil && len(market2Data.Bids) > 0 && len(market2Data.Asks) > 0 {
			log.Printf("   %s (%s): %s @ %s (bid) | %s @ %s (ask)",
				path.Market2, path.Direction2,
				market2Data.Bids[0].Price, market2Data.Bids[0].Amount,
				market2Data.Asks[0].Price, market2Data.Asks[0].Amount)
		}
		if market3Data != nil && len(market3Data.Bids) > 0 && len(market3Data.Asks) > 0 {
			log.Printf("   %s (%s): %s @ %s (bid) | %s @ %s (ask)",
				path.Market3, path.Direction3,
				market3Data.Bids[0].Price, market3Data.Bids[0].Amount,
				market3Data.Asks[0].Price, market3Data.Asks[0].Amount)
		}
	}

	return opportunity
}

// validateMarketData validates that we have sufficient market data
func (ae *ArbitrageEngine) validateMarketData(path models.TriangularPath, snapshot map[string]*models.OrderBook, detectionStart time.Time) (*models.OrderBook, *models.OrderBook, *models.OrderBook, bool) {
	market1Data, _ := snapshot[path.Market1]
	market2Data, _ := snapshot[path.Market2]
	market3Data, _ := snapshot[path.Market3]

	// Helper function to validate order book data
	validateOrderBookData := func(market string, marketData *models.OrderBook) (*models.OrderBook, bool) {
		if marketData == nil {
			return nil, false
		}

		if len(marketData.Bids) == 0 || len(marketData.Asks) == 0 {
			return nil, false
		}

		bestBidPrice, bidErr := strconv.ParseFloat(marketData.Bids[0].Price, 64)
		bestAskPrice, askErr := strconv.ParseFloat(marketData.Asks[0].Price, 64)

		if bidErr != nil || askErr != nil || bestBidPrice <= 0 || bestAskPrice <= 0 {
			return nil, false
		}

		if bestAskPrice < bestBidPrice {
			return nil, false
		}

		return marketData, true
	}

	// Validate all markets
	var success bool
	market1Data, success = validateOrderBookData(path.Market1, market1Data)
	if !success {
		return nil, nil, nil, false
	}

	market2Data, success = validateOrderBookData(path.Market2, market2Data)
	if !success {
		return nil, nil, nil, false
	}

	market3Data, success = validateOrderBookData(path.Market3, market3Data)
	if !success {
		return nil, nil, nil, false
	}

	return market1Data, market2Data, market3Data, true
}

// getIntendedPriceGeneric calculates the execution price for order placement using generics
// For faster execution:
//   - For buy orders: we want to buy at a slightly higher price (closer to the ask), so we add a small premium.
//   - For sell orders: we want to sell at a slightly lower price (closer to the bid), so we subtract a small discount.
func getIntendedPriceGeneric[T any](
	src T,
	getDepthData func(T, string) []models.Depth,
	getMarket func(T) string,
	direction string,
	buyModifier, sellModifier float64,
) (float64, error) {
	var d *models.Depth
	switch direction {
	case "buy":
		bids := getDepthData(src, "buy")
		if len(bids) == 0 {
			return 0, fmt.Errorf("no asks for %s", getMarket(src))
		}
		d = &bids[0]
		p, err := strconv.ParseFloat(d.Price, 64)
		if err != nil {
			return 0, err
		}
		return p * (1 + buyModifier), nil
	case "sell":
		ask := getDepthData(src, "sell")
		if len(ask) == 0 {
			return 0, fmt.Errorf("no bids for %s", getMarket(src))
		}
		d = &ask[0]
		p, err := strconv.ParseFloat(d.Price, 64)
		if err != nil {
			return 0, err
		}
		return p * (1 - sellModifier), nil
	default:
		return 0, fmt.Errorf("unknown direction %q", direction)
	}
}

func (ae *ArbitrageEngine) getExecuteableData(market1Data, market2Data, market3Data *models.OrderBook, path models.TriangularPath) (float64, float64, float64, error) {
	// Helper function to get the appropriate price list (bids or asks) based on direction
	// with volume checking to ensure sufficient liquidity
	getDepthData := func(marketData *models.OrderBook, direction string) ([]models.Depth, int, error) {
		if marketData == nil {
			return nil, -1, fmt.Errorf("no market data provided to get depth data")
		}

		var priceList []models.Depth
		switch direction {
		case "buy":
			priceList = marketData.Bids
		case "sell":
			priceList = marketData.Asks
		default:
			return nil, -1, fmt.Errorf("invalid market direction: %s", direction)
		}

		if len(priceList) == 0 {
			return nil, -1, fmt.Errorf("no %s data available for market %s", direction, marketData.Market)
		}

		// Check for sufficient volume at each price level
		for idx, depth := range priceList {
			amount, err := strconv.ParseFloat(depth.Amount, 64)
			if err != nil {
				continue
			}
			price, err := strconv.ParseFloat(depth.Price, 64)
			if err != nil {
				continue
			}
			// Convert the amount to USDT amount because static amount is in usdt
			usdtAmount := amount * price

			// If this level has enough volume, return remaining levels
			// Later one we need to encapsulate this calculation bellow for price compression because we might need to use dynamic volume calculation
			if usdtAmount >= ae.config.OrderExecutionSettings.StaticOrderAmount {
				// Return remaining levels with sufficient volume
				if idx+1 < len(priceList) {
					return priceList[idx+1:], idx, nil
				}
				return priceList, idx, nil
			}
		}

		// No levels found with sufficient volume
		return nil, -1, fmt.Errorf("no market level found with enough volume for StaticOrderAmount %.6f USDT, Market: %s, Direction: %s",
			ae.config.OrderExecutionSettings.StaticOrderAmount, marketData.Market, direction)
	}

	// Helper function to get market name
	getMarket := func(marketData *models.OrderBook) string {
		if marketData == nil {
			return "unknown"
		}
		return marketData.Market
	}

	// Wrapper function to adapt getDepthData signature for the generic function
	getDepthDataWrapper := func(marketData *models.OrderBook, direction string) []models.Depth {
		depthList, _, err := getDepthData(marketData, direction)
		if err != nil {
			return nil
		}
		return depthList
	}

	// Calculate prices using the generic function
	price1, err := getIntendedPriceGeneric(
		market1Data,
		getDepthDataWrapper,
		getMarket,
		path.Direction1,
		ae.config.ArbitragePriceModifers.BuyPriceModifer,
		ae.config.ArbitragePriceModifers.SellPriceModifer,
	)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("market1 price calculation failed: %v", err)
	}

	price2, err := getIntendedPriceGeneric(
		market2Data,
		getDepthDataWrapper,
		getMarket,
		path.Direction2,
		ae.config.ArbitragePriceModifers.BuyPriceModifer,
		ae.config.ArbitragePriceModifers.SellPriceModifer,
	)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("market2 price calculation failed: %v", err)
	}

	price3, err := getIntendedPriceGeneric(
		market3Data,
		getDepthDataWrapper,
		getMarket,
		path.Direction3,
		ae.config.ArbitragePriceModifers.BuyPriceModifer,
		ae.config.ArbitragePriceModifers.SellPriceModifer,
	)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("market3 price calculation failed: %v", err)
	}

	return price1, price2, price3, nil
}

func (ae *ArbitrageEngine) calculatePrices(path models.TriangularPath, market1Data, market2Data, market3Data *models.OrderBook, detectionStart time.Time, cycleCount int) (float64, float64, float64, error) {
	// Get prices for all markets using the generic function
	price1, price2, price3, err := ae.getExecuteableData(market1Data, market2Data, market3Data, path)
	if err != nil {
		return 0, 0, 0, err
	}

	// Validate prices
	if price1 <= 0 || price2 <= 0 || price3 <= 0 {
		return 0, 0, 0, fmt.Errorf("invalid prices: %.8f, %.8f, %.8f", price1, price2, price3)
	}

	return price1, price2, price3, nil
}

// calculateProfit calculates the profit using the correct formula
func (ae *ArbitrageEngine) calculateProfit(path models.TriangularPath, price1, price2, price3 float64, detectionStart time.Time, snapshot map[string]*models.OrderBook) (float64, float64) {
	// Start with initial USDT amount (use static amount for opportunity calculation)
	initialUSDT := ae.config.OrderExecutionSettings.StaticOrderAmount
	if initialUSDT <= 0 {
		initialUSDT = 3.0 // Default to $3 if not configured
	}

	// Get trading fees
	fee1 := ae.getTradingFee(path.Market1)
	fee2 := ae.getTradingFee(path.Market2)
	fee3 := ae.getTradingFee(path.Market3)

	// Calculate the triangular arbitrage path:
	// Step 1: Buy Asset1 with USDT (spend USDT to get Asset1)
	asset1Amount := initialUSDT / price1 * (1.0 - fee1)

	// Step 2: Sell Asset1 for USDC (get USDC)
	usdcAmount := asset1Amount * price2 * (1.0 - fee2)

	// Step 3: Sell USDC for USDT (get back USDT)
	finalUSDT := usdcAmount * price3 * (1.0 - fee3)

	// Calculate profit
	// The profit is the difference between the final USDT and the initial USDT
	netProfit := finalUSDT - initialUSDT

	// The profit percentage is the net profit divided by the initial USDT
	profitPercentage := 0.0
	if initialUSDT > 0 {
		profitPercentage = netProfit / initialUSDT
	}

	// The roundTripRate is the ratio of final to initial USDT
	roundTripRate := 0.0
	if initialUSDT > 0 {
		roundTripRate = finalUSDT / initialUSDT
	}

	return roundTripRate, profitPercentage
}

// calculateVolume calculates the volume based on configuration
func (ae *ArbitrageEngine) calculateVolume(path models.TriangularPath, snapshot map[string]*models.OrderBook) float64 {
	if ae.config.OrderExecutionSettings.OrderAmountType == "static" {
		return ae.config.OrderExecutionSettings.StaticOrderAmount
	}

	// Dynamic mode: calculate based on order book depth and available balance
	availableBalance := ae.config.OrderExecutionSettings.AccountBalance
	maxVolumeFromDepth := ae.calculateMaxVolumeFromOrderBook(path, snapshot)
	balanceVolume := availableBalance * ae.config.OrderExecutionSettings.DynamicOrderPercentage
	volume := math.Min(balanceVolume, maxVolumeFromDepth)
	volume = math.Min(volume, ae.config.OrderExecutionSettings.MaxOrderAmount)

	// Ensure minimum volume
	minVolume := 0.001
	if volume < minVolume {
		volume = minVolume
	}

	return volume
}

// getTradingFee returns the trading fee for a market
func (ae *ArbitrageEngine) getTradingFee(market string) float64 {
	if fee, exists := ae.config.TradingFees[market]; exists {
		return fee
	}
	return ae.config.DefaultTradingFee
}

// calculateMaxVolumeFromOrderBook calculates the maximum volume available
func (ae *ArbitrageEngine) calculateMaxVolumeFromOrderBook(path models.TriangularPath, snapshot map[string]*models.OrderBook) float64 {
	var maxVolume float64 = math.MaxFloat64

	markets := []string{path.Market1, path.Market2, path.Market3}
	directions := []string{path.Direction1, path.Direction2, path.Direction3}

	for i, market := range markets {
		marketData, exists := snapshot[market]
		if !exists {
			continue
		}

		var availableVolume float64
		if directions[i] == "buy" {
			availableVolume = ae.calculateAvailableVolumeFromAsks(marketData)
		} else {
			availableVolume = ae.calculateAvailableVolumeFromBids(marketData)
		}

		// Convert to USD value for comparison
		var usdVolume float64
		if strings.HasSuffix(market, "USDT") {
			usdVolume = availableVolume
		} else if strings.HasSuffix(market, "USDC") {
			if usdcusdtData, exists := snapshot["USDCUSDT"]; exists && len(usdcusdtData.Bids) > 0 {
				usdcRate, _ := strconv.ParseFloat(usdcusdtData.Bids[0].Price, 64)
				usdVolume = availableVolume * usdcRate
			} else {
				usdVolume = availableVolume
			}
		} else {
			usdVolume = availableVolume * 0.1
		}

		if usdVolume < maxVolume {
			maxVolume = usdVolume
		}
	}

	if maxVolume < 0.001 {
		maxVolume = 0.001
	}

	return maxVolume
}

// calculateAvailableVolumeFromAsks calculates available volume from ask orders
func (ae *ArbitrageEngine) calculateAvailableVolumeFromAsks(marketData *models.OrderBook) float64 {
	var totalVolume float64

	if marketData == nil || len(marketData.Asks) == 0 {
		return 0
	}

	for _, ask := range marketData.Asks {
		price, _ := strconv.ParseFloat(ask.Price, 64)
		amount, _ := strconv.ParseFloat(ask.Amount, 64)
		volumeUSD := price * amount
		totalVolume += volumeUSD

		if len(marketData.Asks) > ae.config.OrderBookDepthLimit {
			break
		}
	}

	return totalVolume
}

// calculateAvailableVolumeFromBids calculates available volume from bid orders
func (ae *ArbitrageEngine) calculateAvailableVolumeFromBids(marketData *models.OrderBook) float64 {
	var totalVolume float64

	if marketData == nil || len(marketData.Bids) == 0 {
		return 0
	}

	for _, bid := range marketData.Bids {
		price, _ := strconv.ParseFloat(bid.Price, 64)
		amount, _ := strconv.ParseFloat(bid.Amount, 64)
		volumeUSD := price * amount
		totalVolume += volumeUSD

		if len(marketData.Bids) > ae.config.OrderBookDepthLimit {
			break
		}
	}

	return totalVolume
}

// isOpportunityAlreadyExecuted checks if an opportunity was already executed recently
func (ae *ArbitrageEngine) isOpportunityAlreadyExecuted(opportunity models.ArbitrageOpportunity) bool {
	key := fmt.Sprintf("%s-%s-%s-%.8f-%.8f-%.8f",
		opportunity.Path.Market1, opportunity.Path.Market2, opportunity.Path.Market3,
		opportunity.Price1, opportunity.Price2, opportunity.Price3)

	ae.executedMutex.RLock()
	lastExecuted, exists := ae.executedOpportunities[key]
	ae.executedMutex.RUnlock()

	if !exists {
		return false
	}

	// Check if it was executed within the last 30 seconds
	cooldownPeriod := 30 * time.Second
	return time.Since(lastExecuted) < cooldownPeriod
}

// markOpportunityAsExecuted marks an opportunity as executed
func (ae *ArbitrageEngine) markOpportunityAsExecuted(opportunity models.ArbitrageOpportunity) {
	key := fmt.Sprintf("%s-%s-%s-%.8f-%.8f-%.8f",
		opportunity.Path.Market1, opportunity.Path.Market2, opportunity.Path.Market3,
		opportunity.Price1, opportunity.Price2, opportunity.Price3)

	ae.executedMutex.Lock()
	ae.executedOpportunities[key] = time.Now()
	ae.executedMutex.Unlock()
}

// logCurrentOrderBookData logs the current order book data for analysis
func (ae *ArbitrageEngine) logCurrentOrderBookData(market, context string) {
	// Get current market data
	currentData, exists := ae.marketDepths.Load(market)
	if !exists {
		log.Printf(" %s | Market: %s | No current order book data available", context, market)
		return
	}

	log.Printf(" %s | Market: %s | Current Order Book:", context, market)

	// Log bids (top 5)
	if len(currentData.Bids) > 0 {
		log.Printf("   BIDS (top 5):")
		for i, bid := range currentData.Bids {
			if i >= 5 {
				break
			}
			log.Printf("     Level %d: %s @ %s", i+1, bid.Price, bid.Amount)
		}
	} else {
		log.Printf("   BIDS: No data available")
	}

	// Log asks (top 5)
	if len(currentData.Asks) > 0 {
		log.Printf("   ASKS (top 5):")
		for i, ask := range currentData.Asks {
			if i >= 5 {
				break
			}
			log.Printf("     Level %d: %s @ %s", i+1, ask.Price, ask.Amount)
		}
	} else {
		log.Printf("   ASKS: No data available")
	}
}
