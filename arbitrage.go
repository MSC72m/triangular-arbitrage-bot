package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// MarketDepths stores the latest order book for each market, protected by a mutex.
type MarketDepths struct {
	m      sync.RWMutex
	depths map[string]*OrderBook
}

// NewMarketDepths creates a new MarketDepths instance.
func NewMarketDepths() *MarketDepths {
	return &MarketDepths{
		depths: make(map[string]*OrderBook),
	}
}

// Store updates the order book for a given market.
func (md *MarketDepths) Store(market string, ob *OrderBook) {
	md.m.Lock()
	defer md.m.Unlock()
	md.depths[market] = ob
}

// Load retrieves the order book for a given market.
func (md *MarketDepths) Load(market string) (*OrderBook, bool) {
	md.m.RLock()
	defer md.m.RUnlock()
	ob, ok := md.depths[market]
	return ob, ok
}

// GetAvailableMarkets returns a list of markets that have order book data
func (md *MarketDepths) GetAvailableMarkets() []string {
	md.m.RLock()
	defer md.m.RUnlock()
	var markets []string
	for market := range md.depths {
		markets = append(markets, market)
	}
	return markets
}

var opportunityCount int

func findArbitrageOpportunities(marketList map[string]interface{}, config *Config, marketDepths *MarketDepths) {
	opportunityCount++

	// Get available markets with actual data
	availableMarkets := marketDepths.GetAvailableMarkets()

	if opportunityCount%1000 == 0 {
		fmt.Printf("Scan #%d: %d markets available: %v\n", opportunityCount, len(availableMarkets), availableMarkets)
	}

	if len(availableMarkets) < 3 {
		return // Need at least 3 markets for triangular arbitrage
	}

	// Create direct triangular paths from available markets
	pathsChecked := 0
	pathsWithData := 0

	// Look for triangular arbitrage patterns in available markets
	for i, market1 := range availableMarkets {
		for j, market2 := range availableMarkets {
			if i == j {
				continue
			}
			for k, market3 := range availableMarkets {
				if k == i || k == j {
					continue
				}

				pathsChecked++

				// Check if these 3 markets form a valid triangular path
				if isValidTriangularPath(market1, market2, market3) {
					pathsWithData++

					depth1, _ := marketDepths.Load(market1)
					depth2, _ := marketDepths.Load(market2)
					depth3, _ := marketDepths.Load(market3)

					// Calculate arbitrage opportunity
					profit := calculateTriangularProfit(depth1, depth2, depth3, market1, market2, market3)

					if profit > config.ProfitThreshold {
						fmt.Printf("🚨 ARBITRAGE! %s->%s->%s: %.4f%% profit\n",
							market1, market2, market3, profit*100)
					}
				}
			}
		}
	}

	if opportunityCount%1000 == 0 {
		fmt.Printf("Checked %d paths, %d valid triangular paths found\n", pathsChecked, pathsWithData)
	}
}

// Check if three markets can form a triangular arbitrage path
func isValidTriangularPath(market1, market2, market3 string) bool {
	// Extract currencies from market pairs
	currencies := make(map[string]int)

	// Count currency occurrences
	for _, market := range []string{market1, market2, market3} {
		base, quote := extractCurrencies(market)
		if base != "" && quote != "" {
			currencies[base]++
			currencies[quote]++
		}
	}

	// For a valid triangular path, we should have exactly 3 currencies, each appearing twice
	if len(currencies) != 3 {
		return false
	}

	for _, count := range currencies {
		if count != 2 {
			return false
		}
	}

	return true
}

// Extract base and quote currencies from market string
func extractCurrencies(market string) (string, string) {
	// Try common quote currencies
	quoteCurrencies := []string{"USDT", "USDC", "BTC", "ETH"}

	for _, quote := range quoteCurrencies {
		if strings.HasSuffix(market, quote) {
			base := strings.TrimSuffix(market, quote)
			if base != "" {
				return base, quote
			}
		}
	}

	return "", ""
}

// Calculate triangular arbitrage profit
func calculateTriangularProfit(depth1, depth2, depth3 *OrderBook, market1, market2, market3 string) float64 {
	if len(depth1.Asks) == 0 || len(depth1.Bids) == 0 ||
		len(depth2.Asks) == 0 || len(depth2.Bids) == 0 ||
		len(depth3.Asks) == 0 || len(depth3.Bids) == 0 {
		return 0
	}

	// Get best prices
	ask1, _ := strconv.ParseFloat(depth1.Asks[0].Price, 64)
	bid1, _ := strconv.ParseFloat(depth1.Bids[0].Price, 64)
	ask2, _ := strconv.ParseFloat(depth2.Asks[0].Price, 64)
	bid2, _ := strconv.ParseFloat(depth2.Bids[0].Price, 64)
	ask3, _ := strconv.ParseFloat(depth3.Asks[0].Price, 64)
	bid3, _ := strconv.ParseFloat(depth3.Bids[0].Price, 64)

	if ask1 <= 0 || ask2 <= 0 || ask3 <= 0 {
		return 0
	}

	// Try different arbitrage directions and return the best profit
	profit1 := (1/ask1)*bid2*bid3 - 1 // Buy market1, sell market2, sell market3
	profit2 := (1/ask2)*bid1*bid3 - 1 // Buy market2, sell market1, sell market3
	profit3 := (1/ask3)*bid1*bid2 - 1 // Buy market3, sell market1, sell market2

	maxProfit := profit1
	if profit2 > maxProfit {
		maxProfit = profit2
	}
	if profit3 > maxProfit {
		maxProfit = profit3
	}

	return maxProfit
}
