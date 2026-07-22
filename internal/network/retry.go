package network

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"triangular-arbitrage-bot/pkg/models"
)

// Add method to get order book via REST API as fallback
func (c *HttpClient) GetOrderBookREST(market string) (*models.OrderBook, error) {
	log.Printf(" Making REST API call for %s order book...", market)

	// Add timestamp to avoid cached responses
	timestamp := time.Now().Unix()
	params := map[string]string{
		"url":    fmt.Sprintf("%s/market/depth?market=%s&merge=0&limit=%d&_t=%d", c.config.APIBaseURL, market, c.config.OrderBookDepthLimit, timestamp),
		"method": GET,
	}

	response, err := c.PerformRequest(params, GET)
	if err != nil {
		log.Printf(" REST API request failed for %s: %v", market, err)
		return nil, err
	}

	// Parse CoinEx depth response
	data, ok := response["data"].(map[string]interface{})
	if !ok {
		log.Printf(" Invalid response format for %s: missing data", market)
		log.Printf(" DEBUG: Response structure: %+v", response)
		return nil, fmt.Errorf("invalid response format")
	}

	log.Printf(" DEBUG: Data keys: %d", len(data))

	asks, ok := data["asks"].([]interface{})
	if !ok {
		log.Printf(" Invalid asks format for %s", market)
		return nil, fmt.Errorf("invalid asks format")
	}

	bids, ok := data["bids"].([]interface{})
	if !ok {
		log.Printf(" Invalid bids format for %s", market)
		log.Printf(" DEBUG: Bids type: %T, value: %+v", data["bids"], data["bids"])
		return nil, fmt.Errorf("invalid bids format")
	}

	orderBook := &models.OrderBook{
		Asks: []models.Depth{},
		Bids: []models.Depth{},
	}

	// Convert asks
	for _, ask := range asks {
		if askArray, ok := ask.([]interface{}); ok && len(askArray) >= 2 {
			if priceStr, ok := askArray[0].(string); ok {
				if amountStr, ok := askArray[1].(string); ok {
					orderBook.Asks = append(orderBook.Asks, models.Depth{
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
					orderBook.Bids = append(orderBook.Bids, models.Depth{
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
		log.Printf(" REST API order book parsed for %s: %d bids, %d asks, latest: %.8f", market, len(orderBook.Bids), len(orderBook.Asks), orderBook.Latest)
	} else {
		// Don't fail the entire order book fetch if ticker is unavailable
		log.Printf(" REST API order book parsed for %s: %d bids, %d asks, ticker unavailable: %v", market, len(orderBook.Bids), len(orderBook.Asks), err)
		// Set a default latest price based on the best bid/ask if available
		if len(orderBook.Bids) > 0 && len(orderBook.Asks) > 0 {
			bidPrice, _ := strconv.ParseFloat(orderBook.Bids[0].Price, 64)
			askPrice, _ := strconv.ParseFloat(orderBook.Asks[0].Price, 64)
			orderBook.Latest = (bidPrice + askPrice) / 2 // Use mid-price as fallback
			log.Printf(" Using mid-price as latest for %s: %.8f (bid: %.8f, ask: %.8f)", market, orderBook.Latest, bidPrice, askPrice)
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

	response, err := c.PerformRequest(params, GET)
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

// AddFailedMarket adds a market to the retry queue
func (c *HttpClient) AddFailedMarket(market string) {
	c.failedMarketsMu.Lock()
	defer c.failedMarketsMu.Unlock()
	c.failedMarkets[market] = time.Now()
	log.Printf(" Added %s to WebSocket retry queue", market)
}

// RemoveFailedMarket removes a market from the retry queue
func (c *HttpClient) RemoveFailedMarket(market string) {
	c.failedMarketsMu.Lock()
	defer c.failedMarketsMu.Unlock()
	delete(c.failedMarkets, market)
	log.Printf(" Removed %s from WebSocket retry queue", market)
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
func (c *HttpClient) ProcessRetryQueue(marketDepths *models.MarketDepths) {
	readyMarkets := c.GetFailedMarkets()
	if len(readyMarkets) == 0 {
		return
	}

	log.Printf(" Processing retry queue: %d markets ready for retry", len(readyMarkets))

	successful := []string{}
	failed := []string{}

	for _, market := range readyMarkets {
		// First try to get initial data via REST API
		orderBook, err := c.GetOrderBookREST(market)
		if err != nil {
			failed = append(failed, market)
			log.Printf(" REST API retry failed for %s: %v", market, err)
			continue
		}

		// Store the initial data
		marketDepths.Store(market, orderBook)

		// Then subscribe to WebSocket with incremental updates
		if err := c.SubscribeWebSocket([]string{market}, false); err != nil {
			failed = append(failed, market)
			log.Printf(" WebSocket retry failed for %s: %v", market, err)
		} else {
			successful = append(successful, market)
			c.RemoveFailedMarket(market)
			log.Printf(" Retry successful for %s", market)
		}

		// Small delay between retries
		time.Sleep(100 * time.Millisecond)
	}

	log.Printf(" Retry queue processed: %d successful, %d failed", len(successful), len(failed))
}
