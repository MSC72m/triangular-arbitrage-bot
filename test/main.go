package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
)

func main() {
	// Markets to check
	markets := []string{"SUIUSDT", "CAKEUSDT", "KDAUSDT", "LINKUSDT"}

	fmt.Printf("Fetching prices for: %v\n", markets)

	for _, market := range markets {
		price, err := getMarketPrice(market)
		if err != nil {
			log.Printf("Error getting %s price: %v", market, err)
			continue
		}
		fmt.Printf("%s: $%.8f\n", market, price)
	}
}

func getMarketPrice(market string) (float64, error) {
	url := fmt.Sprintf("https://api.coinex.com/v1/market/ticker?market=%s", market)

	resp, err := http.Get(url)
	if err != nil {
		return 0, fmt.Errorf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	var response map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return 0, fmt.Errorf("JSON decode failed: %v", err)
	}

	// Parse response
	data, ok := response["data"].(map[string]interface{})
	if !ok {
		return 0, fmt.Errorf("invalid response format")
	}

	ticker, ok := data["ticker"].(map[string]interface{})
	if !ok {
		return 0, fmt.Errorf("invalid ticker format")
	}

	lastPriceStr, ok := ticker["last"].(string)
	if !ok {
		return 0, fmt.Errorf("missing last price")
	}

	price, err := strconv.ParseFloat(lastPriceStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid price format: %v", err)
	}

	return price, nil
}
