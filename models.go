package main

import (
	"encoding/json"
	"fmt"
	"time"
)

// Depth represents a single price level in the order book.
type Depth struct {
	Price  string `json:"price"`
	Amount string `json:"amount"`
}

// UnmarshalJSON custom unmarshaler for Depth to handle array format [price, amount]
func (d *Depth) UnmarshalJSON(data []byte) error {
	var v []string
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	if len(v) != 2 {
		return fmt.Errorf("invalid Depth array length: %d", len(v))
	}
	d.Price = v[0]
	d.Amount = v[1]
	return nil
}

// OrderBook represents the full order book for a market.
type OrderBook struct {
	Asks []Depth `json:"asks"`
	Bids []Depth `json:"bids"`
}

// ArbitrageOpportunity represents a profitable triangular arbitrage path.
// This struct will be logged to a file.
type ArbitrageOpportunity struct {
	Path            string    `json:"path"`
	EstimatedProfit float64   `json:"estimatedProfit"`
	Timestamp       time.Time `json:"timestamp"`
	Leg1Market      string    `json:"leg1Market"`
	Leg1OrderBook   OrderBook `json:"leg1OrderBook"`
	Leg2Market      string    `json:"leg2Market"`
	Leg2OrderBook   OrderBook `json:"leg2OrderBook"`
	Leg3Market      string    `json:"leg3Market"`
	Leg3OrderBook   OrderBook `json:"leg3OrderBook"`
}
