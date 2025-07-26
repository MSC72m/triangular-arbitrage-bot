package main

import (
	"fmt"
	"time"

	"github.com/gorilla/websocket"
)

type WebSocketClient struct {
	conn          *websocket.Conn
	url           string
	subscriptions map[string]bool
	dataFeed      chan []byte
}

func NewWebSocketClient(host string) *WebSocketClient {
	// Use the basic CoinEx WebSocket endpoint that was connecting successfully
	return &WebSocketClient{
		url:           fmt.Sprintf("wss://%s/", host),
		subscriptions: make(map[string]bool),
		dataFeed:      make(chan []byte),
	}
}

func (wsc *WebSocketClient) Connect() error {
	fmt.Printf("Connecting to %s", wsc.url)

	// Add proper headers for CoinEx WebSocket
	headers := map[string][]string{
		"User-Agent": {"triangular-arbitrage-bot/1.0"},
	}

	dialer := &websocket.Dialer{
		HandshakeTimeout: 45 * time.Second,
	}

	conn, response, err := dialer.Dial(wsc.url, headers)
	if err != nil {
		fmt.Printf("WebSocket dial failed. Response: %+v", response)
		return fmt.Errorf("websocket dial error: %w", err)
	}
	wsc.conn = conn
	fmt.Printf("WebSocket connected successfully")

	go wsc.readMessages()

	// Send ping message to keep connection alive
	go wsc.pingLoop()

	return nil
}

func (wsc *WebSocketClient) pingLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if wsc.conn == nil {
			return
		}

		// Send ping message in CoinEx format
		pingMsg := map[string]interface{}{
			"method": "server.ping",
			"params": []interface{}{},
			"id":     time.Now().UnixNano(),
		}

		if err := wsc.conn.WriteJSON(pingMsg); err != nil {
			fmt.Printf("Failed to send ping: %v", err)
			return
		}
	}
}

func (wsc *WebSocketClient) readMessages() {
	defer func() {
		if wsc.conn != nil {
			wsc.conn.Close()
		}
		fmt.Println("WebSocket connection closed.")
	}()

	for {
		if wsc.conn == nil {
			return
		}

		_, message, err := wsc.conn.ReadMessage()
		if err != nil {
			fmt.Printf("WebSocket read error: %v", err)
			return
		}

		// Only log first few messages to avoid spam
		if len(wsc.subscriptions) < 5 {
			fmt.Printf("Received raw message: %s", string(message))
		}
		wsc.dataFeed <- message
	}
}

func (wsc *WebSocketClient) Subscribe(market string) error {
	if wsc.conn == nil {
		return fmt.Errorf("websocket not connected")
	}

	if wsc.subscriptions[market] {
		return nil // Already subscribed
	}

	// Try CoinEx's actual subscription format - different parameter order and types
	subMsg := map[string]interface{}{
		"method": "depth.subscribe",
		"params": []interface{}{market, 5, "0.0001"}, // market, limit, merge_precision
		"id":     time.Now().UnixNano(),
	}

	if err := wsc.conn.WriteJSON(subMsg); err != nil {
		return fmt.Errorf("websocket write error: %w", err)
	}

	fmt.Printf("Subscribed to %s\n", market)
	wsc.subscriptions[market] = true

	// Add small delay to avoid overwhelming the server
	time.Sleep(2 * time.Millisecond)

	return nil
}

func (wsc *WebSocketClient) DataFeed() <-chan []byte {
	return wsc.dataFeed
}
