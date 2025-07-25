package main

import (
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

func main() {
	err := loadCredentials()
	if err != nil {
		fmt.Println(err)
	}

	var wg sync.WaitGroup

	httpClient := newHttpClient()
	httpClient.setheaders(map[string]string{
		"Content-Type": "application/json",
		"User-Agent":   "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
	}).setClient(http.DefaultClient)

	coinexClient := NewCoinexClient(httpClient)

	// Test connection first
	fmt.Println("Testing connection...")
	testResponse, err := coinexClient.TestConnection()
	if err != nil {
		log.Printf("Connection test failed: %v", err)
	} else {
		fmt.Printf("Connection test response: %s\n", testResponse)
	}

	// Authentication headers are now set by the CoinEx client for each request

	for i := range 5 {
		wg.Add(1)
		go func() {
			balance, err := coinexClient.GetBalance()
			if err != nil {
				log.Printf("GetBalance failed: %v", err)
				wg.Done()
				return
			}
			fmt.Println(balance)
			wg.Done()
			time.Sleep(time.Duration(i) * 100 * time.Millisecond)
		}()
	}
	wg.Wait()
}
