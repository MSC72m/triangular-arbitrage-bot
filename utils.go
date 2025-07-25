package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

func loadCredentials() error {
	// Check if .env file exists
	file, err := os.Open(".env")
	if err != nil {
		return fmt.Errorf("no .env file found")
	}
	defer file.Close()

	credentials := map[string]string{}
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			fmt.Println("Skipping malformed line:", line)
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		if key != "" && value != "" {
			credentials[key] = value
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading .env file: %v", err)
	}

	// Export All Credentials
	for key, value := range credentials {
		fmt.Printf("Loaded ENV key and value. Key was: %s\n", key)
		os.Setenv(key, value)
	}

	return nil
}

func getEnv[T any](key string) (T, error) {
	value := os.Getenv(key)
	var result T
	err := json.Unmarshal([]byte(value), &result)
	if err != nil {
		return result, err
	}
	return result, nil
}

func setEnv[T any](key string, value T) error {
	jsonValue, err := json.Marshal(value)
	if err != nil {
		return err
	}
	os.Setenv(key, string(jsonValue))
	return nil
}

func urlQueryEscape(s string) string {
	replacer := strings.NewReplacer(
		" ", "%20",
		"+", "%2B",
		"=", "%3D",
		"&", "%26",
		"?", "%3F",
		"/", "%2F",
		"%", "%25",
	)
	return replacer.Replace(s)
}
