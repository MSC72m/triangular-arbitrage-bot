package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

// LogLevel represents the logging level
type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARN
	ERROR
	FATAL
)

// GlobalLogger is a singleton logger instance
type GlobalLogger struct {
	level LogLevel
}

var globalLogger *GlobalLogger

// GetLogger returns the global logger instance
func GetLogger() *GlobalLogger {
	if globalLogger == nil {
		globalLogger = &GlobalLogger{
			level: INFO, // Default level
		}
	}
	return globalLogger
}

// SetLevel sets the logging level
func (l *GlobalLogger) SetLevel(level LogLevel) {
	l.level = level
}

// shouldLog checks if a message should be logged based on current level
func (l *GlobalLogger) shouldLog(level LogLevel) bool {
	return level >= l.level
}

// formatMessage formats a log message with timestamp and level
func (l *GlobalLogger) formatMessage(level LogLevel, format string, args ...interface{}) string {
	levelStr := ""
	switch level {
	case DEBUG:
		levelStr = "DEBUG"
	case INFO:
		levelStr = "INFO"
	case WARN:
		levelStr = "WARN"
	case ERROR:
		levelStr = "ERROR"
	case FATAL:
		levelStr = "FATAL"
	}

	message := fmt.Sprintf(format, args...)
	return fmt.Sprintf("[%s] %s", levelStr, message)
}

// Debug logs a debug message
func (l *GlobalLogger) Debug(format string, args ...interface{}) {
	if l.shouldLog(DEBUG) {
		message := l.formatMessage(DEBUG, format, args...)
		log.Println(message)
	}
}

// Info logs an info message
func (l *GlobalLogger) Info(format string, args ...interface{}) {
	if l.shouldLog(INFO) {
		message := l.formatMessage(INFO, format, args...)
		log.Println(message)
	}
}

// Warn logs a warning message
func (l *GlobalLogger) Warn(format string, args ...interface{}) {
	if l.shouldLog(WARN) {
		message := l.formatMessage(WARN, format, args...)
		log.Println(message)
	}
}

// Error logs an error message
func (l *GlobalLogger) Error(format string, args ...interface{}) {
	if l.shouldLog(ERROR) {
		message := l.formatMessage(ERROR, format, args...)
		log.Println(message)
	}
}

// Fatal logs a fatal message and exits
func (l *GlobalLogger) Fatal(format string, args ...interface{}) {
	if l.shouldLog(FATAL) {
		message := l.formatMessage(FATAL, format, args...)
		log.Fatalln(message)
	}
}

// Printf is a convenience method for backward compatibility
func (l *GlobalLogger) Printf(format string, args ...interface{}) {
	l.Info(format, args...)
}

// Println is a convenience method for backward compatibility
func (l *GlobalLogger) Println(args ...interface{}) {
	l.Info("%v", args...)
}

// Close is a no-op for compatibility (log module handles file closing)
func (l *GlobalLogger) Close() {
	// No-op - log module handles file closing automatically
}

// SetupLogger configures the global logger with file output
func SetupLogger(config *Config) {
	// Create log directory
	logDir := "log"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		fmt.Printf("Failed to create log directory: %v\n", err)
		return
	}

	// Generate log file name with timestamp
	timestamp := time.Now().Format("2006-01-02")
	logFileName := filepath.Join(logDir, fmt.Sprintf("arbitrage-bot-%s.log", timestamp))

	// Open log file
	logFile, err := os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Printf("Failed to open log file: %v\n", err)
		return
	}

	// Configure log module to write to both file and stdout
	log.SetOutput(logFile)

	// Set log format to include timestamp
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	// Create logger instance
	logger := GetLogger()

	// Set log level based on config
	switch config.LogLevel {
	case "debug":
		logger.SetLevel(DEBUG)
	case "info":
		logger.SetLevel(INFO)
	case "warn":
		logger.SetLevel(WARN)
	case "error":
		logger.SetLevel(ERROR)
	default:
		logger.SetLevel(INFO)
	}

	logger.Info("Logger initialized with level: %s", config.LogLevel)
}
