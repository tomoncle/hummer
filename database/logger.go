/*
 * Copyright 2025 tomoncle.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package database

import (
	"fmt"
	"log"
	"os"
	"time"
)

var dbLogger Logger

// LogLevel defines the severity for database logging.
type LogLevel int

const (
	// LogLevelDebug enables verbose diagnostic logging.
	LogLevelDebug LogLevel = iota
	// LogLevelInfo records general informational events.
	LogLevelInfo
	// LogLevelWarn highlights potential issues.
	LogLevelWarn
	// LogLevelError reports errors and failures.
	LogLevelError
)

func (l LogLevel) String() string {
	switch l {
	case LogLevelDebug:
		return "DEBUG"
	case LogLevelInfo:
		return "INFO"
	case LogLevelWarn:
		return "WARN"
	case LogLevelError:
		return "ERROR"
	default:
		return "DEBUG"
	}
}

type Logger interface {
	// Debug writes a debug-level log message.
	Debug(msg string, fields ...interface{})
	// Info writes an info-level log message.
	Info(msg string, fields ...interface{})
	// Warn writes a warning-level log message.
	Warn(msg string, fields ...interface{})
	// Error writes an error-level log message.
	Error(msg string, fields ...interface{})
}

// InitLogger sets the global database logger.
func InitLogger(logger Logger) {
	dbLogger = logger
}

// GetDBLogger returns the global database logger, creating a default
// logger at info level if none has been initialized.
func GetDBLogger() Logger {
	if dbLogger == nil {
		return NewDefaultLogger(LogLevelInfo)
	}
	return dbLogger
}

// DefaultLogger is a simple standard output logger implementation.
type DefaultLogger struct {
	level  LogLevel
	logger *log.Logger
}

// NewDefaultLogger creates a new DefaultLogger with the provided level.
func NewDefaultLogger(level LogLevel) *DefaultLogger {
	return &DefaultLogger{
		level:  level,
		logger: log.New(os.Stdout, "[DATABASE] ", log.LstdFlags|log.Lshortfile),
	}
}

func (l *DefaultLogger) Debug(msg string, fields ...interface{}) {
	if l.level <= LogLevelDebug {
		l.log(LogLevelDebug, msg, fields...)
	}
}

func (l *DefaultLogger) Info(msg string, fields ...interface{}) {
	if l.level <= LogLevelInfo {
		l.log(LogLevelInfo, msg, fields...)
	}
}

func (l *DefaultLogger) Warn(msg string, fields ...interface{}) {
	if l.level <= LogLevelWarn {
		l.log(LogLevelWarn, msg, fields...)
	}
}

func (l *DefaultLogger) Error(msg string, fields ...interface{}) {
	if l.level <= LogLevelError {
		l.log(LogLevelError, msg, fields...)
	}
}

func (l *DefaultLogger) log(level LogLevel, msg string, fields ...interface{}) {
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	levelStr := level.String()

	fieldsStr := ""
	if len(fields) > 0 {
		fieldsStr = " "
		for i := 0; i < len(fields); i += 2 {
			if i+1 < len(fields) {
				fieldsStr += fmt.Sprintf("%v=%v ", fields[i], fields[i+1])
			}
		}
	}

	logMsg := fmt.Sprintf("%s [%s] %s%s", timestamp, levelStr, msg, fieldsStr)
	l.logger.Println(logMsg)
}

// SetLevel updates the current logging level.
func (l *DefaultLogger) SetLevel(level LogLevel) {
	l.level = level
}

// GetLevel returns the current logging level.
func (l *DefaultLogger) GetLevel() LogLevel {
	return l.level
}
