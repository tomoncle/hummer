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
	"github.com/tomoncle/hummer/utils"
	"strings"
	"sync"
)

var (
	globalLogger   Logger
	globalLoggerMu sync.RWMutex
)

type LogLevel int

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarn
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
	SetLevel(LogLevel)
	Debug(msg string, fields ...interface{})
	Info(msg string, fields ...interface{})
	Warn(msg string, fields ...interface{})
	Error(msg string, fields ...interface{})
}

func InitLogger(log Logger) {
	if log == nil {
		return
	}
	globalLoggerMu.Lock()
	defer globalLoggerMu.Unlock()
	if globalLogger == nil {
		globalLogger = log
	}
}

func GetLogger() Logger {
	globalLoggerMu.RLock()
	l := globalLogger
	globalLoggerMu.RUnlock()
	if l != nil {
		return l
	}

	dl := &DefaultLogger{level: LogLevelInfo, logger: utils.NewLogger("DATABASE")}
	globalLoggerMu.Lock()
	if globalLogger == nil {
		globalLogger = dl
	}
	l = globalLogger
	globalLoggerMu.Unlock()
	return l
}

type DefaultLogger struct {
	level  LogLevel
	logger *utils.Logger
}

func (l *DefaultLogger) Debug(msg string, fields ...interface{}) {
	l.logger.Debug(msg + l.log(fields...))
}

func (l *DefaultLogger) Info(msg string, fields ...interface{}) {
	l.logger.Info(msg + l.log(fields...))
}

func (l *DefaultLogger) Warn(msg string, fields ...interface{}) {
	l.logger.Warn(msg + l.log(fields...))
}

func (l *DefaultLogger) Error(msg string, fields ...interface{}) {
	l.logger.Error(msg + l.log(fields...))
}

func (l *DefaultLogger) SetLevel(level LogLevel) {
	utils.SetLoggerLevel("DATABASE", strings.ToLower(level.String()))
}

func (l *DefaultLogger) log(fields ...interface{}) string {
	if len(fields) == 0 {
		return ""
	}
	fieldsStr := " "
	if len(fields) > 0 {
		fieldsStr = " "
		for i := 0; i < len(fields); i += 2 {
			if i+1 < len(fields) {
				fieldsStr += fmt.Sprintf("%v=%v ", fields[i], fields[i+1])
			}
		}
	}
	return fieldsStr
}
