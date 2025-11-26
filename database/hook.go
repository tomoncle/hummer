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
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/fatih/color"

	"github.com/uptrace/bun"
)

const (
	ansiReset     = "\x1b[0m"
	ansiRed       = "\x1b[31m"
	ansiYellow    = "\x1b[33m"
	ansiGreen     = "\x1b[32m"
	ansiBlue      = "\x1b[34m"
	ansiMagenta   = "\x1b[35m"
	ansiCyan      = "\x1b[36m"
	ansiBGGreen   = "\x1b[42;97m"
	ansiBGYellow  = "\x1b[43;97m"
	ansiBGBlue    = "\x1b[44;97m"
	ansiBGMagenta = "\x1b[45;97m"
	ansiBGRed     = "\x1b[41;97m"
)

var bunSqlSilentMode bool

func EnableBunSqlSilent(b bool) {
	bunSqlSilentMode = b
}

func colorWrap(s, code string) string { return fmt.Sprintf("%s%s%s", code, s, ansiReset) }

func backgroundColorWrap(s, code string) string { return fmt.Sprintf("%s%s%s", code, s, ansiReset) }

type QueryHook struct {
	envName string
	enabled bool
	verbose bool
	writer  io.Writer
}

var _ bun.QueryHook = (*QueryHook)(nil)

func (h *QueryHook) BeforeQuery(ctx context.Context, event *bun.QueryEvent) context.Context {
	return ctx
}

func (h *QueryHook) AfterQuery(ctx context.Context, event *bun.QueryEvent) {
	if bunSqlSilentMode {
		return
	}
	enabled := h.enabled
	verbose := h.verbose
	if env, ok := os.LookupEnv(h.envName); ok {
		enabled = env != "" && env != "0"
		verbose = env == "2"
	}

	if !enabled {
		return
	}

	if !verbose {
		switch {
		case event.Err == nil, errors.Is(event.Err, sql.ErrNoRows), errors.Is(event.Err, sql.ErrTxDone):
			return
		}
	}

	now := time.Now()
	dur := now.Sub(event.StartTime)

	args := []interface{}{
		now.Format("2006-01-02 15:04:05.000"),
		colorWrap(fmt.Sprintf("%15s", "[BUN] ✅"), ansiCyan),
		fmt.Sprintf("%17s", dur.Round(time.Microsecond)),
		"  ", formatOperationColor(event),
	}

	if event.Err != nil {
		typ := reflect.TypeOf(event.Err).String()
		args = append(args,
			"\t",
			color.New(color.BgRed).Sprintf(" %s ", typ+": "+event.Err.Error()),
		)
	}
	_, _ = fmt.Fprintln(h.writer, args...)
}

func formatOperationColor(event *bun.QueryEvent) string {
	operation := event.Operation()
	switch operation {
	case "SELECT":
		return colorWrap(event.Query, ansiGreen)
	case "INSERT":
		return colorWrap(event.Query, ansiBlue)
	case "UPDATE":
		return colorWrap(event.Query, ansiYellow)
	case "DELETE":
		return colorWrap(event.Query, ansiMagenta)
	default:
		return colorWrap(event.Query, ansiRed)
	}
}

func formatOperationBackgroundColor(event *bun.QueryEvent) string {
	operation := event.Operation()
	switch operation {
	case "SELECT":
		return backgroundColorWrap(event.Query, ansiBGGreen)
	case "INSERT":
		return backgroundColorWrap(event.Query, ansiBGBlue)
	case "UPDATE":
		return backgroundColorWrap(event.Query, ansiBGYellow)
	case "DELETE":
		return backgroundColorWrap(event.Query, ansiBGMagenta)
	default:
		return backgroundColorWrap(event.Query, ansiBGRed)
	}
}

type SlowQueryHook struct {
	fromEnv  string
	enabled  bool
	slowTime time.Duration
	writer   io.Writer
}

func (h *SlowQueryHook) BeforeQuery(ctx context.Context, event *bun.QueryEvent) context.Context {
	return ctx
}

func (h *SlowQueryHook) AfterQuery(ctx context.Context, event *bun.QueryEvent) {
	if bunSqlSilentMode {
		return
	}
	if event.Err != nil {
		return
	}
	enabled := h.enabled

	if env, ok := os.LookupEnv(h.fromEnv); ok {
		enabled = strings.TrimSpace(env) == "1"
	}

	if !enabled {
		return
	}

	duration := time.Since(event.StartTime)
	if duration > h.slowTime {
		args := []interface{}{
			time.Now().Format("2006-01-02 15:04:05.000"),
			colorWrap(fmt.Sprintf("%15s", "[BUN_SLOW] 🔴"), ansiYellow),
			fmt.Sprintf("%17s", duration.Round(time.Microsecond)),
			"  ", formatOperationBackgroundColor(event),
		}
		_, _ = fmt.Fprintln(h.writer, args...)
	}

}
