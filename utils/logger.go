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

package utils

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type PathFormat int

type Logger = logrus.Logger

const (
	PathFormatTruncatedRelative PathFormat = iota
	PathFormatFilenameOnly
	PathFormatShortRelative
	PathFormatFullRelative
)

var (
	defaultConsoleLevel = logrus.DebugLevel
	defaultFileLevel    = logrus.TraceLevel
	loggerRegistryMu    sync.RWMutex
	loggerRegistry      = map[string]*logrus.Logger{}
	fileLogEnabled      = EnvDefaultBool("FILE_LOG_ENABLED", false)
	fileLogDir          = "logs"
	fileLogMaxAgeDays   = 0
	fileLogUseLink      = false
	fileLogFormat       = EnvDefaultString("FILE_LOG_FORMAT", "text")
	consoleLogFormat    = EnvDefaultString("CONSOLE_LOG_FORMAT", "text")
)

func ConfigureFileLog(dir string, maxAgeDays int, useLink bool) {
	if dir != "" {
		fileLogDir = dir
	}
	if maxAgeDays >= 0 {
		fileLogMaxAgeDays = maxAgeDays
	}
	fileLogUseLink = useLink
}

func ConfigureFileLogFormat(format string) {
	s := strings.ToLower(strings.TrimSpace(format))
	if s == "json" {
		fileLogFormat = "json"
	} else {
		fileLogFormat = "text"
	}
}

func ConfigureConsoleLogFormat(format string) {
	s := strings.ToLower(strings.TrimSpace(format))
	if s == "json" {
		consoleLogFormat = "json"
	} else {
		consoleLogFormat = "text"
	}
}

type levelWriterHook struct {
	writers   map[logrus.Level]io.Writer
	formatter logrus.Formatter
}

func (h *levelWriterHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (h *levelWriterHook) Fire(e *logrus.Entry) error {
	if e.Level > defaultFileLevel {
		return nil
	}
	w, ok := h.writers[e.Level]
	if !ok || w == nil {
		return nil
	}
	b, err := h.formatter.Format(e)
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

type dailyLevelWriter struct {
	baseDir    string
	level      string
	maxAgeDays int
	mu         sync.Mutex
	curDate    string
	file       *os.File
}

func newDailyLevelWriter(baseDir, level string, maxAgeDays int) (io.Writer, error) {
	return &dailyLevelWriter{baseDir: baseDir, level: level, maxAgeDays: maxAgeDays}, nil
}

func (w *dailyLevelWriter) ensureOpen(date string) error {
	if w.file != nil && w.curDate == date {
		return nil
	}
	if w.file != nil {
		_ = w.file.Close()
		w.file = nil
	}
	dir := filepath.Join(w.baseDir, date)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(dir, fmt.Sprintf("%s.log", w.level))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	w.file = f
	w.curDate = date
	return nil
}

func (w *dailyLevelWriter) cleanup() {

	if w.maxAgeDays < 0 {
		return
	}
	now := time.Now()
	cutoffDate := now.AddDate(0, 0, -w.maxAgeDays)
	cutoffMidnight := time.Date(cutoffDate.Year(), cutoffDate.Month(), cutoffDate.Day(), 0, 0, 0, 0, time.Local)

	entries, err := os.ReadDir(w.baseDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		d, err := time.Parse("2006-01-02", name)
		if err != nil {
			continue
		}
		if d.Before(cutoffMidnight) {
			path := filepath.Join(w.baseDir, name)
			info, err := os.Stat(path)
			if err != nil {

				continue
			}
			if info.IsDir() {
				_ = os.RemoveAll(path)
			} else {
				_ = os.Remove(path)
			}
		}
	}
}

func (w *dailyLevelWriter) Write(p []byte) (int, error) {
	nowDate := time.Now().Format("2006-01-02")
	w.mu.Lock()
	oldDate := w.curDate
	if err := w.ensureOpen(nowDate); err != nil {
		w.mu.Unlock()
		return 0, err
	}
	if oldDate != nowDate {
		w.cleanup()
	}
	f := w.file
	w.mu.Unlock()
	return f.Write(p)
}

func AddDailyRollingFileHook(l *logrus.Logger, name, dir string, maxAgeDays int, _ bool) error {
	if dir == "" {
		dir = "logs"
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	mk := func(level string) (io.Writer, error) {
		return newDailyLevelWriter(dir, level, maxAgeDays)
	}

	traceW, err := mk("trace")
	if err != nil {
		return err
	}
	debugW, err := mk("debug")
	if err != nil {
		return err
	}
	infoW, err := mk("info")
	if err != nil {
		return err
	}
	warnW, err := mk("warn")
	if err != nil {
		return err
	}
	errorW, err := mk("error")
	if err != nil {
		return err
	}

	var fileFmt logrus.Formatter
	if fileLogFormat == "json" {
		fileFmt = &JSONLogFormatter{
			LoggerName:      name,
			TimestampFormat: "2006-01-02 15:04:05.000",
			PathFmt:         PathFormatFullRelative,
		}
	} else {
		fileFmt = &Log4jColorFormatter{
			LoggerName:      name,
			TimestampFormat: "2006-01-02 15:04:05.000",
			PathFmt:         PathFormatFullRelative,
			ColorCaller:     false,
			NameWidth:       10,
			CallerWidth:     0,
		}
	}

	l.AddHook(&levelWriterHook{
		writers: map[logrus.Level]io.Writer{
			logrus.TraceLevel: traceW,
			logrus.DebugLevel: debugW,
			logrus.InfoLevel:  infoW,
			logrus.WarnLevel:  warnW,
			logrus.ErrorLevel: errorW,
			logrus.FatalLevel: errorW,
			logrus.PanicLevel: errorW,
		},
		formatter: fileFmt,
	})
	return nil
}

func ParseLogLevel(s string) logrus.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "trace":
		return logrus.TraceLevel
	case "debug":
		return logrus.DebugLevel
	case "info", "":
		return logrus.InfoLevel
	case "warn", "warning":
		return logrus.WarnLevel
	case "error":
		return logrus.ErrorLevel
	case "fatal":
		return logrus.FatalLevel
	case "panic":
		return logrus.PanicLevel
	default:
		return logrus.InfoLevel
	}
}

func RegisterLogger(name string, l *logrus.Logger) {
	loggerRegistryMu.Lock()
	defer loggerRegistryMu.Unlock()
	loggerRegistry[name] = l
}

func maxLevel(a, b logrus.Level) logrus.Level {
	if a >= b {
		return a
	}
	return b
}

func applyBaseLevelToRegistered() {
	base := maxLevel(defaultConsoleLevel, defaultFileLevel)
	loggerRegistryMu.RLock()
	for _, lg := range loggerRegistry {
		lg.SetLevel(base)
	}
	loggerRegistryMu.RUnlock()
	logrus.SetLevel(base)
}

func SetAllLoggersLevel(lvl logrus.Level) {
	loggerRegistryMu.RLock()
	for _, lg := range loggerRegistry {
		lg.SetLevel(lvl)
	}
	loggerRegistryMu.RUnlock()
	logrus.SetLevel(lvl)
	defaultConsoleLevel = lvl
	defaultFileLevel = lvl
}

func SetLoggerLevel(name string, lvlStr string) bool {
	lvl := ParseLogLevel(lvlStr)
	loggerRegistryMu.RLock()
	lg, ok := loggerRegistry[name]
	loggerRegistryMu.RUnlock()
	if !ok {
		return false
	}
	lg.SetLevel(lvl)
	return true
}

func ConfigureLogLevel(levelStr string) {
	lvl := ParseLogLevel(levelStr)
	defaultConsoleLevel = lvl
	defaultFileLevel = lvl
	applyBaseLevelToRegistered()
}

func ConfigureConsoleLogLevel(levelStr string) {
	defaultConsoleLevel = ParseLogLevel(levelStr)
	applyBaseLevelToRegistered()
}

func ConfigureFileLogLevel(levelStr string) {
	defaultFileLevel = ParseLogLevel(levelStr)
	applyBaseLevelToRegistered()
}

type consoleWriterHook struct {
	formatter logrus.Formatter
}

func (h *consoleWriterHook) Levels() []logrus.Level { return logrus.AllLevels }

func (h *consoleWriterHook) Fire(e *logrus.Entry) error {
	if e.Level > defaultConsoleLevel {
		return nil
	}
	b, err := h.formatter.Format(e)
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(b)
	return err
}

func NewLogger(name string) *logrus.Logger {
	l := logrus.New()
	l.SetOutput(io.Discard)
	l.SetLevel(maxLevel(defaultConsoleLevel, defaultFileLevel))
	l.SetReportCaller(true)
	var consoleFmt logrus.Formatter
	if consoleLogFormat == "json" {
		consoleFmt = &JSONLogFormatter{
			LoggerName:      name,
			TimestampFormat: "2006-01-02 15:04:05.000",
			PathFmt:         PathFormatFullRelative,
		}
	} else {
		consoleFmt = &Log4jColorFormatter{
			LoggerName:      name,
			TimestampFormat: "2006-01-02 15:04:05.000",
			PathFmt:         PathFormatTruncatedRelative,
			ColorCaller:     true,
			NameWidth:       10,
			CallerWidth:     25,
		}
	}
	l.SetFormatter(consoleFmt)
	l.AddHook(&consoleWriterHook{formatter: l.Formatter})
	if fileLogEnabled {
		_ = AddDailyRollingFileHook(l, name, fileLogDir, fileLogMaxAgeDays, fileLogUseLink)
	}
	RegisterLogger(name, l)
	return l
}

type Log4jColorFormatter struct {
	LoggerName      string
	TimestampFormat string
	ShowThreadName  bool
	PathFmt         PathFormat
	ColorCaller     bool
	NameWidth       int
	CallerWidth     int
}

func (f *Log4jColorFormatter) tsFormat() string {
	if f.TimestampFormat != "" {
		return f.TimestampFormat
	}
	return "2006-01-02 15:04:05.000"
}

func (f *Log4jColorFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	ts := time.Now().Format(f.tsFormat())
	lvl := strings.ToUpper(entry.Level.String())
	lvlPad := padLeft(lvl, 7)
	coloredLvl := colorLevel(lvlPad, entry.Level)
	pid := colorMagenta(fmt.Sprintf("%-6d", os.Getpid()))
	thread := colorMagenta("[main]")
	name := limitRunes(f.LoggerName, f.NameWidth)
	namePad := padLeft(name, f.NameWidth)
	cyanName := colorCyan(namePad)
	callerInfo := ""
	if entry.Caller != nil {
		var fileLine string
		switch f.PathFmt {
		case PathFormatFilenameOnly:
			base := filepath.Base(entry.Caller.File)
			fileLine = fmt.Sprintf("%s:%d", base, entry.Caller.Line)
		case PathFormatShortRelative:
			rel := shortRelative(entry.Caller.File)
			fileLine = fmt.Sprintf("%s:%d", rel, entry.Caller.Line)
		case PathFormatFullRelative:
			rel := moduleRelative(filepath.ToSlash(entry.Caller.File))
			osRel := filepath.FromSlash(rel)
			fileLine = fmt.Sprintf("%s:%d", osRel, entry.Caller.Line)
		default: // PathFormatTruncatedRelative
			rel := moduleRelative(filepath.ToSlash(entry.Caller.File))
			lineStr := strconv.Itoa(entry.Caller.Line)
			if f.CallerWidth > 0 {
				reserved := 1 + len(lineStr)
				pathMax := f.CallerWidth - reserved
				if pathMax > 0 {
					rel = dotPathCompact(rel, pathMax)
				} else {
					rel = ""
				}
			}
			fileLine = fmt.Sprintf("%s:%s", rel, lineStr)
		}
		padded := fileLine
		if f.CallerWidth > 0 {
			padded = padLeftRunes(fileLine, f.CallerWidth)
		}

		prefix := " "
		if f.ColorCaller {
			callerInfo = colorFaint(prefix + padded)
		} else {
			callerInfo = prefix + padded
		}
	}
	sep := "" + colorFaint(":")
	msg := entry.Message

	line := fmt.Sprintf("%s %s %s - %s %s%s %s %s\n", ts, coloredLvl, pid, thread, cyanName, callerInfo, sep, msg)
	return []byte(line), nil
}

type JSONLogFormatter struct {
	LoggerName string

	TimestampFormat string

	PathFmt PathFormat
}

func (f *JSONLogFormatter) tsFormat() string {
	if f.TimestampFormat != "" {
		return f.TimestampFormat
	}
	return "2006-01-02 15:04:05.000"
}

func (f *JSONLogFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	ts := time.Now().Format(f.tsFormat())
	lvl := strings.ToLower(entry.Level.String())

	caller := ""
	if entry.Caller != nil {
		switch f.PathFmt {
		case PathFormatFilenameOnly:
			base := filepath.Base(entry.Caller.File)
			caller = fmt.Sprintf("%s:%d", base, entry.Caller.Line)
		case PathFormatShortRelative:
			rel := shortRelative(entry.Caller.File)
			caller = fmt.Sprintf("%s:%d", rel, entry.Caller.Line)
		case PathFormatFullRelative:
			rel := moduleRelative(filepath.ToSlash(entry.Caller.File))
			osRel := filepath.FromSlash(rel)
			caller = fmt.Sprintf("%s:%d", osRel, entry.Caller.Line)
		default:
			rel := moduleRelative(filepath.ToSlash(entry.Caller.File))
			caller = fmt.Sprintf("%s:%d", filepath.Base(rel), entry.Caller.Line)
		}
	}

	type jsonLogRecord struct {
		Time        string                 `json:"time"`
		Level       string                 `json:"level"`
		Model       string                 `json:"model"`
		Caller      string                 `json:"caller"`
		Message     string                 `json:"message"`
		ClientIP    string                 `json:"client_ip,omitempty"`
		Method      string                 `json:"method,omitempty"`
		Path        string                 `json:"path,omitempty"`
		StatusCode  int                    `json:"status_code,omitempty"`
		LatencyTime string                 `json:"latency_time,omitempty"`
		Fields      map[string]interface{} `json:"fields,omitempty"`
	}

	rec := jsonLogRecord{
		Time:    ts,
		Level:   lvl,
		Model:   f.LoggerName,
		Caller:  caller,
		Message: entry.Message,
	}

	extra := make(map[string]interface{}, len(entry.Data))
	for k, v := range entry.Data {
		switch k {
		case "req_uri":
			if s, ok := v.(string); ok && s != "" {
				rec.Path = s
			} else {
				extra[k] = v
			}
		case "req_method":
			if s, ok := v.(string); ok && s != "" {
				rec.Method = s
			} else {
				extra[k] = v
			}
		case "client_ip":
			if s, ok := v.(string); ok && s != "" {
				rec.ClientIP = s
			} else {
				extra[k] = v
			}
		case "latency_time":
			if s, ok := v.(string); ok && s != "" {
				rec.LatencyTime = s
			} else {
				extra[k] = v
			}
		case "status_code":
			switch n := v.(type) {
			case int:
				rec.StatusCode = n
			case int64:
				rec.StatusCode = int(n)
			case float64:
				rec.StatusCode = int(n)
			default:
				extra[k] = v
			}
		default:
			extra[k] = v
		}
	}

	if len(extra) > 0 {
		rec.Fields = extra
	}

	b, err := json.Marshal(rec)
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

func padLeft(s string, width int) string { return fmt.Sprintf("%"+fmt.Sprintf("%d", width)+"s", s) }

const (
	ansiReset   = "\x1b[0m"
	ansiFaint   = "\x1b[2m"
	ansiRed     = "\x1b[31m"
	ansiYellow  = "\x1b[33m"
	ansiGreen   = "\x1b[32m"
	ansiBlue    = "\x1b[34m"
	ansiMagenta = "\x1b[35m"
	ansiCyan    = "\x1b[36m"
)

func colorWrap(s, code string) string { return code + s + ansiReset }

func colorMagenta(s string) string { return colorWrap(s, ansiMagenta) }

func colorCyan(s string) string { return colorWrap(s, ansiCyan) }

func colorFaint(s string) string { return colorWrap(s, ansiFaint) }

func colorLevel(s string, level logrus.Level) string {
	switch level {
	case logrus.ErrorLevel, logrus.FatalLevel, logrus.PanicLevel:
		return colorWrap(s, ansiRed)
	case logrus.WarnLevel:
		return colorWrap(s, ansiYellow)
	case logrus.InfoLevel:
		return colorWrap(s, ansiGreen)
	case logrus.DebugLevel:
		return colorWrap(s, ansiBlue)
	default:
		return colorWrap(s, ansiMagenta)
	}
}

var moduleRootOnce sync.Once
var moduleRoot string

func moduleRelative(p string) string {
	moduleRootOnce.Do(func() {
		moduleRoot = findModuleRootFrom(p)
	})
	if moduleRoot != "" && strings.HasPrefix(p, moduleRoot) {
		rel := strings.TrimPrefix(p, moduleRoot)
		rel = strings.TrimPrefix(rel, "/")
		return rel
	}
	if base := mainModuleBase(); base != "" {
		if idx := strings.Index(p, base); idx >= 0 {
			return p[idx:]
		}
	}
	return p
}

func findModuleRootFrom(p string) string {
	dir := filepath.Dir(p)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return filepath.ToSlash(dir)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return ""
}

var mainModuleBaseOnce sync.Once
var mainModuleBaseCache string

func mainModuleBase() string {
	mainModuleBaseOnce.Do(func() {
		if info, ok := debug.ReadBuildInfo(); ok && info.Main.Path != "" {
			parts := strings.Split(info.Main.Path, "/")
			mainModuleBaseCache = parts[len(parts)-1]
		}
	})
	return mainModuleBaseCache
}

func limitRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}

func padLeftRunes(s string, width int) string {
	r := []rune(s)
	if len(r) >= width {
		return s
	}
	return strings.Repeat(" ", width-len(r)) + s
}

func shortRelative(p string) string {
	sp := filepath.ToSlash(p)
	rel := moduleRelative(sp)
	parts := strings.Split(rel, "/")
	if len(parts) >= 2 {
		return parts[len(parts)-2] + "/" + parts[len(parts)-1]
	}
	return parts[0]
}

func dotPathCompact(p string, max int) string {
	if max <= 0 {
		return ""
	}
	sp := filepath.ToSlash(p)
	parts := strings.Split(sp, "/")
	if len(parts) == 0 {
		return ""
	}
	filename := parts[len(parts)-1]
	dirs := parts[:len(parts)-1]
	join := func(ds []string, fn string) string {
		if len(ds) > 0 {
			return strings.Join(ds, ".") + "." + fn
		}
		return fn
	}
	out := join(dirs, filename)
	if len(out) <= max {
		return out
	}
	ab := make([]string, len(dirs))
	copy(ab, dirs)
	for i := 0; i < len(ab); i++ {
		r := []rune(ab[i])
		if len(r) > 0 {
			ab[i] = string(r[0])
		}
		out = join(ab, filename)
		if len(out) <= max {
			return out
		}
	}
	base := filename
	ext := ""
	if idx := strings.LastIndex(filename, "."); idx > 0 {
		base = filename[:idx]
		ext = filename[idx:]
	}
	br := []rune(base)
	if len(br) == 0 {
		r := []rune(filename)
		if len(r) <= max {
			return filename
		}
		return string(r[len(r)-max:])
	}
	dirsStr := ""
	if len(ab) > 0 {
		dirsStr = strings.Join(ab, ".") + "."
	}
	first := string(br[0])
	capTail := max - len(dirsStr) - len(first) - 2 - len(ext)
	if capTail < 0 {
		capTail = 0
	}
	if capTail > len(br)-1 {
		capTail = len(br) - 1
	}
	tail := ""
	if capTail > 0 {
		tail = string(br[len(br)-capTail:])
	}
	compact := first + ".." + tail + ext
	out = dirsStr + compact
	if len(out) > max {
		r := []rune(out)
		return string(r[len(r)-max:])
	}
	return out
}

func EnvDefaultString(key string, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func EnvDefaultBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		b, _ := strconv.ParseBool(v)
		return b
	}
	return def
}
