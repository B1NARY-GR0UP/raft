// Copyright 2024 BINARY Members
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"context"
	"fmt"
	stdlog "log"
	"log/slog"
	"os"
	"path"
	"runtime"
	"sync"
)

var (
	_ Logger = (*SLogger)(nil)
	_ Logger = (*FLogger)(nil)
)

var (
	loggerMu sync.RWMutex
	logger   = Logger(FLog)
)

const (
	_flogPrefix = "raft "
	_slogPrefix = "raftrpc"
)

var (
	FLog = &FLogger{
		Logger: stdlog.New(os.Stderr, _flogPrefix, stdlog.LstdFlags),
	}
	// SLog TODO: optimize
	SLog = &SLogger{
		Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: levelVar,
			ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
				if a.Key == slog.LevelKey {
					level := a.Value.Any().(slog.Level)
					levelLabel, ok := levels[level]
					if !ok {
						levelLabel = level.String()
					}
					a.Value = slog.StringValue(levelLabel)
				}
				return a
			},
		})).With(slog.String("prefix", _slogPrefix)),
	}
)

// FLogger calldepth
const _calldepth = 2

type Level = slog.Level

// SLogger levels
const (
	LevelDebug = slog.Level(-4)
	LevelInfo  = slog.Level(0)
	LevelWarn  = slog.Level(4)
	LevelError = slog.Level(8)
	LevelFatal = slog.Level(12)
	LevelPanic = slog.Level(16)
)

var (
	levels = map[slog.Leveler]string{
		LevelDebug: "DEBUG",
		LevelInfo:  "INFO",
		LevelWarn:  "WARN",
		LevelError: "ERROR",
		LevelFatal: "FATAL",
		LevelPanic: "PANIC",
	}
	levelVar = &slog.LevelVar{}
)

type Logger interface {
	Debug(msg string, args ...any)
	Debugf(format string, args ...any)
	Info(msg string, args ...any)
	Infof(format string, args ...any)
	Warn(msg string, args ...any)
	Warnf(format string, args ...any)
	Error(msg string, args ...any)
	Errorf(format string, args ...any)
	Fatal(msg string, args ...any)
	Fatalf(format string, args ...any)
	Panic(msg string, args ...any)
	Panicf(format string, args ...any)
}

func SetLogger(l Logger) {
	loggerMu.Lock()
	defer loggerMu.Unlock()
	logger = l
}

func ResetDefaultLogger() {
	SetLogger(FLog)
}

func getLogger() Logger {
	loggerMu.RLock()
	defer loggerMu.RUnlock()
	return logger
}

func SetSLoggerLevel(l Level) {
	loggerMu.Lock()
	defer loggerMu.Unlock()
	levelVar.Set(l)
}

type FLogger struct {
	*stdlog.Logger
	debug bool
}

func (fl *FLogger) EnableDebug() {
	fl.debug = true
}

func (fl *FLogger) Debug(_ string, _ ...any) {
	panic("use SLogger instead")
}

func (fl *FLogger) Debugf(format string, args ...any) {
	if fl.debug {
		_ = fl.Output(_calldepth, fl.header("DEBUG", fmt.Sprintf(format, args...)))
	}
}

func (fl *FLogger) Info(_ string, _ ...any) {
	panic("use SLogger instead")
}

func (fl *FLogger) Infof(format string, args ...any) {
	_ = fl.Output(_calldepth, fl.header("INFO", fmt.Sprintf(format, args...)))
}

func (fl *FLogger) Warn(_ string, _ ...any) {
	panic("use SLogger instead")
}

func (fl *FLogger) Warnf(format string, args ...any) {
	_ = fl.Output(_calldepth, fl.header("WARN", fmt.Sprintf(format, args...)))
}

func (fl *FLogger) Error(_ string, _ ...any) {
	panic("use SLogger instead")
}

func (fl *FLogger) Errorf(format string, args ...any) {
	_ = fl.Output(_calldepth, fl.header("ERROR", fmt.Sprintf(format, args...)))
}

func (fl *FLogger) Fatal(_ string, _ ...any) {
	panic("use SLogger instead")
}

func (fl *FLogger) Fatalf(format string, args ...any) {
	_ = fl.Output(_calldepth, fl.header("FATAL", fmt.Sprintf(format, args...)))
}

func (fl *FLogger) Panic(_ string, _ ...any) {
	panic("use SLogger instead")
}

func (fl *FLogger) Panicf(format string, args ...any) {
	fl.Logger.Panicf(format, args...)
}

func (fl *FLogger) header(lvl, msg string) string {
	_, file, line, ok := runtime.Caller(_calldepth)
	if !ok {
		file = "unknown"
		line = 0
	} else {
		file = path.Base(file)
	}
	return fmt.Sprintf("%s:%d [%s] %s", file, line, lvl, msg)
}

type SLogger struct {
	*slog.Logger
}

func (sl *SLogger) Debug(msg string, args ...any) {
	sl.Logger.Log(context.Background(), LevelDebug, msg, args...)
}

func (sl *SLogger) Debugf(_ string, _ ...any) {
	panic("use FLogger instead")
}

func (sl *SLogger) Info(msg string, args ...any) {
	sl.Logger.Log(context.Background(), LevelInfo, msg, args...)
}

func (sl *SLogger) Infof(_ string, _ ...any) {
	panic("use FLogger instead")
}

func (sl *SLogger) Warn(msg string, args ...any) {
	sl.Logger.Log(context.Background(), LevelWarn, msg, args...)
}

func (sl *SLogger) Warnf(_ string, _ ...any) {
	panic("use FLogger instead")
}

func (sl *SLogger) Error(msg string, args ...any) {
	sl.Logger.Log(context.Background(), LevelError, msg, args...)
}

func (sl *SLogger) Errorf(_ string, _ ...any) {
	panic("use FLogger instead")
}

func (sl *SLogger) Fatal(msg string, args ...any) {
	sl.Logger.Log(context.Background(), LevelFatal, msg, args...)
	os.Exit(1)
}

func (sl *SLogger) Fatalf(_ string, _ ...any) {
	panic("use FLogger instead")
}

func (sl *SLogger) Panic(msg string, args ...any) {
	sl.Logger.Log(context.Background(), LevelPanic, msg, args...)
	panic(msg)
}

func (sl *SLogger) Panicf(_ string, _ ...any) {
	panic("use FLogger instead")
}
