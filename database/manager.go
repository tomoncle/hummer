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
	"fmt"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/mysqldialect"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	"github.com/uptrace/bun/driver/sqliteshim"
	"github.com/uptrace/bun/extra/bundebug"
)

type defaultDatabaseManager struct {
	config          *ConnectionConfig
	db              *bun.DB
	sqlDB           *sql.DB
	logger          Logger
	mu              sync.RWMutex
	connected       bool
	lastError       error
	lastHealthCheck time.Time
	healthStatus    *HealthStatus
	reconnectTries  int
	stopHealthCheck chan struct{}
	healthCheckOnce sync.Once
}

func NewDatabaseManager(config *ConnectionConfig) AbstractDatabaseManager {
	// NewDatabaseManager returns an AbstractDatabaseManager backed by Bun.
	// If config is nil, a sensible default configuration is used.
	if config == nil {
		config = DefaultConnectionConfig()
	}
	return &defaultDatabaseManager{
		config:          config,
		healthStatus:    &HealthStatus{},
		stopHealthCheck: make(chan struct{}),
	}
}

func (dm *defaultDatabaseManager) Connect(ctx context.Context) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if dm.connected && dm.db != nil {
		return nil
	}

	var err error
	dm.sqlDB, dm.db, err = dm.createConnection()
	if err != nil {
		dm.lastError = err
		return fmt.Errorf("failed to create database connection: %w", err)
	}

	dm.configureConnectionPool()

	ctxTimeout, cancel := context.WithTimeout(ctx, dm.config.ConnectTimeout)
	defer cancel()

	if err := dm.db.PingContext(ctxTimeout); err != nil {
		dm.lastError = err
		return fmt.Errorf("database connection test failed: %w", err)
	}

	dm.connected = true
	dm.lastError = nil
	dm.reconnectTries = 0

	if dm.config.HealthCheckInterval > 0 {
		dm.startHealthCheck()
	}

	if dm.logger != nil {
		dm.logger.Info("Database connected successfully:", "type", dm.config.Type, "host", dm.config.Host)
	}
	return nil
}

func (dm *defaultDatabaseManager) createConnection() (*sql.DB, *bun.DB, error) {
	var sqlDB *sql.DB
	var db *bun.DB
	var err error

	if dm.config.ConnectTimeout.Seconds() <= 0 {
		dm.config.ConnectTimeout = 30 * time.Second
	}

	switch dm.config.Type {
	case "mysql":
		sqlDB, db, err = dm.createMySQLConnection()
	case "postgres", "postgresql":
		sqlDB, db, err = dm.createPostgreSQLConnection()
	case "sqlite", "sqlite3":
		sqlDB, db, err = dm.createSQLiteConnection()
	default:
		return nil, nil, fmt.Errorf("unsupported database type: %s", dm.config.Type)
	}

	if err != nil {
		return nil, nil, err
	}

	if dm.config.EnableQueryLog {
		db.AddQueryHook(bundebug.NewQueryHook(
			bundebug.WithVerbose(true),
			bundebug.FromEnv("BUNDEBUG"),
		))
	}

	if dm.config.SlowQueryTime > 0 {
		db.AddQueryHook(&slowQueryHook{
			slowTime: dm.config.SlowQueryTime,
			logger:   dm.logger,
		})
	}

	return sqlDB, db, nil
}

func (dm *defaultDatabaseManager) createMySQLConnection() (*sql.DB, *bun.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local&timeout=%s&readTimeout=%s&writeTimeout=%s",
		dm.config.Username,
		dm.config.Password,
		dm.config.Host,
		dm.config.Port,
		dm.config.DBName,
		dm.config.ConnectTimeout,
		dm.config.ReadTimeout,
		dm.config.WriteTimeout,
	)

	sqlDB, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, nil, err
	}

	db := bun.NewDB(sqlDB, mysqldialect.New())
	return sqlDB, db, nil
}

func (dm *defaultDatabaseManager) createPostgreSQLConnection() (*sql.DB, *bun.DB, error) {
	sslMode := dm.config.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s&connect_timeout=%d",
		dm.config.Username,
		dm.config.Password,
		dm.config.Host,
		dm.config.Port,
		dm.config.DBName,
		sslMode,
		int(dm.config.ConnectTimeout.Seconds()),
	)

	sqlDB, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, nil, err
	}

	db := bun.NewDB(sqlDB, pgdialect.New())
	return sqlDB, db, nil
}

func (dm *defaultDatabaseManager) createSQLiteConnection() (*sql.DB, *bun.DB, error) {
	dsn := fmt.Sprintf("%s.db", dm.config.DBName)

	sqlDB, err := sql.Open(sqliteshim.ShimName, dsn)
	if err != nil {
		return nil, nil, err
	}

	db := bun.NewDB(sqlDB, sqlitedialect.New())
	return sqlDB, db, nil
}

func (dm *defaultDatabaseManager) configureConnectionPool() {
	if dm.sqlDB == nil {
		return
	}

	dm.sqlDB.SetMaxIdleConns(dm.config.MaxIdleConns)
	dm.sqlDB.SetMaxOpenConns(dm.config.MaxOpenConns)
	dm.sqlDB.SetConnMaxLifetime(dm.config.ConnMaxLifetime)
	dm.sqlDB.SetConnMaxIdleTime(dm.config.ConnMaxIdleTime)
}

func (dm *defaultDatabaseManager) Disconnect() error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	select {
	case dm.stopHealthCheck <- struct{}{}:
	default:
	}

	if dm.db != nil {
		err := dm.db.Close()
		dm.db = nil
		dm.sqlDB = nil
		dm.connected = false

		if dm.logger != nil {
			if err != nil {
				dm.logger.Error("Failed to close database connection", "error", err)
			} else {
				dm.logger.Info("Database connection closed")
			}
		}

		return err
	}

	return nil
}

func (dm *defaultDatabaseManager) Reconnect(ctx context.Context) error {
	if dm.logger != nil {
		dm.logger.Info("Attempting to reconnect to the database")
	}

	if err := dm.Disconnect(); err != nil {
		if dm.logger != nil {
			dm.logger.Warn("Error disconnecting existing connection", "error", err)
		}
	}

	return dm.Connect(ctx)
}

func (dm *defaultDatabaseManager) Ping(ctx context.Context) error {
	dm.mu.RLock()
	db := dm.db
	dm.mu.RUnlock()

	if db == nil {
		return fmt.Errorf("database not connected")
	}

	return db.PingContext(ctx)
}

func (dm *defaultDatabaseManager) GetDB() *bun.DB {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	return dm.db
}

func (dm *defaultDatabaseManager) GetSQLDB() *sql.DB {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	return dm.sqlDB
}

func (dm *defaultDatabaseManager) HealthCheck(ctx context.Context) *HealthStatus {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	start := time.Now()
	status := &HealthStatus{
		LastCheckTime: start,
		Connected:     dm.connected,
	}

	if dm.db == nil {
		status.Healthy = false
		status.LastError = "Database not initialized"
		return status
	}

	ctxTimeout, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	err := dm.db.PingContext(ctxTimeout)
	status.ResponseTime = time.Since(start)

	if err != nil {
		status.Healthy = false
		status.Connected = false
		status.LastError = err.Error()
		dm.lastError = err
	} else {
		status.Healthy = true
		status.Connected = true
		dm.lastError = nil
	}

	if dm.sqlDB != nil {
		stats := dm.sqlDB.Stats()
		status.ActiveConns = stats.InUse
		status.IdleConns = stats.Idle
		status.MaxOpenConns = stats.MaxOpenConnections
	}

	dm.healthStatus = status
	dm.lastHealthCheck = start

	return status
}

func (dm *defaultDatabaseManager) startHealthCheck() {
	dm.healthCheckOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(dm.config.HealthCheckInterval)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
					status := dm.HealthCheck(ctx)
					cancel()
					if !status.Healthy && dm.config.EnableReconnect {
						dm.handleReconnect()
					}

				case <-dm.stopHealthCheck:
					return
				}
			}
		}()
	})
}

func (dm *defaultDatabaseManager) handleReconnect() {
	if dm.reconnectTries >= dm.config.MaxReconnectTries {
		if dm.logger != nil {
			dm.logger.Error("Max reconnect attempts reached, stopping", "tries", dm.reconnectTries)
		}
		return
	}

	dm.reconnectTries++
	if dm.logger != nil {
		dm.logger.Info("Starting database reconnect", "try", dm.reconnectTries)
	}

	time.Sleep(dm.config.ReconnectInterval)

	ctx, cancel := context.WithTimeout(context.Background(), dm.config.ConnectTimeout)
	defer cancel()

	if err := dm.Reconnect(ctx); err != nil {
		if dm.logger != nil {
			dm.logger.Error("Reconnect failed", "error", err, "try", dm.reconnectTries)
		}
	} else {
		dm.reconnectTries = 0
		if dm.logger != nil {
			dm.logger.Info("Reconnect succeeded")
		}
	}
}

func (dm *defaultDatabaseManager) GetStats() *DBStats {
	dm.mu.RLock()
	sqlDB := dm.sqlDB
	dm.mu.RUnlock()

	if sqlDB == nil {
		return &DBStats{}
	}

	stats := sqlDB.Stats()
	return &DBStats{
		MaxOpenConns:      stats.MaxOpenConnections,
		OpenConns:         stats.OpenConnections,
		InUse:             stats.InUse,
		Idle:              stats.Idle,
		WaitCount:         stats.WaitCount,
		WaitDuration:      stats.WaitDuration,
		MaxIdleClosed:     stats.MaxIdleClosed,
		MaxIdleTimeClosed: stats.MaxIdleTimeClosed,
		MaxLifetimeClosed: stats.MaxLifetimeClosed,
	}
}

func (dm *defaultDatabaseManager) RunMigrations(ctx context.Context) error {
	db := dm.GetDB()
	if db == nil {
		return fmt.Errorf("database not initialized")
	}

	migrationManager := NewMigrationManager(db, dm.logger)

	return migrationManager.RunMigrations(ctx)
}

func (dm *defaultDatabaseManager) InitData(ctx context.Context) error {
	db := dm.GetDB()
	if db == nil {
		return fmt.Errorf("database not initialized")
	}
	migrationManager := NewMigrationManager(db, dm.logger)
	return migrationManager.InitData(ctx)
}

func (dm *defaultDatabaseManager) SetLogger(logger Logger) {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	dm.logger = logger
}

type slowQueryHook struct {
	slowTime time.Duration
	logger   Logger
}

func (h *slowQueryHook) BeforeQuery(ctx context.Context, event *bun.QueryEvent) context.Context {
	return ctx
}

func (h *slowQueryHook) AfterQuery(ctx context.Context, event *bun.QueryEvent) {
	if event.Err != nil {
		return
	}

	duration := time.Since(event.StartTime)
	if duration > h.slowTime && h.logger != nil {
		h.logger.Warn("\x1b[33;5mDatabase slow query detected:⚠️\x1b[0m",
			"duration", duration,
			"slow_threshold", h.slowTime,
			"query", event.Query,
		)
	}

}
