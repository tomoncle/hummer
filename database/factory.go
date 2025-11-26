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
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/uptrace/bun"
)

// BaseDatabaseFactory creates and manages a configured database manager and
// provides helpers for initialization, health checks, and statistics.
type BaseDatabaseFactory struct {
	manager AbstractDatabaseManager
	logger  Logger
}

// NewDatabaseFactory returns a new database factory using the global logger.
func NewDatabaseFactory() *BaseDatabaseFactory {
	return &BaseDatabaseFactory{
		logger: GetLogger(),
	}
}

// CreateFromConfig constructs a database manager from the given connection
// configuration, applying environment overrides and setting the factory logger.
func (f *BaseDatabaseFactory) CreateFromConfig(cfg *ConnectionConfig) (AbstractDatabaseManager, error) {
	if cfg == nil {
		return nil, fmt.Errorf("database configuration cannot be empty")
	}

	// Check whether the database type is supported
	supportedTypes := []string{"mysql", "postgres", "sqlite"}
	supported := false
	for _, t := range supportedTypes {
		if cfg.Type == t {
			supported = true
			break
		}
	}
	if !supported {
		return nil, fmt.Errorf("unsupported database type: %s, supported types: %v", cfg.Type, supportedTypes)
	}

	// Override sensitive config from environment variables
	f.overrideFromEnv(cfg)

	// Create manager
	manager := NewDatabaseManager(cfg)
	manager.SetLogger(f.logger)

	f.manager = manager
	return manager, nil
}

// overrideFromEnv overrides configuration values from environment variables.
func (f *BaseDatabaseFactory) overrideFromEnv(cfg *ConnectionConfig) {
	// Database connection info
	if host := os.Getenv("DB_HOST"); host != "" {
		cfg.Host = host
	}
	if port := os.Getenv("DB_PORT"); port != "" {
		if p, err := strconv.Atoi(port); err == nil {
			cfg.Port = p
		}
	}
	if username := os.Getenv("DB_USERNAME"); username != "" {
		cfg.Username = username
	}
	if password := os.Getenv("DB_PASSWORD"); password != "" {
		cfg.Password = password
	}
	if dbname := os.Getenv("DB_NAME"); dbname != "" {
		cfg.DBName = dbname
	}
	if sslmode := os.Getenv("DB_SSLMODE"); sslmode != "" {
		cfg.SSLMode = sslmode
	}
	if _, autoCreate := os.LookupEnv("DB_AUTO_CREATE"); autoCreate {
		cfg.AutoCreate = true
	}
	// Connection pool config
	if maxIdle := os.Getenv("DB_MAX_IDLE_CONNS"); maxIdle != "" {
		if val, err := strconv.Atoi(maxIdle); err == nil {
			cfg.MaxIdleConns = val
		}
	}
	if maxOpen := os.Getenv("DB_MAX_OPEN_CONNS"); maxOpen != "" {
		if val, err := strconv.Atoi(maxOpen); err == nil {
			cfg.MaxOpenConns = val
		}
	}
	if maxLifetime := os.Getenv("DB_CONN_MAX_LIFETIME"); maxLifetime != "" {
		if val, err := strconv.Atoi(maxLifetime); err == nil {
			cfg.ConnMaxLifetime = time.Duration(val) * time.Second
		}
	}

	// Reconnect config
	if enableReconnect := os.Getenv("DB_ENABLE_RECONNECT"); enableReconnect != "" {
		cfg.EnableReconnect = enableReconnect == "true"
	}
	if reconnectInterval := os.Getenv("DB_RECONNECT_INTERVAL"); reconnectInterval != "" {
		if val, err := strconv.Atoi(reconnectInterval); err == nil {
			cfg.ReconnectInterval = time.Duration(val) * time.Second
		}
	}

	// Logging config
	if enableQueryLog := os.Getenv("DB_ENABLE_QUERY_LOG"); enableQueryLog != "" {
		cfg.EnableQueryLog = enableQueryLog == "true"
	}
}

// InitializeDatabase connects to the database and optionally runs migrations.
func (f *BaseDatabaseFactory) InitializeDatabase(ctx context.Context, runMigrations bool) error {
	if f.manager == nil {
		return fmt.Errorf("database manager not created")
	}

	// Connect to database
	if err := f.manager.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	// Run migrations
	if runMigrations {
		if err := f.manager.RunMigrations(ctx); err != nil {
			return fmt.Errorf("failed to run database migrations: %w", err)
		}
	}
	f.logger.Info("Database initialization completed!")
	return nil
}

// GetManager returns the underlying database manager.
func (f *BaseDatabaseFactory) GetManager() AbstractDatabaseManager {
	return f.manager
}

// GetDB returns the Bun database instance, or nil if not initialized.
func (f *BaseDatabaseFactory) GetDB() *bun.DB {
	if f.manager == nil {
		return nil
	}
	return f.manager.GetDB()
}

// SetLogger sets the logger on the factory and the underlying manager.
func (f *BaseDatabaseFactory) SetLogger(logger Logger) {
	f.logger = logger
	if f.manager != nil {
		f.manager.SetLogger(logger)
	}
}

// Close closes the database connection managed by the factory.
func (f *BaseDatabaseFactory) Close() error {
	if f.manager == nil {
		return nil
	}
	return f.manager.Disconnect()
}

// GetHealthStatus returns the current database health status from the manager.
func (f *BaseDatabaseFactory) GetHealthStatus(ctx context.Context) *HealthStatus {
	if f.manager == nil {
		return &HealthStatus{
			Healthy:       false,
			Connected:     false,
			LastError:     "Database manager not initialized",
			LastCheckTime: time.Now(),
		}
	}
	return f.manager.HealthCheck(ctx)
}

// GetStats returns database connection statistics from the manager.
func (f *BaseDatabaseFactory) GetStats() *DBStats {
	if f.manager == nil {
		return &DBStats{}
	}
	return f.manager.GetStats()
}
