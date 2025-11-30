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
	"github.com/uptrace/bun"
)

var (
	globalFactory *BaseDatabaseFactory
	globalConfig  *Config
	DB            *bun.DB
)

// GetDB returns the global Bun database instance.
func GetDB() *bun.DB {
	if globalFactory != nil {
		return globalFactory.GetDB()
	}
	return DB
}

// GetDatabaseManager returns the global database manager.
func GetDatabaseManager() AbstractDatabaseManager {
	if globalFactory != nil {
		return globalFactory.GetManager()
	}
	return nil
}

// GetDatabaseFactory returns the global database factory.
func GetDatabaseFactory() *BaseDatabaseFactory {
	return globalFactory
}

// InitDB initializes the global database using the provided configuration.
func InitDB(cfg *Config) (*bun.DB, error) {
	globalConfig = cfg
	return InitDatabaseWithOptions(cfg, cfg.DataMigrateConfig.EnableMigrateOnStartup)
}

// InitDatabaseWithOptions initializes the database and optionally runs migrations.
func InitDatabaseWithOptions(cfg *Config, runMigrations bool) (*bun.DB, error) {
	if cfg == nil {
		return nil, fmt.Errorf("database configuration cannot be empty")
	}
	globalFactory = NewDatabaseFactory()
	manager, err := globalFactory.CreateFromConfig(&cfg.ConnectionConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create database manager: %w", err)
	}

	ctx := context.Background()
	if err := globalFactory.InitializeDatabase(ctx, runMigrations); err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	DB = manager.GetDB()
	DB.RegisterModel(RegisteredModelInstances()...)
	return DB, nil
}

// CloseDB closes the global database connection (backward compatibility).
func CloseDB() error {
	if globalFactory != nil {
		return globalFactory.Close()
	}
	return nil
}

// GetHealthStatus returns the current database health status.
func GetHealthStatus(ctx context.Context) *HealthStatus {
	if globalFactory != nil {
		return globalFactory.GetHealthStatus(ctx)
	}
	return &HealthStatus{
		Healthy:   false,
		Connected: false,
		LastError: "Database not initialized",
	}
}

// GetDatabaseStats returns global database statistics.
func GetDatabaseStats() *DBStats {
	if globalFactory != nil {
		return globalFactory.GetStats()
	}
	return &DBStats{}
}

// RunMigrations executes database migrations (backward compatibility).
func RunMigrations() error {
	if globalFactory == nil {
		return fmt.Errorf("database not initialized")
	}
	manager := globalFactory.GetManager()
	if manager == nil {
		return fmt.Errorf("database manager not initialized")
	}
	return manager.RunMigrations(context.Background())
}

// InitData seeds initial data using the configured environment (backward compatibility).
func InitData() error {
	// Set default value
	defaultEnv := "prod"

	// If configuration is loaded, use its value
	if globalConfig.DataInitConfig.Environment != "" {
		defaultEnv = globalConfig.DataInitConfig.Environment
	}

	return InitDataWithSQL(defaultEnv)
}

// InitDataWithSQL seeds initial data by executing SQL files for the environment.
func InitDataWithSQL(environment string) error {
	if globalFactory == nil {
		return fmt.Errorf("database not initialized")
	}
	manager := globalFactory.GetManager()
	if manager == nil {
		return fmt.Errorf("database manager not initialized")
	}

	// Directly create migration manager and set options
	db := manager.GetDB()
	if db == nil {
		return fmt.Errorf("database instance not initialized")
	}

	// Get SQL file path
	filepath := "configs/sql"
	if globalConfig.DataInitConfig.Filepath != "" {
		filepath = globalConfig.DataInitConfig.Filepath
	}

	// Directly use SQLInitManager
	sqlManager := NewSQLInitManager(db, environment)
	sqlManager.SetSQLRootPath(filepath)
	return sqlManager.ExecuteInitialization()
}
