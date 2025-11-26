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
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/mysqldialect"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	"github.com/uptrace/bun/driver/sqliteshim"
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

	db.AddQueryHook(&QueryHook{
		envName: "BUNDEBUG",
		enabled: dm.config.EnableQueryLog,
		verbose: true,
		writer:  os.Stderr,
	})
	db.AddQueryHook(&SlowQueryHook{
		slowTime: dm.config.SlowQueryTime,
		enabled:  dm.config.SlowQueryTime > 0,
		fromEnv:  "BUNDEBUG_SLOWQUERY",
		writer:   os.Stderr,
	})
	return sqlDB, db, nil
}

func (dm *defaultDatabaseManager) createMySQLConnection() (*sql.DB, *bun.DB, error) {
	dm.ensureMySQLDatabase()
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
	dm.ensurePostgresDatabase()
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
		dm.reconnectTries = 0
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
		DB = dm.GetDB()
		DB.RegisterModel(RegisteredModelInstances()...)
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

func (dm *defaultDatabaseManager) ensurePostgresDatabase() {
	if !dm.config.AutoCreate {
		return
	}
	charset := dm.config.Charset
	if dm.config.Charset == "" {
		charset = "UTF8"
	}
	sslmode := dm.config.SSLMode
	if sslmode == "" {
		sslmode = "disable"
	}
	template := dm.config.Template
	if template == "" {
		template = "template0"
	}
	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(dm.config.Username, dm.config.Password),
		Host:   fmt.Sprintf("%s:%d", dm.config.Host, dm.config.Port),
		Path:   "postgres",
	}
	q := u.Query()
	q.Set("sslmode", sslmode)
	q.Set("connect_timeout", strconv.Itoa(int(dm.config.ConnectTimeout.Seconds())))
	u.RawQuery = q.Encode()
	dsn := u.String()

	root, err := sql.Open("postgres", dsn)
	if err != nil {
		dm.logger.Error("Failed to connect to database:", "address", fmt.Sprintf("%v:%v", dm.config.Host, dm.config.Port), "error", err)
		return
	}
	defer root.Close()

	var exists int
	err = root.QueryRow("SELECT COUNT(*) FROM pg_database WHERE datname = $1", dm.config.DBName).Scan(&exists)
	if err != nil {
		dm.logger.Error("Query database failed:", "database", dm.config.DBName, "error", err)
		return
	}
	if exists == 0 {
		dm.logger.Warn("Database detection does not exist:", "database", dm.config.DBName)
		create := fmt.Sprintf(`CREATE DATABASE "%s" WITH OWNER "%s" ENCODING '%s' TEMPLATE %s`,
			dm.config.DBName, dm.config.Username, charset, template)
		if _, err = root.Exec(create); err != nil {
			dm.logger.Error("Failed to create database:", "database", dm.config.DBName, "error", err)
			return
		}
		dm.logger.Debug("Database has been created successfully:", "database", dm.config.DBName)
	}
}

func (dm *defaultDatabaseManager) ensureMySQLDatabase() {
	if !dm.config.AutoCreate {
		return
	}
	charset := dm.config.Charset
	if dm.config.Charset == "" {
		charset = "utf8mb4 COLLATE utf8mb4_general_ci"
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?parseTime=true&multiStatements=true",
		dm.config.Username,
		dm.config.Password,
		dm.config.Host,
		dm.config.Port)
	root, err := sql.Open("mysql", dsn)
	if err != nil {
		dm.logger.Error("Failed to connect to database:", "address", fmt.Sprintf("%v:%v", dm.config.Host, dm.config.Port), "error", err)
		return
	}
	defer func(root *sql.DB) {
		_ = root.Close()
	}(root)

	var exists int
	err = root.QueryRow("SELECT COUNT(*) FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ?", dm.config.DBName).Scan(&exists)
	if err != nil {
		dm.logger.Error("Query database failed:", "database", dm.config.DBName, "error", err)
		return
	}
	if exists == 0 {
		dm.logger.Warn("Database detection does not exist:", "database", dm.config.DBName)
		create := fmt.Sprintf("CREATE DATABASE /*!32312 IF NOT EXISTS*/`%s` /*!40100 DEFAULT CHARACTER SET %s */",
			dm.config.DBName, charset)
		if _, err = root.Exec(create); err != nil {
			dm.logger.Error("Failed to create database:", "database", dm.config.DBName, "error", err)
			return
		}
		dm.logger.Debug("Database has been created successfully:", "database", dm.config.DBName)
	}
}
