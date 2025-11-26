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
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"time"

	"github.com/uptrace/bun"
)

// AbstractDatabaseManager defines the operations for managing a database
// connection, running migrations, initializing data, and reporting health.
type AbstractDatabaseManager interface {
	Connect(ctx context.Context) error
	Disconnect() error
	Reconnect(ctx context.Context) error
	Ping(ctx context.Context) error
	HealthCheck(ctx context.Context) *HealthStatus
	GetDB() *bun.DB
	GetSQLDB() *sql.DB
	RunMigrations(ctx context.Context) error
	InitData(ctx context.Context) error
	GetStats() *DBStats
	SetLogger(logger Logger)
}

// AbstractDatabaseConfigProvider exposes configuration loading.
type AbstractDatabaseConfigProvider interface {
	ConfigLoader() *Config
}

// HealthStatus holds the result of a health check against the database.
type HealthStatus struct {
	Healthy       bool          `json:"healthy"`
	Connected     bool          `json:"connected"`
	ResponseTime  time.Duration `json:"response_time"`
	ActiveConns   int           `json:"active_conns"`
	IdleConns     int           `json:"idle_conns"`
	MaxOpenConns  int           `json:"max_open_conns"`
	LastError     string        `json:"last_error,omitempty"`
	LastCheckTime time.Time     `json:"last_check_time"`
}

// DBStats mirrors database/sql stats returned by the manager.
type DBStats struct {
	MaxOpenConns      int           `json:"max_open_conns"`
	OpenConns         int           `json:"open_conns"`
	InUse             int           `json:"in_use"`
	Idle              int           `json:"idle"`
	WaitCount         int64         `json:"wait_count"`
	WaitDuration      time.Duration `json:"wait_duration"`
	MaxIdleClosed     int64         `json:"max_idle_closed"`
	MaxIdleTimeClosed int64         `json:"max_idle_time_closed"`
	MaxLifetimeClosed int64         `json:"max_lifetime_closed"`
}

// ConnectionConfig describes how to connect to a database and tune its pool.
type ConnectionConfig struct {
	Type                string        `json:"type"` // postgres、mysql、sqlite
	Host                string        `json:"host"`
	Port                int           `json:"port"`
	Username            string        `json:"username"`
	Password            string        `json:"password"`
	DBName              string        `json:"dbname"`
	SSLMode             string        `json:"sslmode"`
	MaxIdleConns        int           `json:"max_idle_conns"`
	MaxOpenConns        int           `json:"max_open_conns"`
	ConnMaxLifetime     time.Duration `json:"conn_max_lifetime"`
	ConnMaxIdleTime     time.Duration `json:"conn_max_idle_time"`
	ConnectTimeout      time.Duration `json:"connect_timeout"`
	ReadTimeout         time.Duration `json:"read_timeout"`
	WriteTimeout        time.Duration `json:"write_timeout"`
	EnableReconnect     bool          `json:"enable_reconnect"`
	ReconnectInterval   time.Duration `json:"reconnect_interval"`
	MaxReconnectTries   int           `json:"max_reconnect_tries"`
	HealthCheckInterval time.Duration `json:"health_check_interval"`
	EnableQueryLog      bool          `json:"enable_query_log"`
	SlowQueryTime       time.Duration `json:"slow_query_time"`
	AutoCreate          bool          `json:"auto_create"`
	Charset             string        `json:"charset"` // MySQL:utf8mb4  、Postgres:UTF8
	Template            string        `json:"template"`
}

// DataMigrateConfig controls schema migration behavior on startup.
type DataMigrateConfig struct {
	EnableMigrateOnStartup    bool          `json:"enable_migrate_on_startup"`
	EnableForeignKey          bool          `json:"enable_foreign_key"`
	ForeignKeyFile            string        `json:"foreign_key_file"`
	EnableSchemaSync          bool          `json:"enable_schema_sync"`
	AllowColumnAdd            bool          `json:"allow_column_add"`
	AllowColumnModify         bool          `json:"allow_column_modify"`
	AllowColumnDrop           bool          `json:"allow_column_drop"`
	AllowIndexAdd             bool          `json:"allow_index_add"`
	AllowIndexDrop            bool          `json:"allow_index_drop"`
	EnforceNotNullWithDefault bool          `json:"enforce_not_null_with_default"`
	SchemaMetaCacheTTL        time.Duration `json:"schema_meta_cache_ttl"`
	SchemaMetaCacheLoadOnce   bool          `json:"schema_meta_cache_load_once"`
	SchemaMetaAuditLog        bool          `json:"schema_meta_audit_log"`
}

// DataInitConfig controls data seeding behavior and environment selection.
type DataInitConfig struct {
	AutoInitOnStartup   bool   `json:"auto_init_on_startup"`
	AutoInitOnMigration bool   `json:"auto_init_on_migration"`
	Filepath            string `json:"filepath"`
	Environment         string `json:"environment"`
}

// Config aggregates connection, migration, and data initialization settings.
type Config struct {
	ConnectionConfig  ConnectionConfig  `json:"connection_config"`
	DataMigrateConfig DataMigrateConfig `json:"data_migrate_config"`
	DataInitConfig    DataInitConfig    `json:"data_init_config"`
}

// DefaultConnectionConfig returns a connection config with sensible defaults.
func DefaultConnectionConfig() *ConnectionConfig {
	return &ConnectionConfig{
		MaxIdleConns:        10,
		MaxOpenConns:        100,
		ConnMaxLifetime:     time.Hour,
		ConnMaxIdleTime:     time.Minute * 30,
		ConnectTimeout:      time.Second * 10,
		ReadTimeout:         time.Second * 30,
		WriteTimeout:        time.Second * 30,
		EnableReconnect:     true,
		ReconnectInterval:   time.Second * 5,
		MaxReconnectTries:   3,
		HealthCheckInterval: time.Minute * 5,
		EnableQueryLog:      false,
		SlowQueryTime:       time.Second * 2,
	}
}

// ForeignKeyConfig is the YAML structure that lists foreign key constraints.
type ForeignKeyConfig struct {
	ForeignKeys []ForeignKeyConstraintConfig `yaml:"foreign_keys"`
}

// ForeignKeyConstraintConfig describes a single foreign key in configuration.
type ForeignKeyConstraintConfig struct {
	Table           string `yaml:"table"`
	Column          string `yaml:"column"`
	ReferenceTable  string `yaml:"reference_table"`
	ReferenceColumn string `yaml:"reference_column"`
	OnDelete        string `yaml:"on_delete"`
	OnUpdate        string `yaml:"on_update"`
	ConstraintName  string `yaml:"constraint_name"`
	Description     string `yaml:"description"`
}

// ToForeignKeyConstraint converts the config entry into a runtime constraint.
func (fkc *ForeignKeyConstraintConfig) ToForeignKeyConstraint() ForeignKeyConstraint {
	return ForeignKeyConstraint{
		Table:           fkc.Table,
		Column:          fkc.Column,
		ReferenceTable:  fkc.ReferenceTable,
		ReferenceColumn: fkc.ReferenceColumn,
		OnDelete:        fkc.OnDelete,
		OnUpdate:        fkc.OnUpdate,
		ConstraintName:  fkc.ConstraintName,
	}
}

// ConfigurableForeignKeyManager loads foreign key constraints from a YAML
// configuration file and falls back to code-defined defaults.
type ConfigurableForeignKeyManager struct {
	*ForeignKeyManager
	configPath string
}

// NewConfigurableForeignKeyManager creates a foreign key manager using the
// provided YAML configuration file path.
func NewConfigurableForeignKeyManager(logger Logger, configPath string) (*ConfigurableForeignKeyManager, error) {
	manager := &ConfigurableForeignKeyManager{
		configPath: configPath,
	}
	constraints, err := manager.loadFromConfig()
	if err != nil {
		if logger != nil {
			logger.Debug("Failed to load foreign key constraints from config, using code-defined defaults", "error", err.Error(), "config_path", configPath)
		}
		constraints = getForeignKeyConstraints()
	}

	manager.ForeignKeyManager = &ForeignKeyManager{
		constraints: constraints,
		logger:      logger,
	}

	return manager, nil
}

func (cfm *ConfigurableForeignKeyManager) loadFromConfig() ([]ForeignKeyConstraint, error) {
	if _, err := os.Stat(cfm.configPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("config file does not exist: %s", cfm.configPath)
	}

	data, err := os.ReadFile(cfm.configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config ForeignKeyConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	var constraints []ForeignKeyConstraint
	for _, fkConfig := range config.ForeignKeys {
		constraints = append(constraints, fkConfig.ToForeignKeyConstraint())
	}

	return constraints, nil
}

func (cfm *ConfigurableForeignKeyManager) ReloadConfig() error {
	// ReloadConfig refreshes constraints from the YAML configuration file.
	constraints, err := cfm.loadFromConfig()
	if err != nil {
		return err
	}

	cfm.constraints = constraints
	return nil
}

func (cfm *ConfigurableForeignKeyManager) ExportToConfig(outputPath string) error {
	// ExportToConfig writes the current constraints into a YAML configuration
	// file at the given output path, creating directories as needed.
	var configConstraints []ForeignKeyConstraintConfig
	for _, constraint := range cfm.constraints {
		configConstraints = append(configConstraints, ForeignKeyConstraintConfig{
			Table:           constraint.Table,
			Column:          constraint.Column,
			ReferenceTable:  constraint.ReferenceTable,
			ReferenceColumn: constraint.ReferenceColumn,
			OnDelete:        constraint.OnDelete,
			OnUpdate:        constraint.OnUpdate,
			ConstraintName:  constraint.ConstraintName,
			Description:     fmt.Sprintf("%s.%s -> %s.%s", constraint.Table, constraint.Column, constraint.ReferenceTable, constraint.ReferenceColumn),
		})
	}

	config := ForeignKeyConfig{
		ForeignKeys: configConstraints,
	}

	data, err := yaml.Marshal(&config)
	if err != nil {
		return fmt.Errorf("failed to serialize config: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	if err := os.WriteFile(outputPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

func (cfm *ConfigurableForeignKeyManager) GetConfigPath() string {
	// GetConfigPath returns the path to the YAML configuration file.
	return cfm.configPath
}
