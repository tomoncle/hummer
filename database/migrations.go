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
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/uptrace/bun"
)

// MigrationManager coordinates schema migrations and data initialization.
type MigrationManager struct {
	db                   *bun.DB
	logger               Logger
	environment          string
	migrationCache       map[string]struct{}
	tableSchemaCache     map[string]struct{}
	schemaCacheMu        sync.RWMutex
	migrationCacheMu     sync.RWMutex
	tableSchemaCacheMu   sync.RWMutex
	columnSchemaCache    map[string]map[string]columnSpec
	indexSchemaCache     map[string][]indexSpec
	schemaCacheLoadedAt  time.Time
	schemaCacheLoadOnce  bool
	schemaCacheHits      int
	schemaCacheMisses    int
	schemaCacheRefreshes int
}

// Migration represents an applied migration record stored in the database.
type Migration struct {
	Version     string    `bun:"version,pk"`
	Name        string    `bun:"name"`
	AppliedAt   time.Time `bun:"applied_at"`
	Description string    `bun:"description"`
}

// MigrationFunc is a migration step executed within a transaction.
type MigrationFunc func(ctx context.Context, db bun.IDB) error

// MigrationItem describes a single migration version with up/down functions.
type MigrationItem struct {
	Version     string
	Name        string
	Description string
	Up          MigrationFunc
	Down        MigrationFunc
}

// NewMigrationManager constructs a new MigrationManager using the provided Bun
// database and logger. The default environment is "development".
func NewMigrationManager(db *bun.DB, logger Logger) *MigrationManager {
	return &MigrationManager{
		db:          db,
		logger:      logger,
		environment: "development", // default env
	}
}

// SetEnvironment sets the environment used when initializing data from SQL.
func (mm *MigrationManager) SetEnvironment(env string) {
	mm.environment = env
}

// RunMigrations creates the migration tracking table if needed and executes all
// registered migrations in ascending version order.
func (mm *MigrationManager) RunMigrations(ctx context.Context) error {
	// silent migration
	if _, ok := os.LookupEnv("BUNDEBUG_MIGRATION"); !ok {
		EnableBunSqlSilent(true)
	}

	if mm.db == nil {
		return fmt.Errorf("database not initialized")
	}

	if err := mm.createMigrationTable(ctx); err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}

	if globalConfig != nil && globalConfig.DataMigrateConfig.EnableSchemaSync {
		ttl := time.Minute * 5
		if globalConfig != nil && globalConfig.DataMigrateConfig.SchemaMetaCacheTTL > 0 {
			ttl = globalConfig.DataMigrateConfig.SchemaMetaCacheTTL
		}
		_ = mm.ensureSchemaMetadataCaches(ctx, mm.db, ttl)
		if err := mm.SynchronizeSchema(ctx, mm.db); err != nil {
			return fmt.Errorf("sync schema failed: %w", err)
		}
	}
	migrations := mm.getAllMigrations()

	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})

	for _, migration := range migrations {
		if err := mm.runMigration(ctx, migration); err != nil {
			return fmt.Errorf("failed to execute migration %s: %w", migration.Version, err)
		}
	}

	if mm.logger != nil {
		mm.logger.Info("Database migrations completed!")
	}

	EnableBunSqlSilent(false)
	return nil
}

func (mm *MigrationManager) RefreshSchemaMetadataForTables(ctx context.Context, db bun.IDB, tables []string) error {
	mm.schemaCacheMu.Lock()
	defer mm.schemaCacheMu.Unlock()
	if len(tables) == 0 {
		return nil
	}
	colsByTable, errCols := listExistingColumnsSome(ctx, db, tables)
	idxByTable, errIdx := listExistingIndexesSome(ctx, db, tables)
	if errCols != nil {
		return errCols
	}
	if errIdx != nil {
		return errIdx
	}
	if mm.columnSchemaCache == nil {
		mm.columnSchemaCache = make(map[string]map[string]columnSpec)
	}
	if mm.indexSchemaCache == nil {
		mm.indexSchemaCache = make(map[string][]indexSpec)
	}
	updatedTbl := 0
	for _, t := range tables {
		key := strings.ToLower(t)
		if colsByTable != nil {
			if cols, ok := colsByTable[key]; ok && cols != nil {
				mm.columnSchemaCache[key] = cols
				updatedTbl++
			}
		} else {
			cols, err := listExistingColumns(ctx, db, t)
			if err == nil {
				mm.columnSchemaCache[key] = cols
				updatedTbl++
			}
		}
		if idxByTable != nil {
			if idx, ok := idxByTable[key]; ok && idx != nil {
				mm.indexSchemaCache[key] = idx
			} else {
				i, err := listExistingIndexes(ctx, db, t)
				if err == nil {
					mm.indexSchemaCache[key] = i
				}
			}
		} else {
			i, err := listExistingIndexes(ctx, db, t)
			if err == nil {
				mm.indexSchemaCache[key] = i
			}
		}
	}
	mm.schemaCacheLoadedAt = time.Now()
	mm.schemaCacheRefreshes++
	if mm.logger != nil && globalConfig != nil && globalConfig.DataMigrateConfig.SchemaMetaAuditLog {
		mm.logger.Info("Schema metadata cache refreshes", "tables", updatedTbl)
	}
	return nil
}

func (mm *MigrationManager) createMigrationTable(ctx context.Context) error {
	_, err := mm.db.NewCreateTable().
		Model((*Migration)(nil)).
		IfNotExists().
		Exec(ctx)
	return err
}

func (mm *MigrationManager) getAllMigrations() []MigrationItem {

	migrations := []MigrationItem{
		{
			Version:     "001",
			Name:        "create_base_tables",
			Description: "Create base table structure",
			Up:          mm.createBaseTables,
		},
	}
	if globalConfig.DataMigrateConfig.EnableForeignKey {
		migrations = append(migrations, MigrationItem{
			Version:     "002",
			Name:        "add_foreign_keys",
			Description: "Add table foreign key constraints",
			Up:          mm.addForeignKeys,
		})
	}
	if globalConfig.DataInitConfig.AutoInitOnMigration {
		migrations = append(migrations, MigrationItem{
			Version:     "003",
			Name:        "seed_initial_data",
			Description: "Seed initial data",
			Up:          mm.seedInitialData,
		})
	}
	return migrations
}

func (mm *MigrationManager) runMigration(ctx context.Context, migration MigrationItem) error {
	exists, err := mm.db.NewSelect().
		Model((*Migration)(nil)).
		Where("version = ?", migration.Version).
		Exists(ctx)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	tx, err := mm.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	var committed bool
	defer func(tx bun.Tx) {
		if !committed {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				mm.logger.Error("Failed to rollback transaction", "error", rollbackErr)
			}
		}
	}(tx)

	if err := migration.Up(ctx, tx); err != nil {
		return err
	}

	migrationRecord := &Migration{
		Version:     migration.Version,
		Name:        migration.Name,
		AppliedAt:   time.Now(),
		Description: migration.Description,
	}

	_, err = tx.NewInsert().
		Model(migrationRecord).
		Exec(ctx)
	if err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	if mm.logger != nil {
		mm.logger.Info("Migration executed successfully", "version", migration.Version, "name", migration.Name)
	}

	return nil
}

func (mm *MigrationManager) createBaseTables(ctx context.Context, db bun.IDB) error {
	for _, model := range RegisteredModelInstances() {
		_, err := db.NewCreateTable().
			Model(model).
			IfNotExists().
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("failed to create table %T: %w", model, err)
		}
	}
	return nil
}

func (mm *MigrationManager) addForeignKeys(ctx context.Context, db bun.IDB) error {
	configPath := globalConfig.DataMigrateConfig.ForeignKeyFile
	fkManager, err := NewConfigurableForeignKeyManager(mm.logger, configPath)
	if err != nil {
		if mm.logger != nil {
			mm.logger.Debug("Failed to use config-based foreign key manager, falling back to code-defined", "error", err.Error())
		}
		fkManager := NewForeignKeyManager(mm.logger)
		return fkManager.AddAllForeignKeys(ctx, db)
	}

	if errors := fkManager.ValidateConstraints(); len(errors) > 0 {
		for _, err := range errors {
			if mm.logger != nil {
				mm.logger.Debug("Foreign key constraint validation failed", "error", err.Error())
			}
		}
		return fmt.Errorf("foreign key constraint validation failed, %d errors in total", len(errors))
	}

	if mm.logger != nil {
		mm.logger.Debug("Managing foreign key constraints using config file", "config_path", configPath)
	}

	return fkManager.AddAllForeignKeys(ctx, db)
}

func (mm *MigrationManager) InitData(ctx context.Context) error {
	if mm.db == nil {
		return fmt.Errorf("database not initialized")
	}
	return mm.seedInitialData(ctx, mm.db)
}

func (mm *MigrationManager) seedInitialData(ctx context.Context, db bun.IDB) error {
	// Initialize data using SQL files
	return mm.seedDataFromSQL()
}

func (mm *MigrationManager) seedDataFromSQL() error {
	sqlManager := NewSQLInitManager(mm.db, mm.environment)
	if globalConfig.DataInitConfig.Filepath != "" {
		sqlManager.SetSQLRootPath(globalConfig.DataInitConfig.Filepath)
	}

	if mm.logger != nil {
		mm.logger.Info("Starting data initialization using SQL files", "environment", mm.environment)
	}

	err := sqlManager.ExecuteInitialization()
	if err != nil {
		return fmt.Errorf("SQL file initialization failed: %w", err)
	}

	if mm.logger != nil {
		mm.logger.Info("SQL file initialization completed")
	}

	return nil
}

func getModelName(model interface{}) string {
	t := reflect.TypeOf(model)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.Name()
}

func (mm *MigrationManager) GetAppliedMigrations(ctx context.Context) ([]Migration, error) {
	// GetAppliedMigrations returns migration records ordered by version.
	var migrations []Migration
	err := mm.db.NewSelect().
		Model(&migrations).
		Order("version ASC").
		Scan(ctx)
	return migrations, err
}

func (mm *MigrationManager) RollbackMigration(ctx context.Context, version string) error {
	// RollbackMigration is currently not implemented.
	return fmt.Errorf("migration rollback is not implemented yet")
}

func (mm *MigrationManager) ensureSchemaMetadataCaches(ctx context.Context, db bun.IDB, ttl time.Duration) error {
	mm.schemaCacheMu.Lock()
	defer mm.schemaCacheMu.Unlock()
	if mm.columnSchemaCache != nil && mm.indexSchemaCache != nil {
		if mm.schemaCacheLoadOnce {
			return nil
		}
		if ttl <= 0 || time.Since(mm.schemaCacheLoadedAt) < ttl {
			return nil
		}
	}
	colsByTable, errCols := listExistingColumnsAll(ctx, db)
	idxByTable, errIdx := listExistingIndexesAll(ctx, db)
	if errCols != nil {
		return errCols
	}
	if errIdx != nil {
		return errIdx
	}
	mm.columnSchemaCache = colsByTable
	mm.indexSchemaCache = idxByTable
	mm.schemaCacheLoadedAt = time.Now()
	mm.schemaCacheRefreshes++
	if mm.logger != nil && globalConfig != nil && globalConfig.DataMigrateConfig.SchemaMetaAuditLog {
		loadedTables := 0
		if mm.columnSchemaCache != nil {
			loadedTables = len(mm.columnSchemaCache)
		}
		mm.logger.Info("Schema metadata cache refreshed", "tables", loadedTables, "ttl_sec", int(ttl.Seconds()), "load_once", mm.schemaCacheLoadOnce)
	}
	return nil
}

func (mm *MigrationManager) InvalidateSchemaMetadataCaches() {
	mm.schemaCacheMu.Lock()
	defer mm.schemaCacheMu.Unlock()
	mm.columnSchemaCache = nil
	mm.indexSchemaCache = nil
	mm.schemaCacheLoadedAt = time.Time{}
}

func (mm *MigrationManager) getColumnSchemaForTable(ctx context.Context, db bun.IDB, table string) (map[string]columnSpec, error) {
	mm.schemaCacheMu.Lock()
	defer mm.schemaCacheMu.Unlock()
	if mm.columnSchemaCache != nil {
		res := mm.columnSchemaCache[strings.ToLower(table)]
		if res != nil {
			mm.schemaCacheHits++
			return res, nil
		}
		mm.schemaCacheMisses++
		cols, err := listExistingColumns(ctx, db, table)
		if err != nil {
			return nil, err
		}
		if mm.columnSchemaCache == nil {
			mm.columnSchemaCache = make(map[string]map[string]columnSpec)
		}
		mm.columnSchemaCache[strings.ToLower(table)] = cols
		return cols, nil
	}
	return listExistingColumns(ctx, db, table)
}

func (mm *MigrationManager) getIndexSchemaForTable(ctx context.Context, db bun.IDB, table string) ([]indexSpec, error) {
	mm.schemaCacheMu.Lock()
	defer mm.schemaCacheMu.Unlock()
	if mm.indexSchemaCache != nil {
		res := mm.indexSchemaCache[strings.ToLower(table)]
		if res != nil {
			mm.schemaCacheHits++
			return res, nil
		}
		mm.schemaCacheMisses++
		idx, err := listExistingIndexes(ctx, db, table)
		if err != nil {
			return nil, err
		}
		// 写回缓存
		if mm.indexSchemaCache == nil {
			mm.indexSchemaCache = make(map[string][]indexSpec)
		}
		mm.indexSchemaCache[strings.ToLower(table)] = idx
		return idx, nil
	}
	return listExistingIndexes(ctx, db, table)
}
