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
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/uptrace/bun"
)

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

type Migration struct {
	bun.BaseModel `bun:"bun_migrations"`
	Version       string    `bun:"version,pk"`
	Name          string    `bun:"name"`
	AppliedAt     time.Time `bun:"applied_at"`
	Description   string    `bun:"description"`
}

type MigrationFunc func(ctx context.Context, db bun.IDB) error

type MigrationItem struct {
	Version     string
	Name        string
	Description string
	Up          MigrationFunc
	Down        MigrationFunc
}

func NewMigrationManager(db *bun.DB, logger Logger) *MigrationManager {
	return &MigrationManager{
		db:          db,
		logger:      logger,
		environment: "prod",
		schemaCacheLoadOnce: func() bool {
			if globalConfig != nil {
				return globalConfig.DataMigrateConfig.SchemaMetaCacheLoadOnce
			}
			return false
		}(),
	}
}

func (mm *MigrationManager) SetEnvironment(env string) {
	mm.environment = env
}

func (mm *MigrationManager) RunMigrations(ctx context.Context) error {
	if mm.db == nil {
		return fmt.Errorf("database not initialized")
	}
	// silent migration
	if _, ok := os.LookupEnv("BUNDEBUG_MIGRATION"); !ok {
		EnableBunSqlSilent(true)
	}
	if _, err := mm.getExistingTablesSet(ctx, mm.db); err != nil {
		return fmt.Errorf("failed to query existing tables: %w", err)
	}

	if err := mm.createMigrationTable(ctx); err != nil {
		return fmt.Errorf("failed to create migration table: %w", err)
	}

	appliedSet, err := mm.getAppliedMigrationSet(ctx, mm.db)
	if err != nil {
		return fmt.Errorf("failed to query applied migrations: %w", err)
	}
	mm.migrationCacheMu.Lock()
	mm.migrationCache = appliedSet
	mm.migrationCacheMu.Unlock()

	mm.migrationCacheMu.RLock()
	_, ok := mm.migrationCache[mm.tableInitVersion("bun_migrations")]
	mm.migrationCacheMu.RUnlock()
	if !ok {
		if err := mm.recordTableInit(ctx, mm.db, "bun_migrations"); err != nil {
			return fmt.Errorf("failed to record bun_migrations initialization: %w", err)
		}
	}

	if err := mm.createBaseTables(ctx, mm.db); err != nil {
		return fmt.Errorf("failed to sync base table structures: %w", err)
	}

	if globalConfig != nil && globalConfig.DataMigrateConfig.EnableSchemaSync {
		ttl := time.Minute * 5
		if globalConfig != nil && globalConfig.DataMigrateConfig.SchemaMetaCacheTTL > 0 {
			ttl = globalConfig.DataMigrateConfig.SchemaMetaCacheTTL
		}
		_ = mm.ensureSchemaMetadataCaches(ctx, mm.db, ttl)
		if err := mm.SynchronizeSchema(ctx, mm.db); err != nil {
			return fmt.Errorf("failed to synchronize columns and indexes: %w", err)
		}
	}

	if globalConfig != nil && globalConfig.DataMigrateConfig.EnableForeignKey {
		if err := mm.addForeignKeys(ctx, mm.db); err != nil {
			return fmt.Errorf("failed to synchronize foreign keys: %w", err)
		}
	}

	if globalConfig != nil && globalConfig.DataInitConfig.AutoInitOnMigration {
		if err := mm.seedInitialData(ctx, mm.db); err != nil {
			return fmt.Errorf("failed to initialize data: %w", err)
		}
	}

	if mm.logger != nil {
		mm.logger.Info("Database migrations completed!")
	}
	EnableBunSqlSilent(false)
	return nil
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
		if mm.indexSchemaCache == nil {
			mm.indexSchemaCache = make(map[string][]indexSpec)
		}
		mm.indexSchemaCache[strings.ToLower(table)] = idx
		return idx, nil
	}
	return listExistingIndexes(ctx, db, table)
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
		mm.logger.Info("Schema metadata cache incrementally refreshed", "tables", updatedTbl)
	}
	return nil
}

func (mm *MigrationManager) createMigrationTable(ctx context.Context) error {
	existingSet, err := mm.getExistingTablesSet(ctx, mm.db)
	if err != nil {
		return err
	}
	_, exists := existingSet["bun_migrations"]
	if !exists {
		if _, err := mm.db.NewCreateTable().
			Model((*Migration)(nil)).
			IfNotExists().
			Exec(ctx); err != nil {
			return err
		}
		mm.tableSchemaCacheMu.Lock()
		if mm.tableSchemaCache == nil {
			mm.tableSchemaCache = make(map[string]struct{})
		}
		mm.tableSchemaCache["bun_migrations"] = struct{}{}
		mm.tableSchemaCacheMu.Unlock()
	}
	return nil
}

func (mm *MigrationManager) getAllMigrations() []MigrationItem {

	migrations := []MigrationItem{
		{
			Version:     "001",
			Name:        "create_base_tables",
			Description: "create base table structures",
			Up:          mm.createBaseTables,
		},
	}
	if globalConfig.DataMigrateConfig.EnableForeignKey {
		migrations = append(migrations, MigrationItem{
			Version:     "002",
			Name:        "add_foreign_keys",
			Description: "add table foreign key constraints",
			Up:          mm.addForeignKeys,
		})
	}
	if globalConfig.DataInitConfig.AutoInitOnMigration {
		migrations = append(migrations, MigrationItem{
			Version:     "003",
			Name:        "seed_initial_data",
			Description: "seed initial data",
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
		if mm.logger != nil {
			mm.logger.Debug("migration skipped:", "action", migration.Description, "version", migration.Version)
		}
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
				mm.logger.Error("failed to rollback transaction", "error", rollbackErr)
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
		mm.logger.Info("migration executed successfully", "version", migration.Version, "name", migration.Name)
	}

	return nil
}

func (mm *MigrationManager) createBaseTables(ctx context.Context, db bun.IDB) error {
	appliedSet := mm.migrationCache
	if appliedSet == nil {
		var err error
		appliedSet, err = mm.getAppliedMigrationSet(ctx, mm.db)
		if err != nil {
			return fmt.Errorf("failed to query applied migrations: %w", err)
		}
		mm.migrationCache = appliedSet
	}
	recordedSet := make(map[string]struct{})
	for v := range appliedSet {
		if strings.HasPrefix(v, "table_sync:") {
			tbl := strings.TrimPrefix(v, "table_sync:")
			recordedSet[strings.ToLower(tbl)] = struct{}{}
		}
	}
	existingSet, err := mm.getExistingTablesSet(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to query existing tables: %w", err)
	}

	for _, model := range RegisteredModelInstances() {
		tableName, err := resolveTableName(model)
		if err != nil {
			return fmt.Errorf("failed to resolve table name %T: %w", model, err)
		}
		key := strings.ToLower(tableName)
		_, exists := existingSet[key]
		_, recorded := recordedSet[key]

		if exists {
			if !recorded {
				if err := mm.recordTableInit(ctx, db, tableName); err != nil {
					return fmt.Errorf("failed to record table initialization %s: %w", tableName, err)
				}
			}
			continue
		}

		if _, err := db.NewCreateTable().Model(model).IfNotExists().Exec(ctx); err != nil {
			return fmt.Errorf("failed to create table %s: %w", tableName, err)
		}
		if err := mm.recordTableInit(ctx, db, tableName); err != nil {
			return fmt.Errorf("failed to record table initialization %s: %w", tableName, err)
		}
		existingSet[key] = struct{}{}
		recordedSet[key] = struct{}{}
		if mm.tableSchemaCache != nil {
			mm.tableSchemaCacheMu.Lock()
			mm.tableSchemaCache[key] = struct{}{}
			mm.tableSchemaCacheMu.Unlock()
		}
		if mm.migrationCache != nil {
			mm.migrationCacheMu.Lock()
			mm.migrationCache[mm.tableInitVersion(tableName)] = struct{}{}
			mm.migrationCacheMu.Unlock()
		}
	}
	return nil
}

func (mm *MigrationManager) addForeignKeys(ctx context.Context, db bun.IDB) error {
	configPath := globalConfig.DataMigrateConfig.ForeignKeyFile
	cfk, err := NewConfigurableForeignKeyManager(mm.logger, configPath)
	var base *ForeignKeyManager
	if err != nil {
		if mm.logger != nil {
			mm.logger.Debug("failed to use configured foreign key manager, fallback to code-defined", "error", err.Error())
		}
		base = NewForeignKeyManager(mm.logger)
	} else {
		base = cfk.ForeignKeyManager
	}
	if errors := base.ValidateConstraints(); len(errors) > 0 {
		return fmt.Errorf("foreign key constraint validation failed, total %d errors", len(errors))
	}
	constraints := base.ListAllConstraints()
	groups := map[string][]ForeignKeyConstraint{}
	for _, c := range constraints {
		k := strings.ToLower(c.Table)
		groups[k] = append(groups[k], c)
	}
	if mm.migrationCache == nil {
		set, err := mm.getAppliedMigrationSet(ctx, mm.db)
		if err == nil {
			mm.migrationCache = set
		}
	}
	for table, list := range groups {
		var stmts []string
		for _, c := range list {
			stmts = append(stmts, c.GenerateSQL())
		}
		sort.Strings(stmts)
		plan := strings.Join(stmts, ";\n")
		sum := sha256.Sum256([]byte(plan))
		hash := hex.EncodeToString(sum[:])
		version := fmt.Sprintf("fk_sync:%s:%s", table, hash)
		mm.migrationCacheMu.RLock()
		_, exists := mm.migrationCache[version]
		mm.migrationCacheMu.RUnlock()
		if exists {
			continue
		}
		ex, err := db.NewSelect().
			Model((*Migration)(nil)).
			Where("version = ?", version).
			Exists(ctx)
		if err == nil && ex {
			mm.migrationCacheMu.Lock()
			if mm.migrationCache == nil {
				mm.migrationCache = make(map[string]struct{})
			}
			mm.migrationCache[version] = struct{}{}
			mm.migrationCacheMu.Unlock()
			continue
		}
		for _, c := range list {
			if err := base.addForeignKey(ctx, db, c); err != nil {
				msg := strings.ToLower(err.Error())
				if !strings.Contains(msg, "exist") && !strings.Contains(msg, "duplicate") {
					return fmt.Errorf("failed to add foreign key %s: %w", c.GenerateConstraintName(), err)
				}
			}
		}
		rec := &Migration{Version: version, Name: "fk_sync", AppliedAt: time.Now(), Description: fmt.Sprintf("table %s foreign key sync, plan hash=%s", table, hash)}
		dialect := strings.ToLower(globalConfig.ConnectionConfig.Type)
		ins := db.NewInsert().Model(rec)
		switch dialect {
		case "mysql":
			ins = ins.Ignore()
		case "postgres", "postgresql", "sqlite":
			ins = ins.On("CONFLICT DO NOTHING")
		}
		if _, err := ins.Exec(ctx); err != nil {
			return err
		}
		mm.migrationCacheMu.Lock()
		if mm.migrationCache == nil {
			mm.migrationCache = make(map[string]struct{})
		}
		mm.migrationCache[version] = struct{}{}
		mm.migrationCacheMu.Unlock()
	}
	return nil
}

func (mm *MigrationManager) InitData(ctx context.Context) error {
	if mm.db == nil {
		return fmt.Errorf("database not initialized")
	}
	if err := mm.createMigrationTable(ctx); err != nil {
		return fmt.Errorf("failed to create migration table: %w", err)
	}
	return mm.seedInitialData(ctx, mm.db)
}

func (mm *MigrationManager) seedInitialData(ctx context.Context, db bun.IDB) error {
	return mm.seedDataFromSQL()
}

func (mm *MigrationManager) seedDataFromSQL() error {
	sqlManager := NewSQLInitManager(mm.db, mm.environment)
	if globalConfig.DataInitConfig.Filepath != "" {
		sqlManager.SetSQLRootPath(globalConfig.DataInitConfig.Filepath)
	}

	files, err := sqlManager.GetSQLFiles()
	if err != nil {
		return fmt.Errorf("failed to get SQL files: %w", err)
	}

	if mm.migrationCache == nil {
		set, err := mm.getAppliedMigrationSet(context.Background(), mm.db)
		if err != nil {
			return fmt.Errorf("failed to query applied migrations: %w", err)
		}
		mm.migrationCache = set
	}

	for _, f := range files {
		abs := f.Path
		if p, err := filepath.Abs(f.Path); err == nil {
			abs = p
		}
		version := "data_sync:" + f.Name // Migration version uses file name, not path, because path may change
		mm.migrationCacheMu.RLock()
		_, exists := mm.migrationCache[version]
		mm.migrationCacheMu.RUnlock()
		if exists {
			continue
		}

		res := sqlManager.ExecuteFile(f)
		if !res.Success {
			return fmt.Errorf("failed to execute SQL file %s: %w", res.File, res.Error)
		}

		rec := &Migration{
			Version:     version,
			Name:        "data_sync",
			AppliedAt:   time.Now(),
			Description: fmt.Sprintf("data initialization file=%s", abs),
		}
		dialect := strings.ToLower(globalConfig.ConnectionConfig.Type)
		ins := mm.db.NewInsert().Model(rec)
		switch dialect {
		case "mysql":
			ins = ins.Ignore()
		case "postgres", "postgresql", "sqlite":
			ins = ins.On("CONFLICT DO NOTHING")
		}
		if _, err := ins.Exec(context.Background()); err != nil {
			return err
		}
		mm.migrationCacheMu.Lock()
		if mm.migrationCache == nil {
			mm.migrationCache = make(map[string]struct{})
		}
		mm.migrationCache[version] = struct{}{}
		mm.migrationCacheMu.Unlock()
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
	var migrations []Migration
	err := mm.db.NewSelect().
		Model(&migrations).
		Order("version ASC").
		Scan(ctx)
	return migrations, err
}

func (mm *MigrationManager) RollbackMigration(ctx context.Context, version string) error {
	return fmt.Errorf("migration rollback is not implemented yet")
}

func tableExists(ctx context.Context, db bun.IDB, table string) (bool, error) {
	dialect := strings.ToLower(globalConfig.ConnectionConfig.Type)
	switch dialect {
	case "postgres", "postgresql":
		row := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = $1)`, table)
		var exists bool
		if err := row.Scan(&exists); err != nil {
			return false, err
		}
		return exists, nil
	case "mysql":
		row := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?`, table)
		var cnt int
		if err := row.Scan(&cnt); err != nil {
			return false, err
		}
		return cnt > 0, nil
	default: // sqlite
		row := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?`, table)
		var cnt int
		if err := row.Scan(&cnt); err != nil {
			return false, err
		}
		return cnt > 0, nil
	}
}

func (mm *MigrationManager) tableInitVersion(table string) string {
	return "table_sync:" + table
}

func (mm *MigrationManager) isTableInitRecorded(ctx context.Context, db bun.IDB, table string) (bool, error) {
	return db.NewSelect().
		Model((*Migration)(nil)).
		Where("version = ?", mm.tableInitVersion(table)).
		Exists(ctx)
}

func (mm *MigrationManager) recordTableInit(ctx context.Context, db bun.IDB, table string) error {
	version := mm.tableInitVersion(table)
	mm.migrationCacheMu.RLock()
	if mm.migrationCache != nil {
		if _, ok := mm.migrationCache[version]; ok {
			mm.migrationCacheMu.RUnlock()
			return nil
		}
	}
	mm.migrationCacheMu.RUnlock()
	rec := &Migration{
		Version:     version,
		Name:        "table_sync",
		AppliedAt:   time.Now(),
		Description: fmt.Sprintf("table %s schema sync", table),
	}
	dialect := strings.ToLower(globalConfig.ConnectionConfig.Type)
	ins := db.NewInsert().Model(rec)
	switch dialect {
	case "mysql":
		ins = ins.Ignore()
	case "postgres", "postgresql", "sqlite":
		ins = ins.On("CONFLICT DO NOTHING")
	}
	if _, err := ins.Exec(ctx); err != nil {
		return err
	}
	mm.migrationCacheMu.Lock()
	if mm.migrationCache != nil {
		mm.migrationCache[rec.Version] = struct{}{}
	}
	mm.migrationCacheMu.Unlock()
	return nil
}

func (mm *MigrationManager) getAppliedMigrationSet(ctx context.Context, db bun.IDB) (map[string]struct{}, error) {
	result := make(map[string]struct{})
	rows, err := db.QueryContext(ctx, `SELECT version FROM bun_migrations`)
	if err != nil {
		return nil, err
	}
	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)
	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return nil, err
		}
		if version != "" {
			result[version] = struct{}{}
		}
	}
	return result, nil
}

func (mm *MigrationManager) runMigrationWithCache(ctx context.Context, migration MigrationItem) error {
	mm.migrationCacheMu.RLock()
	_, ok := mm.migrationCache[migration.Version]
	mm.migrationCacheMu.RUnlock()
	if ok {
		if mm.logger != nil {
			mm.logger.Debug("migration skipped:", "action", migration.Description, "version", migration.Version)
		}
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
				mm.logger.Error("failed to rollback transaction", "error", rollbackErr)
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

	if _, err = tx.NewInsert().Model(migrationRecord).Exec(ctx); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true

	mm.migrationCacheMu.Lock()
	if mm.migrationCache == nil {
		mm.migrationCache = make(map[string]struct{})
	}
	mm.migrationCache[migration.Version] = struct{}{}
	mm.migrationCacheMu.Unlock()

	if mm.logger != nil {
		mm.logger.Info("migration executed successfully", "version", migration.Version, "name", migration.Name)
	}
	return nil
}

func (mm *MigrationManager) listRecordedInitTables(ctx context.Context, db bun.IDB) (map[string]struct{}, error) {
	result := make(map[string]struct{})
	dialect := strings.ToLower(globalConfig.ConnectionConfig.Type)
	var rows *sql.Rows
	var err error
	switch dialect {
	case "postgres", "postgresql":
		rows, err = db.QueryContext(ctx, `SELECT version FROM bun_migrations WHERE version LIKE $1`, "table_sync:%")
	default: // mysql & sqlite use ? placeholder
		rows, err = db.QueryContext(ctx, `SELECT version FROM bun_migrations WHERE version LIKE ?`, "table_sync:%")
	}
	if err != nil {
		return nil, err
	}
	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)
	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return nil, err
		}
		if strings.HasPrefix(version, "table_sync:") {
			tbl := strings.TrimPrefix(version, "table_sync:")
			result[strings.ToLower(tbl)] = struct{}{}
		}
	}
	return result, nil
}

func listExistingTables(ctx context.Context, db bun.IDB) (map[string]struct{}, error) {
	result := make(map[string]struct{})
	dialect := strings.ToLower(globalConfig.ConnectionConfig.Type)
	var rows *sql.Rows
	var err error
	switch dialect {
	case "postgres", "postgresql":
		rows, err = db.QueryContext(ctx, `SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema()`)
	case "mysql":
		rows, err = db.QueryContext(ctx, `SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()`)
	default: // sqlite
		rows, err = db.QueryContext(ctx, `SELECT name FROM sqlite_master WHERE type='table'`)
	}
	if err != nil {
		return nil, err
	}
	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		if name != "" {
			result[strings.ToLower(name)] = struct{}{}
		}
	}
	return result, nil
}

func (mm *MigrationManager) getExistingTablesSet(ctx context.Context, db bun.IDB) (map[string]struct{}, error) {
	mm.tableSchemaCacheMu.RLock()
	if mm.tableSchemaCache != nil {
		defer mm.tableSchemaCacheMu.RUnlock()
		return mm.tableSchemaCache, nil
	}
	mm.tableSchemaCacheMu.RUnlock()
	set, err := listExistingTables(ctx, db)
	if err != nil {
		return nil, err
	}
	mm.tableSchemaCacheMu.Lock()
	mm.tableSchemaCache = set
	mm.tableSchemaCacheMu.Unlock()
	return mm.tableSchemaCache, nil
}
