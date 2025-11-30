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
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/uptrace/bun"
)

type columnSpec struct {
	Name          string
	Type          string
	NotNull       bool
	Default       string
	PrimaryKey    bool
	UniqueTag     string
	AutoIncrement bool
	RenameFrom    string
}
type indexSpec struct {
	Name    string
	Columns []string
	Unique  bool
}

func (mm *MigrationManager) SynchronizeSchema(ctx context.Context, db bun.IDB) error {
	ttl := time.Minute * 5
	if globalConfig != nil && globalConfig.DataMigrateConfig.SchemaMetaCacheTTL > 0 {
		ttl = globalConfig.DataMigrateConfig.SchemaMetaCacheTTL
	}
	_ = mm.ensureSchemaMetadataCaches(ctx, db, ttl)

	for _, model := range RegisteredModelInstances() {
		tableName, err := resolveTableName(model)
		if err != nil {
			return fmt.Errorf("failed to resolve table name %T: %w", model, err)
		}

		desiredCols, desiredIdx := resolveDesiredSchema(model)
		existingCols, err := mm.getColumnSchemaForTable(ctx, db, tableName)
		if err != nil {
			return fmt.Errorf("failed to query existing columns %s: %w", tableName, err)
		}
		existingIdx, err := mm.getIndexSchemaForTable(ctx, db, tableName)
		if err != nil {
			return fmt.Errorf("failed to query existing indexes %s: %w", tableName, err)
		}

		sigCols := buildDesiredColsSignature(desiredCols)
		sigIdx := buildDesiredIndexSignature(desiredIdx)
		sigText := tableName + "|cols:" + sigCols + "|idx:" + sigIdx
		sum := sha256.Sum256([]byte(sigText))
		hash := hex.EncodeToString(sum[:])
		version := fmt.Sprintf("schema_sync:%s:%s", tableName, hash)

		planCols := planColumns(db, tableName, desiredCols, existingCols)
		planIdx := planIndexes(db, tableName, desiredIdx, existingIdx)
		fullPlan := append(planCols, planIdx...)
		if len(fullPlan) == 0 {
			continue
		}
		sort.Strings(fullPlan)
		planText := strings.Join(fullPlan, ";\n")

		mm.migrationCacheMu.RLock()
		if mm.migrationCache != nil {
			if _, ok := mm.migrationCache[version]; ok {
				mm.migrationCacheMu.RUnlock()
				if mm.logger != nil {
					mm.logger.Debug("skip schema sync (schema unchanged)", "table", tableName)
				}
				continue
			}
		}
		mm.migrationCacheMu.RUnlock()
		exists, err := db.NewSelect().
			Model((*Migration)(nil)).
			Where("version = ?", version).
			Exists(ctx)
		if err != nil {
			return fmt.Errorf("failed to check schema_sync plan record %s: %w", tableName, err)
		}
		if exists {
			mm.migrationCacheMu.Lock()
			if mm.migrationCache == nil {
				mm.migrationCache = make(map[string]struct{})
			}
			mm.migrationCache[version] = struct{}{}
			mm.migrationCacheMu.Unlock()
			if mm.logger != nil {
				mm.logger.Debug("skip schema sync (plan unchanged, DB confirmed)", "table", tableName, "hash", hash)
			}
			continue
		}

		if err := mm.syncColumns(ctx, db, tableName, desiredCols, existingCols); err != nil {
			return err
		}
		if err := mm.syncIndexes(ctx, db, tableName, desiredIdx, existingIdx); err != nil {
			return err
		}

		if err := mm.recordSchemaSyncVersion(ctx, db, version, tableName, hash, planText); err != nil {
			return fmt.Errorf("failed to record schema_sync plan %s: %w", tableName, err)
		}
	}
	if mm.logger != nil && globalConfig != nil && globalConfig.DataMigrateConfig.SchemaMetaAuditLog {
		total := mm.schemaCacheHits + mm.schemaCacheMisses
		hitRate := 0.0
		if total > 0 {
			hitRate = float64(mm.schemaCacheHits) / float64(total)
		}
		mm.schemaCacheMu.Lock()
		mm.logger.Info("Schema metadata cache hit rate", "hits", mm.schemaCacheHits, "misses", mm.schemaCacheMisses, "hit_rate", fmt.Sprintf("%.2f", hitRate), "refreshes", mm.schemaCacheRefreshes)
		mm.schemaCacheHits = 0
		mm.schemaCacheMisses = 0
		mm.schemaCacheMu.Unlock()
	}
	return nil
}

func buildDesiredColsSignature(desired map[string]columnSpec) string {
	if desired == nil {
		return ""
	}
	names := make([]string, 0, len(desired))
	for n := range desired {
		names = append(names, strings.ToLower(n))
	}
	sort.Strings(names)
	var parts []string
	for _, n := range names {
		c := desired[n]
		typ := normalizeSQLType(globalConfig.ConnectionConfig.Type, c.Type)
		def := normalizeDefault(globalConfig.ConnectionConfig.Type, c.Default, !c.NotNull)
		parts = append(parts, n+":"+typ+":"+fmt.Sprintf("%t", c.NotNull)+":"+def+":"+fmt.Sprintf("%t", c.PrimaryKey)+":"+fmt.Sprintf("%t", c.AutoIncrement)+":"+strings.ToLower(strings.TrimSpace(c.UniqueTag)))
	}
	return strings.Join(parts, ",")
}

func buildDesiredIndexSignature(desired []indexSpec) string {
	if len(desired) == 0 {
		return ""
	}
	var items []string
	for _, idx := range desired {
		cols := make([]string, len(idx.Columns))
		for i, c := range idx.Columns {
			cols[i] = strings.ToLower(strings.TrimSpace(c))
		}
		items = append(items, fmt.Sprintf("%t|%s", idx.Unique, strings.Join(cols, ",")))
	}
	sort.Strings(items)
	return strings.Join(items, ";")
}

func planColumns(db bun.IDB, table string, desired map[string]columnSpec, existing map[string]columnSpec) []string {
	cfg := globalConfig.DataMigrateConfig
	var stmts []string

	if cfg.AllowColumnAdd {
		var toAdd []string
		for name := range desired {
			if _, ok := existing[name]; !ok {
				toAdd = append(toAdd, name)
			}
		}
		sort.Strings(toAdd)
		for _, name := range toAdd {
			stmts = append(stmts, buildAddColumnSQL(db, table, desired[name]))
		}
	}

	if cfg.AllowColumnModify {
		var toModify []string
		for name, d := range desired {
			if e, ok := existing[name]; ok {
				if needsModification(d, e) {
					toModify = append(toModify, name)
				}
			}
		}
		sort.Strings(toModify)
		for _, name := range toModify {
			stmt := buildModifyColumnSQL(db, table, desired[name])
			if stmt != "" {
				stmts = append(stmts, stmt)
			}
		}
	}

	if cfg.AllowColumnDrop {
		var toDrop []string
		for name := range existing {
			if _, ok := desired[name]; !ok {
				toDrop = append(toDrop, name)
			}
		}
		sort.Strings(toDrop)
		for _, name := range toDrop {
			stmt := buildDropColumnSQL(db, table, name)
			if stmt != "" {
				stmts = append(stmts, stmt)
			}
		}
	}

	return stmts
}

func planIndexes(db bun.IDB, table string, desired []indexSpec, existing []indexSpec) []string {
	cfg := globalConfig.DataMigrateConfig
	var stmts []string

	desiredMap := map[string]indexSpec{}
	for _, idx := range desired {
		desiredMap[idx.Name] = idx
	}
	existingMap := map[string]indexSpec{}
	for _, idx := range existing {
		existingMap[idx.Name] = idx
	}

	sig := func(s indexSpec) string {
		cols := make([]string, len(s.Columns))
		for i, c := range s.Columns {
			cols[i] = strings.ToLower(strings.TrimSpace(c))
		}
		return fmt.Sprintf("%t|%s", s.Unique, strings.Join(cols, ","))
	}
	desiredSig := map[string]indexSpec{}
	for _, d := range desired {
		desiredSig[sig(d)] = d
	}
	existingSig := map[string]indexSpec{}
	for _, e := range existing {
		existingSig[sig(e)] = e
	}

	for k, d := range desiredSig {
		if _, ok := existingMap[d.Name]; ok {
			continue
		}
		if e, ok := existingSig[k]; ok {
			delete(existingMap, e.Name)
			existingMap[d.Name] = e
		}
	}

	if cfg.AllowIndexAdd {
		var toAdd []string
		for name := range desiredMap {
			if _, ok := existingMap[name]; !ok {
				toAdd = append(toAdd, name)
			}
		}
		sort.Strings(toAdd)
		for _, name := range toAdd {
			stmt := buildCreateIndexSQL(db, table, desiredMap[name])
			if stmt != "" {
				stmts = append(stmts, stmt)
			}
		}
	}

	if cfg.AllowIndexDrop {
		var toDrop []string
		for name := range existingMap {
			if _, ok := desiredMap[name]; !ok {
				toDrop = append(toDrop, name)
			}
		}
		sort.Strings(toDrop)
		for _, name := range toDrop {
			stmt := buildDropIndexSQL(db, table, name)
			if stmt != "" {
				stmts = append(stmts, stmt)
			}
		}
	}

	return stmts
}

func (mm *MigrationManager) recordSchemaSyncVersion(ctx context.Context, db bun.IDB, version, table, hash, planText string) error {
	rec := &Migration{
		Version:     version,
		Name:        "schema_sync",
		AppliedAt:   time.Now(),
		Description: fmt.Sprintf("table %s schema sync, plan hash=%s", table, hash),
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
	if mm.migrationCache == nil {
		mm.migrationCache = make(map[string]struct{})
	}
	mm.migrationCache[version] = struct{}{}
	mm.migrationCacheMu.Unlock()
	return nil
}

func (mm *MigrationManager) syncColumns(ctx context.Context, db bun.IDB, table string, desired map[string]columnSpec, existing map[string]columnSpec) error {
	cfg := globalConfig.DataMigrateConfig
	changed := false

	for newName, spec := range desired {
		if spec.RenameFrom == "" {
			continue
		}
		oldName := spec.RenameFrom
		if _, ok := existing[oldName]; !ok {
			continue
		}
		stmt := buildRenameColumnSQL(db, table, oldName, newName, spec)
		if stmt != "" {
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("failed to rename column %s.%s -> %s: %w", table, oldName, newName, err)
			}
			changed = true
			dialect := strings.ToLower(globalConfig.ConnectionConfig.Type)
			if cfg.AllowColumnModify && (dialect == "postgres" || dialect == "postgresql") {
				oldSpec := existing[oldName]
				if needsModification(spec, oldSpec) {
					m := buildModifyColumnSQL(db, table, spec)
					if m != "" {
						if _, err := db.ExecContext(ctx, m); err != nil {
							return fmt.Errorf("failed to modify column after rename %s.%s: %w", table, newName, err)
						}
						changed = true
					}
				}
			}
			existing[newName] = spec
			delete(existing, oldName)
		}
	}

	if cfg.AllowColumnAdd {
		for name, spec := range desired {
			if _, ok := existing[name]; !ok {
				stmt := buildAddColumnSQL(db, table, spec)
				if _, err := db.ExecContext(ctx, stmt); err != nil {
					return fmt.Errorf("failed to add column %s.%s: %w", table, name, err)
				}
				changed = true
			}
		}
	}

	if cfg.AllowColumnModify {
		for name, d := range desired {
			if e, ok := existing[name]; ok {
				if needsModification(d, e) {
					stmt := buildModifyColumnSQL(db, table, d)
					if stmt == "" {
						continue
					}
					if _, err := db.ExecContext(ctx, stmt); err != nil {
						return fmt.Errorf("failed to modify column %s.%s: %w", table, name, err)
					}
					changed = true
				}
			}
		}
	}

	if cfg.AllowColumnDrop {
		for name := range existing {
			if _, ok := desired[name]; !ok {
				stmt := buildDropColumnSQL(db, table, name)
				if stmt == "" {
					continue
				}
				if _, err := db.ExecContext(ctx, stmt); err != nil {
					return fmt.Errorf("failed to drop column %s.%s: %w", table, name, err)
				}
				changed = true
			}
		}
	}

	if changed {
		_ = mm.RefreshSchemaMetadataForTables(ctx, db, []string{table})
	}
	return nil
}

func (mm *MigrationManager) syncIndexes(ctx context.Context, db bun.IDB, table string, desired []indexSpec, existing []indexSpec) error {
	cfg := globalConfig.DataMigrateConfig
	changed := false
	if fresh, err := mm.getIndexSchemaForTable(ctx, db, table); err == nil && fresh != nil {
		existing = fresh
	}
	desiredMap := map[string]indexSpec{}
	for _, idx := range desired {
		desiredMap[idx.Name] = idx
	}
	existingMap := map[string]indexSpec{}
	for _, idx := range existing {
		existingMap[idx.Name] = idx
	}

	sig := func(s indexSpec) string {
		cols := make([]string, len(s.Columns))
		for i, c := range s.Columns {
			cols[i] = strings.ToLower(strings.TrimSpace(c))
		}
		return fmt.Sprintf("%t|%s", s.Unique, strings.Join(cols, ","))
	}
	desiredSig := map[string]indexSpec{}
	for _, d := range desired {
		desiredSig[sig(d)] = d
	}
	existingSig := map[string]indexSpec{}
	for _, e := range existing {
		existingSig[sig(e)] = e
	}

	for sigKey, d := range desiredSig {
		if _, ok := desiredMap[d.Name]; !ok {
			continue
		}
		if _, ok := existingMap[d.Name]; ok {
			continue
		}
		if e, ok := existingSig[sigKey]; ok {
			delete(existingMap, e.Name)
			existingMap[d.Name] = e
		}
	}

	if cfg.AllowIndexAdd {
		for name, d := range desiredMap {
			if _, ok := existingMap[name]; !ok {
				stmt := buildCreateIndexSQL(db, table, d)
				if stmt == "" {
					continue
				}
				if _, err := db.ExecContext(ctx, stmt); err != nil {
					is, sqlErr := IsSqlError(err)
					if is && sqlErr == ExistIndexErr {
						continue
					}
					return fmt.Errorf("failed to add index %s.%s: %w", table, name, err)
				}
				changed = true
			}
		}
	}

	if cfg.AllowIndexDrop {
		for name := range existingMap {
			if _, ok := desiredMap[name]; !ok {
				stmt := buildDropIndexSQL(db, table, name)
				if stmt == "" {
					continue
				}
				if _, err := db.ExecContext(ctx, stmt); err != nil {
					is, sqlErr := IsSqlError(err)
					if is && sqlErr == NoIndexErr {
						continue
					}
					return fmt.Errorf("failed to drop index %s.%s: %w", table, name, err)
				}
				changed = true
			}
		}
	}
	if changed {
		_ = mm.RefreshSchemaMetadataForTables(ctx, db, []string{table})
	}
	return nil
}

func resolveTableName(model interface{}) (string, error) {
	t := reflect.TypeOf(model)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.Type.Name() == "BaseModel" && strings.Contains(f.Type.PkgPath(), "uptrace/bun") {
			tag := f.Tag.Get("bun")
			for _, part := range strings.Split(tag, ",") {
				part = strings.TrimSpace(part)
				if strings.HasPrefix(part, "table:") {
					return strings.TrimPrefix(part, "table:"), nil
				}
			}
		}
	}
	return "", fmt.Errorf("missing table tag on bun.BaseModel")
}

func resolveDesiredSchema(model interface{}) (map[string]columnSpec, []indexSpec) {
	cols := map[string]columnSpec{}
	uniques := map[string][]string{}

	t := reflect.TypeOf(model)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	collectDesiredSchema(t, t.Name(), cols, uniques)

	idx := make([]indexSpec, 0, len(uniques))
	for name, cols := range uniques {
		idx = append(idx, indexSpec{Name: name, Columns: cols, Unique: true})
	}
	return cols, idx
}

func collectDesiredSchema(t reflect.Type, rootName string, cols map[string]columnSpec, uniques map[string][]string) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return
	}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.Type.Name() == "BaseModel" && strings.Contains(f.Type.PkgPath(), "uptrace/bun") {
			continue
		}
		tag := f.Tag.Get("bun")
		if tag == "-" || strings.Contains(tag, "rel:") || strings.Contains(tag, "m2m:") {
			continue
		}
		if tag == "" {
			if f.Anonymous {
				ft := f.Type
				if ft.Kind() == reflect.Ptr {
					ft = ft.Elem()
				}
				if ft.Kind() == reflect.Struct {
					collectDesiredSchema(ft, rootName, cols, uniques)
				}
			}
			continue
		}

		parts := strings.Split(tag, ",")
		colName := strings.TrimSpace(parts[0])
		if colName == "" || colName == "-" {
			continue
		}

		spec := columnSpec{Name: colName}
		for _, p := range parts[1:] {
			p = strings.TrimSpace(p)
			switch {
			case strings.HasPrefix(p, "type:"):
				spec.Type = strings.TrimPrefix(p, "type:")
			case p == "notnull":
				spec.NotNull = true
			case strings.HasPrefix(p, "default:"):
				spec.Default = strings.TrimPrefix(p, "default:")
			case p == "pk":
				spec.PrimaryKey = true
			case p == "autoincrement" || p == "identity":
				spec.AutoIncrement = true
			case p == "unique" || strings.HasPrefix(p, "unique:"):
				spec.UniqueTag = p
				name := ""
				if strings.HasPrefix(p, "unique:") {
					name = strings.TrimPrefix(p, "unique:")
				} else {
					name = fmt.Sprintf("uk_%s_%s", rootName, colName)
				}
				uniques[name] = append(uniques[name], colName)
			}
		}
		if spec.PrimaryKey || spec.AutoIncrement {
			spec.NotNull = true
		}
		if spec.Type == "" {
			spec.Type = inferSQLType(f.Type)
		}
		hummer := strings.TrimSpace(f.Tag.Get("hummer"))
		if hummer != "" {
			idx := strings.Index(hummer, "rename:")
			if idx >= 0 {
				val := strings.TrimSpace(hummer[idx+len("rename:"):])
				val = strings.Trim(val, "'\"")
				if val != "" {
					spec.RenameFrom = val
				}
			}
		}
		cols[colName] = spec
	}
}

func inferSQLType(rt reflect.Type) string {
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	switch rt.Kind() {
	case reflect.Int, reflect.Int64:
		switch globalConfig.ConnectionConfig.Type {
		case "mysql":
			return "bigint"
		case "postgres", "postgresql":
			return "bigint"
		default:
			return "INTEGER"
		}
	case reflect.String:
		switch globalConfig.ConnectionConfig.Type {
		case "mysql":
			return "varchar(255)"
		case "postgres", "postgresql":
			return "text"
		default:
			return "TEXT"
		}
	case reflect.Bool:
		switch globalConfig.ConnectionConfig.Type {
		case "mysql":
			return "tinyint(1)"
		case "postgres", "postgresql":
			return "boolean"
		default:
			return "BOOLEAN"
		}
	default:
		return "TEXT"
	}
}

func listExistingColumns(ctx context.Context, db bun.IDB, table string) (map[string]columnSpec, error) {
	cols := map[string]columnSpec{}
	dialect := strings.ToLower(globalConfig.ConnectionConfig.Type)
	var rows *sql.Rows
	var err error
	switch dialect {
	case "postgres", "postgresql":
		rows, err = db.QueryContext(ctx, `SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_name = $1`, table)
	case "mysql":
		rows, err = db.QueryContext(ctx, `SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?`, table)
	default:
		rows, err = db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info('%s')", table))
	}
	if err != nil {
		return nil, err
	}
	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)

	for rows.Next() {
		var name, typStr, nullable string
		var defaultNS sql.NullString
		autoExtra := ""
		switch dialect {
		case "postgres", "postgresql":
			if err := rows.Scan(&name, &typStr, &nullable, &defaultNS); err != nil {
				return nil, err
			}
		case "mysql":
			if err := rows.Scan(&name, &typStr, &nullable, &defaultNS, &autoExtra); err != nil {
				return nil, err
			}
		default:
			var cid, notnull, pk int
			if err := rows.Scan(&cid, &name, &typStr, &notnull, &defaultNS, &pk); err != nil {
				return nil, err
			}
			nullable = map[bool]string{true: "NO", false: "YES"}[notnull == 1]
		}
		def := ""
		if defaultNS.Valid {
			def = defaultNS.String
		}
		spec := columnSpec{Name: name, Type: typStr, NotNull: strings.ToUpper(nullable) == "NO", Default: def}
		if strings.Contains(strings.ToLower(autoExtra), "auto_increment") {
			spec.AutoIncrement = true
			spec.NotNull = true
		}
		cols[name] = spec
	}
	return cols, nil
}

func listExistingColumnsAll(ctx context.Context, db bun.IDB) (map[string]map[string]columnSpec, error) {
	dialect := strings.ToLower(globalConfig.ConnectionConfig.Type)
	result := make(map[string]map[string]columnSpec)
	switch dialect {
	case "postgres", "postgresql":
		rows, err := db.QueryContext(ctx, `SELECT table_name, column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_schema = current_schema()`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var tbl, name, typStr, nullable string
			var defaultNS sql.NullString
			if err := rows.Scan(&tbl, &name, &typStr, &nullable, &defaultNS); err != nil {
				return nil, err
			}
			def := ""
			if defaultNS.Valid {
				def = defaultNS.String
			}
			key := strings.ToLower(tbl)
			if _, ok := result[key]; !ok {
				result[key] = make(map[string]columnSpec)
			}
			result[key][name] = columnSpec{Name: name, Type: typStr, NotNull: strings.ToUpper(nullable) == "NO", Default: def}
		}
		return result, nil
	case "mysql":
		rows, err := db.QueryContext(ctx, `SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE()`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var tbl, name, typStr, nullable, extra string
			var defaultNS sql.NullString
			if err := rows.Scan(&tbl, &name, &typStr, &nullable, &defaultNS, &extra); err != nil {
				return nil, err
			}
			def := ""
			if defaultNS.Valid {
				def = defaultNS.String
			}
			key := strings.ToLower(tbl)
			if _, ok := result[key]; !ok {
				result[key] = make(map[string]columnSpec)
			}
			spec := columnSpec{Name: name, Type: typStr, NotNull: strings.ToUpper(nullable) == "NO", Default: def}
			if strings.Contains(strings.ToLower(extra), "auto_increment") {
				spec.AutoIncrement = true
				spec.NotNull = true
			}
			result[key][name] = spec
		}
		return result, nil
	default:
		return nil, nil
	}
}

func listExistingColumnsSome(ctx context.Context, db bun.IDB, tables []string) (map[string]map[string]columnSpec, error) {
	dialect := strings.ToLower(globalConfig.ConnectionConfig.Type)
	result := make(map[string]map[string]columnSpec)
	if len(tables) == 0 {
		return result, nil
	}
	var inList []string
	for _, t := range tables {
		t = strings.TrimSpace(t)
		t = strings.ReplaceAll(t, "'", "''")
		inList = append(inList, fmt.Sprintf("'%s'", t))
	}
	switch dialect {
	case "postgres", "postgresql":
		q := fmt.Sprintf(`SELECT table_name, column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_schema = current_schema() AND table_name IN (%s)`, strings.Join(inList, ","))
		rows, err := db.QueryContext(ctx, q)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var tbl, name, typStr, nullable string
			var defaultNS sql.NullString
			if err := rows.Scan(&tbl, &name, &typStr, &nullable, &defaultNS); err != nil {
				return nil, err
			}
			def := ""
			if defaultNS.Valid {
				def = defaultNS.String
			}
			key := strings.ToLower(tbl)
			if _, ok := result[key]; !ok {
				result[key] = make(map[string]columnSpec)
			}
			result[key][name] = columnSpec{Name: name, Type: typStr, NotNull: strings.ToUpper(nullable) == "NO", Default: def}
		}
		return result, nil
	case "mysql":
		q := fmt.Sprintf(`SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN (%s)`, strings.Join(inList, ","))
		rows, err := db.QueryContext(ctx, q)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var tbl, name, typStr, nullable, extra string
			var defaultNS sql.NullString
			if err := rows.Scan(&tbl, &name, &typStr, &nullable, &defaultNS, &extra); err != nil {
				return nil, err
			}
			def := ""
			if defaultNS.Valid {
				def = defaultNS.String
			}
			key := strings.ToLower(tbl)
			if _, ok := result[key]; !ok {
				result[key] = make(map[string]columnSpec)
			}
			spec := columnSpec{Name: name, Type: typStr, NotNull: strings.ToUpper(nullable) == "NO", Default: def}
			if strings.Contains(strings.ToLower(extra), "auto_increment") {
				spec.AutoIncrement = true
				spec.NotNull = true
			}
			result[key][name] = spec
		}
		return result, nil
	default:
		return nil, nil
	}
}

func listExistingIndexes(ctx context.Context, db bun.IDB, table string) ([]indexSpec, error) {
	typ := strings.ToLower(globalConfig.ConnectionConfig.Type)
	var rows *sql.Rows
	var err error
	var idx []indexSpec

	switch typ {
	case "postgres", "postgresql":
		rows, err = db.QueryContext(ctx, `SELECT indexname, indexdef FROM pg_indexes WHERE tablename = $1`, table)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var name, def string
			if err := rows.Scan(&name, &def); err != nil {
				return nil, err
			}
			spec := indexSpec{Name: name}
			spec.Unique = strings.Contains(strings.ToUpper(def), "UNIQUE")
			open := strings.Index(def, "(")
			close := strings.LastIndex(def, ")")
			if open > 0 && close > open {
				cols := strings.Split(def[open+1:close], ",")
				for _, c := range cols {
					spec.Columns = append(spec.Columns, strings.TrimSpace(strings.Trim(c, `"`)))
				}
			}
			idx = append(idx, spec)
		}
	case "mysql":
		rows, err = db.QueryContext(ctx, `SELECT INDEX_NAME, COLUMN_NAME, NON_UNIQUE FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? ORDER BY SEQ_IN_INDEX`, table)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		temp := map[string]indexSpec{}
		for rows.Next() {
			var name, col string
			var nonUnique int
			if err := rows.Scan(&name, &col, &nonUnique); err != nil {
				return nil, err
			}
			spec := temp[name]
			spec.Name = name
			spec.Unique = nonUnique == 0
			spec.Columns = append(spec.Columns, col)
			temp[name] = spec
		}
		for _, s := range temp {
			idx = append(idx, s)
		}
	default:
		rows, err = db.QueryContext(ctx, fmt.Sprintf("PRAGMA index_list('%s')", table))
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var seq, unique int
			var name string
			var origin, partial string
			if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
				return nil, err
			}
			spec := indexSpec{Name: name, Unique: unique == 1}
			rows2, err := db.QueryContext(ctx, fmt.Sprintf("PRAGMA index_info('%s')", name))
			if err != nil {
				return nil, err
			}
			for rows2.Next() {
				var seqno, cid int
				var col string
				if err := rows2.Scan(&seqno, &cid, &col); err != nil {
					rows2.Close()
					return nil, err
				}
				spec.Columns = append(spec.Columns, col)
			}
			rows2.Close()
			idx = append(idx, spec)
		}
	}
	return idx, nil
}

func listExistingIndexesAll(ctx context.Context, db bun.IDB) (map[string][]indexSpec, error) {
	dialect := strings.ToLower(globalConfig.ConnectionConfig.Type)
	result := make(map[string][]indexSpec)
	switch dialect {
	case "postgres", "postgresql":
		rows, err := db.QueryContext(ctx, `SELECT tablename, indexname, indexdef FROM pg_indexes WHERE schemaname = current_schema()`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var tbl, name, def string
			if err := rows.Scan(&tbl, &name, &def); err != nil {
				return nil, err
			}
			spec := indexSpec{Name: name}
			spec.Unique = strings.Contains(strings.ToUpper(def), "UNIQUE")
			open := strings.Index(def, "(")
			close := strings.LastIndex(def, ")")
			if open > 0 && close > open {
				cols := strings.Split(def[open+1:close], ",")
				for _, c := range cols {
					spec.Columns = append(spec.Columns, strings.TrimSpace(strings.Trim(c, `"`)))
				}
			}
			key := strings.ToLower(tbl)
			result[key] = append(result[key], spec)
		}
		return result, nil
	case "mysql":
		rows, err := db.QueryContext(ctx, `SELECT TABLE_NAME, INDEX_NAME, COLUMN_NAME, NON_UNIQUE FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = DATABASE() ORDER BY TABLE_NAME, SEQ_IN_INDEX`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		temp := make(map[string]map[string]indexSpec)
		for rows.Next() {
			var tbl, name, col string
			var nonUnique int
			if err := rows.Scan(&tbl, &name, &col, &nonUnique); err != nil {
				return nil, err
			}
			key := strings.ToLower(tbl)
			if _, ok := temp[key]; !ok {
				temp[key] = make(map[string]indexSpec)
			}
			spec := temp[key][name]
			spec.Name = name
			spec.Unique = nonUnique == 0
			spec.Columns = append(spec.Columns, col)
			temp[key][name] = spec
		}
		for tbl, idxMap := range temp {
			for _, s := range idxMap {
				result[tbl] = append(result[tbl], s)
			}
		}
		return result, nil
	default:
		return nil, nil
	}
}

func listExistingIndexesSome(ctx context.Context, db bun.IDB, tables []string) (map[string][]indexSpec, error) {
	typ := strings.ToLower(globalConfig.ConnectionConfig.Type)
	result := make(map[string][]indexSpec)
	if len(tables) == 0 {
		return result, nil
	}
	var inList []string
	for _, t := range tables {
		t = strings.TrimSpace(t)
		t = strings.ReplaceAll(t, "'", "''")
		inList = append(inList, fmt.Sprintf("'%s'", t))
	}
	switch typ {
	case "postgres", "postgresql":
		q := fmt.Sprintf(`SELECT tablename, indexname, indexdef FROM pg_indexes WHERE schemaname = current_schema() AND tablename IN (%s)`, strings.Join(inList, ","))
		rows, err := db.QueryContext(ctx, q)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		temp := map[string]map[string]indexSpec{}
		for rows.Next() {
			var tbl, name, def string
			if err := rows.Scan(&tbl, &name, &def); err != nil {
				return nil, err
			}
			key := strings.ToLower(tbl)
			if _, ok := temp[key]; !ok {
				temp[key] = make(map[string]indexSpec)
			}
			spec := temp[key][name]
			spec.Name = name
			spec.Unique = strings.Contains(strings.ToUpper(def), "UNIQUE")
			open := strings.Index(def, "(")
			close := strings.LastIndex(def, ")")
			if open > 0 && close > open {
				cols := strings.Split(def[open+1:close], ",")
				for _, c := range cols {
					spec.Columns = append(spec.Columns, strings.TrimSpace(strings.Trim(c, `"`)))
				}
			}
			temp[key][name] = spec
		}
		for tbl, m := range temp {
			for _, s := range m {
				result[tbl] = append(result[tbl], s)
			}
		}
		return result, nil
	case "mysql":
		q := fmt.Sprintf(`SELECT TABLE_NAME, INDEX_NAME, COLUMN_NAME, NON_UNIQUE FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN (%s) ORDER BY SEQ_IN_INDEX`, strings.Join(inList, ","))
		rows, err := db.QueryContext(ctx, q)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		temp := map[string]map[string]indexSpec{}
		for rows.Next() {
			var tbl, name, col string
			var nonUnique int
			if err := rows.Scan(&tbl, &name, &col, &nonUnique); err != nil {
				return nil, err
			}
			key := strings.ToLower(tbl)
			if _, ok := temp[key]; !ok {
				temp[key] = make(map[string]indexSpec)
			}
			spec := temp[key][name]
			spec.Name = name
			spec.Unique = nonUnique == 0
			spec.Columns = append(spec.Columns, col)
			temp[key][name] = spec
		}
		for tbl, m := range temp {
			for _, s := range m {
				result[tbl] = append(result[tbl], s)
			}
		}
		return result, nil
	default:
		return nil, nil
	}
}

func quoteIdent(db bun.IDB, s string) string {
	typ := strings.ToLower(db.Dialect().Name().String())
	switch typ {
	case "postgres", "postgresql":
		return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
	case "mysql":
		return "`" + strings.ReplaceAll(s, "`", "``") + "`"
	default:
		return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
	}
}

func buildAddColumnSQL(db bun.IDB, table string, c columnSpec) string {
	notNull := ""
	if c.NotNull {
		if globalConfig.DataMigrateConfig.EnforceNotNullWithDefault && c.Default == "" {
			notNull = ""
		} else {
			notNull = " NOT NULL"
		}
	}
	def := ""
	if c.Default != "" {
		def = " DEFAULT " + c.Default
	}
	return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s%s%s", quoteIdent(db, table), quoteIdent(db, c.Name), c.Type, notNull, def)
}

func buildModifyColumnSQL(db bun.IDB, table string, c columnSpec) string {
	typ := strings.ToLower(globalConfig.ConnectionConfig.Type)
	switch typ {
	case "postgres", "postgresql":
		var stmts []string
		if c.Type != "" {
			typeStmt := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s USING %s::%s",
				quoteIdent(db, table), quoteIdent(db, c.Name), c.Type, quoteIdent(db, c.Name), c.Type)
			stmts = append(stmts, typeStmt)
		}
		if c.NotNull {
			stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL", quoteIdent(db, table), quoteIdent(db, c.Name)))
		} else {
			stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL", quoteIdent(db, table), quoteIdent(db, c.Name)))
		}
		if c.Default != "" {
			stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s", quoteIdent(db, table), quoteIdent(db, c.Name), c.Default))
		} else {
			stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT", quoteIdent(db, table), quoteIdent(db, c.Name)))
		}
		return strings.Join(stmts, "; ")
	case "mysql":
		nullStr := " NULL"
		if c.NotNull {
			nullStr = " NOT NULL"
		}
		def := ""
		if c.Default != "" {
			def = " DEFAULT " + c.Default
		}
		auto := ""
		if c.AutoIncrement {
			auto = " AUTO_INCREMENT"
			nullStr = " NOT NULL"
		}
		return fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s %s%s%s%s", quoteIdent(db, table), quoteIdent(db, c.Name), c.Type, nullStr, def, auto)
	default:
		return ""
	}
}

func buildRenameColumnSQL(db bun.IDB, table, oldName, newName string, c columnSpec) string {
	typ := strings.ToLower(globalConfig.ConnectionConfig.Type)
	switch typ {
	case "mysql":
		nullStr := " NULL"
		if c.NotNull {
			nullStr = " NOT NULL"
		}
		def := ""
		if c.Default != "" {
			def = " DEFAULT " + c.Default
		}
		auto := ""
		if c.AutoIncrement {
			auto = " AUTO_INCREMENT"
			nullStr = " NOT NULL"
		}
		return fmt.Sprintf("ALTER TABLE %s CHANGE COLUMN %s %s %s%s%s%s", quoteIdent(db, table), quoteIdent(db, oldName), quoteIdent(db, newName), c.Type, nullStr, def, auto)
	case "postgres", "postgresql":
		return fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s", quoteIdent(db, table), quoteIdent(db, oldName), quoteIdent(db, newName))
	default:
		return ""
	}
}

func buildDropColumnSQL(db bun.IDB, table, col string) string {
	typ := strings.ToLower(globalConfig.ConnectionConfig.Type)
	switch typ {
	case "postgres", "postgresql":
		return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", quoteIdent(db, table), quoteIdent(db, col))
	case "mysql":
		return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", quoteIdent(db, table), quoteIdent(db, col))
	default:
		return ""
	}
}

func buildCreateIndexSQL(db bun.IDB, table string, idx indexSpec) string {
	cols := make([]string, 0, len(idx.Columns))
	for _, c := range idx.Columns {
		cols = append(cols, quoteIdent(db, c))
	}
	unique := ""
	if idx.Unique {
		unique = "UNIQUE "
	}
	name := quoteIdent(db, idx.Name)

	typ := strings.ToLower(globalConfig.ConnectionConfig.Type)
	switch typ {
	case "postgres", "postgresql":
		return fmt.Sprintf("CREATE %sINDEX IF NOT EXISTS %s ON %s (%s)", unique, name, quoteIdent(db, table), strings.Join(cols, ", "))
	case "mysql":
		return fmt.Sprintf("CREATE %sINDEX %s ON %s (%s)", unique, name, quoteIdent(db, table), strings.Join(cols, ", "))
	case "sqlite":
		return fmt.Sprintf("CREATE %sINDEX %s IF NOT EXISTS ON %s (%s)", unique, name, quoteIdent(db, table), strings.Join(cols, ", "))
	default:
		return fmt.Sprintf("CREATE %sINDEX %s ON %s (%s)", unique, name, quoteIdent(db, table), strings.Join(cols, ", "))
	}
}

func isPrimaryIndexName(dialect, table, indexName string) bool {
	d := strings.ToLower(dialect)
	switch d {
	case "mysql":
		return strings.EqualFold(indexName, "PRIMARY")
	case "postgres", "postgresql":
		expected := table + "_pkey"
		return strings.EqualFold(indexName, expected)
	default:
		return false
	}
}

func buildDropIndexSQL(db bun.IDB, table, name string) string {
	if isPrimaryIndexName(globalConfig.ConnectionConfig.Type, table, name) {
		return ""
	}
	typ := strings.ToLower(globalConfig.ConnectionConfig.Type)
	switch typ {
	case "postgres", "postgresql":
		return fmt.Sprintf("DROP INDEX IF EXISTS %s", quoteIdent(db, name))
	case "mysql":
		return fmt.Sprintf("DROP INDEX %s ON %s", quoteIdent(db, name), quoteIdent(db, table))
	default:
		return fmt.Sprintf("DROP INDEX IF EXISTS %s", quoteIdent(db, name))
	}
}

func buildRenameIndexSQL(db bun.IDB, table, oldName, newName string) string {
	typ := strings.ToLower(globalConfig.ConnectionConfig.Type)
	switch typ {
	case "mysql":
		return fmt.Sprintf("ALTER TABLE %s RENAME INDEX %s TO %s", quoteIdent(db, table), quoteIdent(db, oldName), quoteIdent(db, newName))
	case "postgres", "postgresql":
		return fmt.Sprintf("ALTER INDEX %s RENAME TO %s", quoteIdent(db, oldName), quoteIdent(db, newName))
	default:
		return ""
	}
}

func needsModification(desired, existing columnSpec) bool {
	if desired.Type != "" {
		dt := normalizeSQLType(globalConfig.ConnectionConfig.Type, desired.Type)
		et := normalizeSQLType(globalConfig.ConnectionConfig.Type, existing.Type)
		if dt != et {
			return true
		}
	}
	if desired.NotNull != existing.NotNull {
		return true
	}
	if desired.AutoIncrement != existing.AutoIncrement {
		return true
	}
	d := normalizeDefault(globalConfig.ConnectionConfig.Type, desired.Default, !desired.NotNull)
	e := normalizeDefault(globalConfig.ConnectionConfig.Type, existing.Default, !existing.NotNull)
	if d != e {
		return true
	}
	return false
}

func normalizeSQLType(dialect, typ string) string {
	s := strings.ToLower(strings.TrimSpace(typ))
	d := strings.ToLower(strings.TrimSpace(dialect))
	if d == "mysql" {
		replaceInt := func(name string) {
			if strings.HasPrefix(s, name+"(") {
				if strings.Contains(s, "unsigned") {
					s = name + " unsigned"
				} else {
					s = name
				}
			}
		}
		replaceInt("tinyint")
		replaceInt("smallint")
		replaceInt("mediumint")
		replaceInt("int")
		replaceInt("bigint")
		if s == "boolean" {
			s = "tinyint(1)"
		}
		s = strings.Join(strings.Fields(s), " ")
	}
	return s
}

func normalizeDefault(dialect string, def string, nullable bool) string {
	s := strings.TrimSpace(strings.Trim(def, "()"))
	if s == "" {
		return ""
	}
	if strings.EqualFold(s, "null") && nullable {
		return ""
	}
	return s
}
