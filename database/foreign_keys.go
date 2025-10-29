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
	"strings"

	"github.com/uptrace/bun"
)

// ForeignKeyConstraint describes a foreign key relationship between tables.
type ForeignKeyConstraint struct {
	Table           string
	Column          string
	ReferenceTable  string
	ReferenceColumn string
	OnDelete        string // CASCADE, RESTRICT, SET NULL, NO ACTION
	OnUpdate        string // CASCADE, RESTRICT, SET NULL, NO ACTION
	ConstraintName  string
}

// GenerateConstraintName returns the explicit name or a derived name.
func (fk *ForeignKeyConstraint) GenerateConstraintName() string {
	if fk.ConstraintName != "" {
		return fk.ConstraintName
	}
	return fmt.Sprintf("fk_%s_%s", fk.Table, fk.Column)
}

// GenerateSQL returns the ALTER TABLE statement to add the constraint.
func (fk *ForeignKeyConstraint) GenerateSQL() string {
	constraintName := fk.GenerateConstraintName()
	sql := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s(%s)",
		fk.Table, constraintName, fk.Column, fk.ReferenceTable, fk.ReferenceColumn)

	if fk.OnDelete != "" {
		sql += fmt.Sprintf(" ON DELETE %s", fk.OnDelete)
	}
	if fk.OnUpdate != "" {
		sql += fmt.Sprintf(" ON UPDATE %s", fk.OnUpdate)
	}

	return sql
}

// ForeignKeyManager manages adding and validating foreign key constraints.
type ForeignKeyManager struct {
	constraints []ForeignKeyConstraint
	logger      Logger
}

// NewForeignKeyManager creates a manager with code-defined constraints.
func NewForeignKeyManager(logger Logger) *ForeignKeyManager {
	return &ForeignKeyManager{
		constraints: getForeignKeyConstraints(),
		logger:      logger,
	}
}

func getForeignKeyConstraints() []ForeignKeyConstraint {
	return []ForeignKeyConstraint{}
}

// AddAllForeignKeys iterates through all constraints and adds them to the DB.
func (fkm *ForeignKeyManager) AddAllForeignKeys(ctx context.Context, db bun.IDB) error {
	for _, constraint := range fkm.constraints {
		if err := fkm.addForeignKey(ctx, db, constraint); err != nil {
			if fkm.logger != nil {
				fkm.logger.Debug("Failed to add foreign key constraint", "constraint", constraint.GenerateConstraintName(), "error", err.Error())
			}
			continue
		}
		if fkm.logger != nil {
			fkm.logger.Debug("Successfully added foreign key constraint", "constraint", constraint.GenerateConstraintName())
		}
	}
	return nil
}

// addForeignKey executes a single constraint addition.
func (fkm *ForeignKeyManager) addForeignKey(ctx context.Context, db bun.IDB, constraint ForeignKeyConstraint) error {
	sql := constraint.GenerateSQL()
	_, err := db.ExecContext(ctx, sql)
	return err
}

// RemoveForeignKey drops a named foreign key from a table.
func (fkm *ForeignKeyManager) RemoveForeignKey(ctx context.Context, db bun.IDB, tableName, constraintName string) error {
	sql := fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s", tableName, constraintName)
	_, err := db.ExecContext(ctx, sql)
	return err
}

// GetConstraintsByTable returns the constraints defined for a table.
func (fkm *ForeignKeyManager) GetConstraintsByTable(tableName string) []ForeignKeyConstraint {
	var result []ForeignKeyConstraint
	for _, constraint := range fkm.constraints {
		if strings.EqualFold(constraint.Table, tableName) {
			result = append(result, constraint)
		}
	}
	return result
}

// ListAllConstraints returns all configured constraints.
func (fkm *ForeignKeyManager) ListAllConstraints() []ForeignKeyConstraint {
	return fkm.constraints
}

// ValidateConstraints checks the configured constraints for common issues.
func (fkm *ForeignKeyManager) ValidateConstraints() []error {
	var errors []error

	for _, constraint := range fkm.constraints {
		if constraint.Table == "" {
			errors = append(errors, fmt.Errorf("table name cannot be empty"))
		}
		if constraint.Column == "" {
			errors = append(errors, fmt.Errorf("column name cannot be empty: %s", constraint.Table))
		}
		if constraint.ReferenceTable == "" {
			errors = append(errors, fmt.Errorf("reference table name cannot be empty: %s.%s", constraint.Table, constraint.Column))
		}
		if constraint.ReferenceColumn == "" {
			errors = append(errors, fmt.Errorf("reference column name cannot be empty: %s.%s -> %s", constraint.Table, constraint.Column, constraint.ReferenceTable))
		}

		// Validate delete policy
		if constraint.OnDelete != "" {
			validActions := []string{"CASCADE", "RESTRICT", "SET NULL", "NO ACTION"}
			valid := false
			for _, action := range validActions {
				if strings.EqualFold(constraint.OnDelete, action) {
					valid = true
					break
				}
			}
			if !valid {
				errors = append(errors, fmt.Errorf("invalid delete policy: %s, constraint: %s", constraint.OnDelete, constraint.GenerateConstraintName()))
			}
		}
	}

	return errors
}
