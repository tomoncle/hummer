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
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"text/template"
	"time"

	"github.com/uptrace/bun"
)

// SQLInitManager discovers and executes SQL files to seed data.
type SQLInitManager struct {
	db          *bun.DB
	environment string
	sqlRootPath string
	logger      Logger
}

// SQLFileInfo describes a SQL file to be executed during initialization.
type SQLFileInfo struct {
	Path        string
	Name        string
	Order       int
	Environment string
	ModTime     time.Time
}

// ExecutionResult contains the outcome of executing a single SQL file.
type ExecutionResult struct {
	File         string
	Success      bool
	Error        error
	Duration     time.Duration
	RowsAffected int64
}

// NewSQLInitManager creates a SQL initializer for the given environment.
func NewSQLInitManager(db *bun.DB, environment string) *SQLInitManager {
	return &SQLInitManager{
		db:          db,
		environment: environment,
		sqlRootPath: "configs/sql",
		logger:      GetDBLogger(),
	}
}

// SetSQLRootPath sets the root directory from which SQL files are loaded.
func (s *SQLInitManager) SetSQLRootPath(path string) {
	s.sqlRootPath = path
}

// ExecuteInitialization runs all discovered SQL files in the correct order.
func (s *SQLInitManager) ExecuteInitialization() error {
	s.logger.Info("Starting SQL initialization", map[string]interface{}{
		"environment": s.environment,
		"sql_path":    s.sqlRootPath,
	})

	files, err := s.GetSQLFiles()
	if err != nil {
		return fmt.Errorf("failed to get SQL files: %w", err)
	}

	if len(files) == 0 {
		s.logger.Info("No SQL files found to execute")
		return nil
	}

	results := make([]ExecutionResult, 0, len(files))
	for _, file := range files {
		result := s.executeFile(file)
		results = append(results, result)

		if !result.Success {
			s.logger.Error("SQL file execution failed", map[string]interface{}{
				"file":  result.File,
				"error": result.Error.Error(),
			})
			return fmt.Errorf("SQL file execution failed %s: %w", result.File, result.Error)
		}

		s.logger.Info("SQL file executed successfully", map[string]interface{}{
			"file":          result.File,
			"duration":      result.Duration.String(),
			"rows_affected": result.RowsAffected,
		})
	}

	s.logger.Info("SQL initialization completed", map[string]interface{}{
		"total_files": len(results),
		"environment": s.environment,
	})

	return nil
}

// GetSQLFiles returns the list of SQL files from common and environment dirs.
func (s *SQLInitManager) GetSQLFiles() ([]SQLFileInfo, error) {
	var files []SQLFileInfo

	commonFiles, err := s.getFilesFromDir(filepath.Join(s.sqlRootPath, "common"), "common")
	if err != nil {
		return nil, fmt.Errorf("failed to get common SQL files: %w", err)
	}
	files = append(files, commonFiles...)

	envPath := filepath.Join(s.sqlRootPath, "environments", s.environment)
	if _, err := os.Stat(envPath); err == nil {
		envFiles, err := s.getFilesFromDir(envPath, s.environment)
		if err != nil {
			return nil, fmt.Errorf("failed to get environment SQL files: %w", err)
		}
		files = append(files, envFiles...)
	}

	sort.Slice(files, func(i, j int) bool {
		if files[i].Environment != files[j].Environment {
			return files[i].Environment == "common"
		}
		return files[i].Order < files[j].Order
	})

	return files, nil
}

func (s *SQLInitManager) getFilesFromDir(dir, environment string) ([]SQLFileInfo, error) {
	var files []SQLFileInfo

	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(strings.ToLower(d.Name()), ".sql") {
			return nil
		}

		order := s.parseFileOrder(d.Name())
		info, err := d.Info()
		if err != nil {
			return err
		}

		files = append(files, SQLFileInfo{
			Path:        path,
			Name:        d.Name(),
			Order:       order,
			Environment: environment,
			ModTime:     info.ModTime(),
		})

		return nil
	})

	return files, err
}

func (s *SQLInitManager) parseFileOrder(filename string) int {
	re := regexp.MustCompile(`^(\d+)_`)
	matches := re.FindStringSubmatch(filename)
	if len(matches) > 1 {
		var order int
		_, _ = fmt.Sscanf(matches[1], "%d", &order)
		return order
	}
	return 999
}

func (s *SQLInitManager) executeFile(file SQLFileInfo) ExecutionResult {
	start := time.Now()
	result := ExecutionResult{
		File:    file.Path,
		Success: false,
	}

	content, err := os.ReadFile(file.Path)
	if err != nil {
		result.Error = fmt.Errorf("failed to read file: %w", err)
		result.Duration = time.Since(start)
		return result
	}

	processedContent := string(content)
	statements := s.splitSQLStatements(processedContent)
	if len(statements) == 0 {
		result.Success = true
		result.Duration = time.Since(start)
		return result
	}

	ctx := context.Background()
	err = s.db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
		var totalRowsAffected int64

		for _, stmt := range statements {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" || strings.HasPrefix(stmt, "--") {
				continue
			}

			result, execErr := tx.ExecContext(ctx, stmt)
			if execErr != nil {
				return fmt.Errorf("failed to execute SQL statement: %s, error: %w", stmt, execErr)
			}

			rowsAffected, _ := result.RowsAffected()
			totalRowsAffected += rowsAffected
		}

		result.RowsAffected = totalRowsAffected
		return nil
	})

	if err != nil {
		result.Error = err
	} else {
		result.Success = true
	}

	result.Duration = time.Since(start)
	return result
}

func (s *SQLInitManager) replaceEnvVariables(content string) (string, error) {
	tmpl, err := template.New("sql").Parse(content)
	if err != nil {
		return "", fmt.Errorf("failed to parse template: %w", err)
	}

	envVars := make(map[string]string)
	for _, env := range os.Environ() {
		parts := strings.SplitN(env, "=", 2)
		if len(parts) == 2 {
			envVars[parts[0]] = parts[1]
		}
	}

	envVars["ENVIRONMENT"] = s.environment
	envVars["TIMESTAMP"] = time.Now().Format("2006-01-02 15:04:05")

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, envVars)
	if err != nil {
		return "", fmt.Errorf("failed to execute template: %w", err)
	}

	return buf.String(), nil
}

func (s *SQLInitManager) splitSQLStatements(content string) []string {
	var statements []string
	var current strings.Builder

	scanner := bufio.NewScanner(strings.NewReader(content))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "--") {
			continue
		}

		current.WriteString(line)
		current.WriteString(" ")

		if strings.HasSuffix(line, ";") {
			stmt := strings.TrimSpace(current.String())
			if stmt != "" {
				statements = append(statements, stmt)
			}
			current.Reset()
		}
	}

	if current.Len() > 0 {
		stmt := strings.TrimSpace(current.String())
		if stmt != "" {
			statements = append(statements, stmt)
		}
	}

	return statements
}

// GetExecutionHistory returns past execution results if recorded.
func (s *SQLInitManager) GetExecutionHistory() ([]ExecutionResult, error) {
	// Execution history can be implemented here
	// For example, record executed SQL files and results in the database
	return nil, nil
}
