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
	"errors"
	"strings"

	"github.com/go-sql-driver/mysql"
)

type SQLError int

const (
	UnknownErr SQLError = iota
	NoRowsErr
	NoIndexErr
	NoColumnErr
	ExistIndexErr
	ExistColumnErr
	NoTableErr
	ExistTableErr
	DuplicateKeyErr
	NotNullViolationErr
	ForeignKeyViolationErr
	CheckConstraintViolationErr
	DataTruncatedErr
	InvalidTypeCastErr
)

func IsSqlError(err error) (is bool, sqlErr SQLError) {
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		switch mysqlErr.Number {
		case 1091:
			return true, NoIndexErr
		case 1054:
			return true, NoColumnErr
		case 1061:
			return true, ExistIndexErr
		case 1060:
			return true, ExistColumnErr
		case 1062:
			return true, DuplicateKeyErr
		case 1048:
			return true, NotNullViolationErr
		case 1216, 1217:
			return true, ForeignKeyViolationErr
		case 3819:
			return true, CheckConstraintViolationErr
		case 1265:
			return true, DataTruncatedErr
		case 1046, 1049:
			return true, UnknownErr
		default:
			return true, UnknownErr
		}
	}
	s := strings.ToLower(err.Error())
	if strings.Contains(s, "sqlstate 42703") ||
		strings.Contains(s, "undefined column") ||
		strings.Contains(s, "no such column") {
		return true, NoColumnErr
	}
	if strings.Contains(s, "sqlstate 42704") ||
		strings.Contains(s, "no such index") ||
		(strings.Contains(s, "does not exist") && strings.Contains(s, "index")) {
		return true, NoIndexErr
	}
	if strings.Contains(s, "sqlstate 42p01") ||
		strings.Contains(s, "undefined table") ||
		strings.Contains(s, "no such table") {
		return true, NoTableErr
	}
	if strings.Contains(s, "already exists") &&
		strings.Contains(s, "index") {
		return true, ExistIndexErr
	}
	if strings.Contains(s, "already exists") &&
		strings.Contains(s, "table") ||
		strings.Contains(s, "relation") &&
			strings.Contains(s, "already exists") {
		return true, ExistTableErr
	}
	if strings.Contains(s, "duplicate key value") ||
		strings.Contains(s, "unique constraint failed") ||
		strings.Contains(s, "sqlstate 23505") {
		return true, DuplicateKeyErr
	}
	if strings.Contains(s, "not-null constraint") ||
		strings.Contains(s, "sqlstate 23502") ||
		strings.Contains(s, "not null constraint failed") {
		return true, NotNullViolationErr
	}
	if strings.Contains(s, "foreign key violation") ||
		strings.Contains(s, "foreign key constraint failed") ||
		strings.Contains(s, "sqlstate 23503") {
		return true, ForeignKeyViolationErr
	}
	if strings.Contains(s, "check constraint") ||
		strings.Contains(s, "sqlstate 23514") {
		return true, CheckConstraintViolationErr
	}
	if strings.Contains(s, "string data right truncation") ||
		strings.Contains(s, "sqlstate 22001") ||
		strings.Contains(s, "data truncated") {
		return true, DataTruncatedErr
	}
	if strings.Contains(s, "datatype mismatch") ||
		strings.Contains(s, "sqlstate 42804") {
		return true, InvalidTypeCastErr
	}
	return false, UnknownErr
}
