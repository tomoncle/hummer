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

package repository

import (
	"context"
	"github.com/tomoncle/hummer/types"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/schema"
)

// CrudRepository defines basic CRUD operations for a generic entity type.
type CrudRepository[T any] interface {
	GetOne(ctx context.Context, id any) (*T, error)

	GetAll(ctx context.Context) ([]*T, error)

	List(ctx context.Context, filter *types.QueryFilter) ([]*T, error)

	Query(ctx context.Context, query string, args ...interface{}) ([]*T, error)

	Create(ctx context.Context, entity ...*T) error

	Upsert(ctx context.Context, fields []string, duplicateKeys []string, entity ...*T) error

	Update(ctx context.Context, entity *T) error

	Delete(ctx context.Context, id any) error
}

// TransactionRepository defines CRUD operations executed within a transaction.
type TransactionRepository[T any] interface {
	CreateWithTx(ctx context.Context, tx *bun.Tx, entity ...*T) error
	UpsertWithTx(ctx context.Context, tx *bun.Tx, fields []string, duplicateKeys []string, entity ...*T) error
	UpdateWithTx(ctx context.Context, tx *bun.Tx, entity *T) error
	DeleteWithTx(ctx context.Context, tx *bun.Tx, id any) error
}

// PageQueryRepository defines pagination functionality for listing entities.
type PageQueryRepository[T any] interface {
	Page(ctx context.Context, page *types.PageRequest) (*types.Pagination[T], error)
}

// Repository combines CRUD, pagination, and transactional operations and
// exposes Bun query builders for advanced use cases.
type Repository[T any] interface {
	CrudRepository[T]
	PageQueryRepository[T]
	TransactionRepository[T]
	Dialect() schema.Dialect
	NewSelect() *bun.SelectQuery
	NewInsert() *bun.InsertQuery
	NewUpdate() *bun.UpdateQuery
	NewDelete() *bun.DeleteQuery
}
