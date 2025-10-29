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

package hummer

import (
	"context"
	"github.com/tomoncle/hummer/database"
	"github.com/tomoncle/hummer/repository"
	"github.com/tomoncle/hummer/types"
	"sync"

	"github.com/uptrace/bun"
)

type Service[T any] interface {
	// Get returns a single entity by its identifier.
	Get(ctx context.Context, id any) (*T, error)

	// All returns all entities.
	All(ctx context.Context) ([]*T, error)

	// List returns entities that match the provided filter.
	List(ctx context.Context, filter *types.QueryFilter) ([]*T, error)

	// Query executes a raw query and maps the results to entities.
	Query(ctx context.Context, query string, args ...interface{}) ([]*T, error)

	// Page returns a paginated list of entities.
	Page(ctx context.Context, page *types.PageRequest) (*types.Pagination[T], error)

	// Update modifies an existing entity.
	Update(ctx context.Context, model *T) error

	// Delete removes an entity by its identifier.
	Delete(ctx context.Context, id any) error

	// Save inserts one or more new entities.
	Save(ctx context.Context, model ...*T) error

	// SaveOrUpdate upserts entities based on fields and duplicate keys.
	SaveOrUpdate(ctx context.Context, fields []string, duplicateKeys []string, model ...*T) error

	// SaveWithTx inserts entities within an existing transaction.
	SaveWithTx(ctx context.Context, tx *bun.Tx, model ...*T) error

	// SaveOrUpdateWithTx upserts entities within a transaction.
	SaveOrUpdateWithTx(ctx context.Context, tx *bun.Tx, fields []string, duplicateKeys []string, model ...*T) error

	// UpdateWithTx updates an entity within a transaction.
	UpdateWithTx(ctx context.Context, tx *bun.Tx, model *T) error

	// DeleteWithTx removes an entity within a transaction.
	DeleteWithTx(ctx context.Context, tx *bun.Tx, id any) error

	// SelectBuilder returns a Bun select query builder for the entity.
	SelectBuilder() *bun.SelectQuery

	// InsertBuilder returns a Bun insert query builder for the entity.
	InsertBuilder() *bun.InsertQuery

	// UpdateBuilder returns a Bun update query builder for the entity.
	UpdateBuilder() *bun.UpdateQuery

	// DeleteBuilder returns a Bun delete query builder for the entity.
	DeleteBuilder() *bun.DeleteQuery
}

type baseServiceImpl[T any] struct {
	repo repository.Repository[T]
	once sync.Once
}

// NewService returns a default Service implementation using the generic
// repository backed by the global database connection.
func NewService[T any]() Service[T] {
	return newBaseServiceImpl[T]()
}

func newBaseServiceImpl[T any]() *baseServiceImpl[T] {
	return &baseServiceImpl[T]{}
}

func (s *baseServiceImpl[T]) baseRepo() repository.Repository[T] {
	s.once.Do(func() { s.repo = repository.NewRepository[T](database.GetDB()) })
	return s.repo
}

func (s *baseServiceImpl[T]) Save(ctx context.Context, model ...*T) error {
	return s.baseRepo().Create(ctx, model...)
}

func (s *baseServiceImpl[T]) SaveOrUpdate(ctx context.Context, fields []string, duplicateKeys []string, model ...*T) error {
	return s.baseRepo().Upsert(ctx, fields, duplicateKeys, model...)
}

func (s *baseServiceImpl[T]) Get(ctx context.Context, id any) (*T, error) {
	return s.baseRepo().GetOne(ctx, id)
}

func (s *baseServiceImpl[T]) All(ctx context.Context) ([]*T, error) {
	return s.baseRepo().GetAll(ctx)
}

func (s *baseServiceImpl[T]) List(ctx context.Context, filter *types.QueryFilter) ([]*T, error) {
	return s.baseRepo().List(ctx, filter)
}

func (s *baseServiceImpl[T]) Query(ctx context.Context, query string, args ...interface{}) ([]*T, error) {
	return s.baseRepo().Query(ctx, query, args...)
}

func (s *baseServiceImpl[T]) Update(ctx context.Context, model *T) error {
	return s.baseRepo().Update(ctx, model)
}

func (s *baseServiceImpl[T]) Delete(ctx context.Context, id any) error {
	return s.baseRepo().Delete(ctx, id)
}

func (s *baseServiceImpl[T]) Page(ctx context.Context, page *types.PageRequest) (*types.Pagination[T], error) {
	return s.baseRepo().Page(ctx, page)
}

func (s *baseServiceImpl[T]) SaveWithTx(ctx context.Context, tx *bun.Tx, model ...*T) error {
	return s.baseRepo().CreateWithTx(ctx, tx, model...)
}

func (s *baseServiceImpl[T]) SaveOrUpdateWithTx(ctx context.Context, tx *bun.Tx, fields []string, duplicateKeys []string, model ...*T) error {
	return s.baseRepo().UpsertWithTx(ctx, tx, fields, duplicateKeys, model...)
}

func (s *baseServiceImpl[T]) UpdateWithTx(ctx context.Context, tx *bun.Tx, model *T) error {
	return s.baseRepo().UpdateWithTx(ctx, tx, model)
}

func (s *baseServiceImpl[T]) DeleteWithTx(ctx context.Context, tx *bun.Tx, id any) error {
	return s.baseRepo().DeleteWithTx(ctx, tx, id)
}

func (s *baseServiceImpl[T]) SelectBuilder() *bun.SelectQuery {
	return s.baseRepo().NewSelect()
}

func (s *baseServiceImpl[T]) InsertBuilder() *bun.InsertQuery {
	return s.baseRepo().NewInsert()
}

func (s *baseServiceImpl[T]) UpdateBuilder() *bun.UpdateQuery {
	return s.baseRepo().NewUpdate()
}

func (s *baseServiceImpl[T]) DeleteBuilder() *bun.DeleteQuery {
	return s.baseRepo().NewDelete()
}
