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
	"fmt"
	"github.com/tomoncle/hummer/types"
	"strings"

	"github.com/uptrace/bun/schema"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/feature"
)

type baseRepositoryImpl[T any] struct {
	db *bun.DB
}

// NewRepository returns a generic repository backed by the provided Bun DB.
func NewRepository[T any](db *bun.DB) Repository[T] {
	return &baseRepositoryImpl[T]{db: db}
}

func (r *baseRepositoryImpl[T]) Dialect() schema.Dialect { return r.db.Dialect() }

func (r *baseRepositoryImpl[T]) NewSelect() *bun.SelectQuery { return r.db.NewSelect() }

func (r *baseRepositoryImpl[T]) NewInsert() *bun.InsertQuery { return r.db.NewInsert() }

func (r *baseRepositoryImpl[T]) NewUpdate() *bun.UpdateQuery { return r.db.NewUpdate() }

func (r *baseRepositoryImpl[T]) NewDelete() *bun.DeleteQuery { return r.db.NewDelete() }

func (r *baseRepositoryImpl[T]) ValsToSlice(entity ...*T) []*T {
	entities := make([]*T, len(entity))
	copy(entities, entity)
	return entities
}

func (r *baseRepositoryImpl[T]) GetOne(ctx context.Context, id any) (*T, error) {
	var entity T
	err := r.db.NewSelect().Model(&entity).Where("id = ?", id).Scan(ctx)
	if err != nil {
		return nil, err
	}
	return &entity, nil
}

func (r *baseRepositoryImpl[T]) GetAll(ctx context.Context) ([]*T, error) {
	var entities []*T
	err := r.db.NewSelect().Model(&entities).Scan(ctx)
	return entities, err
}

func (r *baseRepositoryImpl[T]) List(ctx context.Context, filter *types.QueryFilter) ([]*T, error) {
	var entities []*T
	query := r.db.NewSelect().Model(&entities)
	if filter != nil {
		query = query.Where(filter.Schema, filter.Args...)
	}
	err := query.Scan(ctx)
	if err != nil {
		return nil, err
	}
	return entities, err
}

func (r *baseRepositoryImpl[T]) Query(ctx context.Context, query string, args ...interface{}) ([]*T, error) {
	var entities []*T
	err := r.db.NewSelect().Model(&entities).Where(query, args...).Scan(ctx)
	return entities, err
}

func (r *baseRepositoryImpl[T]) Page(ctx context.Context, pageRequest *types.PageRequest) (*types.Pagination[T], error) {
	var entities []*T
	query := r.db.NewSelect().Model(&entities)
	if pageRequest.GetFilter() != nil {
		query = query.Where(pageRequest.GetFilter().Schema, pageRequest.GetFilter().Args...)
	}
	pagination := types.NewDefaultPagination[T](pageRequest.GetPage(), pageRequest.GetPageSize())
	total, err := query.Count(ctx)
	if err != nil || total == 0 {
		return pagination, err
	}
	err = query.
		Offset(pageRequest.GetOffset()).
		Limit(pageRequest.GetPageSize()).
		Order(pageRequest.GetOrders()...).
		Scan(ctx)
	if err != nil {
		return nil, err
	}
	pagination.Total = total
	pagination.Items = entities
	return pagination, nil
}

func (r *baseRepositoryImpl[T]) Create(ctx context.Context, entity ...*T) error {
	entities := r.ValsToSlice(entity...)
	_, err := r.db.NewInsert().Model(&entities).Exec(ctx)
	return err
}

func (r *baseRepositoryImpl[T]) Upsert(ctx context.Context, fields []string, duplicateKeys []string, entity ...*T) error {
	return r.multipleUpsert(ctx, nil, fields, duplicateKeys, entity...)
}

func (r *baseRepositoryImpl[T]) Update(ctx context.Context, entity *T) error {
	_, err := r.db.NewUpdate().Model(entity).WherePK().Exec(ctx)
	return err
}

func (r *baseRepositoryImpl[T]) Delete(ctx context.Context, id any) error {
	var entity T
	_, err := r.db.NewDelete().Model(&entity).Where("id = ?", id).Exec(ctx)
	return err
}

func (r *baseRepositoryImpl[T]) CreateWithTx(ctx context.Context, tx *bun.Tx, entity ...*T) error {
	entities := r.ValsToSlice(entity...)
	_, err := tx.NewInsert().Model(&entities).Exec(ctx)
	return err
}

func (r *baseRepositoryImpl[T]) UpsertWithTx(ctx context.Context, tx *bun.Tx, fields []string, duplicateKeys []string, entity ...*T) error {
	return r.multipleUpsert(ctx, tx, fields, duplicateKeys, entity...)
}

func (r *baseRepositoryImpl[T]) UpdateWithTx(ctx context.Context, tx *bun.Tx, entity *T) error {
	_, err := tx.NewUpdate().Model(entity).WherePK().Exec(ctx)
	return err
}

func (r *baseRepositoryImpl[T]) DeleteWithTx(ctx context.Context, tx *bun.Tx, id any) error {
	var entity T
	_, err := tx.NewDelete().Model(&entity).Where("id = ?", id).Exec(ctx)
	return err
}

func (r *baseRepositoryImpl[T]) multipleUpsert(ctx context.Context, tx *bun.Tx, fields []string, duplicateKeys []string, entity ...*T) error {
	if len(fields) == 0 {
		return fmt.Errorf("fields cannot be empty")
	}

	// If transaction is not nil, use it to execute insert/update
	var insertQuery *bun.InsertQuery
	if tx != nil {
		insertQuery = tx.NewInsert()
	} else {
		insertQuery = r.db.NewInsert()
	}

	entities := r.ValsToSlice(entity...)

	if r.db.HasFeature(feature.InsertOnConflict) {
		return r.upsertWithPostgresqlOrSQLite(ctx, insertQuery, fields, duplicateKeys, entities)
	} else if r.db.HasFeature(feature.InsertOnDuplicateKey) {
		return r.upsertWithMySQL(ctx, insertQuery, fields, entities)
	} else {
		// Fallback: Separate insert/update logic
		return r.upsertFallback(ctx, entities)
	}
}

func (r *baseRepositoryImpl[T]) upsertWithMySQL(ctx context.Context, insertQuery *bun.InsertQuery, fields []string, entities []*T) error {
	var queryArgs []string
	for _, field := range fields {
		queryArgs = append(queryArgs, fmt.Sprintf("%s = VALUES(%s)", bun.Ident(field), bun.Ident(field)))
	}
	_, err := insertQuery.
		Model(&entities).
		On("DUPLICATE KEY UPDATE " + strings.Join(queryArgs, ", ")).
		Exec(ctx)
	return err
}

func (r *baseRepositoryImpl[T]) upsertWithPostgresqlOrSQLite(ctx context.Context, insertQuery *bun.InsertQuery, fields []string, duplicateKeys []string, entities []*T) error {
	if len(duplicateKeys) == 0 {
		duplicateKeys = []string{"id"}
	}
	keyNames := strings.Join(duplicateKeys, ",")
	var queryArgs []string
	for _, field := range fields {
		queryArgs = append(queryArgs, fmt.Sprintf("%s = EXCLUDED.%s", bun.Ident(field), bun.Ident(field)))
	}
	_, err := insertQuery.
		Model(&entities).
		On("CONFLICT (" + keyNames + ") DO UPDATE").
		Set(strings.Join(queryArgs, ", ")).
		Exec(ctx)
	return err
}

func (r *baseRepositoryImpl[T]) upsertFallback(ctx context.Context, entities []*T) error {
	for _, entity := range entities {
		_, err := r.db.NewInsert().Model(&entity).Exec(ctx)
		if err != nil {
			_, updateErr := r.db.NewUpdate().Model(&entity).WherePK().Exec(ctx)
			if updateErr != nil {
				return fmt.Errorf("upsert failed for entity: insert error: %v, update error: %v", err, updateErr)
			}
		}
	}
	return nil
}
