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

package types

// QueryFilter describes a WHERE clause schema and its argument values.
type QueryFilter struct {
	Schema string
	Args   []interface{}
}

// NewQueryFilter creates a new query filter with schema and args.
func NewQueryFilter(schema string, args ...interface{}) *QueryFilter {
	return &QueryFilter{schema, args}
}

// PageRequest describes pagination, optional filter, and ordering.
type PageRequest struct {
	page     int
	pageSize int
	filter   *QueryFilter
	orders   []string // "ID ASC", "name DESC"
}

func (p *PageRequest) GetPageSize() int {
	if p.pageSize < 1 {
		p.pageSize = 10
	}
	return p.pageSize
}
func (p *PageRequest) GetPage() int {
	if p.page < 1 {
		p.page = 1
	}
	return p.page
}

func (p *PageRequest) GetOffset() int {
	return (p.GetPage() - 1) * p.GetPageSize()
}

func (p *PageRequest) GetFilter() *QueryFilter {
	return p.filter
}

func (p *PageRequest) GetOrders() []string {
	return p.orders
}

// NewPageRequest constructs a PageRequest with filter and order settings.
func NewPageRequest(page int, pageSize int, filter *QueryFilter, orders []string) *PageRequest {
	return &PageRequest{page, pageSize, filter, orders}
}

// NewPageRequestWithFilter constructs a PageRequest with a filter only.
func NewPageRequestWithFilter(page int, pageSize int, filter *QueryFilter) *PageRequest {
	return NewPageRequest(page, pageSize, filter, make([]string, 0))
}

// NewPageRequestWithOrders constructs a PageRequest with ordering only.
func NewPageRequestWithOrders(page int, pageSize int, orders []string) *PageRequest {
	return NewPageRequest(page, pageSize, nil, orders)
}

// NewDefaultPageRequest constructs a PageRequest with no filter or ordering.
func NewDefaultPageRequest(page int, pageSize int) *PageRequest {
	return NewPageRequest(page, pageSize, nil, make([]string, 0))
}

// Pagination holds paged result items along with pagination metadata.
type Pagination[T any] struct {
	Page     int
	PageSize int
	Total    int
	Items    []*T
}

// NewDefaultPagination constructs an empty pagination container.
func NewDefaultPagination[T any](page int, pageSize int) *Pagination[T] {
	return &Pagination[T]{page, pageSize, 0, make([]*T, 0)}
}
