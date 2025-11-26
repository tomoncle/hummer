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

import "strings"

type Condition struct {
	Schema string
	Args   []interface{}
}

func NewCondition(schema string, args ...interface{}) *Condition {
	return &Condition{schema, args}
}

type ConditionBuilder struct {
	builder   strings.Builder
	condition Condition
}

func NewConditionBuilder() *ConditionBuilder {
	return &ConditionBuilder{builder: strings.Builder{}, condition: Condition{}}
}

func (c *ConditionBuilder) AND(query string, args ...interface{}) *ConditionBuilder {
	if !c.IsEmpty() {
		c.builder.WriteString(" AND ")
	}
	c.builder.WriteString(query)
	c.condition.Args = append(c.condition.Args, args...)
	return c
}

func (c *ConditionBuilder) OR(query string, args ...interface{}) *ConditionBuilder {
	if !c.IsEmpty() {
		c.builder.WriteString(" OR ")
	}
	c.builder.WriteString(query)
	c.condition.Args = append(c.condition.Args, args...)
	return c
}

func (c *ConditionBuilder) IsEmpty() bool {
	return c.builder.Len() == 0
}

func (c *ConditionBuilder) Condition() *Condition {
	if c.IsEmpty() {
		return nil
	}
	c.condition.Schema = c.builder.String()
	return &c.condition
}

type PageRequest struct {
	page      int
	pageSize  int
	condition *Condition
	orders    []string // "ID ASC", "name DESC"
}

func (p *PageRequest) PageSize() int {
	if p.pageSize < 1 {
		p.pageSize = 10
	}
	return p.pageSize
}
func (p *PageRequest) Page() int {
	if p.page < 1 {
		p.page = 1
	}
	return p.page
}
func (p *PageRequest) Offset() int {
	return (p.Page() - 1) * p.PageSize()
}
func (p *PageRequest) Condition() *Condition {
	return p.condition
}
func (p *PageRequest) Orders() []string {
	return p.orders
}

func NewPageRequest(page int, pageSize int, c *Condition, orders []string) *PageRequest {
	return &PageRequest{page, pageSize, c, orders}
}

type Pagination[T any] struct {
	Page     int
	PageSize int
	Total    int
	Items    []*T
}

func NewDefaultPagination[T any](page int, pageSize int) *Pagination[T] {
	return &Pagination[T]{page, pageSize, 0, make([]*T, 0)}
}
