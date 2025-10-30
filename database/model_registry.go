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
	"sort"
	"sync"
)

var defaultRegistry = newModelRegistry()

// SQLModel represents a database model used for automatic migration/initialization.
// Instance should return a struct pointer compatible with Bun, and Priority controls
// ordering when initializing models (lower values first).
type SQLModel interface {
	Instance() interface{}
	Priority() int
}

// ModelRegistry stores SQL models and exposes them in a deterministic order.
type ModelRegistry interface {
	Register(model SQLModel)
	Models() []SQLModel
}

type modelRegistry struct {
	models []SQLModel
	mutex  sync.RWMutex
}

func newModelRegistry() ModelRegistry {
	return &modelRegistry{
		models: make([]SQLModel, 0),
	}
}

func (r *modelRegistry) Register(model SQLModel) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.models = append(r.models, model)
}

func (r *modelRegistry) Models() []SQLModel {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	result := make([]SQLModel, len(r.models))
	copy(result, r.models)
	sort.Slice(result, func(i, j int) bool {
		return result[i].Priority() < result[j].Priority()
	})
	return result
}

type ModelAdapter struct {
	instance interface{}
	priority int
}

// NewModelAdapter wraps a struct instance and priority into an SQLModel.
func NewModelAdapter(instance interface{}, priority int) SQLModel {
	return &ModelAdapter{
		instance: instance,
		priority: priority,
	}
}

// Instance returns the underlying struct used for migrations/initialization.
func (a *ModelAdapter) Instance() interface{} {
	return a.instance
}

// Priority returns the model's ordering value; lower values run earlier.
func (a *ModelAdapter) Priority() int {
	return a.priority
}

// GetRegisteredModels returns all models registered in the default registry
// sorted by ascending priority.
func GetRegisteredModels() []SQLModel {
	return defaultRegistry.Models()
}

// RegisteredModel adds a model to the default registry.
func RegisteredModel(model SQLModel) {
	defaultRegistry.Register(model)
}

func RegisteredModelInstances() []interface{} {
	models := GetRegisteredModels()
	modelInstances := make([]interface{}, len(models))
	for i, model := range models {
		modelInstances[i] = model.Instance()
	}
	return modelInstances
}
