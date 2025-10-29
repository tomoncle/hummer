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

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
)

// JsonObject is a convenience type for JSON columns mapped to objects.
type JsonObject map[string]interface{}

// JsonArray is a convenience type for JSON columns mapped to arrays.
type JsonArray []JsonObject

// Value implements driver.Valuer for JsonObject.
func (j JsonObject) Value() (driver.Value, error) {
	if j == nil {
		return nil, nil
	}
	return json.Marshal(j)
}

// Scan implements sql.Scanner for JsonObject.
func (j *JsonObject) Scan(value interface{}) error {
	if value == nil {
		*j = make(JsonObject)
		return nil
	}
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion must be []byte")
	}
	return json.Unmarshal(bytes, j)
}

// Value implements driver.Valuer for JsonArray.
func (j JsonArray) Value() (driver.Value, error) {
	if j == nil {
		return nil, nil
	}
	return json.Marshal(j)
}

// Scan implements sql.Scanner for JsonArray.
func (j *JsonArray) Scan(value interface{}) error {
	if value == nil {
		*j = make(JsonArray, 0)
		return nil
	}
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion must be []byte")
	}
	return json.Unmarshal(bytes, j)
}
