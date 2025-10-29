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

// Common illegal/default values used by enums.
const (
	IllegalValue = -1
	IllegalName  = "unknown"
	IllegalDesc  = "unknown"
)

// BaseEnum represents a basic enum contract used by domain types.
type BaseEnum interface {
	IsValid() bool
	Number() int
	String() string
	Desc() string
	Name() string
}
