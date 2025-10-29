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

package tests

import (
	"context"
	"github.com/tomoncle/hummer"
	"github.com/tomoncle/hummer/database"
	"github.com/uptrace/bun"
	"testing"
	"time"
)

type Config struct {
	Database DatabaseConfig `mapstructure:"database"`
}

type DatabaseConfig struct {
	Type     string `mapstructure:"type"`
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
	DBName   string `mapstructure:"dbname"`
}

func (c *Config) ConfigLoader() *database.Config {
	return &database.Config{
		ConnectionConfig: database.ConnectionConfig{
			Type:     c.Database.Type,
			Host:     c.Database.Host,
			Port:     c.Database.Port,
			Username: c.Database.Username,
			Password: c.Database.Password,
			DBName:   c.Database.DBName,
		},
	}
}

type SystemConfig struct {
	bun.BaseModel `bun:"table:system_config,alias:sc"`

	ID          int64     `bun:"id,type:bigint,pk,autoincrement" json:"id"`
	ConfigKey   string    `bun:"config_key,notnull,unique" json:"config_key"`
	ConfigValue string    `bun:"config_value" json:"config_value"`
	Description string    `bun:"description" json:"description"`
	ConfigType  string    `bun:"config_type,notnull,default:'string'" json:"config_type"`
	CreatedAt   time.Time `bun:"created_at,nullzero,notnull,default:current_timestamp" json:"created_at"`
	UpdatedAt   time.Time `bun:"updated_at,nullzero,notnull,default:current_timestamp" json:"updated_at"`
}

func TestQuery(t *testing.T) {
	cfg := Config{
		Database: DatabaseConfig{
			Type:     "mysql",
			Host:     "127.0.0.1",
			Port:     3306,
			Username: "",
			Password: "",
			DBName:   "who",
		},
	}
	_, err := database.InitDB(cfg.ConfigLoader())
	if err != nil {
		t.Fatalf("init database error: %v", err)
	}
	defer func() { _ = database.CloseDB() }()

	svc := hummer.NewService[SystemConfig]()
	ctx := context.Background()

	sysConfigs, err := svc.All(ctx)
	if err != nil {
		t.Logf("query error: %v", err)
	} else {
		t.Logf("search with %d rows", len(sysConfigs))
		for _, c := range sysConfigs {
			t.Logf("data: %v", c)
		}
	}

}
