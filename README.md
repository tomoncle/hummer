# hummer

Golang ORM 封装，基于 `uptrace/bun` 提供更简洁的泛型 `Service/Repository` API、自动迁移与 Schema 同步、分页与条件构建、事务支持、外键管理、以及可配置的日志输出。

## 安装

- 运行 `go get github.com/tomoncle/hummer`

## 快速开始

1) 定义模型
```go
package model

import (
    "github.com/uptrace/bun"
)

type SystemConfig struct {
    bun.BaseModel `bun:"table:system_config"`

    ID    int64  `bun:"id,pk,autoincrement"`
    Name  string `bun:"name,unique:uk_system_config_name,notnull,type:varchar(64)"`
    Value string `bun:"value,type:text" hummer:"rename:cfg_value"`
}
```

2) 注册模型
```go
import "github.com/tomoncle/hummer/database"

func init() {
    database.RegisteredModel(database.NewModelAdapter((*SystemConfig)(nil), 10))
}
```

3) 初始化数据库
```go
package main

import (
    "context"
    "github.com/tomoncle/hummer/database"
)

func main() {
    cfg := &database.Config{
        ConnectionConfig: database.ConnectionConfig{
            Type:     "mysql",        // 支持 mysql / postgres / sqlite
            Host:     "127.0.0.1",
            Port:     3306,
            Username: "root",
            Password: "",
            DBName:   "who",
            EnableQueryLog: true,
        },
    }

    _, err := database.InitDB(cfg)
    if err != nil { panic(err) }
    defer func() { _ = database.CloseDB() }()

    _ = context.Background()
}
```

## CRUD 与查询（使用 Service）
```go
import (
    "context"
    "github.com/tomoncle/hummer"
    "github.com/tomoncle/hummer/types"
)

svc := hummer.NewService[SystemConfig]()
ctx := context.Background()

// 创建
_ = svc.Save(ctx, &SystemConfig{Name: "site_title", Value: "Hummer"})

// 获取
cfg, _ := svc.Get(ctx, 1)

// 列表（条件）
cb := types.NewConditionBuilder().AND("name = ?", "site_title").Condition()
list, _ := svc.List(ctx, cb)

// 分页与排序
page := types.NewPageRequest(1, 10, cb, []string{"id DESC"})
pg, _ := svc.Page(ctx, page)

// 条件查询
raw, _ := svc.Query(ctx, "name = ?", "site_title")
```

## 事务
```go
import "github.com/tomoncle/hummer/database"

db := database.GetDB()
tx, err := db.BeginTx(ctx, nil)
if err != nil { panic(err) }
defer func() { _ = tx.Rollback() }()

err = hummer.NewService[SystemConfig]().SaveWithTx(ctx, tx, &SystemConfig{Name: "k", Value: "v"})
if err != nil { panic(err) }

if err := tx.Commit(); err != nil { panic(err) }
```

## Schema 同步与迁移
- `EnableMigrateOnStartup`：启动时执行迁移（创建迁移表，执行已注册迁移）。
- `EnableSchemaSync`：根据 `bun` 标签推断目标表结构并与现有结构比对，同步列与索引。
- 列标签支持：`type:...`、`notnull`、`default:...`、`pk`、`autoincrement`、`unique[:name]`。
- 重命名列：通过 `hummer:"rename:old_name"` 标记，用于在同步时将旧列改为新列。

## 外键管理
- 通过配置启用：`DataMigrateConfig.EnableForeignKey = true`。
- 支持从 YAML 文件加载外键：`DataMigrateConfig.ForeignKeyFile`。

示例 `foreign_keys.yaml`：
```yaml
foreign_keys:
  - table: system_config
    column: user_id
    reference_table: users
    reference_column: id
    on_delete: CASCADE
    on_update: NO ACTION
    constraint_name: fk_system_config_user
```

## 数据初始化（SQL 文件）
- 配置 `DataInitConfig.Filepath` 为 SQL 目录，`Environment` 为环境名（如 `development`）。
- 执行：`database.InitData()` 或 `database.InitDataWithSQL("development")`。

## 使用 Repository（可选）
```go
import "github.com/tomoncle/hummer/repository"

repo := repository.NewRepository[SystemConfig](database.GetDB())
entities, _ := repo.List(ctx, cb)
pagination, _ := repo.Page(ctx, types.NewPageRequest(1, 20, nil, nil))
```

## 日志配置（可选）
```go
import "github.com/tomoncle/hummer/utils"

utils.ConfigureFileLog("logs", 7, false)      // 文件日志按天切分
utils.ConfigureFileLogFormat("json")           // 文件日志格式：json/text
utils.ConfigureConsoleLogFormat("text")        // 控制台日志格式：json/text
utils.ConfigureLogLevel("info")                // 统一日志级别

lg := utils.NewLogger("APP")
lg.Info("service started")
```

## 健康检查与统计
```go
hs := database.GetHealthStatus(ctx) // { healthy, connected, response_time, ... }
stats := database.GetDatabaseStats() // 连接池统计
```

## 环境变量覆盖（节选）
- `DB_HOST`
- `DB_PORT`
- `DB_USERNAME`
- `DB_PASSWORD`
- `DB_NAME`
- `DB_SSLMODE`
- `DB_MAX_IDLE_CONNS`
- `DB_MAX_OPEN_CONNS`
- `DB_CONN_MAX_LIFETIME`
- `DB_ENABLE_QUERY_LOG`

## 兼容数据库
- MySQL、PostgreSQL、SQLite

## 参考
- ORM: `github.com/uptrace/bun`
