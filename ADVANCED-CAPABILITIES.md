# flying-orm 4.1.1 高级能力

普通 CRUD、分页与报表优先使用 [表单或 Repository 入口](EXAMPLES.md)。以下接口用于服务端明确控制的特殊查询与扩展。

## 注册 SQL 模板

注册一次，执行时只提供参数和已批准的标识符：

```java
SqlTemplateRegistry templates = SqlTemplateRegistry.builder()
    .register(SqlTemplate.query("user-by-id",
        "select id, name from ${table} where id = :id", Set.of("table")))
    .build();

FlyingOrmClients clients = FlyingOrmClients.builder(access)
    .dialect(RdbDialect.postgresql())
    .sqlTemplates(templates)
    .build();

Mono<DynamicRow> row = clients.operator().sqlTemplate("user-by-id")
    .identifier("table", "users")
    .bind("id", 1001L)
    .one();
```

值使用命名参数绑定；标识符不能用值参数代替，必须由后端受控映射决定。复杂应用可配置 `sqlTemplateParameterProvider`，JDBC 对应 `syncSqlTemplateParameterProvider`。

## 受控原生 SQL

```java
Flux<DynamicRow> rows = clients.operator()
    .unsafeNativeSql("select id, name from users where org_id = :orgId")
    .bind("orgId", orgId)
    .query();
```

SQL 文本只来自可信后端代码。模板与原生 SQL **不自动注入 Scope、租户、逻辑删除或保护字段规则**；需要这些语义时使用绑定 DynamicForm 的查询。普通值仍须绑定，不能拼接前端输入。

## 扩展配置

| 需要扩展的内容 | 装配入口 / 所有者 |
| --- | --- |
| 标准及业务条件、参数 codec | `SqlRenderer.builder()` → `builder.renderer(...)` |
| ID、字段填充、实体结构声明 | `idGenerator`、`fieldFiller`、`entitySchema` |
| 加密密钥、规范化与脱敏策略 | `protectedFields`、`protectedFieldPolicies` |
| 读取保护与批量预算 | `executionOptions`、`batchWriteOptions`、`batchMemoryLimits` |
| SQL、批量观测与安全日志 | `observers`、`sqlExecutionLog` |
| 缓存和迁移观测 | `cachePolicy`、`migrationObserver`、`migrationExecutionOptions` |

默认配置可直接使用，高级配置按需启用。读取/批量预算是资源边界，不是连接或事务超时策略。

```java
SqlExecutionOptions limits = SqlExecutionOptions.safeDefaults()
    .withMaxRows(1000)
    .withMaxResultBytes(8L * 1024 * 1024)
    .withFetchSize(128);
Flux<DynamicRow> rows = clients.operator().dml().query(users).fetchMap(limits);
```

超出保护预算会报错，不是静默截断；业务分页使用 `page / cursorPage / keysetPage`。

## Schema 与元数据

`ddl().createOrAlter` 用于普通安全结构调整；`plan / review / executeReviewed` 保留完整审核流程。实体关系同步使用 `entitySchemas().synchronizeRelational...`，显式表达外键、CHECK、分区等关系事实。

Schema 规划直接读取数据库结构，不复用普通 CRUD 的元数据缓存。自带缓存的自定义 reader 必须为 `readTableForSchema` / 关系快照提供真实无缓存读取；内建实现已处理此边界。

## 生命周期边界

客户端不保存 DataSource、ConnectionFactory 或具体连接池；只保存上层连接访问端口。连接成功获取后，ORM 清理自身语句资源并调用上层释放回调；获取失败不调用释放，释放失败不会静默吞掉。

事务、分片、路由、健康检查、重连和超时全部由上层负责。`ReadLock` 只表达 SQL 行锁，DDL Builder 的 `commit()` 只结束 DSL。

[README](README.md) · [能力清单](CAPABILITIES.md) · [完整示例](EXAMPLES.md) · [实体注解](ANNOTATIONS.md)
