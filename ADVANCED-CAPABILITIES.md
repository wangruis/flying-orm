# flying-orm 4.1.0 高级能力

## DatabaseOperator

用于程序化 DML、DDL 和原生 SQL。仍使用 flying-orm 的参数绑定、Scope、标识符校验和结果映射。

```java
var rows = clients.operator()
    .dml()
    .query()
    .select("id", "name")
    .from("users")
    .fetchMap();
```

## 注册 SQL 模板

模板在装配阶段注册，运行时只绑定业务参数；动态表名等标识符单独传入。

```java
SqlTemplateRegistry templates = SqlTemplateRegistry.builder()
    .register(SqlTemplate.query("user-by-id",
        "select ${table}.* from ${table} where id = :id",
        Set.of("table")))
    .build();

SqlTemplateEngine engine = SqlTemplateEngine.create(
    templates, RdbDialect.postgresql(), ValueCodecRegistry.standard());
SqlRequest request = engine.render("user-by-id",
    Map.of("id", 1001L), Map.of("table", "users"));
```

## 受控原生 SQL

适用于数据库专有语法。SQL 只能由服务端代码提供；业务值必须绑定，动态标识符必须来自受控映射。

```java
Flux<DynamicRow> rows = clients.operator()
    .unsafeNativeSql("select id, name from users where org_id = :orgId")
    .bind("orgId", orgId)
    .query();
```

## JDBC / R2DBC 边界

- JDBC 同步执行，R2DBC 响应式执行。
- 连接由上层获取、释放和管理；ORM 不持有 DataSource、ConnectionFactory 或连接池。
- 驱动、路由、健康检查、重连、凭据和所有超时策略由上层负责。
- ORM 只清理自己创建的 Statement、ResultSet、LOB 等资源。

## 事务与分片边界

ORM 不创建、探测或参与事务，不提供提交、回滚、事务恢复或分片路由。多语句原子性、重试和分片由上层实现。

## Schema、缓存与保护字段

- Schema：实体注解、审核计划、五方言 DDL、执行前核验和回读验证。
- 缓存：SQL/条件/元数据使用有界缓存，按显式失效更新，不读取事务上下文。
- 保护字段：显式声明后提供加密、EXACT/SUFFIX/CONTAINS 检索和脱敏；密钥由上层提供。

Schema 规划和审核直接读取当前数据库事实，不命中或写入普通 CRUD 元数据缓存。旧版表元数据路径使用
`readTableForSchema(String)`，保留列、索引、外键等原有信息；关系型快照路径继续使用 `readSnapshot`。
内建 JDBC reader 和响应式缓存已实现旁路。无缓存的自定义响应式 reader 可沿用默认方法；自带缓存的
reader 必须覆盖 `readTableForSchema` 并委派自己的无缓存加载路径，不要求调用方手工清空 CRUD 缓存。

## 使用入口

常用接入、连接端口实现和完整示例见 [README](README.md)；实体注解见 [ANNOTATIONS.md](ANNOTATIONS.md)。

返回 [README](README.md) · [常用能力](CAPABILITIES.md)
