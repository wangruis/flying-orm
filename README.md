# flying-orm 4.1.1

轻 SQL，重 Java。面向动态表单和实体的 ORM，提供 JDBC 同步与 R2DBC / Reactor 响应式入口。

- 动态维护表结构，完成查询、插入、更新、删除和批量操作。
- 参数驱动条件，支持前端结构化条件；字段受校验，业务值参数绑定。
- 可扩展业务条件，不局限于 `=`、`>`、`like`。
- Scope、租户、字段治理、加密检索和脱敏进入同一执行内核。
- 返回原生 `Flux / Mono`，不把 JDBC 包装成“响应式”。

## 添加依赖

Java 21+。数据库驱动与连接池由应用自行提供。以下是本分支版本，版本号不表示已经发布到远程仓库。

```xml
<dependency>
    <groupId>io.github.wangruis</groupId>
    <artifactId>flying-orm-rdb</artifactId>
    <version>4.1.1</version>
</dependency>
```

## 接入一次

应用提供连接获取和释放回调。下面的 `connectionFactory` / `dataSource` 已由应用配置；释放方式是示例应用的选择，不是 ORM 的事务或连接池策略。

```java
// R2DBC
var access = R2dbcConnectionAccess.of(
    request -> connectionFactory.create(),
    (signal, connection, request) -> connection.close());

var clients = FlyingOrmClients.builder(access)
    .dialect(RdbDialect.postgresql())
    .build();

var operator = clients.operator();
```

JDBC 使用相同装配入口：

```java
var access = JdbcConnectionAccess.of(
    request -> dataSource.getConnection(),
    (connection, request) -> connection.close());

var clients = FlyingOrmClients.builder(access)
    .dialect(RdbDialect.postgresql())
    .build();

var operator = clients.syncOperator();
```

客户端适合单例共享，在应用关闭时调用 `clients.close()`；它不关闭应用的连接池。若应用使用事务，应在回调中遵守应用框架的借还规则，不能照搬直接获取/关闭连接的示例。

## 动态表单

一份模型供 DDL 与 CRUD 复用：

```java
DynamicForm users = DynamicForm.builder("users", "users")
    .addField(DynamicField.primaryKey("id", "BIGINT"))
    .addField(DynamicField.of("name", "VARCHAR(128)"))
    .addField(DynamicField.of("org_id", "BIGINT"))
    .build();

// 以下 operator 为 clients.operator()：返回 Reactor Publisher。
Mono<Long> migration = operator.ddl().createOrAlter(users);

Flux<DynamicRow> rows = operator.dml().query(users)
    .select("id", "name")
    .where("org_id", 7L)
    .orderByAsc("id")
    .fetchMap();

Mono<Long> inserted = operator.dml()
    .insert(users, Map.of("id", 1L, "name", "Alice", "org_id", 7L));

Mono<Long> updated = operator.dml().update(users)
    .set("name", "Bob")
    .where("id", 1L)
    .execute();
```

DDL 需显式调用，普通查询不会自动改表。响应式代码由调用方组合并订阅，若要先迁移后查询，使用 `migration.thenMany(rows)`；业务代码不需要 `block()` 或手动 `subscribe()`。

## 实体 Repository

```java
var users = clients.repository(User.class);

Flux<String> names = users.createQuery()
    .select(User::getName)
    .where(User::getOrgId, orgId)
    .fetch()
    .map(User::getName);
```

`fetch()` 可把投影映射为部分实体：Bean 未选择的属性保留构造器和字段初始化值，record 未选择的组件使用 Java 默认值。需要完整实体时不写 `select`。注解与实体定义见 [ANNOTATIONS.md](ANNOTATIONS.md)。

## 前端条件与数据范围

```java
var scoped = clients.operator()
    .withDefaultDataScope(DataScope.orgOnly("org_id", trustedOrgId));

var page = scoped.dml().query(users)
    .filter(input)                    // StructuredConditionInput，不是 SQL
    .where("name", "like", "A%")      // 服务端追加条件
    .orderByAsc("id")
    .page(1, 20);
```

前端条件、服务端条件和 Scope 取交集。Scope 必须来自后端可信身份，不由前端决定。可扩展条件的注册和 `where("id", "user-in-org", orgId)` 示例见 [EXAMPLES.md](EXAMPLES.md)。

## 入口怎么选

| 场景 | 入口 |
| --- | --- |
| 动态表单 CRUD、分页、聚合 | `operator.dml().query(form)` / `insert` / `update` / `delete` |
| 实体 CRUD、Lambda 条件 | `clients.repository(User.class)` |
| JOIN / 自关联 | `operator.dml().joinQuery(form)` / `JoinQuerySpec` |
| 批量插入、UPSERT、逐行乐观锁更新 | `operator.dml().insertBatch / upsertBatch / updateBatch` |
| 表结构 / 实体注解同步 | `operator.ddl()` / `clients.entitySchemas()` |
| 高级规格、锁定读取、执行选项 | `clients.forms()` |
| 同步 JDBC | `syncOperator()` / `syncRepository()` / `syncForms()` |

模型、条件和规格可复用；可变查询构建器每次操作新建，不跨线程共享。动态表单入口保留元数据治理；`from("物理表名")`、模板及原生 SQL 是可信后端入口，不会自动套用表单字段保护。

## 职责边界

flying-orm 不提供事务、分片、路由、驱动配置、连接池或超时调度。上层决定连接来源与释放；ORM 清理自己创建的语句、结果集和 LOB。批量返回执行事实，不代表提交、回滚或恢复结果。

支持 PostgreSQL、MySQL、Oracle、SQL Server、H2 的已声明方言能力；具体限制见 [能力清单](CAPABILITIES.md)。

## 文档与构建

- [完整示例](EXAMPLES.md)：查询、报表、写入、批量、Schema、保护字段。
- [能力清单](CAPABILITIES.md)：能力与调用入口。
- [高级能力](ADVANCED-CAPABILITIES.md)：SQL 模板、原生 SQL、扩展配置。
- [实体注解](ANNOTATIONS.md)：注解解释、示例与非表字段。

```bash
mvn -Pquality verify   # 全量质量门禁
mvn install           # 安装到当前配置的本地 Maven 仓库
```

[Apache License 2.0](LICENSE)
