# flying-orm 4.1.0 示例

连接、驱动、连接池和事务由上层应用提供。实体注解示例见 [ANNOTATIONS.md](ANNOTATIONS.md)。

## 目录

1. DynamicForm
2. 查询
3. 写入与批量
4. Schema
5. 字段保护
6. SQL 模板与原生 SQL
7. JDBC 与 R2DBC

## 1. DynamicForm

```java
DynamicForm users = DynamicForm.builder("users")
    .column("id", Long.class)
    .column("org_id", Long.class)
    .column("name", String.class)
    .column("status", String.class)
    .column("created_at", Instant.class)
    .build();
```

## 2. 查询

### 2.1 普通条件查询

```java
ConditionGroup where = ConditionGroup.and()
    .where("status", "=", "ACTIVE")
    .where("name", "like", "A%")
    .build();

Flux<DynamicRow> rows = forms.select(QuerySpec.of(users, where));
```

### 2.2 投影、排序、实体映射

```java
QuerySpec query = QuerySpec.of(users, ConditionGroup.and().build())
    .withProjection(List.of("id", "name", "created_at"), List.of())
    .withSorts(List.of(PageSort.desc("created_at"), PageSort.asc("id")));

Flux<UserView> rows = forms.select(query, UserView.class);
```

### 2.3 Scope 查询

```java
QuerySpec scoped = QuerySpec.of(users, ConditionGroup.and().build())
    .withScope(DataScope.of("org_id", orgId));

forms.select(scoped);
```

### 2.4 前端结构化条件

```java
StructuredConditionInput input = request.conditions();
QuerySpec query = QuerySpec.structured(users, input)
    .withScope(DataScope.of("org_id", orgId));

forms.select(query);
```

结构化条件由字段、操作符和形状策略编译；前端不能传入任意 SQL。

### 2.5 页码分页

```java
Mono<PageResult<DynamicRow>> page = forms.page(
    querySpec, PageQuery.of(1, 20, PageSort.desc("created_at")));
```

### 2.6 游标分页

```java
Mono<CursorPageResult<DynamicRow>> first = forms.cursorPage(
    querySpec, CursorPageQuery.first(20, CursorSort.asc("id")));

Mono<CursorPageResult<DynamicRow>> next = forms.cursorPage(
    querySpec, CursorPageQuery.after(20, List.of(lastId), CursorSort.asc("id")));
```

### 2.7 键集分页

```java
Mono<KeysetPageResult<DynamicRow>> page = forms.keysetPage(
    querySpec, KeysetPageQuery.first(20, KeysetSort.asc("id")));
```

### 2.8 JOIN 与同表自关联

```java
JoinQuerySpec.Builder builder = JoinQuerySpec.builder(users);
JoinSource employee = builder.root();
JoinSource manager = builder.join(JoinType.LEFT, users, employee, "manager_id", "id");

JoinQuerySpec query = builder
    .select(employee, "id")
    .select(employee, "name")
    .selectAs(manager, "name", "manager_name")
    .where(employee, ConditionGroup.and().where("status", "=", "ACTIVE").build())
    .orderBy(employee, "id", PageSort.Direction.ASC)
    .build();

forms.selectJoin(query);
```

### 2.9 分组与报表查询

```java
QuerySpec reportQuery = QuerySpec.of(orders,
    ConditionGroup.and()
        .where("created_at", ">=", start)
        .where("created_at", "<", end)
        .build())
    .withScope(DataScope.of("org_id", orgId));

AggregateSpec report = AggregateSpec.builder(reportQuery)
    .group(GroupSelection.of("status", "status"))
    .aggregate(AggregateExpression.count("id", "order_count"))
    .aggregate(AggregateExpression.sum("amount", "total_amount"))
    .having(AggregateHaving.of(
        ConditionGroup.and().where("order_count", ">", 10L).build()))
    .build();

Flux<AggregateRow> totals = forms.aggregate(report);
```

### 2.10 锁定读取

```java
LockingReadSpec locked = LockingReadSpec.of(
    querySpec, ReadLock.updateNowait());

forms.lockingRead(locked);
```

锁定只生成 SQL 锁语义；事务范围与连接由上层决定。

### 2.11 Repository 查询

```java
SyncFormRepository<User> users = clients.syncRepository(User.class);

User user = users.findById(1L);
List<User> active = users.select(
    ConditionGroup.and().where("status", "=", "ACTIVE").build());
PageResult<User> page = users.page(
    ConditionGroup.and().build(), PageQuery.of(1, 20));
```

## 3. 写入与批量

```java
forms.insert(InsertSpec.of(users)
    .value("name", "Alice")
    .value("status", "ACTIVE"));

forms.update(UpdateSpec.of(users)
    .set("status", "DISABLED")
    .where(ConditionGroup.and().where("id", "=", 1L).build()));

forms.delete(DeleteSpec.of(users)
    .where(ConditionGroup.and().where("id", "=", 1L).build()));
```

```java
List<Map<String, Object>> rows = List.of(
    Map.of("id", 1L, "name", "Alice"),
    Map.of("id", 2L, "name", "Bob"));

forms.writeBatch(BatchSpec.insert(users).rows(rows));
forms.writeBatch(BatchSpec.update(users).rows(rows).where("id"));
forms.writeBatch(BatchSpec.delete(users).rows(List.of(Map.of("id", 1L))).where("id"));

BatchSpec upsert = BatchSpec.upsert(users).rows(rows)
    .withScope(DataScope.of("org_id", orgId));
Mono<BatchExecutionEvidence> evidence = forms.writeBatch(upsert);
```

## 4. Schema

```java
EntitySchemaSynchronizer synchronizer = clients.entitySchemaSynchronizer();
synchronizer.synchronizeRelational(User.class, SchemaSyncMode.SAFE_UPDATE);
```

Schema 同步会审核计划、核验实际结构并在执行后回读。普通 CRUD 不自动建表。

## 5. 字段保护

```java
DynamicForm customers = DynamicForm.builder("customers")
    .encrypted("phone", EncryptionMode.EXACT)
    .masked("phone", MaskMode.MIDDLE)
    .build();

QuerySpec query = QuerySpec.of(customers,
    ConditionGroup.and().where("phone", "=", "13800000000").build())
    .masked();

forms.select(query);
```

## 6. SQL 模板与受控原生 SQL

```java
SqlTemplateRegistry registry = SqlTemplateRegistry.builder()
    .register(SqlTemplate.query("user-by-id",
        "select ${table}.* from ${table} where id = :id",
        Set.of("table")))
    .build();

SqlTemplateEngine engine = SqlTemplateEngine.create(
    registry, RdbDialect.postgresql(), ValueCodecRegistry.standard());
SqlRequest request = engine.render("user-by-id",
    Map.of("id", 1L), Map.of("table", "users"));

Flux<DynamicRow> rows = clients.operator()
    .unsafeNativeSql("select id, name from users where org_id = :orgId")
    .bind("orgId", orgId)
    .query();
```

业务值必须绑定参数；动态标识符必须来自受控映射。

## 7. JDBC 与 R2DBC

```java
// R2DBC：Flux / Mono
Flux<DynamicRow> reactiveRows = forms.select(querySpec);

// JDBC：同步结果
List<DynamicRow> syncRows = syncForms.select(querySpec);
```

上层实现 `R2dbcConnectionAccess` 或 `JdbcConnectionAccess`，并决定连接获取、释放、事务和生命周期。
