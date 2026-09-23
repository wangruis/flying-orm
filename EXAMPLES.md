# flying-orm 4.1.1 示例

默认使用 R2DBC：查询返回 `Flux`，单结果和写入返回 `Mono`。连接装配见 [README](README.md)，注解定义见 [ANNOTATIONS](ANNOTATIONS.md)。

片段按场景独立使用，省略 import；`clients` 已完成装配，`orgId`、`input` 等来自应用。执行一个写入 Publisher 一次，不要为查看结果重复订阅。

## 目录

- [准备模型](#准备模型)
- [常用查询](#常用查询)
- [前端条件与字段治理](#前端条件与字段治理)
- [分页](#分页)
- [报表与聚合](#报表与聚合)
- [关联与自关联](#关联与自关联)
- [实体 Repository](#实体-repository)
- [写入与批量](#写入与批量)
- [结构维护](#结构维护)
- [加密检索与脱敏](#加密检索与脱敏)
- [模板原生-sql-与锁定读取](#模板原生-sql-与锁定读取)
- [同步调用与扩展配置](#同步调用与扩展配置)

## 准备模型

```java
DynamicForm users = DynamicForm.builder("users", "users")
    .addField(DynamicField.primaryKey("id", "BIGINT"))
    .addField(DynamicField.of("name", "VARCHAR(128)"))
    .addField(DynamicField.of("org_id", "BIGINT"))
    .addField(DynamicField.of("manager_id", "BIGINT"))
    .addField(DynamicField.of("status", "VARCHAR(16)"))
    .addField(DynamicField.of("version", "BIGINT"))
    .addField(DynamicField.of("deleted", "INTEGER"))
    .logicDelete("deleted", 0, 1)
    .build();

DatabaseOperator operator = clients.operator();
DmlOperator dml = operator.dml();
ReactiveFormClient forms = clients.forms();
```

逻辑删除字段在模型声明一次，之后表单查询与写入复用。需要显式 schema/catalog 时使用 `DynamicForm.relationalBuilder(id, RelationIdentity.of(catalog, schema, table))`。

## 常用查询

### 全部、等值、范围与集合

```java
Flux<DynamicRow> all = dml.query(users).fetchMap();

Mono<DynamicRow> byId = dml.query(users).where("id", 1L).one();
Flux<DynamicRow> active = dml.query(users).where("status", "ACTIVE").fetchMap();
Flux<DynamicRow> containsName = dml.query(users).where("name", "like", "%Alice%").fetchMap();
Flux<DynamicRow> ids = dml.query(users).where("id", "in", List.of(1L, 2L)).fetchMap();
Flux<DynamicRow> excluded = dml.query(users).where("id", "not-in", List.of(1L, 2L)).fetchMap();
Flux<DynamicRow> interval = dml.query(users).where("id", "between", List.of(10L, 20L)).fetchMap();
Flux<DynamicRow> greater = dml.query(users).where("id", ">", 10L).fetchMap();
```

`one()`：零行返回空 Mono，多行报错，不静默取第一条。`like` 值中的 `%`、`_` 保留 SQL 通配符语义，值仍通过绑定传入。

### AND、OR、空值与可选条件

```java
Flux<DynamicRow> rows = dml.query(users)
    .where(w -> w.is("status", "ACTIVE")
        .or(group -> group.is("name", "Alice").is("name", "Bob"))
        .and(group -> group.where("id", ">", 10L).where("id", "<", 100L))
        .isNotNull("org_id")
        .isNull("manager_id")
        .isIfPresent("name", optionalName))
    .fetchMap();
```

直接 `where(field, value)` 连续调用按 AND 追加；`where(callback)` 替换本次业务条件，但不清除 Scope。可选值才使用 `IfPresent`；不要用可选条件代替更新/删除的必要筛选。严格 `IN` 不会静默丢弃非法空元素。

### 投影、排序、DTO 与单列结果

```java
public record UserView(Long id, String name) {}

Flux<UserView> views = dml.query(users)
    .select("id", "name")
    .orderByAsc("id")
    .fetch(UserView.class);

Flux<Long> ids = dml.query(users)
    .select("id")
    .where("status", "ACTIVE")
    .fetch(row -> (Long) row.get("id"));

Flux<DynamicRow> organizations = dml.query(users)
    .select("org_id")
    .groupBy("org_id")
    .orderByAsc("org_id")
    .fetchMap();
```

DTO 的属性/record 组件应与投影匹配；单列可能为 NULL 时不要从 Reactor 映射器返回 null，应显式包装结果或先过滤。

### 业务语义条件

在客户端装配时注册一次，不在业务查询中拼 SQL：

```java
SqlRenderer renderer = SqlRenderer.builder()
    .addDefaultTerms()
    .addTerm(SqlTermHandler.relationExists(
        "user-in-org", "user_org", "membership", "user_id", "org_id"))
    .build();

FlyingOrmClients clients = FlyingOrmClients.builder(access)
    .dialect(RdbDialect.postgresql())
    .renderer(renderer)
    .build();

Flux<DynamicRow> rows = clients.operator().dml().query(users)
    .where("id", "user-in-org", orgId)
    .fetchMap();
```

这里检索 `user_org` 中属于指定机构的用户。关联表和字段由服务端注册，`orgId` 是绑定参数；扩展处理器是可信后端代码，不能交给前端注册。

## 前端条件与字段治理

```java
StructuredConditionInput input = StructuredConditionInput.or(
    StructuredConditionInput.term("name", "like", "A%"),
    StructuredConditionInput.term("status", "=", "ACTIVE"));

FlyingOrmClients scopedClients = clients.withDefaultDataScope(DataScope.orgOnly("org_id", trustedOrgId));
DatabaseOperator scoped = scopedClients.operator();

Mono<PageResult<DynamicRow>> result = scoped.dml().query(users)
    .filter(input)
    .where("id", ">", 0L)
    .orderByAsc("id")
    .page(1, 20);
```

范围与业务条件始终取交集；租户模型搭配 `DataScope.tenant("tenant_id", trustedTenantId)`。需要相同范围的实体、表单和批量操作，应从 `scopedClients.repository(...)`、`scopedClients.forms()` 或 `scoped` 创建。原 `clients` 保持不变；仅调用 `operator.withDefaultDataScope(...)` 不会改变其他客户端入口的范围。

需要独立控制投影、过滤、排序等字段用途时：

```java
FieldUsePolicy policy = FieldUsePolicy.builder()
    .visibility("id", FieldVisibility.FULL)
    .visibility("name", FieldVisibility.FULL)
    .allow("id", FieldUse.PROJECT, FieldUse.FILTER, FieldUse.SORT)
    .allow("name", FieldUse.PROJECT, FieldUse.FILTER)
    .build();

Flux<DynamicRow> result = scoped.dml().query()
    .from(users, policy)
    .select("id", "name")
    .filter(StructuredConditionInput.term("name", "like", "A%"))
    .orderByAsc("id")
    .fetchMap();
```

这个策略没有允许 `status` 过滤，因此携带该字段的输入会被拒绝。也可使用 `from(form, policy, limits)` 显式指定查询形状预算。

## 分页

### 页码：需要总数

```java
Mono<PageResult<DynamicRow>> page = dml.query(users)
    .where("status", "ACTIVE")
    .orderByDesc("id")
    .page(1, 20);
```

页码从 1 开始，执行总数与页数据查询。跨语句一致性由上层数据库环境决定。

### 游标：稳定非空排序

```java
Mono<CursorPageResult<DynamicRow>> first = dml.query(users)
    .cursorPage(CursorPageQuery.first(20, CursorSort.asc("id")));

Mono<CursorPageResult<DynamicRow>> next = dml.query(users)
    .cursorPage(CursorPageQuery.after(20, List.of(lastId), CursorSort.asc("id")));
```

### Keyset：复合排序、可空字段

```java
Mono<KeysetPageResult<DynamicRow>> first = dml.query(users).keysetPage(KeysetPageQuery.first(
    20, KeysetSort.asc("name", NullOrder.LAST), KeysetSort.asc("id", NullOrder.LAST)));

// previous 是已取得的 KeysetPageResult；仅在 hasMore() 为 true 时继续。
Mono<KeysetPageResult<DynamicRow>> next = dml.query(users).keysetPage(KeysetPageQuery.after(
    20, previous.nextPosition(),
    KeysetSort.asc("name", NullOrder.LAST), KeysetSort.asc("id", NullOrder.LAST)));
```

保持条件、Scope 与排序一致，原样回传完整 `nextPosition`，不要自己截取排序值。游标可包含敏感排序值，跨信任边界时由上层签名或加密。游标与 keyset 不额外查询总数。

## 报表与聚合

```java
AggregateExpression<Long> userCount = AggregateExpression.count("id", "user_count");
AggregateExpression<Long> distinctNames = AggregateExpression.countDistinct("name", "name_count");

Flux<AggregateRow> report = dml.query(users)
    .where("status", "ACTIVE")
    .scope(DataScope.orgOnly("org_id", orgId))
    .aggregate(a -> a
        .group(GroupSelection.of("org_id", "org"))
        .aggregate(userCount)
        .aggregate(distinctNames)
        .having(AggregateHaving.of(ConditionGroup.and()
            .where("user_count", ">", 10L).build())));

Flux<Long> counts = report.map(row -> row.get(userCount));
```

`count(field, alias)` 统计非空值；统计行数应选非空主键。SUM / AVG / MIN / MAX 同样使用类型化表达式：

```java
AggregateExpression<BigDecimal> total = AggregateExpression.sum("amount", "total_amount");
AggregateExpression<BigDecimal> average = AggregateExpression.avg("amount", "average_amount");
AggregateExpression<BigDecimal> largest = AggregateExpression.max("amount", "largest_amount", LogicalType.DECIMAL, BigDecimal.class);
```

这些表达式用于具有 `amount` 字段的订单表单，添加到同一 `aggregate` 回调即可。前端 `filter`、服务端 `where`、Scope 和字段用途检查在报表入口同样生效。

## 关联与自关联

普通多表查询：

```java
Flux<DynamicRow> rows = dml.joinQuery(users)
    .leftJoin(organizations, "org_id", "id")
    .select(users, "id")
    .selectAs(users, "name", "user_name")
    .selectAs(organizations, "name", "org_name")
    .where(users, "status", "=", "ACTIVE")
    .orderByAsc(users, "id")
    .executeRows();
```

`organizations` 是应用定义的机构 DynamicForm。INNER/RIGHT JOIN 分别使用 `join` / `rightJoin`；复合 ON 使用 `andOn`。使用 `page(PageQuery.of(1, 20))` 分页。

同表自关联要区分“员工源”和“主管源”，使用稳定来源引用：

```java
JoinQuerySpec.Builder builder = JoinQuerySpec.builder(users);
JoinSource employee = builder.root();
JoinSource manager = builder.join(JoinType.LEFT, users, employee, "manager_id", "id");

JoinQuerySpec query = builder
    .select(employee, "id")
    .selectAs(employee, "name", "employee_name")
    .selectAs(manager, "name", "manager_name")
    .scope(employee, DataScope.orgOnly("org_id", orgId))
    .scope(manager, DataScope.orgOnly("org_id", orgId))
    .orderBy(employee, "id", PageSort.Direction.ASC)
    .build();

Flux<DynamicRow> rows = forms.selectJoin(query);
```

各来源独立应用 Scope、逻辑删除与字段治理，不把同表的两个角色混成一个身份。

## 实体 Repository

`User` 是带 getter/setter 的实体，含 `id/name/orgId/status/version` 属性；映射和治理注解见 [ANNOTATIONS](ANNOTATIONS.md)。

```java
ReactiveFormRepository<User> repository = clients.repository(User.class);

Flux<User> active = repository.createQuery()
    .where(User::getStatus, "ACTIVE")
    .in(User::getOrgId, List.of(7L, 8L))
    .orderByAsc(User::getId)
    .fetch();

Flux<String> names = repository.createQuery()
    .select(User::getName)
    .where(User::getOrgId, 7L)
    .fetch()
    .map(User::getName);

Mono<User> one = repository.createQuery().where(User::getId, 1L).one();
Mono<PageResult<User>> page = repository.createQuery().orderByAsc(User::getId).page(1, 20);

Mono<Long> update = repository.createUpdate()
    .set(User::getName, "Alice")
    .where(User::getId, 1L)
    .optimisticLock(3L)  // User.version 声明 @Version
    .execute();

Mono<Long> deleted = repository.createDelete().in(User::getId, List.of(1L, 2L)).execute();
```

投影 `fetch()` 返回部分实体；Bean 未选择的属性保留构造器和字段初始化值，record 未选择的组件使用 Java 默认值。旧 `execute()/one()/page()` 保留完整实体约束；动态投影可用 `executeRows()`。实体也支持 `or`、`andGroup`、`between`、`isNull`、`isNotNull` 和 `and(property, operator, value)`。

## 写入与批量

### 单条插入、范围更新、逻辑删除

```java
Mono<Long> inserted = dml.insert(users, Map.of(
    "id", 1L, "name", "Alice", "org_id", 7L,
    "status", "ACTIVE", "version", 0L, "deleted", 0));

Mono<Long> updated = dml.update(users)
    .set("status", "DISABLED")
    .where("id", "in", List.of(1L, 2L))
    .scope(DataScope.orgOnly("org_id", 7L))
    .execute();

Mono<Long> deleted = dml.delete(users)
    .where("id", "in", List.of(1L, 2L))
    .scope(DataScope.orgOnly("org_id", 7L))
    .execute();
```

无业务条件的更新/删除会被拒绝。模型声明逻辑删除时默认软删除；确需物理删除才显式 `physical()`，仍不绕过 Scope。批量同值更新、批量删除用上面的 `IN` 范围条件，不存在 `BatchSpec.delete` 工厂。

### 批量插入与 UPSERT

```java
Flux<Map<String, Object>> inputRows = Flux.fromIterable(rows);
Mono<BatchExecutionEvidence> inserted = dml.insertBatch(users, inputRows);
Mono<BatchExecutionEvidence> upserted = dml.upsertBatch(users, inputRows);
```

两种写入按业务需求二选一。UPSERT 以模型主键识别冲突，已有冲突目标必须满足 Scope。

### 每行不同值：乐观锁批量更新

```java
Flux<BatchOptimisticUpdate> updates = Flux.just(new BatchOptimisticUpdate(
    Map.of("name", "Alice"),
    ConditionGroup.and().where("id", "=", 1L).build(),
    OptimisticLockOptions.increment("version", 3L)));

Mono<BatchExecutionEvidence> result = dml.updateBatch(users, updates);
```

每行携带自己的条件和预期版本，冲突不会伪装成成功。实体 Repository 也提供 `insertBatch`、`upsertBatch`、`updateBatch`，保留生成键回填和实体生命周期处理。

### 批量预算与结果

```java
BatchSpec batch = BatchSpec.insert(users, Flux.fromIterable(rows))
    .withScope(DataScope.orgOnly("org_id", orgId))
    .withOptions(BatchWriteOptions.of(500)
        .withMemoryLimits(100_000, 32L * 1024 * 1024)
        .withMaxRowBytes(1024 * 1024));

Mono<BatchExecutionEvidence> result = forms.writeBatch(batch);
```

输入有界缓冲，不收集整个 Publisher。结果记录接收数、已证明成功/失败的位置、影响行数与冲突；未知计数不当作零，失败证据不代表事务提交状态，不能据此盲目重放整批。

## 结构维护

### 读取已有表

```java
Mono<TableMetadata> metadata = operator.metadata().readTable("users");
Flux<DynamicRow> rows = operator.metadata().readForm("existingUsers", "users")
    .flatMapMany(form -> operator.dml().query(form).where("id", 1L).fetchMap());
```

数据库元数据不能推断应用的租户、加密或脱敏声明；这些规则仍应由应用模型显式提供。外部工具修改结构后，可用 `operator.metadata().invalidate("users")` 使该表的读取缓存失效。

### 用同一份动态模型

```java
Mono<Long> changed = operator.ddl().createOrAlter(users);
```

### 用 Java DSL 描述结构

```java
Mono<Long> changed = operator.ddl().createOrAlter("notes")
    .addColumn().name("id").number(18).primaryKey().comment("ID").commit()
    .addColumn().name("body").varchar(128).comment("正文").commit()
    .commit();
```

这里的 `commit()` 表示结束 DSL 并生成执行 Publisher，不是数据库事务提交。调用 `plan()` 可先获取计划；危险变更走 `review / executeReviewed`，不默认放行。

### 实体注解同步

```java
Mono<EntitySchemaSyncReport> validation = clients.entitySchemas()
    .synchronizeReactive(EntitySchemaSyncMode.VALIDATE, User.class);

Mono<EntitySchemaSyncReport> safeUpdate = clients.entitySchemas()
    .synchronizeReactive(EntitySchemaSyncMode.SAFE_UPDATE, User.class);
```

校验与更新是两种独立选择；JDBC 使用 `synchronize`。包含多表外键、CHECK、分区等完整关系声明时，使用 `synchronizeRelationalReactive(databaseDescriptor, mode, entityTypes)` 或 JDBC 对应入口，先审核真实结构，执行前核验并在执行后回读。

## 加密检索与脱敏

先在客户端装配时配置应用提供的密钥：

```java
FlyingOrmClients clients = FlyingOrmClients.builder(access)
    .dialect(RdbDialect.postgresql())
    .protectedFields(ProtectedFieldKeyRing.single("v1", applicationEncryptionKey))
    .build();

DynamicForm contacts = DynamicForm.builder("contacts", "contacts")
    .addField(DynamicField.primaryKey("id", "BIGINT"))
    .addField(DynamicField.of("phone", "VARCHAR(128)"))
    .encrypted("phone", EncryptedFieldDefinition.builder()
        .searchModes(EncryptedSearchMode.EXACT, EncryptedSearchMode.SUFFIX, EncryptedSearchMode.CONTAINS)
        .suffixLengths(4)
        .containsMinLength(3)
        .build())
    .masked("phone", MaskedFieldDefinition.builder("partial").prefix(3).suffix(4).build())
    .build();
```

结构维护会处理声明对应的物理列和辅助关系；写入使用逻辑字段名，不手动加密：

```java
DmlOperator dml = clients.operator().dml();
Mono<Long> inserted = dml.insert(contacts, Map.of("id", 1L, "phone", "13800001234"));

Flux<DynamicRow> exact = dml.query(contacts)
    .where("phone", ProtectedConditions.EXACT, "13800001234").masked().fetchMap();

Flux<DynamicRow> suffix = dml.query(contacts)
    .where("phone", ProtectedConditions.SUFFIX, "1234").fetchMap();

Flux<DynamicRow> contains = dml.query(contacts)
    .where("phone", ProtectedConditions.CONTAINS, "0000").fetchMap();
```

未配置显式字段可见性策略时，`declaredDisplay()` 遵循字段声明，`masked()` 对已声明脱敏的字段使用脱敏展示。配置 `FieldUsePolicy` 后，以其可见性规则为准：需要脱敏应设置 `FieldVisibility.MASKED`，不能用 `.masked()` 覆盖策略中的 `FULL`。`showSensitive()` 只应在应用已经授权后调用。普通 `=` / `like` 不是加密检索的替代品。CONTAINS 保留候选验证和资源预算，不通过无限扫描实现。

## 模板、原生 SQL 与锁定读取

模板注册及执行、受控原生 SQL 的完整短例见 [高级能力](ADVANCED-CAPABILITIES.md)。

锁定读取仍使用模型和条件：

```java
QuerySpec query = QuerySpec.of(users, ConditionGroup.and().where("id", "=", 1L).build());
Flux<DynamicRow> locked = forms.lockingRead(LockingReadSpec.of(query, ReadLock.updateNowait()));
```

锁定语义需数据库支持，其有效范围由上层连接和事务边界决定；ORM 不开启事务。

## 同步调用与扩展配置

```java
SyncDmlOperator sync = clients.syncOperator().dml();
List<DynamicRow> rows = sync.query(users).where("org_id", 7L).fetchMap();
DynamicRow one = sync.query(users).where("id", 1L).one(); // 无匹配时为 null
BatchExecutionEvidence evidence = sync.insertBatch(users, inputList);

List<User> entities = clients.syncRepository(User.class).createQuery()
    .where(User::getOrgId, 7L).fetch();
```

JDBC 是同步阻塞调用，不放进 Reactor 事件循环。R2DBC 不使用隐藏 `block/subscribe`。

高级配置集中在装配阶段：`renderer`（条件/codec）、`executionOptions`（读取保护）、`batchWriteOptions`（批量预算）、`cachePolicy`、`idGenerator`、`fieldFiller`、`entitySchema`、`protectedFields`、`sqlTemplates`、`observers`。一次请求的特殊执行选项仍可通过 QuerySpec/WriteSpec 或已有终端重载传入，不需要重新创建整套客户端。
