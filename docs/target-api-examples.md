# flying-orm 目标 API 示例

这些示例说明 flying-orm 的目标用法，核心原则是：轻 SQL、重 Java，动态表单是底座；同步入口走原生 JDBC，响应式入口走真正非阻塞的 R2DBC/Reactor。

## Maven 坐标

应用通常只需要依赖 `flying-orm-rdb`，再按执行方式和数据库选择 JDBC 驱动、R2DBC 驱动或两者。

```xml
<dependency>
    <groupId>com.flying.orm</groupId>
    <artifactId>flying-orm-rdb</artifactId>
    <version>2.0.0</version>
</dependency>
```

项目本体不依赖应用框架，也不包含 starter 或自动配置。框架接入与集成验证放在仓库外的示例项目中；示例只能
调用下面的统一入口，不能复制 SQL、事务、安全或批量实现。

普通使用方不需要自己拼装这些对象。上层容器或纯 Java 启动代码通过统一入口创建一次 `FlyingOrmClients`，
业务代码只拿已经组装好的客户端或 Operator：

```java
FlyingOrmConfiguration configuration = FlyingOrmConfiguration.defaults();
FlyingOrmEnvironment environment = FlyingOrmEnvironment.of(connectionFactory);
FlyingOrmClients clients = FlyingOrmBootstrap.create(configuration, environment);

ReactiveFormClient forms = clients.forms();
DatabaseOperator operator = clients.operator();
```

默认会从 `ConnectionFactory` 自动识别方言。配置了方言时使用显式值，同时仍会校验它是否与物理数据库一致。

## 动态表单定义

```java
DynamicForm userForm = DynamicForm.builder("userForm", "Users")
        .addField(DynamicField.primaryKey("id", "BIGINT"))
        .addField(DynamicField.of("name", "VARCHAR"))
        .addField(DynamicField.of("age", "INTEGER"))
        .addField(DynamicField.of("orgId", "BIGINT"))
        .addField(DynamicField.of("enabled", "BOOLEAN"))
        .addField(DynamicField.of("deleted", "INTEGER"))
        .logicDelete("deleted", 0, 1)
        .build();
```

动态表单就是运行时表模型。DDL、CRUD、批量写入、Repository 和元数据反读都围绕它走。
声明了逻辑删除后，查询、分页、更新和默认删除都会自动限制在未删除数据上；确实要物理删除时调用 `physicalDelete(...)`。

## 纯 Java 组装

上层项目提供 `ConnectionFactory` 后，使用类型化配置和运行环境完成一次组装。动态数据源把物理库清单交给启动入口，
这样方言冲突会在启动时暴露，不会拖到第一条业务 SQL：

```java
FlyingOrmConfiguration configuration = FlyingOrmConfiguration.defaults()
        .withDialect("postgresql");
FlyingOrmEnvironment environment = FlyingOrmEnvironment.of(routingConnectionFactory)
        .withPhysicalDataSources(Map.of(
                "primary", primaryConnectionFactory,
                "replica", replicaConnectionFactory))
        .withTransactionParticipant(transactionParticipant)
        .withObservers(sqlObserver, batchObserver);

FlyingOrmClients clients = FlyingOrmBootstrap.create(configuration, environment);
```

框架适配器也调用同一个入口，只负责提供配置和运行环境。业务代码不创建方言、渲染器或执行器。

需要注册自定义条件、codec 等启动期扩展时，再使用底层 Builder；它仍然生成同一种 `FlyingOrmClients` 对象图：

```java
DataSource dataSource = ...;
ConnectionFactory connectionFactory = ...;

SqlTermPackage organizationTerms = RelationTermPackage.of(
        "user-organization",
        "org_user",
        "ou",
        "user_id",
        "org_id",
        "user-in-org",
        "user-not-in-org");
SqlRenderer renderer = SqlRenderer.builder()
        .addDefaultTerms()
        .addTermPackage(JsonTermHandlers.mysql())
        .addTermPackage(organizationTerms)
        .build();
StructuredConditionResolver resolver = StructuredConditionResolver.composite(
        JsonStructuredConditions.standard(),
        StructuredConditionCustomizer.allowOperator("user-in-org"));

FlyingOrmClients clients = FlyingOrmClients.builder(dataSource, connectionFactory)
        .renderer(renderer)
        .structuredConditionResolver(resolver)
        .executionOptions(SqlExecutionOptions.maxRows(1000)
                .withTimeout(Duration.ofSeconds(3)))
        .batchWriteOptions(BatchWriteOptions.atomic(500)
                .withConnectionAcquireTimeout(Duration.ofSeconds(1))
                .withTimeout(Duration.ofMinutes(2))
                .withMaxRows(100_000))
        .cachePolicy(OrmCachePolicy.safeDefaults())
        .build();

ReactiveFormClient forms = clients.forms();
ReactiveSchemaClient schema = clients.schema();
ReactiveFormMetadataReader metadata = clients.metadata();

SyncFormClient syncForms = clients.syncForms();
SyncSqlExecutor syncExecutor = clients.syncExecutor();

DatabaseOperator operator = clients.operator();
SyncDatabaseOperator syncOperator = clients.syncOperator();
```

这里没有任何框架生命周期。上层服务接入依赖注入、配置中心或事务管理时，只需要把对应能力转换为
`FlyingOrmConfiguration` 和 `FlyingOrmEnvironment`。

方言默认会从 `ConnectionFactory` 识别。只有驱动名不标准、代理库、多数据源很特殊时，才显式传配置里的兜底值：

```java
FlyingOrmClients clients = FlyingOrmClients.builder(connectionFactory)
        .renderer(renderer)
        .configuredDialect("mysql")
        .structuredConditionResolver(resolver)
        .build();
```

业务值对象或项目自己的 Boolean/枚举规则可以在启动时一次注册。新注册表不会修改全局默认值，
条件、动态表单写入、批量和实体回读都会复用它：

```java
ValueCodecRegistry valueCodecs = ValueCodecRegistry.standard()
        .withFirst(new OrderNoCodec());
SqlRenderer customRenderer = SqlRenderer.builder()
        .addDefaultTerms()
        .valueCodecs(valueCodecs)
        .build();

FlyingOrmClients clients = FlyingOrmClients.builder(connectionFactory)
        .renderer(customRenderer)
        .build();
```

默认结构化条件解析器会自动复用这套注册表。需要组合 JSON 或业务 operator 时，显式解析器也传入同一个实例：

```java
StructuredConditionResolver conditions = StructuredConditionResolvers.composite(
        valueCodecs,
        StructuredConditionResolvers.allowOperator("user-in-org"));

FlyingOrmClients clients = FlyingOrmClients.builder(connectionFactory)
        .renderer(customRenderer)
        .structuredConditionResolver(conditions)
        .build();
```

## 响应式 CRUD

```java
Mono<Long> inserted = forms.insert(WriteSpec.insert(userForm, Map.of(
        "id", 1L,
        "name", "Alice",
        "age", 18,
        "orgId", 100L,
        "enabled", true
)));

Flux<DynamicRow> rows = forms.select(QuerySpec.of(
        userForm,
        ConditionGroup.and()
                .where("enabled", "=", true)
                .where("age", ">", 18)
                .build()
));

Mono<Long> updated = forms.update(WriteSpec.update(
        userForm,
        Map.of("name", "Alice 2"),
        ConditionGroup.and().where("id", "=", 1L).build()
));

Mono<Long> deleted = forms.delete(WriteSpec.delete(
        userForm,
        ConditionGroup.and().where("id", "=", 1L).build()
));
```

所有值都走参数绑定，不拼进 SQL 字符串。

如果 `userForm` 配了 `.logicDelete("deleted", 0, 1)`，上面的 `delete(...)` 会渲染成更新删除标记，而不是物理删除。需要真删时显式写：

```java
Mono<Long> removed = forms.physicalDelete(WriteSpec.delete(
        userForm,
        ConditionGroup.and().where("id", "=", 1L).build()
));
```

## 前端结构化条件

前端可以传结构化条件，后端用安全策略编译成条件树。前端不能传 SQL。

```java
StructuredConditionInput input = StructuredConditionInput.and(
        StructuredConditionInput.term("enabled", "eq", true),
        StructuredConditionInput.term("age", "gt", 18)
);

Flux<DynamicRow> rows = forms.select(
        QuerySpec.structured(userForm, input)
                .withStructuredPolicy(StructuredConditionPolicies.dynamicForm())
);
```

字段、term、分组、节点数、集合大小和字符串长度都会被策略校验，未知字段和未知操作符默认直接失败。

编译时还会参考动态字段类型做轻量 value 规范化：比如前端传 `"18"`，字段是 `INTEGER` 时内部 AST 里会是 `Integer`；字段是 `BOOLEAN` 时 `"true"` 会变成 `Boolean`。集合条件也会逐项处理。这样前端仍然只传结构化 JSON，不碰 SQL，后端拿到的参数类型也更稳。

动态表单字段声明为 `JSON` 后，可以直接写 Map 或 List，不需要业务层先转字符串：

```java
DynamicForm form = DynamicForm.builder("userProfile", "user_profile")
        .addField(DynamicField.primaryKey("id", "BIGINT"))
        .addField(DynamicField.of("profile", "JSON"))
        .build();

forms.insert(WriteSpec.insert(form, Map.of(
        "id", 1L,
        "profile", Map.of("name", "Alice", "roles", List.of("admin"))
)));
```

写入前会校验 JSON；查询动态表单时 `profile` 会还原成 Map/List。普通 `in` 条件里的 List 仍按集合参数处理，不会被误当成 JSON。

JSON 高级条件按方言注册一次，前端只传结构化数据：

```java
SqlRenderer renderer = SqlRenderer.builder()
        .addDefaultTerms()
        .addTermPackage(JsonTermHandlers.postgresql())
        .build();

StructuredConditionResolver resolver = StructuredConditionResolver.composite(
        JsonStructuredConditions.standard());

StructuredConditionInput condition = StructuredConditionInput.term(
        "profile",
        "json-path-eq",
        Map.of("path", "$.contact.name", "value", "Alice"));
```

`json-path-eq`、`json-contains`、`json-exists` 都使用固定 SQL 模板。嵌套路径先拆成安全 key 段，再作为参数交给驱动，不会拼进 SQL。

PostgreSQL 原生数组直接使用 `VARCHAR[]`、`BIGINT[]` 这类字段类型。写入可以传数组或集合，查询动态表单时稳定返回 List：

```java
DynamicForm form = DynamicForm.builder("article", "article")
        .addField(DynamicField.primaryKey("id", "BIGINT"))
        .addField(DynamicField.of("tags", "VARCHAR[]"))
        .build();

SqlRenderer renderer = SqlRenderer.builder()
        .addDefaultTerms()
        .addTermPackage(ArrayTermHandlers.postgresql())
        .build();

StructuredConditionResolver resolver = StructuredConditionResolver.composite(
        ArrayStructuredConditions.postgresql());

StructuredConditionInput condition = StructuredConditionInput.term(
        "tags",
        "array-overlaps",
        Map.of("values", List.of("java", "r2dbc")));
```

数组条件还支持 `array-contains`、`array-contained-by`、`array-any-eq`。元素类型只从 `DynamicForm` 读取，前端不能伪造。

BLOB/CLOB 写入使用普通 Java 值，业务层不需要接触数据库驱动类型：

```java
DynamicForm attachment = DynamicForm.builder("attachment", "attachments")
        .addField(DynamicField.primaryKey("id", "BIGINT"))
        .addField(DynamicField.of("payload", "BLOB"))
        .addField(DynamicField.of("content", "CLOB"))
        .build();

forms.insert(WriteSpec.insert(attachment, Map.of(
        "id", 1L,
        "payload", new byte[]{1, 2, 3},
        "content", new StringBuilder("large text")
)));

SqlExecutionOptions lobProtection = SqlExecutionOptions.timeout(Duration.ofSeconds(5))
        .withMaxRows(100)
        .withMaxLargeObjectBytes(10 * 1024 * 1024)
        .withMaxLargeObjectChars(1_000_000);

// 可以单次指定，也可以通过 withDefaultExecutionOptions 给整个客户端统一设置。
forms.select(QuerySpec.of(attachment, ConditionGroup.and().build())
        .withExecutionOptions(lobProtection));
```

动态表单读取时 BLOB 统一为 `byte[]`，CLOB 统一为 `String`；实体字段也可以声明成 `ByteBuffer`。驱动返回 R2DBC `Blob/Clob` 时，响应式客户端会直接订阅内容流，不会 `block`。字节数和字符数上限都按单个字段计算，`0` 表示不限；超限、超时或下游取消时，当前内容流会随 Reactor 订阅一起取消。

Java DSL 对空值采用显式语义。严格 `where(...)` 不会悄悄丢条件，空值、空字符串或清理后为空的集合会在 SQL 前失败；确实允许不传的搜索项要写 `whereIfPresent(...)`。字符串会先去掉前后空白，集合中的 `null`、空字符串和纯空白项会被清掉。数据库空值判断使用专门方法：

```java
ConditionGroup where = ConditionGroup.and()
        .where("status", "=", "  enabled  ")
        .whereIfPresent("name", "like-ignore-case", request.name())
        .whereIfPresent("id", "in", request.ids())
        .whereNull("deleted_at")
        .build();

operator.dml().query()
        .from("users")
        .where(dsl -> dsl.isIfPresent("name", request.name())
                         .isNotNull("created_at"));
```

内置 `like-ignore-case` / `not-like-ignore-case` 同时对安全字段标识符和绑定参数应用数据库 `lower(...)`，
不会在 Java 端按默认 Locale 改写业务值。`%`、`_` 的通配符含义与普通 LIKE 相同；高频路径应由应用按数据库
能力配置匹配的函数索引、计算列索引或大小写不敏感排序规则。

内置 `in` / `not-in` 要求集合，`between` / `not-between` 要求两个同类型且顺序正确的边界，`is-null` / `is-not-null` 不接受值。未知业务 term 只按单值处理；自定义 `SqlTermHandler` 可在创建时声明值形状，`SqlTermPackage.terms()` 会自动汇总成条件构建可复用的 `TermRegistry`。`ParameterConditionPackage.of(...)` 也能把参数映射和同一份 term 元数据装在一起。

如果一个字段只该支持少数 operator，可以给它单独收口：

```java
StructuredConditionPolicy policy = StructuredConditionPolicies.publicApi(List.of("name", "age"))
        .allowFieldOperators("name", List.of("eq", "like"))
        .allowFieldOperators("age", List.of("eq", "gt", "lt", "in"));
```

配置过字段级 operator 后，其他 operator 即使在全局策略里存在，也不能用在这个字段上。

条件编译失败会抛 `StructuredConditionException`。上层不用解析异常文本，直接看结构化字段：

```java
try {
    forms.select(QuerySpec.structured(userForm, input).withStructuredPolicy(policy));
} catch (StructuredConditionException error) {
    StructuredConditionErrorCode code = error.code();
    String path = error.path();
    String field = error.field();
    String operator = error.operator();
    OrmErrorReport report = error.toErrorReport();
}
```

常见错误码包括 `FIELD_NOT_ALLOWED`、`OPERATOR_NOT_ALLOWED`、`FIELD_OPERATOR_NOT_ALLOWED`、`VALUE_TYPE_MISMATCH`、`VALUE_COLLECTION_TOO_LARGE`、`VALUE_RANGE_SIZE_INVALID`、`VALUE_RANGE_TYPE_MISMATCH`、`VALUE_RANGE_ORDER_INVALID`、`DEPTH_EXCEEDED`。`path` 会尽量贴近前端条件数组，比如 `conditions[2].value`，这样前端可以把“字段不允许查询”“操作符不允许”“值类型不正确”“嵌套层级过深”标到具体条件行上，上层服务也能按原因做业务处理。动态表单客户端还会在编译前拒绝由服务端控制的租户字段、逻辑删除字段和不在 `FieldScope` 读取白名单里的字段，例如 `conditions[0].field` 会返回 `FIELD_NOT_ALLOWED`。

JSON 数组元素条件也走结构化输入，路径和值不会拼进 SQL：

```json
{
  "field": "profile",
  "operator": "json-array-contains",
  "value": {"path": "roles", "value": "admin"}
}
```

条件、数据范围和数据库异常还可以统一转成同一种上层返回结构：

```java
OrmErrorReport report = exception.toErrorReport();
// category/code/resource/path/field/message
```

`StructuredConditionException`、`ScopeAccessException`、`RdbException` 都提供这个统一方法，不需要为不同异常维护不同报告类型。

## 服务端 DataScope

前端结构化条件只表达“用户想查什么”。租户、数据权限、组织范围这类服务端规则要由上层业务算好，再作为 `DataScope` 传给 flying-orm。flying-orm 会把它和业务 where 做 AND 合并，前端条件只能把结果查窄，不能把服务端范围查宽。

```java
DataScope scope = DataScope.tenant("tenant_id", currentTenantId);

Flux<DynamicRow> rows = forms.select(QuerySpec.of(
        userForm,
        ConditionGroup.and().where("enabled", "=", true).build())
        .withScope(scope));

Mono<Long> updated = forms.update(WriteSpec.update(
        userForm,
        Map.of("name", "Alice 2"),
        ConditionGroup.and().where("id", "=", 1L).build())
        .withScope(scope));
```

也可以把复杂业务权限提前编译成条件：

```java
DataScope scope = DataScope.where(
        ConditionGroup.and()
                .where("org_id", "in", allowedOrgIds)
                .build()
);
```

常见数据范围可以直接用预设，读起来更像业务代码：

```java
// 无租户系统：只收组织范围。
DataScope orgScope = DataScope.orgOnly("org_id", currentOrgId);
DataScope treeScope = DataScope.orgAndChildren("org_id", currentOrgId);
DataScope selfScope = DataScope.self("creator_id", currentUserId);

// SaaS 系统：租户范围和数据范围继续 AND，DataScope.all() 不会清掉租户。
DataScope tenantData = DataScope.tenant("tenant_id", currentTenantId)
                                .and(DataScope.orgAndChildren("org_id", currentOrgId));

DataScope tenantAll = DataScope.tenant("tenant_id", currentTenantId)
                               .and(DataScope.all());
```

时间范围也由上层先算成明确边界，再和租户、组织、字段范围继续 AND：

```java
LocalDateTime start = LocalDateTime.of(2026, 7, 1, 0, 0);
LocalDateTime end = LocalDateTime.of(2026, 8, 1, 0, 0);

DataScope scope = DataScope.tenant("tenant_id", currentTenantId)
        .and(DataScope.time(TimeScope.between("created_at", start, end)))
        .withFields(FieldScope.readable("id", "name", "created_at"));
```

Repository 接收 scope 时不会自己拆条件，而是把完整对象交给 FormClient。这样租户可信值、字段裁剪、写字段保护和时间范围不会因为实体映射而丢失：

```java
Flux<UserEntity> rows = repository.select(where, scope);
Mono<Long> updated = repository.update(entity, where, scope);
Mono<Long> removed = repository.physicalDelete(where, scope);
```

设备归属、组织树、用户分享、告警可见时间等只是上层权限计算的例子，不是 ORM 内置模型。上层把最终结果转换成普通 `DataScope.where(...)`、`DataScope.time(...)`、`FieldScope` 或已注册的业务 term 即可，flying-orm 不认识具体业务表。

`between(...)` 是 `[start, end)`，`closed(...)` 是 `[start, end]`，`from(...)` 和 `before(...)` 是单边窗口。flying-orm 不猜系统时区，也不计算“今天”“最近七天”；上层负责把业务时间算清楚，所有边界值仍然走 SQL 参数绑定。常用时间字段应按实际查询方式建立数据库索引。

`orgAndChildren(...)` 只生成 `org-and-children` 业务 term。组织树可能来自闭包表、路径字段、缓存或上层权限结果，这些不该由 ORM 内核猜；应用只要给 SQL renderer 注册对应 term handler，最终仍然是占位符参数，不拼接前端 SQL。

`DataScope` 是服务端参数，不应该从前端透传。它会和逻辑删除一起生效，比如最终 SQL 可能是 `where id = ? and tenant_id = ? and deleted = ?`。

动态表单需要租户隔离时，在表单定义上声明一次即可；不声明就是 `NONE`，适合无租户系统和公共表。`AUTO` 会从服务端 `DataScope.tenant(...)` 自动补值，`MANUAL` 则要求调用方明确传值并接受一致性校验：

```java
DynamicForm userForm = DynamicForm.builder("user", "users")
        .addField(DynamicField.primaryKey("id", "BIGINT"))
        .addField(DynamicField.of("tenant_id", "BIGINT"))
        .addField(DynamicField.of("name", "VARCHAR"))
        .tenant("tenant_id", TenantStrategy.AUTO)
        .build();
```

如果一个服务里大部分操作都必须带同一套租户/数据权限，可以在客户端创建好以后挂默认 scope。后续没有显式传 scope 的查询、分页、更新、删除都会自动带上；显式传 scope 时也只是继续收窄，不会覆盖默认兜底范围：

```java
DataScope defaultScope = DataScope.tenant("tenant_id", currentTenantId);

ReactiveFormClient scopedForms = forms.withDefaultDataScope(defaultScope);
DatabaseOperator scopedOperator = operator.withDefaultDataScope(defaultScope);
FlyingOrmClients scopedClients = clients.withDefaultDataScope(defaultScope);

Flux<DynamicRow> rows = scopedForms.select(
        userForm,
        ConditionGroup.and().where("status", "=", "enabled").build()
);

Mono<Long> updated = scopedOperator.dml()
        .update("Users")
        .set("name", "Alice")
        .where(where -> where.is("id", userId))
        .execute();
```

当前用户、租户和组织范围由上层解析，再把不可变的 `DataScope` 快照交给 ORM。不要在 SQL 渲染过程中查库或调用远程接口，否则会直接放大每次 ORM 调用的延迟和失败面。

字段范围也走同一个 `DataScope`。读字段白名单会裁剪 `select/page` 的列，写字段白名单会在 SQL 渲染前拦住 `insert/update/batch` 里的越权字段：

```java
DataScope fieldScope = DataScope.none()
        .withFields(new FieldScope(
                Set.of("id", "name", "tenant_id"), // 能读这些列
                Set.of("name")                      // 只能改业务名称
        ));

ReactiveFormClient protectedForms = forms.withDefaultDataScope(fieldScope);

// 实际 SQL 只会 select id, name, tenant_id
Flux<DynamicRow> visibleRows = protectedForms.select(
        userForm,
        ConditionGroup.and().where("id", "=", userId).build()
);

// OK
Mono<Long> rename = protectedForms.update(
        userForm,
        Map.of("name", "Alice"),
        ConditionGroup.and().where("id", "=", userId).build()
);

// 会在渲染 SQL 前失败，不会把 password 带进 SQL。
protectedForms.update(
        userForm,
        Map.of("password", "secret"),
        ConditionGroup.and().where("id", "=", userId).build()
);
```

Scope 拒绝不是只能看 message。上层可以直接按稳定错误码处理，同时旧代码继续按 `IllegalArgumentException` 捕获也有效：

```java
try {
    protectedForms.insert(userForm, values);
} catch (ScopeAccessException error) {
    if (error.code() == ScopeErrorCode.TENANT_SCOPE_REQUIRED) {
        // 交给上层业务决定返回未登录、租户上下文缺失或配置错误。
    }
}
```

`ScopeAccessException` 还会带 `formId()` 和 `field()`；租户值冲突、租户字段缺失、字段不可写等情况不需要解析异常文本。

## 可扩展业务条件

业务条件不局限于 `=`、`>`、`like`。例如数据权限：

```java
ConditionGroup where = renderer.conditions()
        .where("userId", "user-in-org", List.of(100L, 200L))
        .build();
```

`user-in-org` 这样的 term 会保留业务语义，到 SQL 渲染阶段才展开。renderer 同时提供它的值形状，
Java DSL、参数条件包和前端结构化条件不需要再重复声明“单值还是集合”；业务代码也不需要手写复杂 SQL。

## Operator 门面

如果业务侧喜欢链式 operator 风格，可以用 `DatabaseOperator`。它只是易用门面，底层仍然是动态表单、共享渲染规则和
对应的 JDBC/R2DBC 执行器。

```java
Mono<Long> created = operator.ddl()
        .createOrAlter("test_table")
        .addColumn().name("id").number(32).primaryKey().comment("ID").commit()
        .addColumn().name("name").varchar(128).comment("名称").commit()
        .addColumn().name("user_id").number(19).commit()
        .addIndex("idx_test_table_name").column("name").commit()
        .addForeignKey("fk_test_table_user")
        .column("user_id")
        .referenceTable("users")
        .referenceColumn("id")
        .commit()
        .commit();

Flux<DynamicRow> rows = operator.dml()
        .query()
        .select("id")
        .from("test_table")
        .where(dsl -> dsl.is("name", "张三"))
        .scope(DataScope.tenant("tenant_id", currentTenantId))
        .logicDelete("deleted", 0, 1)
        .fetchMap();

Mono<Long> updated = operator.dml()
        .update("test_table")
        .set("name", "李四")
        .where(dsl -> dsl.is("id", 1L))
        .scope(DataScope.tenant("tenant_id", currentTenantId))
        .logicDelete("deleted", 0, 1)
        .optimisticLock(OptimisticLockOptions.increment("version", 3))
        .execute();

Mono<Long> deleted = operator.dml()
        .delete("test_table")
        .where(dsl -> dsl.is("id", 1L))
        .scope(DataScope.tenant("tenant_id", currentTenantId))
        .logicDelete("deleted", 0, 1)
        .optimisticLock(OptimisticLockOptions.increment("version", 4))
        .execute();

Mono<Long> physicalDeleted = operator.dml()
        .delete("test_table")
        .where(dsl -> dsl.is("id", 1L))
        .logicDelete("deleted", 0, 1)
        .physical()
        .execute();

Mono<DynamicForm> currentForm = operator.metadata()
        .readForm("testForm", "PUBLIC", "TEST_TABLE");
```

DML operator 的 query/update/delete 是轻门面：表名、字段名会先做标识符校验，值仍然全部走参数绑定；乐观锁直接复用 `OptimisticLockOptions`，数字版本用 `increment(...)`，时间版本或手工新版本用 `assign(...)`。没有完整 `DynamicForm` 时，operator 不会猜业务语义，需要逻辑删除就显式 `.logicDelete(...)`。同步门面同样可用：

```java
long rows = clients.syncOperator()
        .dml()
        .update("test_table")
        .set("name", "李四")
        .where(dsl -> dsl.is("id", 1L))
        .logicDelete("deleted", 0, 1)
        .optimisticLock(OptimisticLockOptions.increment("version", 3))
        .execute();
```

元数据、SQL 结构计划和实体映射默认使用安全有界策略；通过 `operator.ddl().createOrAlter(...).commit()` 改表后会自动清对应缓存：

```java
FlyingOrmClients cachedClients = FlyingOrmClients.builder(connectionFactory)
        .configuredDialect("mysql")
        .renderer(SqlRenderer.builder().addDefaultTerms().build())
        .cachePolicy(OrmCachePolicy.builder()
                .metadata(new CacheRegionPolicy(true, 2048, 256, Duration.ofMinutes(5), true))
                .build())
        .build();
DatabaseOperator cachedOperator = cachedClients.operator();

Mono<DynamicForm> cachedForm = cachedOperator.metadata()
        .readForm("testForm", "test_table");

Mono<Long> altered = cachedOperator.ddl()
        .createOrAlter("test_table")
        .addColumn().name("nickname").varchar(64).commit()
        .commit();
```

`maximumWeight` 是稳定逻辑权重而不是条目数，并可限制单项最大权重。需要接监控时，
可以通过稳定的缓存能力接口读取框架无关的快照；主项目不绑定具体指标框架：

```java
ReactiveFormMetadataCache cachedMetadata =
        ReactiveFormMetadataReaders.cached(
                rawMetadata,
                new CacheRegionPolicy(true, 2048, 256, Duration.ofMinutes(5), true),
                MetadataCacheInvalidator.none());

MetadataCacheSnapshot snapshot = cachedMetadata.snapshot();
long entries = snapshot.combined().entries();
double hitRate = snapshot.combined().hitRate();
long failedLoads = snapshot.combined().loadFailureCount();
```

`snapshot()` 统计的是底层响应式元数据查询真正完成后的成功或失败，不会把“Caffeine 成功放入一个 Mono”误算成
查库成功。外部监控集成可以定时读取这些基础值，再交给自己的指标系统；两边的 Caffeine 实例和生命周期互不接管。

如果表结构是被其他服务、运维脚本或数据库控制台改掉的，flying-orm 不可能知道这件事，这时再手动失效：

```java
ReactiveFormMetadataReader metadata =
        ReactiveFormMetadataReaders.cached(rawMetadata,
                CacheRegionPolicy.metadataDefaults(),
                MetadataCacheInvalidator.none());

((MetadataCacheInvalidator) metadata).invalidate("test_table");

// 批量改了很多动态表结构时，清空比逐个算表名更省心。
((MetadataCacheInvalidator) metadata).invalidateAll();
```

`createOrAlter` 默认是安全策略：只自动建表、补缺失字段、补注释、创建缺失索引。删字段、改主键、改字段类型、重建索引等高风险动作会进入计划结果，让上层先审核。

```java
Mono<SchemaMigrationPlan> plan = operator.ddl()
        .createOrAlter("test_table")
        .addColumn().name("id").number(32).primaryKey().comment("ID").commit()
        .addColumn().name("name").varchar(128).comment("名称").commit()
        .plan();

Mono<SchemaMigrationResult> result = operator.ddl()
        .createOrAlter("test_table")
        .addColumn().name("id").number(32).primaryKey().comment("ID").commit()
        .addColumn().name("name").varchar(128).comment("名称").commit()
        .commitDetailed();
```

## 执行观测

执行层可以接一个轻量 observer。默认不做事，不引入日志框架，也不记录参数值：

```java
ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(connectionFactory, observation -> {
    SqlExecutionOperation operation = observation.operation();
    SqlStatementType statementType = observation.statementType();
    SqlExecutionStatus status = observation.status();
    SqlFailureCategory failureCategory = observation.failureCategory();
    long rows = observation.rows();
    long costNanos = observation.durationNanos();
});
```

常用组合可以直接用工具方法，上层想接日志、指标、链路追踪时各取所需：

```java
SqlExecutionObserver observer = SqlExecutionObservers.composite(
        SqlExecutionObservers.slow(Duration.ofMillis(500), slowSqlLog::record),
        SqlExecutionObservers.errors(errorLog::record),
        SqlExecutionObservers.sample(0.05D, metrics::record)
);

ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(connectionFactory, observer);
```

分片批量写入可以单独接细粒度观测。它会发出 `CHUNK`、`SUMMARY`、`RECOVERY` 三类事件，方便定位大批量导入时哪个分片失败、哪个分片 UNKNOWN、后续恢复查询是什么结果：

```java
ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(
        connectionFactory,
        sqlObserver,
        batchObservation -> {
            if (batchObservation.eventType() == BatchExecutionEventType.CHUNK) {
                int chunkIndex = batchObservation.chunkIndex();
                BatchChunkResult.Status status = batchObservation.chunkStatus();
                long inputCount = batchObservation.inputCount();
                long affectedRows = batchObservation.affectedRows();
            }
            if (batchObservation.eventType() == BatchExecutionEventType.RECOVERY) {
                BatchResolution.Status status = batchObservation.recoveryStatus();
                BatchChunkResult.RecoveryToken token = batchObservation.recoveryToken();
            }
        }
);
```

自定义 `ReactiveSqlExecutor` 也可以包一层：

```java
ReactiveSqlExecutor observed = executor.withObserver(observation -> {
    // 接日志、指标或链路追踪都放在上层做。
});

ReactiveSqlExecutor batchObserved = executor.withBatchObserver(batchObservation -> {
    // 这里看批量分片、汇总和 UNKNOWN 恢复。
});
```

观测结果里有 SQL 类型、执行状态、耗时、行数、参数个数、批量大小和异常分类。SQL 文本会保留，参数值不会进入观测对象，避免日志里泄漏业务数据。

统一启动入口还可以直接安装安全日志，并按需筛选字段和批量事件。慢阈值只省略成功的快 SQL，失败、取消和
`UNKNOWN` 不会被过滤：

```java
SqlExecutionLogSelection selection = new SqlExecutionLogSelection(
        true,  // 写入影响行数
        true,  // 查询返回行数
        true,  // 执行耗时
        Duration.ofMillis(500).toNanos(),
        false, // 不逐个记录 CHUNK
        true,  // 保留 SUMMARY
        true   // 保留 RECOVERY
);

FlyingOrmConfiguration configuration = FlyingOrmConfiguration.defaults()
        .withSqlLog(FlyingOrmConfiguration.SqlLog.enabled(
                SqlExecutionLogOptions.defaults(), selection));
```

日志出口由 `FlyingOrmEnvironment.withSqlLogSink(...)` 提供。完整 SQL 和参数默认关闭；即使开启，脱敏和长度硬限制
仍然生效，日志出口异常也不会反向改变数据库结果。

## 执行保护

普通查询和更新可以显式传 `SqlExecutionOptions`，用来限制执行时间、查询返回行数和结果总内存：

```java
SqlExecutionOptions options = SqlExecutionOptions.unlimited()
        .withConnectionAcquireTimeout(Duration.ofMillis(500))
        .withTimeout(Duration.ofSeconds(3))
        .withMaxRows(1000)
        .withMaxResultBytes(64L * 1024 * 1024);

Flux<DynamicRow> rows = executor.query(
        new SqlRequest("select id, name from Users where enabled = ?", List.of(true)),
        options
);

Mono<Long> updated = executor.rowsUpdated(
        new SqlRequest("update Users set enabled = ? where org_id = ?", List.of(false, "org-1")),
        SqlExecutionOptions.timeout(Duration.ofSeconds(2))
);
```

`maxRows` 限制查询返回行数，超过后会抛出 `SqlRowLimitExceededException`，观测分类是 `ROW_LIMIT`。`maxResultBytes` 限制一次订阅累计返回的估算字节数，超过后会抛出 `SqlResultMemoryLimitExceededException`，观测分类是 `RESULT_MEMORY_LIMIT`。动态表单的 BLOB/CLOB 等字段在解码后还会再检查一次，避免驱动对象很小、实际 `byte[]` 或 `String` 很大时绕过限制。字节数是稳定、偏保守的保护估算，不是 JVM 对象布局的精确测量。

`timeout` 会取消上游响应式订阅并抛出 `SqlExecutionTimeoutException`，观测分类是 `TIMEOUT`。`safeDefaults()` 默认最多返回 100000 行、累计估算 64 MiB；只有明确接受无限结果风险时才使用 `unlimited()`。

R2DBC 的 `connectionAcquireTimeout` 单独限制等待连接池的时间。超时会抛出
`RdbConnectionAcquireTimeoutException`，错误种类和观测分类都是 `CONNECTION`。批量写入使用
`BatchWriteOptions.withConnectionAcquireTimeout(...)`。JDBC 标准没有可靠的单次取连接超时入口，原生 JDBC
必须在 DataSource/连接池上配置获取超时，ORM 不会为每次同步调用偷偷套线程池。

真实环境做轻量并发检查时，可以在 testkit 中复用 `ReactiveConcurrencyProbe`。它只订阅调用方给出的响应式操作，不创建线程池、不包装数据源：

```java
ReactiveConcurrencyProbe.Plan plan = new ReactiveConcurrencyProbe.Plan(
        1000,
        32,
        Duration.ofSeconds(30)
);

Mono<ReactiveConcurrencyProbe.Result> probe = ReactiveConcurrencyProbe.run(
        plan,
        index -> executor.query(request, options).then()
);
```

`Result` 会给出完成、失败、取消、峰值并发、耗时和按异常类汇总的失败数量。它用于确认资源边界，不代替 JMH 或正式数据库压测工具。

不启动数据库也能先验证上层的重试、降级和 UNKNOWN 处理。testkit 的故障规则按订阅序号触发，没命中规则的调用继续走原执行器：

```java
ReactiveSqlFaultInjector faultExecutor = ReactiveSqlFaultInjector.builder(executor)
        .fail(ReactiveSqlFaultInjector.Operation.UPDATE, 2, RdbFaults.deadlock())
        .hang(ReactiveSqlFaultInjector.Operation.QUERY, 1)
        .returnBatch(1, unknownBatchResult)
        .returnRecovery(1, BatchResolution.committed(recoveryToken))
        .build();
```

`hang` 不占用额外线程，它返回一个等待取消的响应式流，可直接配合 `SqlExecutionOptions.timeout(...)` 检查超时后的取消和资源释放。`returnBatch` 直接返回脚本结果，不会误调用底层写入。真实驱动的错误码、事务结果和连接释放仍在 V2 最终阶段逐库认证。

观测对象提供 `resultKind()`，上层不用自己拼 `status` 和 `failureCategory`：

```java
ReactiveSqlExecutor observed = executor.withObservers(
        observation -> {
            switch (observation.resultKind()) {
                case TIMEOUT -> slowQueryCounter.increment();
                case ROW_LIMIT, RESULT_MEMORY_LIMIT -> largeResultCounter.increment();
                case CANCELLED -> cancelledCounter.increment();
                default -> {
                    // 其他结果按普通 SQL 观测处理。
                }
            }
        },
        batch -> {
            switch (batch.resultKind()) {
                case PARTIAL -> batchPartialCounter.increment();
                case UNKNOWN -> batchUnknownCounter.increment();
                case ROLLED_BACK -> batchRollbackCounter.increment();
                default -> {
                    // 已确认提交或普通失败走默认处理。
                }
            }
        }
);
```

默认保护可以在创建入口统一设置，后续没有显式传 options 的查询、分页和更新都会用这份默认值：

```java
SqlExecutionOptions defaults = SqlExecutionOptions.maxRows(1000)
        .withTimeout(Duration.ofSeconds(3));

FlyingOrmClients clients = FlyingOrmClients.builder(connectionFactory)
        .configuredDialect("mysql")
        .renderer(SqlRenderer.builder().addDefaultTerms().build())
        .executionOptions(defaults)
        .build();

DatabaseOperator operator = clients.operator();
```

动态表单客户端也能直接传同一组选项，不需要绕到底层 SQL executor：

```java
Flux<DynamicRow> formRows = forms.select(QuerySpec.of(
        userForm,
        ConditionGroup.and().where("enabled", "=", true).build())
        .withExecutionOptions(options));

Mono<PageResult<DynamicRow>> formPage = forms.page(
        QuerySpec.of(userForm,
                ConditionGroup.and().where("name", "like", "王%").build())
                .withExecutionOptions(options),
        PageQuery.of(1, 20)
);

Mono<Long> deleted = forms.delete(WriteSpec.delete(
        userForm,
        ConditionGroup.and().where("id", "=", "u1").build())
        .withExecutionOptions(SqlExecutionOptions.timeout(Duration.ofSeconds(2))));
```

Repository 和链式 operator 也能直接使用默认保护，显式传 options 时仍然以显式值为准：

```java
ReactiveFormRepository<UserRow> users = ReactiveFormRepository.create(
        forms,
        userForm,
        UserRow.class
);

Flux<UserRow> safeUsers = users.select(
        ConditionGroup.and().where("enabled", "=", true).build()
);

Flux<DynamicRow> operatorRows = operator.dml()
        .query()
        .select("id", "name")
        .from("Users")
        .where(where -> where.is("enabled", true))
        .fetchMap();
```

分片批量写入继续用 `BatchWriteOptions` 里的保护项：

```java
BatchWriteOptions options = BatchWriteOptions.independent(500, 4)
        .withMaxRows(100_000)
        .withTimeout(Duration.ofMinutes(2));
```

上面的 Builder 可以给 Publisher 结构化批量、同步门面、Repository 和 DML operator 的无 options 入口
统一设置默认值。默认仍然是 `ATOMIC`；业务明确接受部分成功时，才在启动配置或单次调用中显式使用
`INDEPENDENT`。单次 options 始终优先。动态表单的 List 和 Publisher 批量入口都走
`BatchWriteRequest + BatchWriteOptions`，返回相同的 `BatchWriteResult`；List 只是更方便的输入形式，
同步 List 入口会先校验空行，再在方法返回前流式消费，不会复制整份引用数组；调用期间不要并发修改集合。
它不会绕开分片、事务、观测和输入行数保护。

## 同步 JDBC

传统同步代码使用 `SyncFormClient`、`SyncSqlExecutor` 或 `clients.syncOperator()`。这些入口直接使用
`DataSource` 和 JDBC，不创建 Reactor Publisher，也不等待 R2DBC。

```java
List<DynamicRow> rows = syncForms.select(
        userForm,
        ConditionGroup.and().where("enabled", "=", true).build()
);

List<DynamicRow> protectedRows = syncSql.query(
        new SqlRequest("select id, name from Users", List.of()),
        SqlExecutionOptions.maxRows(1000).withTimeout(Duration.ofSeconds(3))
);

long created = syncOperator.ddl()
        .createOrAlter("test_table")
        .addColumn().name("id").number(32).primaryKey().comment("ID").commit()
        .addColumn().name("name").varchar(128).comment("名称").commit()
        .commit();
```

同步 JDBC 同样遵守 `SqlExecutionOptions` 的 timeout、fetch size、最大行数、结果内存和取消保护。
为了避免阻塞事件循环，Reactor 非阻塞线程上会拒绝进入同步 JDBC API；普通工作线程和虚拟线程可以正常使用。
响应式业务改用 `ReactiveFormClient`、`ReactiveSqlExecutor` 和 `clients.operator()`，两种执行方式不在运行时偷偷切换。

## 分页

```java
Mono<PageResult<DynamicRow>> page = forms.page(
        QuerySpec.of(userForm,
                ConditionGroup.and().where("enabled", "=", true).build()),
        PageQuery.of(1, 20, PageSort.asc("id"))
);
```

分页 SQL 由 `RdbDialect` 负责，业务代码不用判断数据库类型。

## 批量写入

默认 `ATOMIC`，整批原子：

```java
Mono<BatchWriteResult> result = forms.writeBatch(
        BatchSpec.insert(userForm, Flux.fromIterable(rows))
                .withOptions(BatchWriteOptions.defaults())
);
```

显式 `INDEPENDENT`，逐分片提交并返回每片结果：

```java
Flux<BatchChunkResult> chunks = forms.writeBatchChunks(
        BatchSpec.insert(userForm, Flux.fromIterable(rows))
                .withOptions(BatchWriteOptions.independent(500))
);
```

上层可以根据 `BatchWriteResult` / `BatchChunkResult` 判断：

- `COMMITTED`
- `ROLLED_BACK`
- `PARTIAL`
- `FAILED`
- `CONFLICTED`
- `UNKNOWN`

R2DBC 开启回执模式后，默认会在提交响应丢失时主动确认最多 3 秒；数据库中已有匹配回执时直接返回
`COMMITTED`，避免上层对已经提交的数据重复写入：

```java
BatchWriteOptions autoConfirm = BatchWriteOptions.atomic(500)
        .withReceipt("user-import-20260814");
```

需要由上层统一调度恢复时，显式把确认时限设为零。此时提交结果无法确认仍返回 `UNKNOWN` 和恢复令牌；上层先用
`resolveUnknown(token)` 查询，再以同一 operation ID 和同一请求幂等重放，不能生成新的 operation ID 猜测执行结果：

```java
BatchWriteOptions manualRecovery = BatchWriteOptions.atomic(500)
        .withReceipt("user-import-20260814", Duration.ZERO);

Mono<BatchResolution> resolution = executor.resolveUnknown(unknownChunk.recoveryToken());
```

JDBC 返回相同的未知状态，但当前使用业务唯一键或上层幂等记录确认，不生成无法查询的令牌。

底层批量更新需要逐行确认乐观锁时，把请求声明为 `EXACTLY_ONE`。普通 insert/upsert 不要开启，
它们会继续使用驱动原生批处理；`EXACTLY_ONE` 会在同一连接和事务内逐行确认影响行数，确保返回的
`inputOffset` 在不同 R2DBC 驱动上都可信：

```java
BatchWriteRequest request = new BatchWriteRequest(
        "update users set name = ?, version = version + 1 where id = ? and version = ?",
        3,
        List.of(String.class, String.class, Long.class),
        SqlBindMarkerStyle.CANONICAL,
        parameterRows,
        BatchWriteOptions.defaults(),
        BatchRowCountPolicy.EXACTLY_ONE
);

executor.writeBatch(request)
        .doOnNext(result -> result.conflicts().forEach(conflict ->
                log.warn("conflict input offset: {}", conflict.inputOffset())));
```

默认 `ATOMIC` 下，任意冲突会让整批回滚并通过 `BatchWriteException.result()` 返回明细；
`INDEPENDENT` 下，只回滚包含冲突的分片，整体结果为 `PARTIAL`。提交响应丢失时仍使用原来的
`UNKNOWN` 和恢复令牌协议，不会把未知状态猜成乐观锁冲突。

## Upsert

```java
Mono<BatchWriteResult> result = forms.upsertBatch(
        userForm,
        Flux.fromIterable(rows),
        BatchWriteOptions.defaults()
);
```

当前已有 H2、MySQL、PostgreSQL、Oracle、SQL Server 的首版 upsert SQL 渲染。

## 类型化查询

```java
record UserRow(Long id, String name, Integer age, Boolean enabled) {
}

Flux<UserRow> users = forms.select(QuerySpec.of(
        userForm,
        ConditionGroup.and().where("enabled", "=", true).build()),
        UserRow.class);

Mono<PageResult<UserRow>> page = forms.page(
        QuerySpec.of(userForm, ConditionGroup.and().build()),
        PageQuery.of(1, 20),
        UserRow.class
);
```

列名会按大小写和下划线做宽松匹配，比如 `USER_ID` 可以映射到 `userId`。

## Repository

Repository 是很薄的一层，底层仍然是动态表单：

```java
ReactiveFormRepository<UserRow> users =
        ReactiveFormRepository.create(forms, userForm, UserRow.class);

Mono<Long> inserted = users.insert(new UserRow(1L, "Alice", 18, true));

Flux<UserRow> enabledUsers = users.select(
        ConditionGroup.and().where("enabled", "=", true).build()
);

Mono<BatchWriteResult> batch = users.upsertBatch(Flux.fromIterable(userRows));
```

它不是另一套 ORM 内核，只是把动态表单 Map 读写包装成实体读写。

如果实体类已经按约定写了表名和列名，Repository 会直接用这些元数据。实体映射使用
`com.flying.orm.core.annotation` 下的 flying-orm 自有注解：

```java
@TableName("Users")
record UserEntity(
        @TableId("uid") Long id,
        @TableField("user_name") String name,
        @Version Integer version,
        Boolean enabled
) {
}

ReactiveFormRepository<UserEntity> users =
        clients.repository(UserEntity.class);

Mono<Long> inserted = users.insert(new UserEntity(1L, "Alice", 1, true));

Flux<UserEntity> rows = users.select(
        ConditionGroup.and().where("user_name", "=", "Alice").build()
);
```

这段代码里 Java 属性叫 `name`，数据库列叫 `user_name`。写入时会按列名渲染 SQL，读库时也能把 `user_name` 写回 `name`，业务层不用再维护一份字段名转换表。

实体上有数字型 `@Version` 时，Repository 的普通更新会自动走乐观锁：用实体里的旧版本拼到 `where version = ?`，同时把 set 里的版本字段换成 `version = version + 1`。

```java
Mono<Long> updated = users.update(
        new UserEntity(1L, "Alice 2", 3, true),
        ConditionGroup.and().where("id", "=", 1L).build()
);

Mono<Long> deleted = users.delete(
        new UserEntity(1L, "Alice 2", 4, true),
        ConditionGroup.and().where("id", "=", 1L).build()
);
```

delete 如果只传 `where`，Repository 拿不到旧版本值，就不会自动加乐观锁；需要版本保护时传实体。时间版本、手工新版本这类场景用显式 `OptimisticLockOptions.assign(...)`，不要自动猜。

实体同时有 `@TableId` 和数字型 `@Version` 时，可以直接批量更新。主键不会进入 `set`，旧版本会逐行进入条件：

```java
Mono<BatchWriteResult> result = users.updateBatch(List.of(
        new UserEntity(1L, "Alice 3", 4, true),
        new UserEntity(2L, "Bob 2", 7, true)
));
```

默认是 `ATOMIC`，任何一行版本冲突都会回滚整批。需要让不同分片独立提交时，显式传
`BatchWriteOptions.independent(chunkSize)` 并使用 `updateBatchChunks(...)`，返回值会区分成功分片和冲突分片。
动态表单 Map 入口使用通用的 `BatchOptimisticUpdate(values, where, lock)`；这不是某类业务的特例，任意表单都能使用。
批量更新和普通更新一样，会自动叠加默认/显式 `DataScope`、租户范围、字段写权限和逻辑删除条件。

逻辑删除可以显式声明字段和值。标在字段上最简单；标在类上时用 `field` 指明哪个字段是删除标记：

```java
record UserEntity(
        @TableId("uid") Long id,
        @TableField("user_name") String name,
        @Version Integer version,
        @FlyingLogicDelete(notDeletedValue = "0", deletedValue = "1")
        Integer deleted
) {
}
```

```java
@FlyingLogicDelete(field = "removed", notDeletedValue = "N", deletedValue = "Y")
record UserEntity(Long id, String name, String removed) {
}
```

实体元数据生成 `DynamicForm` 时会带上逻辑删除配置。Repository 会把查询、分页、更新和删除都限制在未删除数据上。普通删除不会真正删行，而是更新删除标记：

```java
Mono<Long> deleted = users.delete(
        ConditionGroup.and().where("id", "=", 1L).build()
);
```

这类调用会渲染成类似 `update Users set deleted = ? where id = ? and deleted = ?` 的 SQL。确实要物理删除时要显式写出来：

```java
Mono<Long> removed = users.physicalDelete(
        ConditionGroup.and().where("id", "=", 1L).build()
);
```

## 注册复杂 SQL 查询

复杂联表、CTE、聚合、窗口函数和数据库专有查询不需要强塞进结构化 DML。SQL 在启动阶段注册一次，
业务代码按稳定 ID 调用；租户、用户等安全参数由可信提供器在每次订阅时读取，不能由普通 `bind(...)` 覆盖。

```java
SqlTemplateRegistry templates = SqlTemplateRegistry.builder()
        .register(SqlTemplate.query(
                "monthly-sales",
                """
                with totals as (
                    select department_id, sum(amount) total
                    from sales
                    where tenant_id = :tenantId and created_at >= :startTime
                    group by department_id
                )
                select department_id, total from totals order by total desc
                """,
                Set.of()),
                Set.of("tenantId"))
        .build();

FlyingOrmClients clients = FlyingOrmClients.builder(connectionFactory)
        .sqlTemplates(templates)
        .sqlTemplateParameterProvider((templateId, names) -> Mono.deferContextual(context ->
                Mono.just(Map.of("tenantId", context.get("tenantId")))))
        .build();

Flux<DynamicRow> rows = clients.operator()
        .sqlTemplate("monthly-sales")
        .bind("startTime", startTime)
        .options(SqlExecutionOptions.maxRows(10_000)
                .withTimeout(Duration.ofSeconds(5)))
        .query();
```

默认结果是紧凑只读 `DynamicRow`，也可以调用 `query(ReportRow.class)` 或传入自定义 `RowMapper`。
同步入口是 `clients.syncOperator().sqlTemplate("monthly-sales")`，它直接使用原生 JDBC，不创建 Reactor Publisher，
也不等待 R2DBC。响应式模板继续由 `clients.operator()` 执行，两条链共享模板编译和参数规则。

模板入口只允许查询。需要总数时单独注册统计模板；分页和游标参数直接写进可信模板。flying-orm 不解析、包装或
猜测任意复杂 SQL，避免破坏 CTE、窗口函数和数据库专有语法。临时后台 SQL 仍可使用
`unsafeNativeSql(...)`，但它不会自动追加租户、DataScope、逻辑删除或乐观锁条件。

## 异常处理

```java
forms.insert(WriteSpec.insert(userForm, values))
        .onErrorResume(RdbException.class, error -> {
            if (error.kind() == RdbErrorKind.DUPLICATE_KEY) {
                return Mono.empty();
            }
            return Mono.error(error);
        });
```

当前统一分类：

- `DUPLICATE_KEY`
- `CONSTRAINT`
- `BAD_SQL`
- `CONNECTION`
- `TIMEOUT`
- `DEADLOCK`
- `LOCK_TIMEOUT`
- `CANCELLED`
- `UNKNOWN`

批量写入的失败分片使用同一套分类，不需要再解析驱动消息：

```java
BatchWriteResult result = forms.writeBatch(
        BatchSpec.insert(form, rows)
                .withOptions(BatchWriteOptions.independent(500, 4)))
        .block();
for (BatchChunkResult chunk : result.chunks()) {
    if (chunk.failure() != null && chunk.failure().kind() == RdbErrorKind.DEADLOCK) {
        // 是否重试由上层结合业务幂等性决定。
    }
}
```

flying-orm 不会因为 `DEADLOCK`、`LOCK_TIMEOUT` 或 `CONNECTION` 就自动重试写入。尤其是 `UNKNOWN`，必须先用恢复令牌或业务唯一键确认，不能盲目重放。

## 真实库兼容测试入口

testkit 分别提供外部 R2DBC 与 JDBC 认证入口。默认没有对应 URL 就跳过；正式认证 Profile 会要求目标库参数完整：

```text
mvn -pl flying-orm-testkit -am -Pmysql-compat "-Dflying.orm.compat.mysql.url=r2dbc:mysql://user:password@localhost:3306/test?sslMode=REQUIRED" test

mvn -pl flying-orm-testkit -am -Ppostgresql-compat -Dflying.orm.compat.postgresql.url=r2dbc:postgresql://user:password@localhost:5432/test test

mvn -pl flying-orm-testkit -am -Poracle-compat -Dflying.orm.compat.oracle.url=r2dbc:oracle://user:password@localhost:1521/service test

mvn -pl flying-orm-testkit -am -Psqlserver-compat -Dflying.orm.compat.sqlserver.url=r2dbc:mssql://user:password@localhost:1433/test test
```

JDBC 使用同名数据库前缀下的 `.jdbc.url`、`.jdbc.user` 和 `.jdbc.password`，例如：

```text
mvn -pl flying-orm-testkit -am -Pmysql-compat \
  -Dflying.orm.compat.mysql.jdbc.url=jdbc:mysql://localhost:3306/test \
  -Dflying.orm.compat.mysql.jdbc.user=user \
  -Dflying.orm.compat.mysql.jdbc.password=password test
```

Oracle 和 SQL Server 当前入口覆盖基础建表、插入、批量 upsert、分页、删除和查询。没有配置 URL 时测试会跳过；只有在目标版本真实数据库执行成功后，才算该版本完成实库认证。

## 发布后增强 API

深分页优先使用复合游标，不再不断放大 offset：

```java
CursorPageResult<DynamicRow> page = forms.cursorPage(
        form,
        where,
        CursorPageQuery.after(100, cursor,
                              CursorSort.desc("createdAt"),
                              CursorSort.asc("id")))
        .block();
```

数据变更需要跨步骤补偿时，正向 SQL 和补偿 SQL 必须一起进入服务端计划：

```java
DataMigrationPlan plan = DataMigrationPlan.builder("normalize-state")
        .step("users",
              new SqlRequest("update users set state=? where state=?", List.of("ACTIVE", "1")),
              new SqlRequest("update users set state=? where state=?", List.of("1", "ACTIVE")))
        .build();

ReactiveDataMigration.create(executor).execute(plan);
```

缓存监控和驱动特殊值保持上层可选接入：

```java
MetadataCacheMetricsBridge.export(cachedReader.snapshot(), metrics::put);
ValueCodecRegistry codecs = ValueCodecRegistry.standard().withDriverAdapter(driverAdapter);
RowMapper<User> mapper = RowMapper.of(User.class, codecs)
        .withAliases(Map.of("display_name", "name"));
```

两个 profile 只给 testkit 加对应的测试驱动；日常构建不会下载外部数据库驱动，也不会自动拉起容器。
配置 URL 后会验证动态表单 CRUD、JSON Map/List 往返、PostgreSQL 原生数组写入/批量/读回/数组条件、BLOB/CLOB 的 insert、batch upsert、响应式读取与超限保护、元数据，以及批量乐观锁的提交与 ATOMIC 冲突回滚。H2 JSON 和 LOB 已在项目测试中直接连接真实 R2DBC 驱动；MySQL/PostgreSQL 没配置 URL 时相关用例会明确跳过。
