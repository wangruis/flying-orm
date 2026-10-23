# flying-orm 4.1.2 示例

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

### Java 运行时默认值

```java
DynamicForm tasks = DynamicForm.builder("tasks", "tasks")
    .addField(DynamicField.primaryKey("id", "VARCHAR(36)")
        .withRuntimeDefault(() -> UUID.randomUUID().toString()))
    .addField(DynamicField.of("title", "VARCHAR(128)"))
    .addField(DynamicField.of("status", "VARCHAR(16)").withRuntimeDefault(() -> "NEW"))
    .build();

Mono<Long> inserted = dml.insert(tasks, Map.of("title", "整理报表"));
```

表单插入、批量插入及 UPSERT 的插入阶段，在字段缺失或为 `null` 时按行生成；显式非空值优先，生成值仍走字段编码和参数绑定。响应式调用订阅时才求值，不修改输入 Map，也不会把生成值带入冲突更新。普通更新不触发；提供者返回 `null` 时保留原输入，抛出异常则原样传播。它不生成数据库 `DEFAULT`，不影响 Schema 差异；数据库默认值见[列默认值与数据库表达式](#列默认值与数据库表达式)。

## 常用查询

### 全部、等值、范围与集合

```java
Flux<DynamicRow> all = dml.query(users).fetch();

Mono<DynamicRow> byId = dml.query(users).where("id", 1L).one();
Flux<DynamicRow> active = dml.query(users).where("status", "ACTIVE").fetch();
Flux<DynamicRow> containsName = dml.query(users).where("name", "like", "%Alice%").fetch();
Flux<DynamicRow> ids = dml.query(users).where("id", "in", List.of(1L, 2L)).fetch();
Flux<DynamicRow> excluded = dml.query(users).where("id", "not-in", List.of(1L, 2L)).fetch();
Flux<DynamicRow> interval = dml.query(users).where("id", "between", List.of(10L, 20L)).fetch();
Flux<DynamicRow> greater = dml.query(users).where("id", ">", 10L).fetch();
```

`one()`：零行返回空 Mono，多行报错，不静默取第一条。`like` 值中的 `%`、`_` 保留 SQL 通配符语义，值仍通过绑定传入。

### 首条与计数

```java
Mono<DynamicRow> newest = dml.query(users)
    .where("status", "ACTIVE").orderByDesc("id").first();
Mono<Long> total = dml.query(users).where("status", "ACTIVE").count();
Mono<Long> organizations = dml.query(users).groupBy("org_id").count();

// 后端可信的物理表入口也支持首条和计数；没有元数据治理或自动逻辑删除。
Mono<DynamicRow> rawFirst = dml.query().from("users").orderByAsc("id").first();
Mono<Long> rawCount = dml.query().from("users").count();
```

`first()` 不查询总数，普通查询由 SQL 限制一行；未排序时由数据库决定首行。`first(Type.class)` 映射 DTO。`count()` 返回 long 计数，忽略投影与排序，保留条件和 Scope；`groupBy(...).count()` 统计组数，无需 `select`。元数据入口继续执行字段治理、逻辑删除和加密检索；CONTAINS 精确核验候选值后取首条或计数，不能分组计数。同步入口采用相同写法，首条无结果返回 null。

### AND、OR、空值与可选条件

```java
Flux<DynamicRow> rows = dml.query(users)
    .where(w -> w.is("status", "ACTIVE")
        .or(group -> group.is("name", "Alice").is("name", "Bob"))
        .and(group -> group.where("id", ">", 10L).where("id", "<", 100L))
        .isNotNull("org_id")
        .isNull("manager_id")
        .whereIfPresent("name", optionalName))
    .fetch();
```

`where(field, value)` 和 `where(callback)` 连续调用都按 AND 追加；需要替换业务条件时使用 `replaceWhere(callback)`，不会清除 Scope。可选值才使用 `IfPresent`；不要用可选条件代替更新/删除的必要筛选。严格 `IN` 不会静默丢弃非法空元素。

### 投影、排序、DTO 与单列结果

```java
public record UserView(Long id, String name) {}

Flux<UserView> views = dml.query(users)
    .select("id", "name")
    .orderByAsc("id")
    .fetch(UserView.class);

// 默认可读字段减去指定列；同步查询也使用 exclude(...)
Flux<DynamicRow> briefRows = dml.query(users)
    .exclude("name")
    .orderByAsc("id")
    .fetch();

Mono<UserView> one = dml.query(users)
    .select("id", "name")
    .where("id", 1L)
    .one(UserView.class);

Flux<Long> ids = dml.query(users)
    .select("id")
    .where("status", "ACTIVE")
    .fetch(row -> (Long) row.get("id"));

Flux<DynamicRow> organizations = dml.query(users)
    .select("org_id")
    .groupBy("org_id")
    .orderByAsc("org_id")
    .fetch();
```

DTO 的属性/record 组件应与投影匹配；`one(Type.class)` 无匹配时为空，多行时报错。单列可能为 NULL 时不要从 Reactor 映射器返回 null，应显式包装结果或先过滤。

`exclude(...)` 可排除字段或计算列别名，不改变条件、排序和计数，也不放宽字段权限。全部列被排除会报错。纯表名入口没有元数据，需先显式选列或配置有限可读字段，不能从 `*` 推断剩余列。

### 结果别名与组合 DTO

```java
record Summary(String name, Long count, BigDecimal amount) {}
record Position(Long position) {}
record RankedSummary(Summary summary, Position position) {}

RowMapper<RankedSummary> mapper = RowMapper.nested(RankedSummary.class)
    .withAliases(Map.of("department", "summary.name", "orders", "summary.count",
        "gross", "summary.amount", "rank", "position.position"));

Flux<RankedSummary> reports = reportQuery.query(mapper);
```

`reportQuery` 是已绑定参数的 `SqlTemplateOperator`，返回 `department/orders/gross/rank` 列。别名映射为“结果列名 → DTO 属性路径”；SQL 已返回 `summary.name` 等点分标签时，无需 `withAliases`。mapper 在查询外创建并复用，同步 `query(mapper)` 返回 `List`，普通查询也可使用 `fetch(mapper)`。

`nested` 支持多层 Bean/record，仅映射当前行，不触发关联查询、不合并多行集合。子对象已映射的列全部为 NULL 时返回 null；部分列为 NULL 时按真实 NULL 写入。需要按业务主键决定子对象存在性时，仍可使用自定义 `RowMapper`。普通 `RowMapper.of` 保持平铺映射规则。

不需要 DTO 时，使用同一个入口生成嵌套 Map：

```java
RowMapper<Map<String, Object>> nestedMap = RowMapper.nested()
    .withAliases(Map.of("department", "summary.name", "orders", "summary.count"));
Flux<Map<String, Object>> reports = reportQuery.query(nestedMap);
```

Map 保留列名大小写、顺序和 NULL，包括全 NULL 的子 Map；SQL 已返回点分标签时直接传 `RowMapper.nested()`。同一路径不能同时是标量和子对象，路径各段不能为空。路径按结果布局绑定一次，不逐行拆分列名；普通查询仍返回平铺 `DynamicRow`。

### 计算列

```java
public record SaleTotal(Long id, BigDecimal subtotal) {}

// sales 是声明 id、amount、quantity 字段的 DynamicForm。
ValueExpression subtotal = ValueExpression.field("amount")
    .multiply(ValueExpression.field("quantity").coalesce(ValueExpression.value(1)));

Mono<PageResult<SaleTotal>> page = dml.query(sales)
    .select("id").select(subtotal, "subtotal")
    .orderByDesc("subtotal").orderByAsc("id")
    .page(1, 20, SaleTotal.class);
```

同一选列支持 `fetch/one/first/page/keysetPage`，同步入口写法相同；`count()` 忽略计算列。参数绑定、不可信字段名校验及来源字段权限继续生效。直接使用规格时写 `query.withExpression("subtotal", subtotal)`；普通字段和计算别名可通过 `withProjection` 重新选择。

让指定值优先出现，不必选择或命名辅助列：

```java
Flux<DynamicRow> rows = dml.query(sales).select("id")
    .orderByFirst("quantity", 5, 3).orderByAsc("id").fetch();

Flux<User> users = clients.repository(User.class).createQuery()
    .orderByFirst(User::getStatus, Status.DISABLED, Status.ACTIVE)
    .orderByAsc(User::getId).fetch();
```

优先值沿用来源字段编码；实体入口还保留属性映射（含 `@EnumValue`）。同步入口相同；排序调用顺序就是优先级，不改变返回列。
指定值需要排在最后时用 `orderByLast("quantity", 5, 3)` 或 `orderByLast(User::getStatus, Status.DISABLED)`。
跨字段按首个匹配规则决定顺序，使用一个合并表达式；这不同于连续追加多个排序项：

```java
ValueExpression firstMatch = ValueExpression.firstMatch(
    ValueExpression.field("quantity").priority(5, 3),
    ValueExpression.field("id").priority(10L));
Flux<DynamicRow> matched = dml.query(sales).select("id")
    .orderBy(firstMatch, PageSort.Direction.ASC).orderByAsc("id").fetch();
```

未命中排在最后；改为 `lastMatch(...)` 则未命中排在前面，命中仍按声明顺序。实体表达式用 `e.priority(getter, values...)` 组成同样的分组，保留属性值转换。
需要混合“排前”和“排后”规则时，把 `lastMatch` 分组放入 `firstMatch`：

```java
ValueExpression mixed = ValueExpression.firstMatch(
    ValueExpression.lastMatch(ValueExpression.field("id").priority(1L)),
    ValueExpression.field("quantity").priority(3, 5));
```

此例 ID 为 1 的行排后，即使数量也命中后面的规则；其他行中数量为 3、5 的排前，其余居中。
规则始终按声明顺序取首个命中，内部排名不经过业务 codec；比较值仍按字段编码与参数绑定处理。
自定义表达式用动态查询的 `orderBy(expression, direction)`，实体查询用 `orderByExpression(e -> ..., direction)`；
其中 `e.field(getter)`、`e.value(getter, value)`、`e.priority(getter, values...)` 保留实体映射。
动态表达式排序需要 `DynamicForm` 元数据入口。直接使用 Form 规格可追加 `withOrderBy`：

```java
ValueExpression priority = ValueExpression.field("quantity").priority(5, 3);
QuerySpec ordered = QuerySpec.of(sales, ConditionGroup.and().build())
    .withProjection(List.of("id"), List.of())
    .withOrderBy(priority, PageSort.Direction.ASC);
Flux<DynamicRow> orderedRows = clients.forms().select(ordered);
```

这里数量为 5、3 的记录依次优先，其余在后，只返回 `id`。优先值走参数绑定，重复值以首次匹配为准，NULL 沿用 SQL 等值语义；支持 `first`、普通分页和加密 CONTAINS 条件，`count` 不执行排序。表达式排序不作为 cursor/keyset 游标字段。

```java
Mono<KeysetPageResult<SaleTotal>> firstBatch = dml.query(sales)
    .select("id").select(subtotal, "subtotal")
    .keysetPage(KeysetPageQuery.first(20, KeysetSort.asc("id", NullOrder.LAST)), SaleTotal.class);
```

游标按真实字段排序；后续页把完整 `nextPosition()` 传给 `KeysetPageQuery.after(...)`。计算别名不作为游标排序字段。

只需计算结果、不想返回排序字段时，使用 keyset：

```java
record Subtotal(BigDecimal subtotal) {}

Mono<KeysetPageResult<Subtotal>> batch = dml.query(sales)
    .select(subtotal, "subtotal")
    .keysetPage(KeysetPageQuery.first(20, KeysetSort.asc("quantity", NullOrder.FIRST)), Subtotal.class);
```

真实排序字段和主键由内部补选、解码并从结果行剥离；下一页回传完整 `nextPosition()`。计算别名不作为 keyset 排序字段，游标值仍遵循原字段权限与 codec。

实体 Repository 通过 Getter 构造表达式，自动解析列名：

```java
Flux<SaleTotal> totals = orderRepository.createQuery()
    .select(Order::getId)
    .selectExpression(e -> e.field(Order::getAmount)
        .multiply(e.field(Order::getQuantity).coalesce(ValueExpression.value(1))), "subtotal")
    .orderByDesc("subtotal")
    .fetch(SaleTotal.class);
```

投影使用 `fetch(Type.class)`、`one(Type.class)`、`first(Type.class)`、`page(page, size, Type.class)` 或 `executeRows()`，不触发实体 `onLoad`；无参实体终端不接受计算列，避免未映射列被静默丢弃。`copy/exclude/count` 沿用原语义。

计算列使用表单元数据入口；来源字段必须允许完整值读取，加密字段和需要脱敏的来源不能借表达式输出明文。普通查询排序可引用原表字段或已选计算别名，按别名排序需要全部来源字段的排序权限。计算列支持普通锁定读取及上述两种游标分页；锁定 keyset 仍遵循方言支持范围。

也可在加密 CONTAINS 条件下计算普通字段，支持查询、首条、offset 和 cursor 分页。例如 `secret` 是已声明 CONTAINS 检索的加密字段：

```java
QuerySpec query = QuerySpec.of(form, ConditionGroup.and()
        .add(ProtectedConditions.contains("secret", "关键字")).build())
        .withProjection(List.of("id"), List.of())
        .withExpression("total", ValueExpression.field("amount").add(ValueExpression.value(7)))
        .withSorts(List.of(PageSort.desc("total")));
Mono<PageResult<DynamicRow>> page = clients.forms().page(query, PageQuery.of(1, 20));
```

候选先解密并精确复核，再计算分页总数或截取结果；内部复核字段不会额外出现在返回投影中。Keyset 同样在核验后分页，支持 nullable 排序及未选中的排序字段；下一页位置仍包含这些排序值。表达式不能直接计算加密字段，CONTAINS 不支持分组和锁定读取。

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
    .fetch();

// 实体字段使用同一种条件写法。
Flux<User> entities = clients.repository(User.class).createQuery()
    .where(User::getId, "user-in-org", orgId)
    .fetch();
```

这里检索 `user_org` 中属于指定机构的用户。关联表和字段由服务端注册，`orgId` 是绑定参数；扩展处理器是可信后端代码，不能交给前端注册。

同一个关联行需要满足多个条件，或需要复合关联键时，用现有 Java 查询描述关联关系：

```java
DynamicForm items = DynamicForm.builder("items", "order_items")
    .addField(DynamicField.of("order_id", "BIGINT"))
    .addField(DynamicField.of("tenant_id", "BIGINT"))
    .addField(DynamicField.of("amount", "INTEGER"))
    .addField(DynamicField.of("status", "VARCHAR")).build();
FormDataSqlRenderer relatedSql = FormDataSqlRenderer.create(
    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms()
    .addTerm(relatedSql.relationExists("has-matching-item", minimum -> {
        JoinQuerySpec.Builder query = JoinQuerySpec.builder(items);
        JoinSource item = query.root();
        return query.selectAs(item, "order_id", "id")
            .selectAs(item, "tenant_id", "tenant_id")
            .where(item, ConditionGroup.and().where("amount", ">=", minimum)
                .add(ConditionGroup.or().where("status", "=", "A")
                    .where("status", "=", "B").build()).build())
            .build();
    })).build();
FlyingOrmClients clients = FlyingOrmClients.builder(access)
    .dialect(RdbDialect.postgresql()).renderer(renderer).build();
Flux<DynamicRow> matchedOrders = clients.operator().dml().query(orders)
    .where("id", "has-matching-item", 100).fetch();
```

`selectAs` 的别名对应外层表单字段：上例同时关联订单 `id` 和 `tenant_id`，金额与状态必须由同一条明细满足。子查询可继续使用 `join`、嵌套条件和源级 `scope`；不会因多条明细匹配而放大外层行数。`relationNotExists` 使用相同写法取反。

关联键使用未加密的普通字段；子查询 Scope 由服务端显式声明，不自动继承外层客户端默认 Scope。此入口是可信服务端扩展，不自动获得受治理外部查询的权限；内外渲染器使用相同方言。

### 按表或字段覆盖条件实现

同一操作符可按模型选择实现，未声明的模型保持默认行为：

```java
SqlTermHandler ignoreCase = SqlTermHandler.of("=", (term, context) ->
    new SqlFragment("lower(" + context.identifier(term.field()) + ") = lower(?)",
        List.of(context.parameter(term.value()))));

DynamicForm accounts = DynamicForm.builder("accounts", "accounts")
    .addField(DynamicField.primaryKey("id", "BIGINT"))
    .addField(DynamicField.of("name", "VARCHAR(128)")
        .withFeatures(FeatureRegistry.builder().add(ignoreCase).build()))
    .build();

Flux<DynamicRow> matched = dml.query(accounts).where("name", "ALICE").fetch();
```

字段配置优先于表配置；表通过 `DynamicForm.Builder.features(...)` 设置。`FeatureRegistry.builder().parent(schema.features())` 可继承 Schema 的共享配置，本层相同 ID 覆盖父层。处理器通过 `context.field(term.field())` 获取当前字段声明及扩展。

覆盖只改变已注册操作符的渲染实现，输入形状与外部条件白名单仍由操作符注册配置决定；新增操作符先在客户端 renderer 注册。字段级实现用于该字段的查、改、删条件，不改变 SET 编码或数据库结构。聚合 HAVING 继承表配置，字段上下文是聚合结果声明。

### JSON 路径条件

按数据库注册 JSON 条件包，前端与后端共用同一个结构化条件入口：

```java
SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms()
    .addTermPackage(JsonTermHandlers.postgresql()).build();
FlyingOrmClients clients = FlyingOrmClients.builder(access)
    .dialect(RdbDialect.postgresql()).renderer(renderer)
    .structuredConditionResolver(JsonStructuredConditions.standard()).build();
DynamicForm documents = DynamicForm.builder("documents", "documents")
    .addField(DynamicField.of("payload", "JSON")).build();

Flux<DynamicRow> rows = clients.operator().dml().query(documents)
    .filter(StructuredConditionInput.term("payload", "json-path-ge",
        Map.of("path", "profile.age", "value", 18)))
    .where("payload", "json-path-ne",
        JsonConditionValue.pathCompare(List.of("status"), "disabled"))
    .where("payload", "json-not-contains",
        JsonConditionValue.contains(Map.of("deleted", true)))
    .fetch();

Flux<DynamicRow> named = clients.operator().dml().query(documents)
    .filter(StructuredConditionInput.term("payload", "json-path-like",
        Map.of("path", "profile.name", "value", "张%")))
    .where("payload", "json-path-is-null",
        JsonConditionValue.pathNull(List.of("deletedAt")))
    .where("payload", "json-path-in",
        JsonConditionValue.pathIn(List.of("status"), List.of("active", "pending")))
    .fetch();

Flux<DynamicRow> firstItem = clients.operator().dml().query(documents)
    .filter(StructuredConditionInput.term("payload", "json-path-eq",
        Map.of("path", "items[0].name", "value", "张三")))
    .fetch();
```

MySQL 改用 `JsonTermHandlers.mysql()` 和 `RdbDialect.mysql()`。以下比较、文本、集合和包含条件适用于 MySQL / PostgreSQL。支持 `json-path-eq/ne/gt/ge/lt/le`；后端等值使用 `pathEquals`，其余比较使用 `pathCompare`。

文本匹配用 `json-path-like/not-like` 和 `pathLike(path, pattern)`；模式原样绑定，不自动补 `%`，大小写及转义遵循数据库 LIKE 规则。JSON null 不转换成字符串 `"null"` 参与匹配。空值用 `json-path-is-null/not-null` 和 `pathNull(path)`；前端直接传路径字符串或 `Map.of("path", "deletedAt")`。缺失路径、JSON null、整列 SQL NULL 均算空值，字符串 `"null"` 和空字符串不算。[MySQL JSON 类型规则](https://dev.mysql.com/doc/refman/8.4/en/json-attribute-functions.html)。

集合匹配用 `json-path-in/not-in` 和 `pathIn(path, values)`；前端传 `Map.of("path", "status", "value", List.of("active", "pending"))`。支持 Collection 与 Java 数组，不拆分逗号字符串；成员保持 JSON 类型，`null` 成员只匹配 JSON null，不匹配缺失路径。空 IN 为假，空 NOT IN 为真，不删除条件或跳过字段权限；集合大小和实际绑定数仍由调用方策略控制。

包含条件用 `json-contains`（字段包含参数）或 `json-contained-by`（字段被参数包含），否定为 `json-not-contains`、`json-not-contained-by`。前端 `filter` 直接传 JSON 对象/数组，后端 `where` 使用 `JsonConditionValue.contains(value)`。路径存在性用 `json-exists/not-exists` 搭配 `exists(path)`；数组路径成员匹配用 `json-array-contains/array-not-contains` 搭配 `arrayContains(path, value)`，该路径应存储 JSON 数组。

路径支持根值 `$`、点分隔属性名及从零开始的非负数组下标，如 `items[0].name`、`$[0]`。后端片段写作 `List.of("items", "[0]", "name")`，空列表表示根值。带点号、空格或中文的属性名用 JSON 双引号包裹，例如 `$."user.name"`、`$."显示名称"`；后端对应 `List.of("\"user.name\"")`，引号内按 JSON 字符串规则转义。不接受通配符、脚本或任意 JSONPath 表达式。路径和值均绑定。[MySQL 路径规则](https://dev.mysql.com/doc/refman/8.4/en/json.html)、[PostgreSQL 路径规则](https://www.postgresql.org/docs/current/functions-json.html)。

数字 `18` 不会转换成字符串 `"18"`；空字符串有效。比较保留数据库原生 JSON 类型、排序及 NULL 语义，建议同一路径保持同一种数据类型；不承诺混合类型、字符串排序及数值范围跨库一致。[PostgreSQL JSON 规则](https://www.postgresql.org/docs/current/functions-json.html)、[MySQL JSON 比较规则](https://dev.mysql.com/doc/refman/8.4/en/json.html#json-comparison)。

`json-not-exists` 判断路径不存在，不是判断 JSON `null`。当整列为 SQL NULL 时，PostgreSQL 的路径提取 `IS NULL` 会匹配，MySQL 的 `NOT JSON_CONTAINS_PATH` 不会匹配；按需另加列级空值条件。[MySQL 路径与包含规则](https://dev.mysql.com/doc/refman/8.4/en/json-search-functions.html)。

Oracle 默认采用 19c 基线，支持存在性、六种比较、IN/NOT IN、LIKE/NOT LIKE 和路径空值判断：

```java
SqlRenderer oracleRenderer = SqlRenderer.builder().addDefaultTerms()
    .addTermPackage(JsonTermHandlers.oracle()).build();
FlyingOrmClients oracleClients = FlyingOrmClients.builder(access)
    .dialect(RdbDialect.oracle()).renderer(oracleRenderer)
    .structuredConditionResolver(JsonStructuredConditions.standard()).build();
Flux<DynamicRow> withName = oracleClients.operator().dml().query(documents)
    .filter(StructuredConditionInput.term("payload", "json-exists", "profile.name"))
    .where("payload", "json-path-ge", JsonConditionValue.pathCompare(List.of("profile", "age"), 18))
    .where("payload", "json-path-in", JsonConditionValue.pathIn(List.of("status"), List.of("active", "")))
    .where("payload", "json-path-like", JsonConditionValue.pathLike(List.of("profile", "name"), "张%"))
    .where("payload", "json-path-is-null", JsonConditionValue.pathNull(List.of("deletedAt")))
    .fetch();
```

Oracle 要求路径是 SQL 字面量，由 ORM 校验路径段并安全引用，不接受原生 SQL。普通字符串、数字经 PASSING 绑定，比较按 JSON 类型区分 `18`、`"18"` 和布尔值；空字符串保持 JSON 空串，不绑定成 Oracle SQL NULL。IN 支持混合类型及 JSON null 成员；非空 NOT IN 不匹配缺失路径，空集合仍分别为假/真。JSON null 算路径存在，整列 SQL NULL 为 UNKNOWN；保留 Oracle 原生数组放宽匹配及非法 JSON 的 FALSE ON ERROR 行为。[Oracle 类型比较规则](https://docs.oracle.com/en/database/oracle/oracle-database/19/adjsn/json-path-expressions.html)。

LIKE 仅匹配 JSON 字符串，不经过有长度限制的标量文本提取；NOT LIKE 也要求路径中存在字符串值。模式遵循 Oracle JSON 的原生 `like` 规则：无转义字符，空模式匹配空串，非空模式不匹配空串。路径空值判断将缺失路径、JSON null、整列 SQL NULL 算作空值，不把对象、空字符串等当成 null；保留 Oracle 路径放宽匹配语义。

12c 使用 `JsonTermHandlers.oracle(OracleVersion.V12C)` 配合 `RdbDialect.oracle(OracleVersion.V12C)`，该包仅提供 `json-exists/not-exists`。

SQL Server 注册 `JsonTermHandlers.sqlServer()`，默认与 `RdbDialect.sqlServer()` 同为 2022。提供六种标量比较（`json-path-eq/ne/gt/ge/lt/le`）、`json-path-in/not-in`、`json-path-like/not-like`、`json-path-is-null/not-null` 和 `json-exists/not-exists`，条件写法与上例相同。存在性保留 JSON null 与缺失路径的区别，整列 SQL NULL 不匹配存在或不存在。

2016/2019 显式选择 `JsonTermHandlers.sqlServer(SqlServerVersion.V2016)` 或 `V2019`，配合同版本方言；同样提供上述算子，存在性通过 `OPENJSON` 成员判断实现，2022 使用原生 `JSON_PATH_EXISTS`。集合判断及旧版本存在性定位的末级成员名受 `OPENJSON` 原生 `key` 类型 `nvarchar(4000)` 约束，超出范围的键名不在这些实现的支持范围内。数据库兼容级别需至少为 130，输入列应保存有效 JSON。

标量比较中的文本和布尔值按 `nvarchar(max)` 文本及数据库排序规则比较，不使用 `JSON_VALUE` 的 4,000 字符返回类型。集合判断区分字符串、数字、布尔值和 JSON null；非空 IN/NOT IN 均不匹配缺失路径或整列 SQL NULL，空集合沿用前述规则。路径空值包含缺失、JSON null 和 SQL NULL，对象、数组和空字符串不算空值。[SQL Server OPENJSON](https://learn.microsoft.com/en-us/sql/t-sql/functions/openjson-transact-sql?view=sql-server-ver16)。

Oracle 19c+、SQL Server 2016+ 还支持对象条件简写：

```java
Flux<DynamicRow> matched = clients.operator().dml().query(documents)
    .filter(StructuredConditionInput.term("payload", "json-contains",
        Map.of("profile", Map.of("active", true, "level", 3))))
    .fetch();
```

这里把对象展开为标量路径条件后按 AND 组合，`json-not-contains` 对整个组合取反；整列 SQL NULL 均不匹配。键名是字面属性（`"a.b"` 不会拆成两级），null 叶子只匹配实际存在的 JSON null。输入必须是非空对象，叶子支持字符串、数字、布尔值和 null；不支持数组或空对象叶子，不忽略无法表达的条件。它不是 MySQL/PostgreSQL 的完整结构包含，且不提供 `json-contained-by`；类型、排序规则、数值投影和路径放宽仍遵循前述方言规则。

数值投影默认沿用 Schema 的 `DECIMAL(38,10)`，可在注册时指定类型及精度；`BigInteger` 无损转换成 `BigDecimal` 绑定。舍入、溢出遵循数据库规则；六种标量比较允许数据库数值字符串转换，集合判断则按 JSON 类型区分数字与数字字符串：

```java
SqlRenderer sqlServerRenderer = SqlRenderer.builder().addDefaultTerms()
    .addTermPackage(JsonTermHandlers.sqlServer(SqlServerVersion.V2022,
        com.flying.orm.core.type.DatabaseType.of("DECIMAL(38,18)")))
    .build();
```

### PostgreSQL 数组条件

在装配时注册数组条件及前端值适配器；元素类型取自模型，值始终绑定为数组参数：

```java
SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms()
    .addTermPackage(ArrayTermHandlers.postgresql()).build();
FlyingOrmClients clients = FlyingOrmClients.builder(access)
    .dialect(RdbDialect.postgresql()).renderer(renderer)
    .structuredConditionResolver(ArrayStructuredConditions.postgresql()).build();
DynamicForm events = DynamicForm.builder("events", "events")
    .addField(DynamicField.of("tags", "TEXT[]")).build();

Flux<DynamicRow> rows = clients.operator().dml().query(events)
    .filter(StructuredConditionInput.term("tags", ArrayStructuredConditions.NOT_OVERLAPS,
        List.of("archived", "disabled")))
    .fetch();
```

支持 `array-contains`、`array-contained-by`、`array-overlaps` 及对应的 `array-not-*`；单元素匹配使用 `array-any-eq`。后端 `where` 使用 `ArrayConditionValue.of(values, "TEXT[]")` 传入数组值。空数组是有效参数，不会删除条件；否定保留数据库 NULL 语义，不自动把 NULL 当作匹配。

### PostgreSQL 向量条件

数据库需已具备 pgvector，向量维度来自字段模型。注册现有条件包后，前后端均可使用结构化条件：

```java
SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms()
    .addTermPackage(VectorTermHandlers.postgresql()).build();
FlyingOrmClients clients = FlyingOrmClients.builder(access)
    .dialect(RdbDialect.postgresql()).renderer(renderer)
    .structuredConditionResolver(VectorStructuredConditions.postgresql()).build();
DynamicForm vectors = DynamicForm.builder("vectors", "vectors")
    .addField(DynamicField.of("embedding", "VECTOR").withLength(3)).build();

Flux<DynamicRow> rows = clients.operator().dml().query(vectors)
    .filter(StructuredConditionInput.term("embedding", "vector-l2-le",
        Map.of("vector", List.of(1F, 2F, 3F), "distance", 0.5)))
    .fetch();
```

`vector-l2-*`、`vector-cosine-*`、`vector-inner-product-*` 均支持 `eq/ne/lt/le/gt/ge`。前两种传 `distance`；内积传 `similarity`，按真实内积比较，可为负数。SQL 使用 pgvector 的负内积算子时由 ORM 反转比较及阈值，调用方无需手动取负。向量和阈值均绑定，不自动归一化、不替调用方选择阈值。

这些是过滤条件，不等同于最近邻 Top K，也不承诺索引命中；索引及查询形状遵循 [pgvector 查询规则](https://github.com/pgvector/pgvector#querying)。

### 枚举集合与位掩码

枚举数组可按集合存成 BIGINT；推荐显式稳定的单比特值，不依赖枚举声明顺序：

```java
enum Feature {
    READ(1L), EXPORT(8L);
    private final long bit;
    Feature(long bit) { this.bit = bit; }
    public long bit() { return bit; }
}
@TableName("feature_rows")
record FeatureRow(@TableId Long id, Feature[] features) { }

EnumMaskValueCodec<Feature> mask = EnumMaskValueCodec.of(Feature.class, Feature::bit);
EntitySchemaDescriptor<FeatureRow> schema = EntitySchemaDescriptor.builder(FeatureRow.class)
    .typeMappings(EntityTypeMappingRegistry.builder()
        .register("feature-mask", Feature[].class, DatabaseType.of("BIGINT"), mask).build())
    .build();
SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms()
    .addTermPackage(BitMaskTermHandlers.postgresql()).build();
FlyingOrmClients clients = FlyingOrmClients.builder(access)
    .dialect(RdbDialect.postgresql()).renderer(renderer).entitySchema(schema)
    .structuredConditionResolver(BitMaskTermHandlers.conditions()).build();

Mono<Long> saved = clients.repository(FeatureRow.class)
    .insert(new FeatureRow(1L, new Feature[]{Feature.READ, Feature.EXPORT}));
Flux<FeatureRow> rows = clients.repository(FeatureRow.class).createQuery()
    .where(FeatureRow::features, "mask-all", mask.mask(Feature.READ, Feature.EXPORT))
    .fetch();
```

`mask-any` 表示交集非空，`mask-all` 表示字段包含全部参数位，`mask-contained-by` 表示字段的位全部属于参数；否定为对应 `mask-not-*`。前端 `filter` 的值直接传整数掩码。按数据库选择 `postgresql()`、`mysql()`、`sqlServer()` 或 `oracle()` 条件包。

数组按集合处理：重复合并、空数组存零、null 保留 null，回读按枚举声明顺序。未知位、重复位配置或非整数掩码会报错，不静默丢数据。`ordinal(Feature.class)` 可按序号分配位，但使用后不可重排枚举；单枚举 `@EnumValue` 规则不受影响。查询不把 SQL NULL 当空集合，不因零掩码跳过字段权限。MySQL 位运算结果额外转回有符号 BIGINT，以保持符号位语义，见 [MySQL 位运算规则](https://dev.mysql.com/doc/refman/8.4/en/bit-functions.html)。

### 条件追加与明确替换

动态查询、更新和删除的 `where(...)` 都按 AND 追加，包括回调和已有 `ConditionGroup`。OR 条件组保留括号：

```java
Flux<DynamicRow> rows = clients.operator().dml().query(users)
    .where("org_id", 7L)
    .where(where -> where.or(group -> group.is("status", "ACTIVE").is("status", "PENDING")))
    .fetch();
```

确实需要清掉此前业务条件时，显式使用 `.replaceWhere(where -> where.is("id", 1L))`。它不会清除 Scope、前端过滤、逻辑删除或乐观锁约束。升级已有调用时注意：过去依赖回调 `where` 覆盖旧条件的代码应改为 `replaceWhere`。

已有条件可以直接复用，同步入口用法相同：

```java
ConditionGroup pending = ConditionGroup.or()
    .where("status", "=", "NEW").where("status", "=", "RETRY").build();
Flux<DynamicRow> rows = operator.dml().query(orders).where(pending).fetch();
Mono<Long> changed = operator.dml().update(orders)
    .set("status", "DONE").where(pending).execute();
```

## 前端条件与字段治理

完整查询参数使用 `QueryInput`（`com.flying.orm.rdb.form.spec`），不用在业务层分别拆解条件、选列、排序和分页：

```java
QueryInput query = new QueryInput(
    StructuredConditionInput.term("name", "like", "A%"),
    List.of("id", "name"), List.of(), List.of(PageSort.asc("id")),
    PageQuery.of(2, 20));

Mono<PageResult<DynamicRow>> page = operator.dml().query(users)
    .where("id", ">", 0L).input(query).page();
Flux<DynamicRow> rows = operator.dml().query(users).input(query).fetch(); // 只查该页，不查总数
// 实体查询同样支持 repository.createQuery().input(query).fetch() / .page()。
```

`input` 替换前一次输入的条件、选列、排除、排序和窗口，不清除服务端 `where`、Scope、分组或已声明计算列。实体入口支持属性名并沿用枚举映射；动态表入口使用字段名。`page = null` 表示不分页；`first()` 取首条，`count()` 统计全部匹配行，显式 `page(页码, 大小)` 覆盖输入窗口。分页大小由使用者决定。

只传入条件时仍可使用 `filter`：

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
    .fetch();
```

这个策略没有允许 `status` 过滤，因此携带该字段的输入会被拒绝。也可使用 `from(form, policy, limits)` 显式指定查询形状预算。

条件规模按需配置，下面的数字只是应用示例，不是框架上限：

```java
StructuredConditionPolicy inputPolicy = StructuredConditionPolicy.defaults()
    .withMaxDepth(128)
    .withMaxNodes(20000)
    .withMaxCollectionSize(5000)
    .withMaxStringLength(16384);

Flux<DynamicRow> rows = scoped.dml().query(users)
    .filter(input, inputPolicy)
    .fetch();
```

## 分页

### 页码：需要总数

```java
Mono<PageResult<DynamicRow>> page = dml.query(users)
    .where("status", "ACTIVE")
    .orderByDesc("id")
    .page(1, 20);

Mono<PageResult<UserView>> views = dml.query(users)
    .select("id", "name")
    .where("status", "ACTIVE")
    .orderByDesc("id")
    .page(1, 20, UserView.class);

// 后端可信表名入口，无需先构建 DynamicForm。
Mono<PageResult<DynamicRow>> rawPage = dml.query().from("users")
    .select("id", "name")
    .where("status", "ACTIVE")
    .orderByDesc("id")
    .page(1, 20);

// 按机构分组分页，total 是分组数。
Mono<PageResult<DynamicRow>> groups = dml.query(users)
    .select("org_id")
    .groupBy("org_id")
    .orderByAsc("org_id")
    .page(1, 20);

// 复用基础条件，分别统计全部活跃用户、分页查询指定姓名。
QueryOperator activeUsers = dml.query(users).where("status", "ACTIVE");
Mono<Long> activeTotal = activeUsers.copy().count();
Mono<PageResult<DynamicRow>> namedPage = activeUsers.copy()
    .where("name", "张三").orderByAsc("id").page(1, 20);
```

表名字符串入口沿用参数绑定和显式 Scope，不自动推断字段类型、逻辑删除或加密/脱敏定义；需要这些元数据能力时使用 `DynamicForm` 入口。同步入口用法相同。

分组分页的普通投影与排序字段须属于分组字段；需要聚合函数或 HAVING 时使用下文的 `aggregatePage`。加密字段分组和受保护的 CONTAINS 分组查询不支持。

`copy()` 保留基础查询的 Scope、前端过滤及字段策略，各副本后续修改互不影响；同步查询与实体查询同样支持。每个可变构建器仍只在单个线程内使用。

页码从 1 开始，执行总数与页数据查询。两种分页的 `size` 均由开发者指定正整数，不设 1000/999 等框架固定上限；执行仍遵循开发者配置的行数和内存预算。keyset 为判定下一页会多取一行，`size + 1` 须能由现有 `int` 参数表示。跨语句一致性由上层数据库环境决定。

### Keyset 游标：主键排序

```java
Mono<KeysetPageResult<DynamicRow>> first = dml.query(users)
    .keysetPage(KeysetPageQuery.first(20, KeysetSort.asc("id", NullOrder.LAST)));

// previous 是已取得的结果，仅在 hasMore() 为 true 时继续。
Mono<KeysetPageResult<DynamicRow>> next = dml.query(users)
    .keysetPage(KeysetPageQuery.after(20, previous.nextPosition(), KeysetSort.asc("id", NullOrder.LAST)));

Mono<KeysetPageResult<UserView>> views = dml.query(users)
    .select("id", "name")
    .keysetPage(KeysetPageQuery.first(20, KeysetSort.asc("id", NullOrder.LAST)), UserView.class);
```

### Keyset：复合排序、可空字段

```java
Mono<KeysetPageResult<DynamicRow>> first = dml.query(users).keysetPage(KeysetPageQuery.first(
    20, KeysetSort.asc("name", NullOrder.LAST), KeysetSort.asc("id", NullOrder.LAST)));

// previous 是已取得的 KeysetPageResult；仅在 hasMore() 为 true 时继续。
Mono<KeysetPageResult<DynamicRow>> next = dml.query(users).keysetPage(KeysetPageQuery.after(
    20, previous.nextPosition(),
    KeysetSort.asc("name", NullOrder.LAST), KeysetSort.asc("id", NullOrder.LAST)));

Mono<KeysetPageResult<UserView>> views = dml.query(users)
    .select("id", "name")
    .keysetPage(KeysetPageQuery.first(20,
        KeysetSort.asc("name", NullOrder.LAST), KeysetSort.asc("id", NullOrder.LAST)), UserView.class);
```

保持条件、Scope 与排序一致，原样回传完整 `nextPosition`，不要自己截取排序值。keyset 的排序只在分页参数内声明，不再调用 `orderByAsc/Desc`。游标可包含敏感排序值，跨信任边界时由上层签名或加密。keyset 不额外查询总数。

## 报表与聚合

```java
Flux<AggregateRow> report = dml.query(users)
    .where("status", "ACTIVE")
    .scope(DataScope.orgOnly("org_id", orgId))
    .aggregate(a -> a
        .group("org_id", "org")
        .countRows("user_count")
        .countDistinct("name", "name_count")
        .having(having -> having.where("user_count", ">", 10L)));

Flux<Long> counts = report.map(row -> row.get("user_count", Long.class));
```

报表分页直接统计 HAVING 后的分组总数，并由数据库返回当前页：

```java
Mono<PageResult<AggregateRow>> page = dml.query(users)
    .where("status", "ACTIVE")
    .scope(DataScope.orgOnly("org_id", orgId))
    .orderByDesc("user_count")
    .aggregatePage(a -> a
        .group("org_id", "org")
        .countRows("user_count")
        .having(having -> having.where("user_count", ">", 10L)), 1, 20);
```

已有 `AggregateSpec` 可直接调用 `forms.aggregatePage(spec, PageQuery.of(1, 20))`；同步客户端同名方法返回 `PageResult<AggregateRow>`。排序使用报表结果别名，页大小由调用方指定；总数和数据是两次查询，一致性隔离由上层决定。

指定分组值排前、聚合值排后，直接在报表回调配置，不增加辅助返回列：

```java
Flux<AggregateRow> prioritized = dml.query(users)
    .aggregate(a -> a
        .group("status", "state")
        .countRows("users")
        .orderByFirst("state", "ACTIVE", "PENDING")
        .orderByLast("users", 1L)
        .orderBy(PageSort.desc("users")));
```

动态、实体及关联报表均提供这三个排序入口，按调用顺序确定优先级。实体分组及直接字段 MIN/MAX 的结果别名保留 `@EnumValue`，例如 `a.group(User.class, User::getStatus, "state"); a.orderByFirst("state", Status.DISABLED, Status.ACTIVE);`，无需手写存储值。COUNT/SUM 等数值结果、计算表达式及自定义函数结果按其结果类型传值，不继承源字段的枚举语义；字段 codec 仍在 SQL 规划时处理。

复杂优先级可用 `a.orderBy(ValueExpression.firstMatch(ValueExpression.field("state").priority("ACTIVE"), ValueExpression.field("users").priority(1L)), PageSort.Direction.ASC)`；实体报表同样支持直接传枚举。表达式只能引用该报表声明的结果别名；排序不改变分页总数，`PageQuery` 显式排序会替换报表排序。

配置聚合也支持 `sum(field, alias)` 和 `avg(field, alias)`。结果可按声明的别名和类型读取，或保留表达式句柄使用 `row.get(expression)`；别名大小写精确匹配，未知别名报错，SQL NULL 返回 null，不隐式转换类型。`having(callback)` 替换此前的整棵 HAVING 条件树。

`countRows(alias)` 统计行数，不因字段为 NULL 而漏计；`count(field, alias)` 只统计该字段的非空值，`countDistinct(field, alias)` 统计不同的非空值。SUM / AVG / MIN / MAX 同样使用类型化表达式：

```java
AggregateExpression<BigDecimal> total = AggregateExpression.sum("amount", "total_amount");
AggregateExpression<BigDecimal> average = AggregateExpression.avg("amount", "average_amount");
AggregateExpression<BigDecimal> spread = AggregateExpression.stddevSample("amount", "amount_spread");
AggregateExpression<BigDecimal> largest = AggregateExpression.max("amount", "largest_amount", LogicalType.DECIMAL, BigDecimal.class);
```

这些表达式用于具有 `amount` 字段的订单表单，添加到同一 `aggregate` 回调即可。前端 `filter`、服务端 `where`、Scope 和字段用途检查在报表入口同样生效。

标准差使用 `stddevPop`（总体）/ `stddevSample`（样本），方差使用 `variancePop` / `varianceSample`；聚合 Builder 可直接调用，如 `a.varianceSample("amount", "amount_variance")`。四者均支持字段和计算表达式，由数据库计算并忽略 NULL；空集返回 null，样本非空值不足两个也返回 null。结果统一为 `BigDecimal`，但不会提高数据库原生浮点计算的精度。

实体 Repository 直接使用实体属性，不必转换成表单或手写列名：

```java
Mono<PageResult<AggregateRow>> report = repository.createQuery()
    .where(User::getStatus, "ACTIVE")
    .aggregatePage(a -> {
        a.group(User::getOrgId, "org");
        a.countRows("users");
        a.orderBy(PageSort.desc("users"));
    }, 1, 20);
```

实体报表支持 `countRows`、`count`、`countDistinct`、`sum`、`avg`、`min`、`max` 及上述四种统计函数，例如 `a.stddevSample(Order::getAmount, "spread")`。单实体直接传“Getter＋结果别名”，多实体关联增加实体类型以指定来源；返回已注册的 `GroupSelection` 或 `AggregateExpression` 句柄。`min/max` 额外传入与字段 codec 匹配的结果类型，如 `a.max(Order::getAmount, "largest", BigDecimal.class)`。分组、输出和排序在聚合回调中声明，不继承普通实体查询的 `select/exclude/groupBy/orderBy`；条件、Scope、字段治理和展示策略保留，不触发实体 `onLoad`。

### 计算后聚合

分组也可以使用计算表达式，不要求额外添加计数或求和列。以下把空状态归为 `unknown`：

```java
ValueExpression status = ValueExpression.field("status").coalesce(ValueExpression.value("unknown"));
Flux<AggregateRow> groups = dml.query(orders)
    .aggregate(report -> report.group(status, "state"));

Flux<AggregateRow> totalsByStatus = dml.query(orders)
    .aggregate(report -> report.group(status, "state").countRows("rows"));

Flux<AggregateRow> entityGroups = orderRepository.createQuery()
    .aggregate(report -> report.group(
        report.field(Order::getAmount).coalesce(ValueExpression.value(BigDecimal.ZERO)), "amount"));
```

`group(ValueExpression, alias)` 同样支持已注册函数；关联报表用 `report.field(form, name)` 或实体 Getter 指定来源。HAVING、排序引用结果别名，分页总数是最终分组数。同步入口同名。

订单表单包含数值字段 `amount` 和 `quantity` 时，可先计算再求和；常量仍通过参数绑定：

```java
ValueExpression subtotal = ValueExpression.field("amount")
    .coalesce(ValueExpression.value(BigDecimal.ZERO))
    .multiply(ValueExpression.field("quantity"));
AggregateExpression<BigDecimal> gross = AggregateExpression.sum(subtotal, "gross");

Flux<BigDecimal> totals = dml.query(orders)
    .where("status", "paid")
    .aggregate(report -> report.aggregate(gross))
    .map(row -> row.get(gross));
```

实体 Repository 使用同一个模型，Getter 自动解析实际列名：

```java
Flux<AggregateRow> totals = orderRepository.createQuery()
    .aggregate(report -> report.sum(
        report.field(Order::getAmount)
            .coalesce(ValueExpression.value(BigDecimal.ZERO))
            .multiply(report.field(Order::getQuantity)),
        "gross"));
```

`Order` 声明 `getAmount/getQuantity`。计算表达式支持字段、参数、四则运算、`coalesce` 和后端注册的函数，可与联表、HAVING、分页组合。SUM/AVG 要求数值结果并返回 `BigDecimal`；COUNT/COUNT DISTINCT 返回 `Long`；MIN/MAX 按声明类型返回数值、文本或时间等可比较结果。除法沿用数据库原生语义。同步入口同名，返回普通结果集合。

例如，对计算金额统计非空数、去重数和最大值：

```java
AggregateExpression<Long> present = AggregateExpression.count(subtotal, "present");
AggregateExpression<Long> distinct = AggregateExpression.countDistinct(subtotal, "distinct_subtotals");
AggregateExpression<BigDecimal> largest = AggregateExpression.max(
    subtotal, "largest", LogicalType.DECIMAL, BigDecimal.class);

Flux<BigDecimal> largestAmounts = dml.query(orders)
    .aggregate(report -> report.aggregate(present).aggregate(distinct).aggregate(largest))
    .map(row -> row.get(largest));
```

实体回调同样支持 `report.count(expression, alias)` 和 `report.min/max(expression, alias, logicalType, javaType)`。所有操作数字段都参与治理；加密字段不能直接在 SQL 中计算，需脱敏的源字段不能借计算函数输出明文。直接字段 MIN/MAX 保留原字段的 codec 和脱敏行为。

MIN/MAX 和 COUNT DISTINCT 要求可比较的标量；目前不把来源于 CLOB/TEXT 等 LOB 的计算文本当作 VARCHAR。`length(LOB)` 等明确返回数值的函数不受此限制。

### 扩展报表函数

函数在装配客户端时注册。例如统计文本长度，不需要修改 ORM：

```java
SqlExpressionFunctions functions = SqlExpressionFunctions.builder()
    .register("text-lower", "lower", LogicalType.TEXT, LogicalType.TEXT)
    .register("weighted-mean", call -> {
        call.requireArity(2);
        call.requireNumericArguments();
        call.sql("(sum(").argument(0).sql(" * ").argument(1)
            .sql(") / nullif(sum(").argument(1).sql("), 0))");
        return LogicalType.DECIMAL;
    })
    .register("text-length", call -> {
        call.requireArity(1);
        String name = switch (call.dialect()) {
            case "sqlserver" -> "len(";
            case "mysql" -> "char_length(";
            default -> "length(";
        };
        call.sql(name).argument(0, LogicalType.TEXT).sql(")");
        return LogicalType.INTEGER;
    }).build();

SqlRenderer renderer = SqlRenderer.builder()
    .addDefaultTerms()
    .expressionFunctions(functions)
    .build();
// 在现有 FlyingOrmClients.builder(...) 上调用 .renderer(renderer)。

AggregateExpression<BigDecimal> nameLength = AggregateExpression.sum(
    ValueExpression.call("text-length", ValueExpression.field("name")), "name_length");
```

普通函数用 `register(id, sqlName, resultType, argumentTypes...)` 一次声明，按参数类型数量确定参数个数，类型只用于 NULL 绑定推导，不限制或强转非空值；SQL 名称支持点分限定名。需要方言选择、特殊语法或变参时使用回调。

把 `nameLength` 添加到包含 `name` 字段的报表即可。注册表构建后不可变；SQL 文本只由可信后端提供，`argument` 保留字段解析和参数绑定，重复引用会重复绑定。结果类型声明应与函数真实返回类型一致；`argument(0, LogicalType.TEXT)` 为 NULL 输入提供类型，不强转非空值。未知函数直接报错，不自动退回全部字段。示例各数据库的长度语义仍以相应函数为准。

已注册的聚合函数可直接用于报表，不需要再套一层 `sum/min`：

```java
AggregateExpression<BigDecimal> weighted = AggregateExpression.function(
    "weighted-mean", "weighted_price", LogicalType.DECIMAL, BigDecimal.class,
    ValueExpression.field("amount"), ValueExpression.field("quantity"));

Flux<BigDecimal> prices = dml.query(orders)
    .aggregate(report -> report.aggregate(weighted))
    .map(row -> row.get(weighted));
```

实体报表使用 `report.function(id, alias, logicalType, javaType, arguments...)`，单实体参数用 `report.field(Entity::getter)`，多实体关联用 `report.field(Entity.class, Entity::getter)`。分组、HAVING、分页及所有参数字段的治理仍走同一聚合链路。注册方负责实际函数的聚合语义和方言支持；声明的逻辑结果类型须与注册函数一致，Java 结果沿用现有 codec 转换。

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

`organizations` 是应用定义的机构 DynamicForm。INNER/RIGHT/FULL JOIN 分别使用 `join` / `rightJoin` / `fullJoin`；复合 ON 使用 `andOn`。使用 `page(PageQuery.of(1, 20))` 分页。

保留两侧没有匹配的记录，使用 FULL JOIN，并为两侧字段分别取别名：

```java
Flux<DynamicRow> rows = dml.joinQuery(users)
    .fullJoin(organizations, "org_id", "id")
    .selectAs(users, "id", "user_id")
    .selectAs(organizations, "id", "org_id")
    .executeRows();
```

实体入口同样支持 `fullJoin(Order.class, User::getId, Order::getUserId)` 和复杂 ON 回调；同步入口同名。内置 PostgreSQL、Oracle、SQL Server 方言声明 `FULL_OUTER_JOIN` 能力；MySQL、H2 和未声明该能力的自定义方言在规划时拒绝，不自动改写成 UNION。ON 条件仍受数据库原生语法约束。Scope 在各来源内部生效；JOIN 后追加的业务 `where` 可能排除未匹配行。

关联结果可以直接选计算列，参数仍走绑定：

```java
JoinQueryOperator query = dml.joinQuery(orders)
    .join(items, "id", "order_id");

Flux<DynamicRow> totals = query
    .selectAs(orders, "id", "order_id")
    .select(query.field(orders, "price")
        .multiply(query.field(items, "quantity").coalesce(ValueExpression.value(1))), "subtotal")
    .orderByAsc(orders, "id")
    .executeRows();
```

此例 `orders` 声明 `id/price`，`items` 声明 `order_id/quantity`。实体 JOIN 使用 `query.field(Order.class, Order::getPrice)` 引用 Getter；同步入口同名。计算投影也可调用 `page(...)`，总数不包含投影参数。直接字段别名保留字段 codec；派生计算需全部来源允许完整值读取，不能绕过加密或脱敏。

关联排序不必增加辅助结果列：

```java
Flux<DynamicRow> preferred = dml.joinQuery(orders).join(items, "id", "order_id")
    .selectAs(orders, "id", "order_id")
    .orderByFirst(items, "quantity", 5, 3).orderByAsc(orders, "id")
    .executeRows();
```

指定值排在最后用 `orderByLast`；实体入口用 `orderByFirst(Order.class, Order::getStatus, OrderStatus.PAID)`，保留 `@EnumValue`。
跨来源首匹配使用 `query.orderBy(ValueExpression.firstMatch(...), PageSort.Direction.ASC)`：
动态分组用 `query.field(form, field).priority(values...)`，实体分组用 `query.priority(Type.class, getter, values...)`。
同表自关联在 `JoinQuerySpec.Builder.orderBy(expression, direction)` 中使用各自的 `JoinSource`；来源必须允许排序，加密来源不参与计算排序。

复杂 ON 可以组合字段比较和参数条件。例如仅关联数量达标的已付款订单，同时保留没有匹配订单的用户：

```java
Flux<DynamicRow> rows = dml.joinQuery(users)
    .leftJoin(orders, (user, order) -> JoinCondition.and(
        JoinCondition.compare(user.field("id"), "=", order.field("user_id")),
        JoinCondition.compare(order.field("quantity"), ">=", user.field("minimum")),
        JoinCondition.where(order, ConditionGroup.and()
            .where("status", "=", "paid").build())))
    .selectAs(users, "id", "user_id")
    .selectAs(orders, "id", "order_id")
    .executeRows();
```

这里 `users` 声明 `id/minimum`，`orders` 声明 `id/user_id/quantity/status`。`JoinCondition.or(...)` 可嵌套分组；字段比较支持 `=、<>、!=、>、>=、<、<=`，条件值始终参数绑定。ON 决定是否关联，`where(...)` 过滤关联后的结果，二者含义不同。租户、逻辑删除与来源 Scope 仍独立生效。

实体入口用 Getter 引用字段，值条件自动处理 `@EnumValue`：

```java
Flux<DynamicRow> rows = dml.joinQuery(User.class)
    .leftJoin(Order.class, (user, order) -> JoinCondition.and(
        JoinCondition.compare(user.field(User::getId), "=", order.field(Order::getUserId)),
        order.where(where -> where.where(Order::getStatus, OrderStatus.PAID))))
    .selectAs(User.class, User::getId, "user_id")
    .executeRows();
```

`Order`、`OrderStatus` 是应用实体及枚举。同步入口同名；动态关联的 `aggregate/aggregatePage` 复用同一 ON。受字段治理的自定义 ON term 需用 `TermExtensionDescriptor.join(...)` 显式声明用途，不能把仅允许 FILTER 的处理器自动用于 JOIN。

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

关联报表可以直接聚合，不需要先查询明细或声明普通投影：

```java
AggregateExpression<BigDecimal> gross = AggregateExpression.sum("amount", "gross");
Mono<PageResult<AggregateRow>> report = dml.joinQuery(customers)
    .leftJoin(orders, "id", "customer_id")
    .aggregatePage(a -> a
        .group(GroupSelection.of("name", "customer"))
        .aggregate(orders, gross)
        .having(condition -> condition.where("gross", ">", BigDecimal.ZERO))
        .orderBy(PageSort.desc("gross")), 1, 20);
```

`customers` 声明 `id/name`，`orders` 声明 `customer_id/amount`。未指定来源的选择使用根表；其他来源显式传入表单。自关联使用 `AggregateSpec.builder(builder.buildRelation())`，通过 `group(manager, selection)` / `aggregate(manager, expression)` 指定来源。同步入口同名；总数统计 HAVING 后的分组数，排序引用结果别名。

实体关联也直接做报表，Getter 自动解析 `@TableField` 的列名：

```java
Mono<PageResult<AggregateRow>> report = dml.joinQuery(User.class)
    .leftJoin(Order.class, User::getId, Order::getUserId)
    .aggregatePage(a -> {
        a.group(User.class, User::getName, "user");
        a.sum(Order.class, Order::getAmount, "gross");
        a.having(condition -> condition.where("gross", ">", BigDecimal.ZERO));
        a.orderBy(PageSort.desc("gross"));
    }, 1, 20);
```

`Order` 在此声明 `userId/amount` 属性。复杂 ON、各来源 Scope 与普通关联查询一致；不分页时调用 `aggregate(a -> ...)`。同步入口使用同一配置，返回 `List<AggregateRow>` 或 `PageResult<AggregateRow>`。

## 实体 Repository

`User` 是带 getter/setter 的实体，含 `id/name/orgId/status/version` 属性；映射和治理注解见 [ANNOTATIONS](ANNOTATIONS.md)。

需要在查询前异步补充条件时，可在派生 Repository 上配置一次：

```java
ReactiveFormRepository<User> prepared = clients.repository(User.class)
    .withQueryPreparation(spec -> Mono.deferContextual(context -> Mono.just(
        spec.withScope(DataScope.orgOnly("org_id", context.get("orgId"))))));

Mono<PageResult<User>> page = prepared.createQuery().orderByAsc(User::getId)
    .page(1, 20).contextWrite(context -> context.put("orgId", 7L));
```

准备器处理物理列规格，每次订阅执行并等待完成；查询、计数、分页、游标和锁定读取都适用，分页的 count 与数据查询共用一次准备结果。多次配置按顺序串联，每项必须返回恰好一个 `QuerySpec`；原 Repository 不变。同步 Repository 同名方法接收 `spec -> QuerySpec`，表单客户端也提供同名入口。

此入口不处理 JOIN、聚合、原生 SQL、写入或字段用途预览，不能替代全局权限控制；需要跨操作一致的数据范围，使用前面的 `clients.withDefaultDataScope(...)`。原有默认 Scope 和字段治理仍在准备之后执行。

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
Mono<User> first = repository.createQuery().where(User::getOrgId, 7L)
    .orderByAsc(User::getId).first();
Mono<PageResult<User>> page = repository.createQuery().orderByAsc(User::getId).page(1, 20);
Mono<Long> total = repository.createQuery().where(User::getStatus, "ACTIVE").count();
Mono<Long> orgCount = repository.createQuery().groupBy(User::getOrgId).count();

Mono<KeysetPageResult<User>> cursor = repository.createQuery()
    .where(User::getOrgId, 7L)
    .keysetPage(KeysetPageQuery.first(20, KeysetSort.asc("id", NullOrder.LAST)));
Mono<KeysetPageResult<User>> keyset = repository.createQuery()
    .where(User::getOrgId, 7L)
    .keysetPage(KeysetPageQuery.first(20, KeysetSort.asc("name", NullOrder.LAST)));

Mono<PageResult<User>> namesPage = repository.createQuery()
    .select(User::getName)
    .or(group -> group
        .andGroup(activeGroup -> activeGroup.where(User::getStatus, "ACTIVE").where(User::getOrgId, 7L))
        .andGroup(otherGroup -> otherGroup.where(User::getStatus, "PENDING").where(User::getOrgId, 8L)))
    .orderByAsc(User::getName)
    .page(1, 20);

// 属性名条件与 Getter 共用实体映射，也支持映射后的列名。
Flux<User> selected = repository.createQuery()
    .select("id", "name").exclude("id")
    .where("orgId", 7L).orderByAsc("name").fetch();
Mono<Long> grouped = repository.createQuery().groupBy("orgId").count();
Mono<Long> renamed = repository.createUpdate().set("name", "Alice")
    .where("id", 1L).execute();
Mono<Long> deletedByName = repository.createDelete().where("id", 1L).execute();

Mono<Long> update = repository.createUpdate()
    .set(User::getName, "Alice")
    .where(User::getId, 1L)
    .optimisticLock(3L)  // User.version 声明 @Version
    .execute();

Mono<Long> deleted = repository.createDelete().in(User::getId, List.of(1L, 2L)).execute();
```

`fetch()/execute()/one()/first()/page()` 统一支持部分实体投影；Bean 未选择的属性保留构造器和字段初始化值，record 未选择的组件使用 Java 默认值。`one()` 返回零或一条，多条报错；`first()` 只取排序后的首条，不查询总数，未排序时由数据库决定首行。普通查询在 SQL 中限制为一行，加密 CONTAINS 查询先精确校验候选集再取首条。动态行投影使用 `executeRows()`。实体条件可递归嵌套 `or/andGroup`，支持 `in/notIn/between/isNull/isNotNull` 和 `where(property, operator, value)`。

`count()` 返回 `Mono<Long>`（同步返回 `long`），忽略输出投影与排序，保留条件、Scope 和字段治理；带 `groupBy` 时统计分组数，无需 `select`。普通计数只执行一条 COUNT SQL；加密 CONTAINS 先核验候选值，再统计真实匹配数，不支持分组。已有 `QuerySpec` 可直接调用 `forms.count(spec)`。

Repository 配置的 `POST_LOAD` 监听器同样作用于 `createQuery()` 的实体终端；`executeRows()`、`count()` 不触发实体监听。单次查询可追加 `onLoad(entity -> publisher)`（响应式）或 `onLoad(entity -> { ... })`（同步），回调按注册顺序、结果顺序执行，成功完成后才交付实体。

实体 `keysetPage` 也保留加载回调；加上结果类型参数可读取 DTO/record，不触发实体回调。排序仅在分页参数内声明，可用实体属性名或列名。后续页原样回传 `nextPosition`；未投影的排序字段由内部补选再剥离。同步 Repository 用法相同。

### 可选条件、排除字段与查询分支

```java
EntityDmlQueryOperator<User> base = repository.createQuery()
    .where(User::getOrgId, trustedOrgId)
    .whereIfPresent(User::getName, "like", namePattern)
    .exclude(User::getVersion)
    .orderByAsc(User::getId);

Mono<Long> total = base.copy().count();
Flux<User> active = base.copy().where(User::getStatus, "ACTIVE").fetch();
Mono<PageResult<User>> page = base.copy().where(User::getStatus, "PENDING").page(1, 20);
```

`whereIfPresent` 沿用统一空值、空文本及空集合规则；必需条件仍用严格 `where`。嵌套条件、实体更新和删除也支持这两个可选条件重载。

`exclude` 从显式 `select` 或默认可读字段中排除属性，服从字段 Scope；排除全部输出字段时报错，不会变成查询所有字段。`count` 不受输出字段排除影响。

`copy` 保留当前条件、前端过滤、Scope、排序、分组、投影、显示方式和加载回调，之后修改两边的查询互不影响。回调捕获的外部对象、可信自定义条件的领域对象仍由应用管理，不做任意对象深拷贝。同步入口同名。

### 从实体选择更新字段

```java
Mono<Long> changed = repository.createUpdate()
    .set(user, User::getName, User::getStatus)
    .where(User::getId, user.getId())
    .optimisticLock(user.getVersion()) // 使用 @Version 时显式传入预期版本
    .execute();

// 读取全部普通可更新字段；条件仍显式指定。
repository.createUpdate().set(user).where(User::getId, user.getId()).execute();

// 排除字段在取值前生效，不读取其 Getter。
repository.createUpdate().setExcept(user, User::getName)
    .where(User::getId, user.getId()).execute();

// 绑定 Getter 与属性名赋值同样使用实体映射，包括 @EnumValue。
repository.createUpdate().set(user::getStatus).set("name", "Alice")
    .where(User::getId, user.getId()).execute();
```

`set(user, ...)` 只读取选中字段，沿用字段填充、更新策略及 `@EnumValue`。不指定字段时排除主键、版本、逻辑删除及不可更新字段；指定这些字段会报错。后续 `set` 覆盖同列，显式 `.set(User::getName, null)` 可赋 SQL NULL。实体值在 `set` 调用时读取，未选字段不会触发 Getter 或填充器。

这是 SET 值来源，不自动推断条件或版本，也不触发实体生命周期；需要自动主键、版本和实体事件时用下方 `repository.update(user)`。同步入口用法相同。

绑定 Getter 只接受直接方法引用，在 `set` 时调用一次，不随执行或重复订阅再次取值；显式赋值不自动套用整实体的空值跳过或填充策略。

### 数据库端计算更新

```java
import static com.flying.orm.core.expression.ValueExpression.*;

// salesForm 声明 amount、quantity 的字段类型；计算和更新在同一条 SQL 内完成。
operator.dml().update(salesForm)
    .setExpression("amount", field("amount").add(value(3)).multiply(field("quantity")))
    .where("id", 1L)
    .execute();

// 实体入口使用 Getter；字段常量沿用 @EnumValue 和该字段的自定义 codec。
repository.createUpdate()
    .setExpression(User::getStatus,
        e -> e.field(User::getStatus).coalesce(e.value(User::getStatus, Status.ACTIVE)))
    .where(User::getId, 1L)
    .execute();
```

同步入口同名。`update("表名")` 可直接复制字段或调用不要求数值类型的函数；数值运算使用带字段类型的 `DynamicForm` 或实体入口。普通赋值仍用 `set`，原子增减仍可用 `increment/decrement`。

表达式复用报表的函数注册表，常量始终参数绑定。Scope 校验所有来源；启用字段用途策略时，来源须允许 `FULL_VALUE`，目标须允许 `UPDATE`。加密字段不参与数据库明文计算。普通及乐观锁批量可在 `BatchUpdate` 的值 Map 中放入同一个表达式模型；原生执行器支持各行使用不同 SQL 布局。

### 按主键直接读写

```java
Mono<User> found = repository.findById(1L);
Mono<Long> changed = repository.update(user);
Mono<Long> removed = repository.delete(user);
Mono<Long> removedById = repository.deleteById(1L);
Flux<User> foundMany = repository.findByIds(List.of(1L, 2L, 3L));
Mono<Long> removedMany = repository.deleteByIds(List.of(1L, 2L, 3L));
List<User> syncFoundMany = syncRepository.findByIds(List.of(1L, 2L, 3L));

// membershipRepository 的实体以 orgId、userId 共同声明主键。
membershipRepository.findById(Map.of("orgId", 7L, "userId", 9L));
membershipRepository.deleteById(Map.of("orgId", 7L, "userId", 9L));
membershipRepository.findByIds(List.of(
        Map.of("orgId", 7L, "userId", 9L),
        Map.of("orgId", 8L, "userId", 10L)));
```

`user` 须带完整主键；声明了 `@Version` 的实体还须带预期版本。`update(user)` 按字段更新策略生成 SET，不更新主键；`delete(user)` 沿用实体事件和版本锁。`deleteById` 只有主键，不触发实体事件或自动版本校验。两种删除均沿用逻辑删除及 Scope，不会先查一次数据库。

`update` / `updateBatch` 在数据库中递增版本，不回写传入实体的版本字段。再次更新或删除时，应使用重新读取的版本，不能把原实体的旧版本当作新版本。

复合键 Map 必须恰好包含全部主键，字段名支持 Java 属性及列名，枚举键按实体映射转换。`findById` 未找到时返回空 Mono（同步返回 null），多行时报错；同步 Repository 方法同名，返回实体或 `long`。

`findByIds` / `deleteByIds` 接收主键集合：单主键使用 `IN`，复合主键按完整键组匹配，不逐键查询。空集合不执行 SQL；结果不保证输入键顺序。集合在调用时快照，响应式执行仍在订阅时开始；不隐式分批，数据库参数数量限制由调用方按需组织输入。`deleteByIds` 与 `deleteById` 一样保留 Scope、逻辑删除，不触发实体事件或自动版本校验。

### 单实体插入或更新

```java
Mono<BatchExecutionEvidence> saved = repository.upsert(user);
BatchExecutionEvidence syncSaved = syncRepository.upsert(user);
```

按主键保存，无需包装集合或先查询。支持 `NOT_NULL/NOT_EMPTY`、Scope、枚举映射、字段保护及 `PRE/POST_PERSIST` 回调。数据库生成主键为空时 INSERT 并回填；非空时原生 UPSERT，保留原主键，批量可混合两种行。没有待插入字段时使用数据库默认值；显式写入 identity 仍受数据库规则约束（例如 SQL Server 不默认允许）。返回执行事实，不推断事务结果；需要版本比较时使用 `update`。

### 实体查询直接接收前端条件

```java
StructuredConditionInput input = StructuredConditionInput.and(
    StructuredConditionInput.term("name", "like", "A%"),
    StructuredConditionInput.term("status", "in", List.of("ACTIVE", "DISABLED")));
StructuredConditionPolicy policy = StructuredConditionPolicy.defaults()
    .allowOnlyFields(List.of("name", "status"));

Mono<PageResult<User>> page = repository.createQuery()
    .filter(input, policy)
    .where(User::getOrgId, trustedOrgId)
    .orderByAsc(User::getId)
    .page(1, 20);
```

`filter(input)` 使用默认策略；同一入口可接 `fetch/one/first/count/page`，同步 Repository 用法相同。字段支持 Java 属性名及映射列名，策略字段也按同一映射处理。服务端传入枚举实例时自动使用 `@EnumValue` 持久值；JSON 字符串应传实际持久值，不猜测枚举常量名。数组、集合和 Iterable 使用同一转换规则。

动态排序可直接用 `.orderByAsc("name")` / `.orderByDesc("status")`，支持实体属性名和映射列名；存在同名计算别名时按别名排序。字段使用权限仍由客户端策略校验。

前端过滤、服务端 `where` 与 Scope 取交集。再次调用 `filter` 只替换前端过滤，不会清除服务端条件；客户端已配置的条件定制器仍然生效。

### 按实体属性名转换条件和局部赋值

封装层只有属性名和值时，使用当前客户端的 `EntityModelRegistry`。例如 `Account.state` 使用 [注解示例](ANNOTATIONS.md#枚举加密与脱敏) 中的 `UserStatus`，并映射到 `state_code` 列：

```java
EntityModelRegistry models = forms.entityModels();
EntityMetadata<Account> metadata = models.metadata(Account.class);
String column = metadata.field("state").columnName();

// ACTIVE 转为 @EnumValue 声明的 "A"；同一条件用于查询、分页、更新和删除。
ConditionGroup where = ConditionGroup.and()
    .add(models.condition(Account.class, "state", "=", UserStatus.ACTIVE))
    .build();
QuerySpec query = QuerySpec.of(metadata.toDynamicForm(), where);
Flux<Account> rows = forms.select(query, Account.class);
Mono<PageResult<Account>> page = forms.page(query, PageQuery.of(1, 20), Account.class);

// account.getState() 返回 DISABLED 时转换为 "D"；显式 null 仍保留。
Map<String, Object> values = new LinkedHashMap<>();
values.put(column, models.writeValue(Account.class, "state", account.getState()));
Mono<Long> updated = forms.update(WriteSpec.update(metadata.toDynamicForm(), values, where));
Mono<Long> deleted = forms.delete(WriteSpec.delete(metadata.toDynamicForm(), where));
```

绑定 Getter 可先取值，例如 `Supplier<UserStatus> getter = account::getState`，再将 `getter.get()` 传入 `writeValue`。`condition` 同时支持 `in`、`between` 等多值条件；自定义运算符使用带 `TermRegistry` 的重载，传实际 renderer 的 `terms()`。转换后的值继续走表单执行链；封装层保留字段可写性、乐观锁和 Scope 的原有检查。

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

空业务条件的更新/删除作用于有效 Scope 内全部行；未配置 Scope 时作用于全表，由调用方决定操作范围。模型声明逻辑删除时默认软删除；确需物理删除才显式 `physical()`，仍不绕过 Scope。批量同值更新、批量删除用上面的 `IN` 范围条件，不存在 `BatchSpec.delete` 工厂。

已有表单修改 Map 可以直接传入；`null` 表示写入 SQL NULL，未提供的字段不更新，同名字段以后一次 `set` 为准。同步入口用法相同。

```java
Map<String, Object> patch = new LinkedHashMap<>();
patch.put("name", null);
patch.put("status", "DISABLED");
Mono<Long> changed = dml.update(users).set(patch).where("id", 1L).execute();
```

### 批量插入与 UPSERT

```java
Flux<Map<String, Object>> inputRows = Flux.fromIterable(rows);
Mono<BatchExecutionEvidence> inserted = dml.insertBatch(users, inputRows);
Mono<BatchExecutionEvidence> upserted = dml.upsertBatch(users, inputRows);
```

两种写入按业务需求二选一。UPSERT 以模型主键识别冲突，已有冲突目标必须满足 Scope。

原生 JDBC/R2DBC 的表单和实体批量 INSERT/UPSERT/UPDATE 支持各行不同的字段布局（含 `NOT_NULL/NOT_EMPTY`、`INSERT_DEFAULT`）；UPDATE 的条件形状也可不同。按原顺序执行，相邻同布局行合并批量，不补 null，不重排。同批 UPDATE 不混用普通与乐观锁语义。底层 renderer 请求仍为单一 SQL；自定义执行器未声明布局支持时明确报错。发生错误即停止，但不表示此前 SQL 已回滚。

PostgreSQL 的 R2DBC 普通批量 INSERT 会将同一输入窗口合为多行 `VALUES`，参数仍逐项绑定；生成键、LOB、逐行影响数校验及 UPSERT 保留各自执行路径。窗口内的语句级触发器、时间函数和失败粒度因此按一条 SQL 处理。业务需要逐条语句语义时，设置 `BatchWriteOptions.of(1)`；批量失败不代表此前窗口已回滚。

### 每行不同值：普通与乐观锁批量更新

```java
// 普通更新：每行有自己的赋值和条件。
Flux<BatchUpdate> ordinary = Flux.just(new BatchUpdate(
    Map.of("name", "Alice"),
    ConditionGroup.and().where("id", "=", 1L).build()));
Mono<BatchExecutionEvidence> ordinaryResult = dml.updateBatch(users, ordinary);

// 需要乐观锁时，额外提供预期版本。
Flux<BatchUpdate> updates = Flux.just(new BatchUpdate(
    Map.of("name", "Alice"),
    ConditionGroup.and().where("id", "=", 1L).build(),
    OptimisticLockOptions.increment("version", 3L)));

Mono<BatchExecutionEvidence> result = dml.updateBatch(users, updates);
```

两种方式按需选择，不在同一批中混用。普通更新允许零影响行，保留驱动的已知或未知计数；乐观锁更新逐行验证影响数，冲突不会伪装成成功。实体 Repository 也提供 `insertBatch`、`upsertBatch`、`updateBatch`，保留生成键回填和实体生命周期处理。

更新 CONTAINS 加密字段时，只有主表影响行数与预读目标一致才替换检索索引；目标未命中时普通更新返回零，预读后目标变化或驱动计数未知则报错，不盲目覆盖索引。

```java
Flux<User> input = Flux.fromIterable(entityList); // entityList 为待写入的 List<User>
Mono<BatchExecutionEvidence> inserted = repository.insertBatch(input);
Mono<BatchExecutionEvidence> upserted = repository.upsertBatch(input);
Mono<BatchExecutionEvidence> updated = repository.updateBatch(input);
```

以上三种操作按需选择；更新实体须带完整主键，声明了 `@Version` 才要求预期版本并自动启用乐观锁。直接传入实体流即可沿用客户端批量预算，订阅后才消费输入；需要本次覆盖配置时再传 `BatchWriteOptions`。批量行模型已统一为 `BatchUpdate`，直接使用旧乐观更新行类型的代码需要相应更新类型名。

### UPSERT 保留已有字段

```java
BatchSpec batch = BatchSpec.upsert(users, inputRows)
    .ignoreUpdates("status");
Mono<BatchExecutionEvidence> result = forms.writeBatch(batch);
```

新行仍写入输入中的 `status`，主键冲突时保留已有状态，只更新其他字段。字段名来自表单，多次调用累加；同步表单客户端使用相同规格。实体注解已有的阶段策略仍生效，不修改输入数据。加密字段的 EXACT/SUFFIX 检索列与密文一起保留；CONTAINS 字段仍须保持 INSERT/UPDATE 阶段一致，否则执行前拒绝。

### 按业务唯一键 UPSERT

实体直接传业务属性名，自动应用列名、枚举和字段策略：

```java
Mono<BatchExecutionEvidence> saved = repository.upsert(user, "emailAddress");
Mono<BatchExecutionEvidence> batchSaved = repository.upsertBatch(userList, "emailAddress");
Mono<BatchExecutionEvidence> streamSaved = repository.upsertBatch(
    userStream, BatchWriteOptions.of(500), "orgId", "emailAddress");
```

同步 Repository 同名调用。实体可以没有主键；若主键由数据库生成，其值须为空。此入口不回填或校正主键，也不执行版本比较；应用侧主键仍按声明策略准备，其值不能用来推断冲突命中行的实际主键。冲突字段在调用时快照，须对应已有普通唯一约束；不传字段仍沿用上面的主键入口。

动态表单使用相同执行能力，并可逐次排除冲突更新字段：

```java
BatchSpec batch = BatchSpec.upsert(users, inputRows)
    .onConflict("email")
    .ignoreUpdates("created_at");
Mono<BatchExecutionEvidence> result = forms.writeBatch(batch);
```

`email` 须对应数据库已有的普通唯一约束，并出现在每行 INSERT 字段中；不要求表单声明主键。复合键使用 `.onConflict("org_id", "email")`，冲突键本身不更新。不调用 `onConflict` 或调用 `.onConflict()` 时仍按主键处理；同步客户端使用相同规格。

不自动创建或探测约束，不回填生成键，不支持表达式或过滤唯一索引作为此入口的冲突目标。随机加密字段不能作为冲突键；CONTAINS 侧索引要求所选键包含全部主键。空值匹配遵循内置方言的普通唯一约束语义，MySQL 不会把其他唯一约束命中的行当作指定业务键冲突。

### 满足条件才进行冲突更新

```java
BatchSpec batch = BatchSpec.upsert(users, inputRows)
    .onConflict("email")
    .withUpdateCondition(ConditionGroup.and().where("status", "=", "DRAFT").build());
Mono<BatchExecutionEvidence> result = forms.writeBatch(batch);
```

条件针对数据库中的冲突目标行：满足才更新，false/NULL 跳过，无冲突仍插入。字段转换和参数绑定沿用普通条件；同步客户端使用相同规格。Scope 独立生效，不能用业务条件绕过。PostgreSQL、Oracle、SQL Server、H2 使用原生语法；MySQL 不支持此条件式操作，会明确报错。`ignoreConflicts()` 或没有更新列时不执行更新条件。

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
Flux<String> tables = operator.metadata().readTableNames();         // 当前 Schema 的表和视图
Flux<String> appTables = operator.metadata().readTableNames("app"); // 指定 Schema
Flux<RelationalTableDefinition> structures = operator.metadata().readTables(); // 当前 Schema 全部表结构
Flux<RelationalTableDefinition> appStructures = operator.metadata().readTables("app");
Mono<RelationalTableDefinition> metadata = operator.metadata().readTable("users");
Mono<String> tableComment = metadata.mapNotNull(RelationalTableDefinition::comment);
Flux<DynamicRow> rows = operator.metadata().readForm("existingUsers", "users")
    .flatMapMany(form -> operator.dml().query(form).where("id", 1L).fetch());
```

表名枚举每次只查名称，不缓存、不逐表加载结构；同步入口返回 `List<String>`。数据库元数据不能推断应用的租户、加密或脱敏声明；这些规则仍应由应用模型显式提供。外部工具修改结构后，可用 `operator.metadata().invalidate("users")` 使该表的读取缓存失效。

`readTables` 批量直读字段、主键、索引、外键和表注释，不命中或填充逐表缓存；同步入口返回 `List<RelationalTableDefinition>`。`name()` 是物理表名，`identity()` 分别保存 catalog/schema/table；`comment()` 未声明时为 null。内置查询次数不随表数增长：PostgreSQL、MySQL 各 4 次，其他内置方言各 3 次。它与 `readTable` 使用相同表定义，不代替记录完整观察事实的 `readSnapshot`，也不保证多次字典读取间的事务一致性。

自定义数据库字典读取可直接接入客户端：

```java
FlyingOrmClients clients = FlyingOrmClients.builder(jdbcAccess, r2dbcAccess)
        .dialect(customDialect)
        .metadataReader(jdbcMetadataReader)     // 继承 JdbcFormMetadataReader
        .metadataReader(reactiveMetadataReader) // 实现 ReactiveFormMetadataReader
        .build();
```

读取器原样复用，不额外添加缓存。同步与响应式扩展均实现 `readForm(String formId, RelationIdentity relation)` 和 `readTable(RelationIdentity relation)`：前者只读表单所需列事实，后者读取表结构。字符串便利入口统一转为分段身份；显式传入 schema/table 时，表名中的点号保留为字面值。完整结构规划还需实现 `readSnapshot`、声明真实的 `snapshotCoverage`，并保持 Schema 读取直读数据库。自定义缓存负责失效，响应式缓存还应实现 `MetadataCacheInvalidator`；客户端的 DDL/手动失效会联动已注册的失效器。

### 用同一份动态模型

```java
Mono<Long> changed = operator.ddl().createOrAlter(users);
```

### 用 Java DSL 描述结构

```java
Mono<Long> changed = operator.ddl().createOrAlter("notes")
    .addColumn("id", column -> column.type("BIGINT").primaryKey().generation(ValueGeneration.identity()))
    .addColumn("body", column -> column.varchar(128).comment("正文"))
    .addColumn("code", column -> column.varchar(40).nullable(false).unique(true))
    .addColumn("amount", column -> column.number(18, 4))
    .addColumn("created_at", column -> column.type("TIMESTAMP(6)"))
    .addIndex("ix_notes_created").column("created_at", IndexKeyPart.Direction.DESC).commit()
    .commit();
```

列配置回调返回时自动提交该列，无需手动 `commit()`；原来的 `.addColumn("body").varchar(128).commit()` 写法仍可用。表级 `commit()` 表示结束 DSL 并生成执行 Publisher，不是数据库事务提交。默认不执行破坏性变更；通过 `options(SchemaMigrationOptions.safe().allowColumnChange())` 等选项显式允许后可直接执行。调用 `plan()` 可先看计划，再用 `execute(plan)` 执行。需要方言支持的并发索引语法时，显式设置 `SchemaMigrationOptions.safe().preferConcurrentIndexes()`；会话和上线策略由应用管理。

`addColumn(DynamicField)` 直接复用字段模型的类型、精度和生成策略，不必在列构建器中重新配置。同步入口为 `syncOperator.ddl()`，写法相同，`commit()` 直接返回影响行数。默认值、CHECK 和命名约束可在下面的表配置回调中一起声明。

### 列默认值与数据库表达式

```java
Mono<SchemaMigrationResult> result = operator.ddl().createOrAlter(database, "notes", table -> table
    .addColumn("id", "UUID", column -> column.nullable(false)
        .defaultValue(ColumnDefault.nativeExpression("gen_random_uuid()"))) // PostgreSQL
    .addColumn("status", "VARCHAR(20)", column -> column.defaultValue(ColumnDefault.literal("NEW")))
    .primaryKey(PrimaryKeyDefinition.of("pk_notes", "id"))
    .addCheck(CheckConstraintDefinition.of("ck_notes_status", CheckPredicate.in("status", List.of("NEW", "DONE")))));
```

`database` 是应用提供的 `DatabaseDescriptor`（数据库产品、版本与方言），不包含连接信息。`createOrAlter` 增量调整已声明的对象，保留未声明的现有列和约束。同步入口写法相同，返回 `SchemaMigrationResult`，通过 `steps()` 读取已执行语句。先查看 SQL 使用 `plan(database, table)`，再调用 `execute(plan)`；显式完整替换使用 `planReplacement(database, table)`，目标之外的现有对象会进入删除计划。执行失败向上传播异常，不附加审批或前后回读流程。

配置回调在调用时执行一次并冻结；响应式入口订阅才执行数据库操作。指定 catalog/schema 时，直接传入分段身份，不拼接表名：

```java
Mono<SchemaMigrationResult> result = operator.ddl().createOrAlter(
    database, RelationIdentity.of(null, "sales", "notes"), table -> table
        .addColumn("status", "VARCHAR(20)", column -> column.defaultValue(ColumnDefault.literal("NEW"))));
```

目标 schema 由应用预先创建。已有 `RelationalTableDefinition` 可直接传给 `createOrAlter(database, table)`；需要先查看增量计划时，将定义传给 `plan`。

普通值使用 `literal`，原生表达式只允许可信后端代码提供，不能拼接前端输入；表达式使用目标数据库语法，不自动跨库翻译。回读保留无法归一为普通值的原生 SQL，参与结构比较；不猜测不同 SQL 的语义等价性，数据库改写表达式后应检查实际差异。

### MySQL 枚举列与集合列

```java
Mono<Long> changed = operator.ddl().createOrAlter("tasks")
    .addColumn("status", column -> column.type("ENUM('NEW','DONE')"))
    .addColumn("permissions", column -> column.type("SET('Read','Write')"))
    .commit();
```

成员的大小写、空白和顺序会保留，不要再设置 `length/precision`；成员中的单引号写成 `''`。这是数据库原生类型声明，不是跨库枚举转换，也不替代 Java 枚举的 `@EnumValue` 映射；成员变化仍按现有结构变更流程审核。

反斜杠转义依赖 SQL 模式，不直接作为安全类型接受。`X'...'` 十六进制成员可用于 DDL 渲染，但其含义依赖列字符集，不与引号文本猜测等价；数据库去除尾部空格、改写字面量后的结构差异也不自动忽略。这些声明不保证 `createOrAlter` 回读收敛，使用前应检查数据库实际定义和审核计划。

### 按列尺寸定制 DDL 类型

```java
SchemaDialect schema = SchemaDialect.builder()
    .mapType("label", type -> "VARCHAR(" + type.arguments().getFirst() + ")")
    .build();

DynamicForm labels = DynamicForm.builder("labels", "labels")
    .addField(DynamicField.of("name", "label").withLength(80))
    .build();
List<SqlRequest> ddl = FormSchemaSqlRenderer.create(schema).createTable(labels);
```

固定映射仍可用 `mapType("label", "VARCHAR(80)")`。回调接收合并列尺寸后的 `DatabaseType`，返回完整数据库类型；它由后端注册、需支持并发调用，不能接收前端 SQL。返回声明仍经过类型语法校验，建表、变更与目标结构比较复用该映射；读回元数据不再运行该回调。此配置只改变 DDL 类型，不代替读写值的 codec。

### 定制完整关系模型的 DDL

```java
RdbDialect base = RdbDialect.postgresql();
SchemaDialect schema = base.schema().withOperationRenderer((operation, defaults) -> {
    if (operation.kind() == SchemaOperation.Kind.DROP_TABLE) {
        String table = base.schema().relationIdentifier(operation.relation());
        return List.of(new SqlRequest("drop table if exists " + table, List.of()));
    }
    return defaults.get();
});
RdbDialect customized = base.withSchema(schema);
```

将 `customized` 用于客户端装配。两次 `with...` 都返回新实例，保留原有类型、引号、生成值、分页、锁定读取、版本和能力声明。普通表单与完整关系结构共用操作渲染器，`FormSchemaSqlRenderer` 也通过它渲染结构操作。回调只负责可信后端 DDL，必须无状态、可并发调用；完全替换时负责该操作的全部依赖语句。`defaults.get()` 在当次回调内只计算一次，不可保存或异步调用。结构差异及依赖顺序仍由同一规划器处理。

### 定制动态表单的 DDL

```java
RdbDialect base = RdbDialect.postgresql();
SchemaDialect schema = base.schema().withFormRenderer((current, snapshot, plan) ->
    plan.requests().stream()
        .map(sql -> new SqlRequest("/* form migration */ " + sql.sql(), sql.parameters()))
        .toList());
RdbDialect customized = base.withSchema(schema);
```

将 `customized` 用于客户端装配即可；动态表单的建表、变更和计划生成都会使用该扩展。每张主表、检索辅助表分别渲染，`current` 在新表时为 `null`，`snapshot` 仅在本次规划需要读取物理快照时提供。回调接收已允许的内置计划，必须保留结构、依赖、跳过项和在线执行语义，不能借此扩大变更；空计划不调用，非空计划不能返回空 SQL。

`FormSchemaDdlRenderer` 定制正向 DDL，不生成逆向恢复计划。独立 `createIndexes` 等低层工具不调用此扩展；完整关系模型使用上一节入口。

### 实体注解同步

```java
Mono<EntitySchemaSyncReport> validation = clients.entitySchemas()
    .synchronizeReactive(databaseDescriptor, EntitySchemaSyncMode.VALIDATE, User.class);

Mono<EntitySchemaSyncReport> safeUpdate = clients.entitySchemas()
    .synchronizeReactive(databaseDescriptor, EntitySchemaSyncMode.SAFE_UPDATE, User.class);
```

校验与更新是两种独立选择；JDBC 使用 `synchronize`，参数相同。`databaseDescriptor` 由应用提供实际数据库版本与能力。普通实体、多表外键、CHECK、分区和加密辅助表共用完整结构规划；先读取实际结构，再执行生成的 DDL，不管理外部事务。

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
    .where("phone", ProtectedConditions.EXACT, "13800001234").masked().fetch();

Flux<DynamicRow> suffix = dml.query(contacts)
    .where("phone", ProtectedConditions.SUFFIX, "1234").fetch();

Flux<DynamicRow> contains = dml.query(contacts)
    .where("phone", ProtectedConditions.CONTAINS, "0000").fetch();
```

未配置显式字段可见性策略时，`declaredDisplay()` 遵循字段声明，`masked()` 对已声明脱敏的字段使用脱敏展示。配置 `FieldUsePolicy` 后，以其可见性规则为准：需要脱敏应设置 `FieldVisibility.MASKED`，不能用 `.masked()` 覆盖策略中的 `FULL`。`showSensitive()` 只应在应用已经授权后调用。普通 `=` / `like` 不是加密检索的替代品。响应式 CONTAINS 查询逐行复核并返回最终投影；与普通 Flux 一样，后续失败前可能已返回部分行，需要整批成功结果时使用 `collectList()`。分页在复核完成后形成分页结果。候选读取遵循本次 `SqlExecutionOptions`，不另设固定候选数上限，也不静默截断。

## 模板、原生 SQL 与锁定读取

模板注册及执行、受控原生 SQL 的完整短例见 [高级能力](ADVANCED-CAPABILITIES.md)。

锁定读取仍使用模型和条件：

```java
QuerySpec query = QuerySpec.builder(users)
    .where(ConditionGroup.and().where("id", "=", 1L).build())
    .lock(ReadLock.updateNowait()).build();
Flux<DynamicRow> locked = forms.lockingRead(query);

// 计算列复用相同入口，字段映射和权限规则不变。
QuerySpec amounts = QuerySpec.builder(sales)
    .where(ConditionGroup.and().where("id", "=", 1L).build())
    .expression("subtotal", subtotal)
    .lock(ReadLock.update()).build();
Flux<DynamicRow> lockedAmounts = forms.lockingRead(amounts);
```

锁定语义需数据库支持，其有效范围由上层连接和事务边界决定；ORM 不开启事务。

## 同步调用与扩展配置

### 表级写入扩展

按需给表单注册扩展，动态 DSL、绑定此表单的实体 DSL 和 Repository 共用；不配置时沿普通写入路径执行。

```java
ReactiveWriteInterceptor writes = new ReactiveWriteInterceptor() {
    @Override
    public <R> Mono<R> write(WriteSpec spec, Function<WriteSpec, Mono<R>> next) {
        // beforeWrite / afterWrite 是应用自己的异步处理方法。
        return beforeWrite(spec)
            .then(Mono.defer(() -> next.apply(spec)))
            .flatMap(result -> afterWrite(spec, result).thenReturn(result));
    }

    @Override
    public Mono<BatchExecutionEvidence> batch(
            BatchSpec spec, Function<BatchSpec, Mono<BatchExecutionEvidence>> next) {
        return next.apply(spec).doOnNext(result -> recordBatchResult(result));
    }
};
DynamicForm users = DynamicForm.builder("users", "users")
    .addField(DynamicField.primaryKey("id", "BIGINT"))
    .addField(DynamicField.of("name", "VARCHAR(128)"))
    .features(FeatureRegistry.builder().add(writes).build())
    .build();
Mono<Long> updated = clients.operator().dml().update(users)
    .set("name", "Ada").where("id", 7L).execute();
```

同步对应 `SyncWriteInterceptor`：`<R> R write(WriteSpec spec, Function<WriteSpec, R> next)` 直接调用同步 `next`，批量返回 `BatchExecutionEvidence`。两种扩展可注册在同一表单中；保持默认 ID，父层配置通过已有 `FeatureRegistry.parent(...)` 继承，本层同 ID 覆盖。扩展实例须可并发复用。

`next` 可接收派生规格（例如 `spec.withScope(...)`）；返回键插入保留 `SqlWriteResult`，普通写入保留影响行数。批量回调针对整次操作，不预收集输入；返回证据仍须检查实际状态，替换规格须保持行顺序与生成键、逐行回调的对应关系。响应式前后处理每次订阅执行，不自行 `subscribe()`。原生 SQL 不触发该扩展。

实体 PRE/POST 仍围绕整个实体写入，条件式命令没有实体事件。后置处理只说明 SQL 已执行，不代表事务提交；后置处理失败也不代表 SQL 未执行，ORM 不替应用重试或补偿。

### 数据库异常转换

```java
RdbDialect dialect = RdbDialect.postgresql().withExceptionTranslator(
    RdbExceptionTranslator.firstMatch(
        error -> error.kind() == RdbErrorKind.CONSTRAINT
            ? new IllegalArgumentException("数据违反约束", error) : error,
        error -> error)); // 返回原对象表示本转换器不处理
```

装配客户端或 JDBC/R2DBC 执行器时使用这个 `dialect`。转换器按声明顺序匹配，首个返回不同异常的结果生效；应使用线程安全的无副作用函数。原始数据库异常保留在上述示例的 cause 链中。普通业务异常不转换；批量仍抛出 `BatchExecutionEvidenceException`，保留真实执行事实，只转换其 cause。生成键读取失败的已影响行数载体保持不变。不配置则使用默认分类，不增加重试或恢复。

### 同步读取

```java
SyncDmlOperator sync = clients.syncOperator().dml();
List<DynamicRow> rows = sync.query(users).where("org_id", 7L).fetch();
DynamicRow one = sync.query(users).where("id", 1L).one(); // 无匹配时为 null
BatchExecutionEvidence evidence = sync.insertBatch(users, inputList);

List<User> entities = clients.syncRepository(User.class).createQuery()
    .where(User::getOrgId, 7L).fetch();

// 不需要 Repository 时，直接绑定实体；保留当前客户端的映射和默认 Scope。
List<User> selected = sync.entity(User.class).query().where(User::getOrgId, 7L).fetch();
```

JDBC 是同步阻塞调用，不放进 Reactor 事件循环。R2DBC 不使用隐藏 `block/subscribe`。

同步逐行消费，不需要先取得结果列表：

```java
long consumed = sync.query(users).where("org_id", 7L)
    .forEach(row -> consume(row));

long entityCount = clients.syncRepository(User.class).createQuery()
    .where(User::getOrgId, 7L).forEach(user -> consumeUser(user));

long dtoCount = sync.query(users).select("id", "name")
    .forEach(UserView.class, view -> consumeView(view));

// 处理当前行后决定是否继续；false 正常停止，返回数包含该行。
long visited = clients.syncRepository(User.class).createQuery()
    .where(User::getOrgId, 7L).forEachWhile(user -> {
        consumeUser(user);
        return shouldContinue(user); // 应用自己的继续条件
    });
```

`consume`、`consumeUser`、`consumeView` 是应用自己的处理方法。返回成功交付的行数；表单和实体入口保留 Scope、字段治理、解码、检索复核和脱敏，实体入口也保留加载回调。原生 JDBC 实现不收集结果列表，回调在读取期间执行，异常会停止读取并清理资源；不要在回调里重入同一连接。驱动是否缓存结果由上层配置，自定义执行器的默认实现可能仍先读取列表。原有 `fetch()` 行为不变。

同步模板可用 `clients.syncOperator().withSqlTemplates(registry, parameterProvider)` 派生门面，再调用 `sqlTemplate(id)`；`parameterProvider` 为 `SyncSqlTemplateParameterProvider`，可信参数每次执行时读取。原门面和其他配置不变。

高级配置集中在装配阶段：`renderer`（条件/codec）、`executionOptions`（读取保护）、`batchWriteOptions`（批量预算）、`cachePolicy`、`idGenerator`、`fieldFiller`、`entitySchema`、`protectedFields`、`sqlTemplates`、`observers`。一次请求的特殊执行选项仍可通过 QuerySpec/WriteSpec 或已有终端重载传入，不需要重新创建整套客户端。

### 字段级类型映射

同一个 Java 类型可以在不同字段使用不同存储方式。`registerNamed` 只作用于显式选中该 ID 的字段，不改变其他字段的默认转换：

```java
enum AccountStatus { ACTIVE, DISABLED }

@TableName("accounts")
record Account(@TableId Long id,
               AccountStatus statusName,
               @TableColumn(databaseTypeId = "status-number") AccountStatus statusNumber) {}

ValueCodec numericStatus = new ValueCodec() {
    public boolean supports(Class<?> type) { return type == AccountStatus.class; }
    public Object write(Object value) {
        if (value == null) return null;
        return switch ((AccountStatus) value) {
            case ACTIVE -> 1;
            case DISABLED -> 0;
        };
    }
    public Object read(Object value, Class<?> type) {
        if (value == null) return null;
        return switch (((Number) value).intValue()) {
            case 1 -> AccountStatus.ACTIVE;
            case 0 -> AccountStatus.DISABLED;
            default -> throw new IllegalArgumentException("unknown account status");
        };
    }
};
EntityTypeMappingRegistry mappings = EntityTypeMappingRegistry.builder()
    .registerNamed("status-number", AccountStatus.class, DatabaseType.of("INTEGER"), numericStatus)
    .build();
EntitySchemaDescriptor<Account> accountSchema = EntitySchemaDescriptor.builder(Account.class)
    .typeMappings(mappings).build();
FlyingOrmClients mappedClients = FlyingOrmClients.builder(access)
    .dialect(RdbDialect.postgresql()).entitySchema(accountSchema).build();

Flux<Account> active = mappedClients.repository(Account.class).createQuery()
    .where(Account::statusName, AccountStatus.ACTIVE)   // 绑定 "ACTIVE"
    .where(Account::statusNumber, AccountStatus.ACTIVE) // 绑定 1
    .fetch();
Mono<Long> changed = mappedClients.repository(Account.class).createUpdate()
    .set(Account::statusNumber, AccountStatus.DISABLED) // 绑定 0
    .where(Account::id, 1L).execute();
```

读回、条件、SET 和 Schema 共用该字段映射。`register` 仍用于类型的默认映射；`registerNamed` 可为同类型注册多个不同 ID。使用 `@EnumValue` 时，具名映射的 Java 类型是该枚举值成员的类型，例如 `String.class`，不是枚举类本身。同一客户端的实体描述复用同一个 `mappings` 实例。
