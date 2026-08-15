# V1 后置增强使用说明

这一批能力仍属于 flying-orm 主项目，全部是纯 Java/R2DBC API，不需要 Spring。目标数据库认证已经完成；
新增入口仍保留独立契约测试，生产升级时可以按认证脚本复跑当前数据库和驱动组合。

## PostgreSQL Vector

动态字段使用逻辑类型 `VECTOR`，字段长度就是向量维度。写入接受 `float[]`、其他数字数组或数字集合，进入驱动前会校验维度、空值和 NaN/Infinity。

```java
DynamicForm form = DynamicForm.builder("documents", "documents")
                              .addField(DynamicField.primaryKey("id", "BIGINT"))
                              .addField(DynamicField.of("embedding", "VECTOR").withLength(2))
                              .build();

StructuredConditionInput input = StructuredConditionInput.term(
        "embedding",
        "vector-l2-lt",
        Map.of("vector", List.of(0.12F, 0.34F), "distance", 0.8D));
ConditionGroup where = VectorStructuredConditions.postgresql().compile(form, input);
```

`vector-l2-lt`、`vector-cosine-lt`、`vector-inner-product-gt` 只使用固定 PostgreSQL 运算符，向量和阈值仍走参数绑定。非 PostgreSQL 方言会明确报告不支持，不会退回字符串拼接。

## 受控 SQL 模板

模板必须在服务端启动阶段注册。业务调用只传模板 ID 和结构化参数，前端不能把 SQL 文本交给执行器。

```java
SqlTemplateRegistry registry = SqlTemplateRegistry.builder()
        .register(SqlTemplate.query("active-users",
                                    "select * from ${table} where status = :status",
                                    Set.of("table")))
        .build();

SqlRequest request = SqlTemplateEngine.create(registry,
                                               RdbDialect.postgresql(),
                                               ValueCodecRegistry.standard())
        .render("active-users",
                Map.of("status", "ACTIVE"),
                Map.of("table", "sys_user"));
```

值参数必须和模板中的 `:name` 完全一致。`${name}` 只用于注册时明确声明的标识符槽，并经过当前方言的标识符校验和引号处理。模板拒绝多语句分号，并能跳过引号内文本和 PostgreSQL `::cast`。

## 实体映射语义和事件

V2 使用 `com.flying.orm.core.annotation` 下的 flying-orm 自有注解，不读取 Jakarta Persistence 或 Javax Persistence
实体注解。当前入口包括：

- `@TableName`、`@TableId`、`@KeySequence`
- `@TableField`、`@Version`、`@TableLogic`、`@EnumValue`、`@OrderBy`

`IdType.AUTO` 会省略数据库自增列并读取生成键；`ASSIGN_ID`、`ASSIGN_UUID` 在 SQL 前生成；字段填充和空值策略由
`@TableField` 的 `fill`、`insertStrategy`、`updateStrategy` 控制。生命周期扩展使用 flying-orm 自己的
`ReactiveEntityListener` 和 `EntityLifecyclePhase` 契约，不通过注解全名反射调用。

```java
EntityMappingListener listener = new EntityMappingListener() {
    @Override
    public void beforeWrite(EntityMappingEvent event) {
        audit(event.metadata().table(), event.values());
    }
};

RowMapper<User> mapper = RowMapper.of(User.class, listener);
```

写入事件通过 `ReactiveFormRepository.withListener(listener)` 或同步 Repository 的同名方法安装。
监听器可能被多个响应式订阅并发调用，不能保存单次行状态，也不应执行阻塞网络调用。

## 数据库、Schema 和 RLS 隔离

隔离位于 `ConnectionFactory` 层，因此普通 CRUD、批量、事务和 UNKNOWN 回执查询都会经过同一条路。

```java
RoutingConnectionFactory routing = new RoutingConnectionFactory(
        defaultFactory,
        databaseKey -> tenantPools.get(databaseKey),
        new PostgresqlRlsSessionCustomizer());

IsolationContext isolation = IsolationContext.database("tenant-7", "tenant-db-7")
                                            .withSchema("tenant_7")
                                            .withRlsSettings(Map.of("app.tenant_id", "tenant-7"));

Mono<List<DynamicRow>> result = IsolationContexts.with(
        formClient.select(form, where).collectList(),
        isolation);
```

上层负责数据库键到连接池的映射、当前租户识别和鉴权。所有路由连接池必须使用相同数据库类型；连接工厂元数据来自默认池，用于执行器选择 bind marker。PostgreSQL 会话在连接借出后设置，归还池前逐项清理；清理失败会作为关闭错误上报，连接池应配置失效连接淘汰策略。

## 回滚和在线 DDL 审核

先使用原有 `migrateSafelyPlan(...)` 生成正向计划，再用当前数据库元数据做上线前审核。

```java
ReviewedSchemaMigrationPlan reviewed = SchemaMigrationReviewer.create(FormSchemaSqlRenderer.create(dialect))
        .review(currentTable,
                migrationPlan,
                SchemaMigrationReviewPolicy.requireOnline());

List<SqlRequest> forward = reviewed.requestsForExecution();
SchemaRollbackPlan rollback = reviewed.rollback();
```

`ALLOW_BLOCKING` 允许普通 DDL，`PREFER_ONLINE` 标出需要外部在线工具或维护窗口的语句，`REQUIRE_ONLINE` 则直接拒绝潜在阻塞语句。回滚 SQL按反向顺序生成；删列、主键、外键和大索引等无法自动恢复的部分会进入 `SchemaRollbackGap`，必须结合备份和人工审核处理。
# 原生参数化 SQL

复杂联表、CTE、窗口函数或数据库专有语法可以直接使用后端代码中的单条 SQL：

```java
Flux<DynamicRow> rows = operator.sql("""
        select id, name
        from user_info
        where tenant_id = :tenantId and state = :state
        """)
        .bind("tenantId", tenantId)
        .bind("state", "ACTIVE")
        .query();

Mono<Long> affected = operator.sql("""
        update user_info
        set state = :state
        where tenant_id = :tenantId and id = :id
        """)
        .bindAll(Map.of("state", "DISABLED", "tenantId", tenantId, "id", userId))
        .update();
```

参数只按 SQL 中的出现顺序转换成 R2DBC 位置参数，不能用字符串拼接业务值。原生 SQL 不会自动追加
TenantScope、DataScope、逻辑删除或乐观锁条件，必须在 SQL 中明确写出并绑定可信服务端参数。SQL 正文不能来自前端。

## 游标分页和深分页

```java
CursorPageQuery page = CursorPageQuery.after(
        100,
        previousCursor,
        CursorSort.desc("createdAt"),
        CursorSort.asc("id"));

Mono<CursorPageResult<DynamicRow>> result = forms.cursorPage(
        eventForm,
        where,
        page);
```

游标按排序字段顺序保存结构化值，不接收 SQL。复合排序会生成字典序 seek 条件并多取一行判断是否还有下一页，
不会执行 count，也不会生成随页数增长的大 offset。排序末尾必须带稳定唯一键；字段受 DynamicForm、FieldScope
和方言标识符规则保护。同步业务使用 `SyncFormClient.cursorPage(...)`，底层仍走同一条 R2DBC 链。

## 数据迁移和数据级补偿

```java
DataMigrationPlan plan = DataMigrationPlan.builder("backfill-user-state")
        .step("active-users",
              new SqlRequest("update users set state=? where enabled=?", List.of("ACTIVE", true)),
              new SqlRequest("update users set state=? where enabled=?", List.of("OLD", true)))
        .build();

Mono<DataMigrationResult> result = ReactiveDataMigration.create(executor, executionOptions)
        .execute(plan);
```

正向步骤按声明顺序执行；任一步失败时，只把已经成功的步骤按相反顺序执行补偿 SQL。失败异常携带
`DataMigrationResult`，状态会明确区分 `ROLLED_BACK` 和 `ROLLBACK_FAILED`。这套能力处理数据行补偿，不声称
DDL 反向 SQL 能恢复删列前的数据；高风险上线仍需要备份、审核和维护窗口。

## 缓存指标和驱动适配

```java
MetadataCacheMetricsBridge.export(cachedReader.snapshot(), meterSink::record);

ValueCodecRegistry codecs = ValueCodecRegistry.standard()
        .withDriverAdapter(customDriverAdapter);

RowMapper<User> mapper = RowMapper.of(User.class, codecs)
        .withAliases(Map.of("display_name", "name"));
```

指标桥输出 `flying.orm.metadata.cache.*` 稳定名称，上层自行接 Micrometer、OpenTelemetry 或其他监控系统。
`DriverValueAdapter` 只负责把可选驱动包装值拆成普通 Java 值，之后仍走统一 codec 校验。列别名映射不会修改
驱动返回的 Map，也不会污染共享反射计划；默认映射同时兼容带表限定符和常见引用符的列标签。
