# flying-orm 常用正式能力

本页承接 [README](README.md) 的 DynamicForm 主路径。这里列出的是 `3.2.0` 提供的公开能力。

## 分页与游标分页

`ReactiveFormClient` 和 `SyncFormClient` 都支持普通分页与游标分页。查询条件、Scope、投影、排序和敏感字段显示策略仍来自同一个 `QuerySpec`，不会形成第二套 SQL 语义。

```java
QuerySpec query = QuerySpec.of(userForm, where)
        .withSorts(List.of(PageSort.asc("id")));

Mono<PageResult<DynamicRow>> page = forms.page(query, PageQuery.of(1, 50));
```

既有 `CursorPageQuery` 保留非空游标语义。对 nullable、复合排序或混合方向，使用新的类型化 keyset 入口：

```java
KeysetPageQuery first = KeysetPageQuery.first(
        50,
        KeysetSort.desc("created_at", NullOrder.LAST),
        KeysetSort.asc("id", NullOrder.LAST));

Mono<KeysetPageResult<DynamicRow>> page = forms.keysetPage(query, first);
```

`KeysetPageResult.nextPosition()` 保留完整类型位置，上层原样保存后传回 `KeysetPageQuery.after(...)`。planner 只从完整主键或已确认的非空唯一约束补齐稳定 tie-breaker；补齐列只用于位置，会在返回业务行前移除。无法证明稳定性或方言空值语义未声明时直接拒绝，不静默退回 offset。大数据量连续读取优先使用 keyset，普通跳页使用 offset 分页。

## 轻量 JOIN

JOIN 使用不可变 `JoinQuerySpec`，支持 `JoinType` 当前声明的 INNER、LEFT 和 RIGHT。源级租户、Scope 和逻辑删除条件在各自数据源内生效，避免外连接被最终 WHERE 意外收紧。

```java
JoinQuerySpec.Builder join = JoinQuerySpec.builder(userForm);
JoinSource user = join.root();
JoinSource department = join.join(
        JoinType.LEFT, departmentForm, user, "department_id", "id");

JoinQuerySpec query = join
        .select(user, "id")
        .select(user, "name")
        .selectAs(department, "name", "department_name")
        .build();

Flux<DynamicRow> rows = forms.selectJoin(query);
```

JOIN 面向受控等值关联和常规多表读取；复杂数据库专有查询可使用正式的模板或受控原生 SQL 能力。

## 参数条件与结构化条件

- `ConditionGroup`：Java 代码直接构建 AND/OR 条件树。
- `ParameterConditionCompiler`：把预先声明的请求参数规则编译为条件树。
- `StructuredConditionInput`：接收前端结构化条件，并通过字段、operator、深度和容量策略校验。
- `TermRegistry`：注册受控扩展 term；扩展处理器仍必须返回参数化 SQL 片段。

```java
ConditionGroup where = ConditionGroup.and()
        .whereIfPresent("status", "=", status)
        .or(group -> group
                .where("name", "like-ignore-case", keyword)
                .where("code", "like-ignore-case", keyword))
        .build();
```

字段名和 operator 必须来自应用允许的规则；结构化输入不能作为任意 SQL 文本入口。

## Scope、逻辑删除与乐观锁

flying-orm 正式支持 `TenantScope`、`DataScope`、`FieldScope`、`TimeScope`、逻辑删除和乐观锁。这些规则在 SQL 计划阶段组合，而不是查询后在内存中过滤。

- 默认 Scope 可以在客户端装配时声明，请求级 Scope 可以通过 `QuerySpec.withScope` 或 `WriteSpec.withScope` 收紧。
- 逻辑删除由 `DynamicForm.logicDelete(...)` 或实体注解声明。
- 乐观锁写入使用 `OptimisticLockOptions`，影响行数为零时由调用方按业务冲突处理。
- FieldScope 的交集为空表示无字段权限，不会被解释成“全部放行”。

## 字段用途与查询预算

`FieldUsePolicy` 按字段分开授予投影、明文/脱敏展示、过滤、HAVING、排序、JOIN、分组、聚合、插入和更新用途。它与 `FieldScope` 只求交集，不能放宽 Scope。租户、逻辑删除、版本列和 keyset tie-breaker 以独立的 `INTERNAL_*` 来源授权，永不反向变成 caller 可见字段。

```java
FieldUsePolicy policy = FieldUsePolicy.builder()
        .visibility("id", FieldVisibility.FULL)
        .visibility("name", FieldVisibility.MASKED)
        .allow("status", FieldUse.FILTER)
        .build();

ReactiveFormClient governed = forms
        .withFieldUsePolicy(policy)
        .withQueryShapeLimits(QueryShapeLimits.defaults()
                .withMaxProjectionCount(20)
                .withMaxSortCount(4)
                .withMaxBindCount(100));
```

`previewFieldUse(...)` 与执行入口复用同一审批逻辑，但不获取连接、不执行 SQL。`QueryShapeLimits` 可以限制 projection、JOIN、group、aggregate、HAVING、sort、bind 数和 SQL 长度；默认值不收紧旧入口。只有绑定治理策略的视图进入这条路径，未启用时继续复用静态快路。

## 类型化常用聚合

`AggregateSpec` 提供 COUNT、COUNT DISTINCT、SUM、AVG、MIN 和 MAX，支持分组和参数化 HAVING。COUNT 固定返回 `Long`，SUM/AVG 固定返回 `BigDecimal`，MIN/MAX 按字段逻辑类型和 codec 校验。

```java
AggregateExpression<Long> count = AggregateExpression.count("id", "user_count");
AggregateSpec aggregate = AggregateSpec.builder(QuerySpec.of(userForm, where))
        .group(GroupSelection.of("status", "status"))
        .aggregate(count)
        .build();

Flux<AggregateRow> rows = forms.aggregate(aggregate);
```

HAVING 只能引用已声明的分组别名或聚合别名。Scope、逻辑删除、字段用途、保护语义和查询预算在同一 planner 内生效；JDBC/R2DBC 和 Entity/DynamicForm 共享一套 SQL 与结果布局。

## 批量写入

`BatchSpec` 支持 INSERT、UPSERT 和乐观锁 UPDATE，执行模式包括：

- `ATOMIC`：整批在 flying-orm 自有事务中提交或回滚；外部事务存在时参与外部事务边界。
- `INDEPENDENT`：按分片独立完成，适合允许部分成功并需要分片结果的场景；在不兼容的外部事务边界中会拒绝执行。

批量具有最大行数、分片大小、并发度、内存预算、结果分片数和超时边界。恢复回执用于明确配置的幂等恢复，不会默认改变普通批量语义。该能力当前由 R2DBC 批量执行器提供；JDBC 保留 ATOMIC/INDEPENDENT 批量，并在订阅输入 Publisher 和获取连接前拒绝回执恢复配置。

新的 `writeBatchEvidence(...)` 只返回当前时点可以证明的 SQL 执行事实，不改变原 `writeBatch(...)` 和 `BatchWriteResult` 语义。`BatchAffectedRows` 明确区分 `KNOWN(value)` 与 `UNKNOWN`；失败、超时或部分完成通过 `BatchExecutionEvidenceException` 保留已形成的不可变证据。外部事务通常返回 `PENDING_EXTERNAL`，上层事务管理器才能决定最终业务成功；flying-orm 不等待同一事务的 completion，也不接管外部事务。

## 实体映射与 Repository

实体 Repository 与 DynamicForm 共享映射、条件、Scope、逻辑删除、乐观锁、字段保护和执行器，不是另一套 ORM 内核。

```java
ReactiveFormRepository<UserEntity> users = clients.repository(UserEntity.class);
SyncFormRepository<UserEntity> syncUsers = clients.syncRepository(UserEntity.class);
```

响应式 Repository 返回 Reactor 类型；同步 Repository 使用 JDBC。主键类型由实体元数据和 Repository 方法契约解析，不要求再维护独立的运行时表定义。

实体的非数据库属性统一使用 `@TableField(exist = false)` 或 Java `transient`。它们不参与查询投影、实体取值、插入、更新、批量或 DDL；对应的 bean/record 构造位置由映射计划保留 Java 默认值。请不要叠加列、主键、索引或约束注解；这类冲突会被结构编译器拒绝。

实体关系结构由同一份启动期描述符编译：表/catalog/schema、表和列注释、列类型/长度/精度/默认值/生成方式、命名主键、唯一约束、复合索引及方向、外键及引用动作、受控 CHECK 都进入不可变 `RelationalTableDefinition` 和稳定指纹。属性名到列名仍复用现有实体映射规则；`Map<String, Object>` 等结构化属性通过明确注册的 JSON 类型映射承接，不把 hsweb/easy-orm 的 `@Comment`、`@ColumnType` 或 `@JsonCodec` 变成 flying-orm 依赖。

## Schema 与元数据

Schema 能力包括数据库元数据读取、纯函数 diff、风险审核、多表依赖与 FK 环的两阶段计划、显式同步、迁移执行、回滚计划和观测。`EXACT`、`ROLLING_COMPATIBLE` 和 `SAFE_INCREMENTAL` 是三种明确的兼容边界，`INCOMPATIBLE` 是比对结果，不是绕过审核的强制开关。

`ReviewedSchemaPlan` 冻结数据库描述、capability 指纹、desired/actual 指纹、精确 `SqlRequest`、顺序、风险和前置条件。执行前重读 actual，不一致则以 `PRECONDITION_FAILED` 结束且不执行 SQL；执行后再重读并验证目标结构。因此“SQL 已发送”或 `rowsUpdated` 不会被冒充为“Schema 已收敛”。

`EntitySchemaSynchronizer.synchronizeRelational(...)` 和 `synchronizeRelationalReactive(...)` 直接使用实体描述符中的完整关系模型，不再降级为旧的 `DynamicForm + index` 投影。`VALIDATE` 只比对；`SAFE_UPDATE` 只接受低风险且无需人工 SQL 的增量；`FULL_UPDATE` 对非低风险计划要求与审核计划指纹完全一致的批准。响应式入口保持冷发布器并按表串行执行，JDBC/R2DBC 都只执行已经审核冻结的 SQL。

MySQL 表或列注释含反斜线时，上层必须把每个 Schema 同步连接配置为 `NO_BACKSLASH_ESCAPES`。flying-orm 会在第一条相关 DDL 前抽取一个 Schema 连接做 fail-fast 校验，不满足则以 `EXECUTOR_CAPABILITY_REQUIRED` 拒绝且不发送 DDL；ORM 不修改 `sql_mode`，也不把这次抽样冒充为异构连接池的同连接证明。正确性契约仍是所有 Schema 连接配置一致；不含反斜线的注释不会执行该查询，普通 CRUD 路径也不受影响。

执行后成功结论依赖元数据读取器能回读被比较的全部事实。`SchemaSnapshotCoverage` 会把读取器可稳定观察的事实和指纹冻结进审核计划；审阅与执行使用的 coverage 不一致时，执行前置条件失败。内置 PostgreSQL、MySQL、Oracle、SQL Server 和 H2 读取器均提供 complete coverage，并把表与列、PK、UK、索引、FK、CHECK、默认值、生成方式及注释转换为完整快照；第三方部分读取器仍保留 `UNKNOWN` 并在审阅阶段生成单一人工步骤和零 SQL，不会把未知当作相等，也不会先执行再失败。

flying-orm 不替代企业迁移平台，也不会在普通 CRUD 热路径自动执行 DDL。生产环境应把 Schema 权限与业务 DML 权限分离。

## 字段加密、保护搜索与脱敏

字段保护只对以下显式声明生效：

- 实体字段上的 `@EncryptedField` 或 `@MaskedField`。
- `DynamicForm.Builder.encrypted(...)` 或 `masked(...)`。

未声明字段不会自动加密、生成搜索 token 或脱敏。上层服务只需提供版本化密钥材料；密钥来源、部署配置和权限体系不进入 flying-orm。

- EXACT：使用字段和租户隔离的搜索 token 进行精确匹配。
- SUFFIX：按声明的后缀长度生成 token，适合手机号后几位等明确需求。
- CONTAINS：按受控长度生成更多 token，索引和写入成本更高，只应在明确需要时启用。
- 脱敏：控制结果展示，不等同于解密授权；调用方通过声明的显示模式选择默认、脱敏或完整显示。

字段保护适用于任意显式声明的业务字段，不绑定手机号或身份证等固定字段类型。

## 错误处理

公开异常和执行结果提供稳定的分类信息；SQL 日志默认不应输出原始敏感值。应用可以接入自己的日志、指标和告警体系，而不需要引入 flying-orm 专属监控运行时。

## 继续阅读

- [五分钟上手](README.md#五分钟上手)
- [专业正式能力](ADVANCED-CAPABILITIES.md)
