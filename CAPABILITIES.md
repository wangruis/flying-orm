# flying-orm 常用正式能力

本页承接 [README](README.md) 的 DynamicForm 主路径。这里列出的是 `4.0.0` 提供的公开能力。

首次接入请先看 [Spring Boot 与普通 Java 完整示例](README.md#上层接入示例)：包含使用方 POM、环境配置、建表前提、读写代码与资源关闭。

本页已同步 **2026-09-09 本轮职责调整**，尚未运行本轮回归与质量门禁。下面的“支持”限定于公开合同及方言已声明的形状，不表示任意数据库结构均能自动迁移，也不表示已完成当前源码的外部实库认证。

| 使用场景 | 主要入口 | 4.0.0 当前边界 |
| --- | --- | --- |
| 动态表单与实体读写 | `QuerySpec`、`WriteSpec`、Repository | 共用 SQL、字段 codec、Scope 和结果映射。 |
| 同表自关联与字段治理 | `JoinQuerySpec`、`JoinSource` | 按来源分别授权、过滤和展示，别名不作为授权身份。 |
| 批量范围内更新 | `BatchSpec.upsert(...).withScope(...)` | 在冲突更新 SQL 内核验原目标行；范围外失败，新行遵循 INSERT 合同。 |
| 实体注解同步结构 | `EntitySchemaSynchronizer` | 保护物理投影、精确审阅、执行前核验、DDL 和回读验证；不在 CRUD 中自动建表。 |
| 可空字段唯一 | `TableUnique.nullPolicy` | 显式 DISTINCT 映射五方言等价语义；默认行为不变。 |
| 加密、检索与脱敏 | `EncryptedField`、`MaskedField`、表单保护声明 | 显式启用；隐藏列和辅助关系由 ORM 维护，不要求第二套业务 DDL。 |

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

同一物理表可以作为多个独立来源，包括重复使用同一个 `DynamicForm`。自关联使用每次返回的
`JoinSource` 表达“员工”“主管”等角色，不需要伪造另一张表或另写 SQL：

```java
DynamicForm employeeForm = DynamicForm.builder("employees", "employees")
        .addField(DynamicField.primaryKey("id", "BIGINT"))
        .addField(DynamicField.of("manager_id", "BIGINT"))
        .addField(DynamicField.of("name", "VARCHAR(80)"))
        .build();
JoinQuerySpec.Builder selfJoin = JoinQuerySpec.builder(employeeForm);
JoinSource employee = selfJoin.root();
JoinSource manager = selfJoin.join(JoinType.LEFT, employeeForm, employee, "manager_id", "id");
JoinQuerySpec selfQuery = selfJoin.selectAs(employee, "name", "employee_name")
        .selectAs(manager, "name", "manager_name").build();
Flux<DynamicRow> selfRows = forms.selectJoin(selfQuery);
// 同步使用 syncForms.selectJoin(selfQuery)，仍是相同来源与 SQL 计划。
```

对各来源分别调用 `scope(source, ...)`、`where(source, ...)`、`orderBy(source, ...)`。
实体可复用 `EntityModelRegistry` 编译出的 `EntityMetadata.toDynamicForm()`，不必重复维护表结构。
仅按 form 对象或实体 Class 定位的 DatabaseOperator 简便 JOIN 不能区分同对象/同类的两个角色，
因此在歧义发生时明确指向上述来源式入口；不同 form/实体类映射同一物理表时可继续使用简便入口。

JOIN 面向受控等值关联和常规多表读取；复杂数据库专有查询可使用正式的模板或受控原生 SQL 能力。

受治理 JOIN 的字段用途始终保留 `JoinSource + field` 的来源身份。两个来源存在同名字段时，PROJECT、FILTER、SORT、JOIN、FULL、MASKED 和 HIDDEN 决策分别按来源审批；表别名、JOIN 别名和投影别名只影响 SQL/结果表达，不会改变授权身份。缺少任一来源字段授权时，在申请连接或生成 SQL 前失败。

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

内建 `relationExists` / `relationNotExists` handler 自身发布稳定的 `FILTER` descriptor，受治理查询直接消费已经注册的 handler，不根据裸字段重新构造。经校验的关联表、关联列和外层 correlation identity 会完整保留，关联值继续使用参数绑定；启用治理不会改变相关子查询的来源关联。

注册关系谓词不会替换普通字段的条件处理器；同一查询可继续组合已声明加密字段的 EXACT 过滤。实体字段的显式 codec 同样用于该字段的条件值编码，不能先按通用逻辑类型改写值再交给应用 codec。

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

- `ATOMIC`：非空整批必须参与上层管理的外部事务；ORM 不开始、提交或回滚该事务。缺少前提时在业务 SQL 前报 `IllegalStateException`，不降级为逐语句自动提交；空批保留无工作结果。
- `INDEPENDENT`：仅显式选择该模式时，允许按片自有局部事务、提交及失败回滚，适合允许部分成功并需要分片结果的场景；在不兼容的外部事务边界中会拒绝执行。输入被切成多个片本身不构成这一授权。

UPSERT 合并默认 Scope 与 `BatchSpec.withScope(...)`，在同一条 SQL 内约束已有冲突目标行。五个内置方言只在条件为 TRUE 时更新；FALSE 或 NULL 明确产生数据库失败，不会静默更新范围外记录。同步/响应式的结果、执行证据、分片和 Repository 共用该计划。新行沿用 INSERT 的租户值与字段权限规则；没有更新列时保持原幂等 no-op。无行范围或纯 FieldScope 的既有 SQL 不变。自定义方言未实现目标 Scope 时，在写入前明确拒绝。不能把提交值属于范围误当作原目标行属于范围，也不能把独立分片的失败结果当作整批原子回滚。

批量保留最大行数、分片大小、并发度、内存预算和结果分片数等输入/结果边界；整体执行时限由上层拥有。正批量 timeout、旧 `RECEIPT` 和确认时限配置在配置边界明确报不支持，JDBC/R2DBC 均不再执行 ORM 内置回执治理；旧 `resolveUnknown` 签名保留但明确拒绝。幂等、回查、重放和事务结果恢复由上层负责，不静默忽略旧配置。

新的 `writeBatchEvidence(...)` 只返回当前时点可以证明的 SQL 执行事实，不改变原 `writeBatch(...)` 和 `BatchWriteResult` 语义。`BatchAffectedRows` 明确区分 `KNOWN(value)` 与 `UNKNOWN`；失败、超时或部分完成通过 `BatchExecutionEvidenceException` 保留已形成的不可变证据。外部事务通常返回 `PENDING_EXTERNAL`，上层事务管理器才能决定最终业务成功；flying-orm 不等待同一事务的 completion，也不接管外部事务。

整批事务由上层管理；允许的分片局部事务不是通用事务管理器或分布式事务能力。已确认的提交/回滚事实不能被后续上层/驱动超时、通知或清理异常改写；结果未知时也不会猜测成功。执行证据不替代上层恢复、业务任务调度或自动重试。

## 实体映射与 Repository

实体 Repository 与 DynamicForm 共享映射、条件、Scope、逻辑删除、乐观锁、字段保护和执行器，不是另一套 ORM 内核。

```java
ReactiveFormRepository<UserEntity> users = clients.repository(UserEntity.class);
SyncFormRepository<UserEntity> syncUsers = clients.syncRepository(UserEntity.class);
```

响应式 Repository 返回 Reactor 类型；同步 Repository 使用 JDBC。主键类型由实体元数据和 Repository 方法契约解析，不要求再维护独立的运行时表定义。

实体的非数据库属性统一使用 `@TableField(exist = false)` 或 Java `transient`。它们不参与查询投影、实体取值、插入、更新、批量或 DDL；对应的 bean/record 构造位置由映射计划保留 Java 默认值。请不要叠加列、主键、索引或约束注解；这类冲突会被结构编译器拒绝。

实体关系结构由同一份启动期描述符编译：表/catalog/schema、表和列注释、列类型/长度/精度/默认值/生成方式、命名主键、唯一约束、复合索引及方向、外键及引用动作、受控 CHECK 和受控分区声明都进入不可变 `RelationalTableDefinition` 和稳定指纹。属性名到列名仍复用现有实体映射规则；`Map<String, Object>` 等结构化属性通过明确注册的 JSON 类型映射承接，不把 hsweb/easy-orm 的 `@Comment`、`@ColumnType` 或 `@JsonCodec` 变成 flying-orm 依赖。

Java `UUID` 属性统一映射为逻辑 `UUID`。默认物理表示为 PostgreSQL/H2 原生 `UUID`、MySQL `CHAR(36)`、Oracle `VARCHAR2(36)`、SQL Server `UNIQUEIDENTIFIER`。Repository 写值、批量及等值/IN 查询使用对应驱动载体：PG/H2 保留 UUID 对象及 `UUID.class` 空值绑定类型，其余三方言使用标准 UUID 字符串；回读恢复 Java UUID。显式文本字段和应用 codec 的优先级不变，已有明确物理类型声明仍按原身份处理。非 PostgreSQL 的 UUID 数组仍需要明确支持，不能从标量适配推断数组能力。

MySQL 的 `Instant` 需要数据库驱动正确保留时间点并明确时区；已确认 `preserveInstants` 与 UTC 配置下标准映射可用。
这属于驱动配置合同，ORM 不新增猜测时区的 codec，也不自动修改会话时区。

## Schema 与元数据

Schema 能力包括数据库元数据读取、纯函数 diff、风险审核、多表依赖与 FK 环的两阶段计划、显式同步、迁移执行、回滚计划和观测。`EXACT`、`ROLLING_COMPATIBLE` 和 `SAFE_INCREMENTAL` 是三种明确的兼容边界，`INCOMPATIBLE` 是比对结果，不是绕过审核的强制开关。

迁移执行和锁等待预算默认 `ZERO`；正治理预算明确拒绝。ORM 不自动发送会话锁等待参数的 SET/RESET，也不在清理中恢复这类政策。事务前提、会话设置和时限由上层或基础设施提供；SQL 审核、顺序执行、实际状态及回读验证继续保留。

`ReviewedSchemaPlan` 冻结数据库描述、capability 指纹、desired/actual 指纹、精确 `SqlRequest`、顺序、风险和前置条件。执行前重读 actual，不一致则以 `PRECONDITION_FAILED` 结束且不执行 SQL；执行后再重读并验证目标结构。因此“SQL 已发送”或 `rowsUpdated` 不会被冒充为“Schema 已收敛”。

Schema 在外部事务中完成执行与回读，但事务尚未结束时，报告为 `EXTERNAL_TRANSACTION_PENDING`，`successful()` 不返回 true。这个不可变报告描述返回时点的事实，不会替上层确认未来提交，也不因后续回滚继续宣称已成功落库。

`EntitySchemaSynchronizer.synchronizeRelational(...)` 和 `synchronizeRelationalReactive(...)` 使用实体描述符投影出的最终关系模型，不再降级为旧的 `DynamicForm + index` 投影。字段保护产生的密文列、搜索列和按需辅助表，与 CRUD 使用同一物理关系定义；`@TablePartition` 的策略和分区键也沿同一链路进入指纹、DDL、回读和验证。`VALIDATE` 只比对；`SAFE_UPDATE` 只接受低风险且无需人工 SQL 的增量；`FULL_UPDATE` 对非低风险计划要求与审核计划指纹完全一致的批准。响应式入口保持冷发布器并按表串行执行，JDBC/R2DBC 都只执行已经审核冻结的 SQL。

关系身份始终分段保存。`RelationIdentity.table("accounts.v2")` 的点号属于字面表名；catalog/schema/table 限定关系只由 `RelationIdentity.of(...)` 表达。关系感知的 metadata、缓存和失效主链路不会再拆分字面点号。

PostgreSQL 完整快照把 CRUD 使用的逻辑类型与 Schema 使用的物理类型分开：schema-qualified domain、enum、citext、自定义类型、interval、真实数组和经扩展归属证明的 pgvector 都保留物理身份。`DATE` 不携带无意义的小数秒精度，`TIME`/`TIMESTAMP` 的零精度仍保留。PostgreSQL catalog 只能证明数组类型，不能恢复源声明的维数，因此比较时保留“标量或数组”身份而不虚构维数；无法安全表示的 quoted 或 mixed-case 类型会失败关闭。

五个内建方言可为已支持形状的显式 DROP、单事实列 CHANGE 和索引替换生成冻结 SQL。PostgreSQL/MySQL 的候选键替换使用单条 ALTER；MySQL 唯一索引变化也使用单条 ALTER。H2 / SQL Server 的 PK/UK 替换先建立目标唯一保护，再交接最终约束；Oracle 已支持的 UK 替换通过目标索引保护与接管保持连续唯一仲裁。Oracle 保留全部旧主键列的 PK 扩展先建立唯一索引与非空 CHECK 两种保护，新主键显式接管索引并验证后才移除 CHECK，避免旧主键隐含非空随 DROP 消失。安全扩容或 NOT NULL 收紧的先行证明由依赖的 PK/UK/索引复用，不按 JDBC/R2DBC 复制实现。

H2 / SQL Server 的 FK 替换使用不带级联动作的临时外键保护，依次执行建立保护、删除旧外键、建立目标外键、移除保护。它避免 SQL Server 同时存在两条级联路径，但不承诺变更期间父表写入零失败，也不绕过数据库对目标级联图的检查。每条中间 SQL 都进入审阅计划和执行报告；失败后保留可能仍在保护数据的对象，不自动清理或冒充完整成功。

MySQL/Oracle FK 替换提供明确的非原子执行合同：审核分别冻结 DROP 旧外键与 ADD 目标外键，Oracle ADD 使用 ENABLE VALIDATE；MySQL 同名外键不合并到一条 ALTER 中。
`ReviewedSchemaPlan.requiresWritesQuiesced()` 返回 `true`。调用方确认旧、新引用关系涉及的写入已静止后，
使用 `SchemaMigrationApproval.approveWithWritesQuiesced(plan, reason)`；新建且尚无写入者的数据库也适用。
普通批准不会默认声明该前提。JDBC/R2DBC 与实体 FULL_UPDATE 共用批准规则，缺少确认时不执行 DDL。
窗口必须覆盖父子表及会间接触发相关写入的路径，保持到整个计划验证成功；失败或取消后先检查实际结构并恢复，
不得直接重放旧计划。ORM 不实施停写、集群协调、补偿或事务管理；DROP 成功而 ADD 失败如实报告部分完成。
`SchemaMigrationApproval` 保留旧两参构造器和访问器，但 record 新增 `writesQuiesced` 成分，使用旧二元 record 解构的源码需调整。

窄化、生成策略或混合事实变更、未证明安全的候选键替换及不安全 FK 依赖仍进入人工步骤。Oracle 旧主键列退出目标键、同列索引顺序替换以及 H2 CHECK 替换不据此宣称支持。SQL Server 完整 reader 将默认约束名放入 `ColumnDefinition.defaultConstraintName()` 和实际快照指纹，移除默认值或删除所属列使用冻结名称；第三方 reader 未提供名称时不猜测，序列默认值改名也不能被误当成删除默认值。删除整张关系必须显式调用 `reviewRelationalAbsent(...)` 声明 ABSENT 目标，不能由“期望模型缺少该表”推断；执行后还必须回读确认表确实不存在。

同一次实体同步所管理的快照会在第一条 SQL 前检查 self/cross-table FK：仍被引用的 PK、UK、唯一索引或相关列不能被破坏。PostgreSQL 候选键按“列数相同且完整列集相同”匹配，引用列顺序可以不同；其他方言保留有序匹配。该检查只覆盖本次 managed snapshots，不扫描数据库全局，也不是分布式 DDL 治理。

创建计划先建立外键所需的候选键，再添加外键；多表两阶段计划在所有基础表就绪后，先建立全部索引，再闭合全部外键，覆盖跨表外键环引用独立唯一索引的情况。H2/PostgreSQL 回读按数据库证据区分约束拥有的索引与独立索引，不因一个索引被外键引用就把它从结构快照中抹去。

MySQL 安全扩容列时按方言顺序生成完整列定义，保留实际字符集、排序规则等未变属性。新增加密字段时按目标物理顺序追加业务密文列和派生检索列，执行计划与回读比较一致；无法保留旧列顺序的重排仍需人工处理，不能用忽略 `column-order` 差异冒充验证通过。

当前分区原语只支持 PostgreSQL 单列时间 `RANGE`。其他方言遇到分区实体会在 SQL 发送前明确失败关闭，不会静默创建普通表；分区子表创建、范围、留存和归档仍由上层编排。

可空唯一提供显式 `@TableUnique(nullPolicy = UniqueNullPolicy.DISTINCT, ...)`，表示任一键列为 NULL 时不参与唯一仲裁，全部非空时保持唯一。默认 `DEFAULT` 保留原数据库行为。PG/MySQL 的原生 UNIQUE 与该语义等价；H2 显式生成 `UNIQUE NULLS DISTINCT`，不依赖兼容模式默认值；SQL Server 使用受控过滤索引，Oracle 使用每个键都检查完整非空键集合的固定 CASE 索引。实体属性与加密稳定唯一哈希列沿同一关系链进入 DDL、回读、指纹和 diff；不开放任意过滤或表达式索引。PG/MySQL/H2 回读保留观察到的普通唯一事实，只在方言感知 diff 中证明等价，不伪造显式声明的指纹。新语义应使用 relational Schema 入口；旧元数据模型不能无损表示过滤/表达式索引。`UniqueConstraintDefinition` 保留原两参构造器、`of` 和访问器，但 record 新增 `nullPolicy` 成分，使用旧二元 record 解构的源码需要调整。

MySQL 表或列注释含反斜线时，上层必须把每个 Schema 同步连接配置为 `NO_BACKSLASH_ESCAPES`。flying-orm 会在第一条相关 DDL 前抽取一个 Schema 连接做 fail-fast 校验，不满足则以 `EXECUTOR_CAPABILITY_REQUIRED` 拒绝且不发送 DDL；ORM 不修改 `sql_mode`，也不把这次抽样冒充为异构连接池的同连接证明。正确性契约仍是所有 Schema 连接配置一致；不含反斜线的注释不会执行该查询，普通 CRUD 路径也不受影响。

执行后成功结论依赖元数据读取器能回读被比较的全部事实。`SchemaSnapshotCoverage` 会把读取器可稳定观察的事实和指纹冻结进审核计划；审阅与执行使用的 coverage 不一致时，执行前置条件失败。内置 PostgreSQL、MySQL、Oracle、SQL Server 和 H2 读取器在各自声明且可表示的支持形状内提供 complete coverage，并把表与列、PK、UK、索引、FK、CHECK、默认值、生成方式及注释转换为完整快照；第三方部分读取器仍保留 `UNKNOWN` 并在审阅阶段生成单一人工步骤和零 SQL，不会把未知当作相等，也不会先执行再失败。complete coverage 是读取契约，不等同于已经完成对应数据库的真实往返认证。

coverage 与数据库版本一起选择；例如 Oracle 12c 的回读配置明确保留默认值、生成方式和排序规则等事实的缺口，不继承较新版本的完整声明。应用应使用实际版本对应的 descriptor，不能通过改版本标签绕过检查。

flying-orm 不替代企业迁移平台，也不会在普通 CRUD 热路径自动执行 DDL。生产环境应把 Schema 权限与业务 DML 权限分离。

## 字段加密、保护搜索与脱敏

字段保护只对以下显式声明生效：

- 实体字段上的 `@EncryptedField` 或 `@MaskedField`。
- `DynamicForm.Builder.encrypted(...)` 或 `masked(...)`。

未声明字段不会自动加密、生成搜索 token 或脱敏。上层服务只需提供版本化密钥材料；密钥来源、部署配置和权限体系不进入 flying-orm。

Schema 冷路径会把同一实体的保护声明投影为最终物理关系：密文列、EXACT/SUFFIX 搜索列和按需的 CONTAINS 辅助表统一进入关系元数据、DDL、回读和差异。只有能够保持语义的唯一约束和等值索引才会投影；主键、外键、分区键、范围约束或不安全的复合保护索引会在 SQL 前拒绝。

CONTAINS 辅助表携带业务表全部主键。业务主键恰好叫 `field_tag` 或 `token_hash` 时，ORM 保留业务列名并为内部协议列确定性避让；Schema、辅助表写入及查询使用同一布局。调用方不应引用或自行维护这些内部列名。

涉及业务行和辅助索引的多语句普通保护写入必须参与上层外部事务，ORM 不自行开事务；明确的 INDEPENDENT 批量可在每片允许的自有事务内保持该片一致性。不能为去掉事务治理而把多语句写入降级为独立自动提交。

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
