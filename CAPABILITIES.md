# flying-orm 其他能力

本文承接 [README](README.md) 中的 DynamicForm 主路径，说明完成基本 CRUD 后可以按需使用的常用能力。
这些能力继续复用同一套表单模型、条件 AST、Scope、参数绑定、事务和错误语义，不会建立第二套 ORM 内核。

## 阅读前提

开始前应已经完成三件事：

1. 通过 `FlyingOrmClients` 创建客户端。
2. 使用 `DynamicForm` 描述运行时表结构。
3. 通过 `ReactiveFormClient` 或 `SyncFormClient` 完成基本增删改查。

下面的响应式示例使用 `forms` 表示 `clients.forms()`。同步入口使用 `clients.syncForms()`，规格对象和安全规则保持一致。

## 分页与游标分页

### 适用场景

普通分页适合需要总数和页码跳转的管理端查询；游标分页适合持续向后读取的大结果集，可以避免页码越深时不断扩大
offset。两种分页都要求稳定排序，游标分页还要求排序值能够唯一确定下一页边界。

### 最小示例

```java
QuerySpec query = QuerySpec.of(userForm, ConditionGroup.and()
        .where("enabled", "=", true)
        .build());

Mono<PageResult<DynamicRow>> page = forms.page(
        query,
        PageQuery.of(1, 20, PageSort.asc("id")));

Mono<CursorPageResult<DynamicRow>> first = forms.cursorPage(
        query,
        CursorPageQuery.first(20, CursorSort.asc("id")));
```

读取第一页返回的 `nextCursor()` 后，通过 `CursorPageQuery.after(...)` 构造下一页请求。

### 安全与行为边界

- 页大小、结果行数和结果字节数继续受执行保护限制。
- 游标字段必须进入最终投影；复合排序需要提供完整游标值。
- 加密或最终会被脱敏的字段不能作为游标边界。
- 不要用无序分页换取表面成功；跨页稳定性取决于明确、唯一的排序。

### 进一步阅读

- [动态表单分页需求](docs/requirements/2026-07-22-dynamic-form-pagination.md)
- [数据库支持矩阵](docs/database-support-matrix.md)

## 轻量 JOIN

### 适用场景

轻量 JOIN 用于已注册 DynamicForm 之间的只读等值连接，支持 inner join、left outer join 和 right outer join。
它适合普通多表投影与筛选，不用于构建任意复杂 SQL、实体图或写 JOIN。

### 最小示例

```java
JoinQuerySpec.Builder join = JoinQuerySpec.builder(userForm);
JoinSource users = join.root();
JoinSource orders = join.join(
        JoinType.LEFT,
        orderForm,
        users,
        "id",
        "user_id");

JoinQuerySpec spec = join
        .selectAs(users, "name", "userName")
        .selectAs(orders, "order_no", "orderNo")
        .where(users, ConditionGroup.and().where("enabled", "=", true).build())
        .orderBy(orders, "order_no", PageSort.Direction.ASC)
        .build();

Flux<DynamicRow> rows = forms.selectJoin(spec);
```

### 安全与行为边界

- 每个数据源分别执行 TenantScope、DataScope、FieldScope 和逻辑删除规则。
- 外连接可选侧的范围条件进入 `ON`，不会被错误地放入 `WHERE` 后退化成内连接。
- 当前不提供 FULL JOIN、CROSS JOIN、自连接、写 JOIN、子查询 JOIN 或 JOIN 游标分页。
- 加密字段可以按展示策略投影，但不能作为 ON、排序或普通范围条件。

### 进一步阅读

- [轻量 JOIN 与受保护字段](docs/join-and-protected-fields.md#轻量-join)
- [轻量 JOIN 设计](docs/superpowers/specs/2026-08-09-lightweight-join-design.md)

## 参数条件与结构化条件

### 适用场景

Java 业务代码可以直接构造 `ConditionGroup`。HTTP、消息或其他不可信边界只能传结构化条件，由后端使用字段、
operator、类型、深度、节点数和集合上限明确的策略编译，不能透传 SQL。

### 最小示例

```java
StructuredConditionInput input = StructuredConditionInput.and(
        StructuredConditionInput.term("enabled", "eq", true),
        StructuredConditionInput.term("age", "gt", 18));

QuerySpec query = QuerySpec.structured(userForm, input)
        .withStructuredPolicy(StructuredConditionPolicies.dynamicForm());

Flux<DynamicRow> rows = forms.select(query);
```

服务端还可以注册受控的业务 term，例如组织关系、JSON 条件或数据库扩展操作；注册过程发生在客户端装配阶段。
可选的大小写无关模糊搜索应显式使用 `like-ignore-case`：

```java
ConditionGroup where = ConditionGroup.and()
        .whereIfPresent("name", "like-ignore-case", request.name())
        .build();
```

该 operator 会生成参数化的 `lower(字段) like lower(?)`。高频查询应由上层针对目标数据库配置匹配的函数索引、
计算列索引或大小写不敏感排序规则，ORM 不会隐藏创建索引。

### 安全与行为边界

- 未知字段、未知 operator、类型不匹配和超出预算的条件在获取连接前失败。
- 租户、逻辑删除和受保护字段不能由不可信结构化条件伪造。
- 普通 `where(...)` 使用严格空值语义；可选搜索项应显式使用 `whereIfPresent(...)`。
- 所有业务值最终进入参数绑定，不拼接进 SQL。

### 进一步阅读

- [参数条件需求](docs/requirements/2026-07-22-parameter-driven-conditions.md)
- [可扩展条件 operator](docs/requirements/2026-07-22-extensible-condition-operators.md)
- [错误码手册](docs/error-code-reference.md#结构化条件)

## Scope、逻辑删除与乐观锁

### 适用场景

Scope 用于可信后端施加租户、数据、字段和时间范围；逻辑删除把默认删除变成标记更新；乐观锁用于检测并发写冲突。
它们都在业务条件之外继续收窄访问范围。

### 最小示例

```java
DataScope tenant = DataScope.tenant("tenant_id", tenantId);

QuerySpec query = QuerySpec.of(userForm, ConditionGroup.and()
        .where("status", "=", "ACTIVE")
        .build())
        .withScope(tenant);

WriteSpec update = WriteSpec.update(
        userForm,
        Map.of("name", "Alice"),
        ConditionGroup.and().where("id", "=", userId).build())
        .withScope(tenant)
        .withLock(OptimisticLockOptions.increment("version", expectedVersion));
```

### 安全与行为边界

- Scope 和逻辑删除不能替代更新或删除所需的明确业务条件。
- SaaS 通常使用 TenantScope 与 DataScope；无租户系统可以只使用 DataScope。
- FieldScope 同时约束投影、排序、分组和写字段，不能通过另一入口绕过。
- 乐观锁冲突使用稳定异常和结构化字段表达，不把原始业务值写入公共错误消息。

### 进一步阅读

- [Scope 协作说明](docs/business-scope-collaboration.md)
- [Scope 错误码](docs/error-code-reference.md#scope-安全)

## 批量写入

### 适用场景

批量入口按有界 chunk 消费 `Publisher`，适合动态表单批量 insert、upsert 或乐观更新。默认使用 ATOMIC；只有业务明确
接受分片独立提交时才使用 INDEPENDENT。

### 最小示例

```java
Flux<Map<String, Object>> source = Flux.just(
        Map.of("id", 1L, "name", "Alice"),
        Map.of("id", 2L, "name", "Bob"));

BatchSpec batch = BatchSpec.insert(userForm, source)
        .withOptions(BatchWriteOptions.atomic(500)
                .withMaxRows(100_000)
                .withTimeout(Duration.ofMinutes(2)));

Mono<BatchWriteResult> result = forms.writeBatch(batch);
```

### 安全与行为边界

- ATOMIC 由唯一事务控制者整批提交或回滚。
- INDEPENDENT 分片独立提交，可能返回部分完成；外部事务中会在执行 SQL 前拒绝。
- `UNKNOWN` 表示提交或回滚事实无法确认，不能当作成功或普通失败重试。
- 输入行、chunk、内存、并发和总执行时间都有显式上限；连接等待由上层连接池治理。

### 进一步阅读

- [批量事务设计](docs/requirements/2026-07-23-reactive-batch-chunking-transaction-design.md)
- [V2.0.0 执行契约](docs/v2.0.0-execution-contract.md)

## 实体映射与 Repository

### 适用场景

固定领域对象可以使用 flying-orm 自有注解映射为 DynamicForm，并通过 Repository 使用同一 SQL、安全、Scope、批量和
事务内核。动态表单仍是底座，Repository 不是另一套 ORM。

### 最小示例

```java
ReactiveFormRepository<User> users = clients.repository(User.class);
Mono<Long> inserted = users.insert(user);
Flux<User> active = users.select(
        ConditionGroup.and().where("enabled", "=", true).build());
```

这里的 `User` 使用 flying-orm 自有的 `@TableName`、`@TableId`、`@TableField` 等注解完成映射。

### 安全与行为边界

- 主键生成策略必须显式声明；数据库生成键会返回并按实体可变性规则处理。
- `@Version`、`@TableLogic`、字段读写策略和生命周期继续走统一写入规则。
- 同一关系表不应同时由另一 ORM 和 flying-orm 共同拥有映射、事务、乐观锁和缓存失效。
- 不可变实体不会通过不安全反射强行回写数据库生成键。

### 进一步阅读

- [目标 API 示例](docs/target-api-examples.md)
- [公共 API 稳定边界](docs/public-api-stability.md)

## Schema 与元数据

### 适用场景

动态表单需要显式建表、校验或演进时，可以从统一客户端取得 Schema 和 Metadata 门面。默认不自动修改数据库；安全
更新和危险更新必须选择明确策略，审核计划与执行计划分离。

### 最小示例

```java
ReactiveSchemaClient schema = clients.schema();

Mono<SchemaMigrationResult> migrated = schema.createOrAlterDetailed(
        userForm,
        List.of(),
        clients.metadata(),
        SchemaMigrationOptions.safe());
```

同步入口使用 `clients.syncSchema()`，返回对应的同步结果。

### 安全与行为边界

- 破坏性 DDL 不能绕过审核指纹和显式策略。
- 迁移成功后精确失效主表、保护侧表及相关元数据缓存。
- 数据库对事务 DDL、锁超时和回滚的支持不同，必须遵守方言与认证边界。
- 动态表单结构变化不等于业务数据迁移；历史明文、枚举重编码等工作需要显式迁移流程。

### 进一步阅读

- [数据库支持矩阵](docs/database-support-matrix.md)
- [真实数据库认证方法](docs/real-database-certification.md)
- [已知限制](docs/known-limitations.md)

## 字段加密、保护搜索与脱敏

### 适用场景

任何需要加密入库、受控搜索或结果脱敏的文本业务字段都可以显式声明保护，不限于手机号和身份证。没有注解或
DynamicForm 声明的普通字段不会自动加密、解密、建立搜索索引或脱敏。

### 最小示例

```java
DynamicForm customer = DynamicForm.builder("customer", "customer")
        .addField(DynamicField.primaryKey("id", "BIGINT"))
        .addField(DynamicField.of("contact", "VARCHAR"))
        .encrypted("contact", EncryptedFieldDefinition.builder()
                .searchModes(EncryptedSearchMode.EXACT, EncryptedSearchMode.SUFFIX)
                .normalizer("digits")
                .suffixLengths(4)
                .build())
        .masked("contact", MaskedFieldDefinition.builder("partial")
                .prefix(3)
                .suffix(4)
                .build())
        .build();

QuerySpec query = QuerySpec.of(customer, ConditionGroup.and()
        .add(ProtectedConditions.exact("contact", "13800138000"))
        .build());
```

### 安全与行为边界

- 上层只提供版本化 32 字节主密钥；KMS、Vault、HSM 和厂商 SDK 不进入 flying-orm。
- AES-GCM 密文使用随机 nonce；精确、后缀和包含搜索使用独立的保护索引语义。
- CONTAINS 先查候选再解密复核，并受候选数和绑定参数上限约束。
- `showSensitive()` 只面向已经授权的可信服务端代码，不会放宽日志、异常和观测脱敏。
- 搜索能力会泄露受控重复模式；不需要查询的字段不要启用搜索模式。

### 进一步阅读

- [受保护字段完整说明](docs/join-and-protected-fields.md#显式受保护字段)
- [字段保护错误码](docs/error-code-reference.md#受保护字段)

## 错误处理

### 适用场景

业务边界应根据稳定异常类型、错误码、字段路径、批量状态和事务结果处理失败，而不是解析底层驱动消息。

### 最小示例

```java
Mono<Long> updated = forms.update(writeSpec)
        .doOnError(error -> OrmErrors.report(error).ifPresent(report ->
                log.warn("flying-orm code={}, field={}", report.code(), report.field())));
```

响应式业务链应使用 `doOnError`、`onErrorResume`、`retryWhen` 等组合方式处理；不要在响应式请求线程或事件循环中
调用 `block()` 或手工 `subscribe()`。

### 安全与行为边界

- 公共错误消息不复制 SQL、参数、凭据、连接串或无界驱动文本。
- 批量结果区分 COMMITTED、ROLLED_BACK、PARTIAL、FAILED、UNKNOWN 和外部事务 ENLISTED。
- VME 等 JVM 致命错误保持原对象传播，资源清理不能把已确认事务终态改写成另一结果。

### 进一步阅读

- [错误码手册](docs/error-code-reference.md)
- [V2.0.0 执行契约](docs/v2.0.0-execution-contract.md)

## 进一步阅读

- [高级能力](ADVANCED-CAPABILITIES.md)
- [数据库支持矩阵](docs/database-support-matrix.md)
- [公共 API 稳定边界](docs/public-api-stability.md)
- [真实数据库认证方法](docs/real-database-certification.md)
