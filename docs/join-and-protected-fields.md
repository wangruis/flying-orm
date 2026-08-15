# 轻量 JOIN 与受保护字段

本文说明 2.0.0 当前基线提供的轻量多表查询、字段加密搜索和通用脱敏能力。两类能力都复用
DynamicForm、条件 AST、Scope、方言、参数绑定和 JDBC/R2DBC 执行内核，不建立第二套 SQL 或事务实现。

## 轻量 JOIN

### DynamicForm 查询

JOIN 是只读能力。调用方只提供已经注册的表单和字段；框架生成内部 `t0`、`t1` 别名，不要求业务代码管理
SQL 表别名。只有结果列重名或 DTO 映射需要稳定名称时才使用 `selectAs(...)`。
显式结果别名必须是最多 30 个 ASCII 字符的普通标识符；自动别名过长时会改用稳定的短别名，以兼容 Oracle 12c
以及其他受支持数据库。

```java
Flux<DynamicRow> rows = operator.dml()
        .joinQuery(userForm)
        .leftJoin(orderForm, "id", "user_id")
        .andOn("tenant_id", "tenant_id")
        .selectAs(userForm, "name", "userName")
        .selectAs(orderForm, "order_no", "orderNo")
        .where(userForm, "enabled", "=", true)
        .orderByAsc(orderForm, "order_no")
        .executeRows();
```

`join(...)` 生成 `INNER JOIN`，`leftJoin(...)` 生成 `LEFT OUTER JOIN`，`rightJoin(...)` 生成
`RIGHT OUTER JOIN`。同步入口使用相同链式 API，终止方法返回 `List<DynamicRow>`。

JOIN 的 offset 分页必须先通过 `orderByAsc(form, field)` 或 `orderByDesc(form, field)` 声明至少一个带来源的稳定排序。
普通 `PageQuery.sorts()` 没有表来源，在多表同名列场景中存在歧义，因此 JOIN 分页会拒绝它，不能用无序分页换取表面成功。

### 实体 Lambda 查询

```java
Flux<DynamicRow> rows = operator.dml()
        .joinQuery(User.class)
        .leftJoin(Order.class, User::getId, Order::getUserId)
        .andOn(User::getTenantId, Order::getTenantId)
        .selectAs(User.class, User::getName, "userName")
        .selectAs(Order.class, Order::getOrderNo, "orderNo")
        .where(User.class, User::isEnabled, "=", true)
        .executeRows();
```

Lambda 必须是已映射实体属性的直接 getter。计算 Lambda、未加入查询的实体、重复实体、自连接和未持久化字段会在
获取连接前拒绝。查询结果仍是扁平 `DynamicRow`；需要 DTO 时继续使用现有 `RowMapper`，不创建懒加载实体图。

### 安全边界

- 每个源分别应用 TenantScope、DataScope、FieldScope 和逻辑删除规则；外连接可选侧的约束进入 `ON`，避免被
  `WHERE` 意外改成内连接。
- 支持显式投影、排序、offset/page 和 count。JOIN cursor page 需要跨源唯一键和外连接 null 排序的独立契约，
  当前版本不提供，避免用不稳定游标造成漏行或重复。
- 首版不提供 FULL JOIN、CROSS JOIN、自连接、子查询 JOIN、写 JOIN、GROUP BY/HAVING 或任意 SQL 片段。
- 加密字段允许投影并按展示策略解密/脱敏；EXACT、SUFFIX 仅可用于 `WHERE`。加密字段不能用于 `ON` 或排序，
  CONTAINS 首版不能放入 JOIN。

## 显式受保护字段

只有实体注解或 DynamicForm 显式声明的字段才启用加密、保护搜索或脱敏；普通字段行为完全不变。该能力不限定于
手机号和身份证，可用于任何业务敏感字段。

### 实体注解

```java
@EncryptedField(
        search = {EncryptedSearchMode.EXACT, EncryptedSearchMode.SUFFIX},
        normalizer = "digits",
        suffixLengths = {4})
@MaskedField(policy = "partial", prefix = 3, suffix = 4)
private String contact;
```

`@EncryptedField` 负责密文和允许的搜索能力，`@MaskedField` 负责业务结果展示。两者可以单独使用，也可以同时声明。
当前加密和脱敏字段必须具有稳定的文本编码，不限制它代表手机号、证件号、邮箱、姓名、地址、账号或其他业务语义。

### DynamicForm 声明

```java
DynamicForm form = DynamicForm.builder("customer", "customer")
        .addField(DynamicField.primaryKey("id", "BIGINT"))
        .addField(DynamicField.of("contact", "VARCHAR"))
        .encrypted("contact", EncryptedFieldDefinition.builder()
                .searchModes(EncryptedSearchMode.EXACT,
                             EncryptedSearchMode.SUFFIX,
                             EncryptedSearchMode.CONTAINS)
                .normalizer("digits")
                .suffixLengths(4)
                .build())
        .masked("contact", MaskedFieldDefinition.builder("partial")
                .prefix(3)
                .suffix(4)
                .build())
        .build();
```

内置规范化器为 `identity`、`case-fold` 和 `digits`。搜索声明决定允许的操作，不会把普通 `=`、`LIKE` 或范围条件
猜成加密搜索。密文列仍保留标准 `IS NULL / IS NOT NULL` 语义，这两个无值判断不会生成搜索 token。

### 简单密钥设计

上层服务只负责提供版本化的 32 字节主密钥，不需要把 KMS、Vault、HSM 或厂商 SDK 接入 flying-orm：

```java
ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.builder()
        .current("v2", currentMasterKey)
        .readable("v1", previousMasterKey)
        .uniqueSearchKey(stableUniqueSearchKey)
        .build();

FlyingOrmClients clients = FlyingOrmClients.builder(dataSource, connectionFactory)
        .protectedFields(keys)
        .build();
```

内置规范化器和脱敏策略可以直接使用。业务需要自定义规则时，在启动期构造不可变 registry，并通过同一个 Builder
装配；mask-only 表单不要求配置无关的加密密钥：

```java
ProtectedValueNormalizerRegistry normalizers = ProtectedValueNormalizerRegistry.standard()
        .with("account-code", value -> value.strip().toUpperCase(Locale.ROOT));
MaskingPolicyRegistry policies = MaskingPolicyRegistry.standard()
        .with("employee-code", (value, parameters) -> "***");

FlyingOrmClients clients = FlyingOrmClients.builder(dataSource, connectionFactory)
        .protectedFields(keys)
        .protectedFieldPolicies(normalizers, policies)
        .build();
```

新写入始终使用 current 版本，查询和解密同时接受 current 与旧 readable 版本；密钥环最多保存四个版本。ORM 对
上层传入密钥做防御复制，并在关闭时清零自己持有的副本。密文采用 AES-256-GCM 随机 nonce；HKDF-SHA-256 按表、
字段、租户、用途和搜索长度派生隔离子密钥。日志、异常和观测不会输出主密钥、明文或完整密文。

`uniqueSearchKey(...)` 在“多版本轮换期间存在加密唯一字段”或“受保护 R2DBC 批量启用事务回执”时必需。它必须是
独立、稳定的 32 字节随机密钥，不能跟随 current 加密密钥一起切换，否则同一业务值会产生不同唯一 token，数据库
唯一约束或幂等回执身份将失效。单密钥配置会自动复用 current 密钥；多版本配置缺少稳定身份密钥时，相关唯一搜索或
受保护回执写入会在 SQL 前稳定拒绝。

### 搜索

```java
ConditionGroup where = ConditionGroup.and()
        .add(ProtectedConditions.exact("contact", "13800138000"))
        .build();

QuerySpec suffix = QuerySpec.of(form, ConditionGroup.and()
        .add(ProtectedConditions.suffix("contact", "8000"))
        .build());

QuerySpec contains = QuerySpec.of(form, ConditionGroup.and()
        .add(ProtectedConditions.contains("contact", "0013"))
        .build());
```

- EXACT 和固定长度 SUFFIX 使用版本化 HMAC 盲索引。单字段最多声明 32 个后缀长度；业务值短于某个已声明长度时，
  该长度的隐藏列写入 `NULL`，其他可用长度、加密入库和解密不受影响。
- CONTAINS 使用 Unicode code point trigram 辅助表先求候选，再解密复核真实 substring，避免哈希碰撞或 token
  命中被误报为最终结果。
- CONTAINS 候选硬上限为 1000；超过上限会稳定失败，不会静默截断。
- 单个密钥版本的 CONTAINS 查询最多绑定 1000 个 trigram token，跨版本查询的总绑定参数最多 2100 个；
  超过任一四库共同安全边界会在 SQL 交给驱动前稳定拒绝。较长字段仍可入库并用较短片段检索。
- 侧索引维护与业务写入使用同一 JDBC/R2DBC 连接和事务，覆盖单条、批量 insert/upsert/update 与数据库生成主键。
  ATOMIC 中任何侧索引失败都会回滚业务行；外部事务中只参与上层事务，不自行提交。
- CONTAINS 侧索引以业务表主键作为稳定 owner。启用该能力的表不能通过普通 update 改写主键；单条和批量入口都会
  在执行 SQL 前拒绝。需要更换业务身份时应显式删除旧行并插入新行，让主表与侧索引在同一事务内完成迁移。
- 受保护 R2DBC 批量启用事务回执时，业务表仍绑定随机 AES-GCM 密文；回执摘要使用用途隔离的稳定 HMAC 身份。
  因此同一冷 Publisher 的再次订阅不会因 nonce 改变而误报 payload mismatch，摘要也不会保存可离线猜测的明文哈希。
- 保护搜索会泄露受控的相等、后缀或 trigram 重复模式，不能提供“完全无泄露”的可搜索加密。不要为不需要查询的
  字段启用搜索模式。

### 解密与脱敏

声明了 `@MaskedField` 或 DynamicForm `masked(...)` 的字段，默认按字段声明展示。调用方可以显式覆盖：

```java
QuerySpec declared = QuerySpec.of(form, where).declaredDisplay();
QuerySpec alwaysMasked = QuerySpec.of(form, where).masked();
QuerySpec full = QuerySpec.of(form, where).showSensitive();
```

`showSensitive()` 只面向已经完成授权的可信后端代码；它只改变业务结果，不放宽 SQL 日志、异常、观测或恢复结果的
脱敏。内置 policy 包括 `full`、`partial`、`email`、`person-name`、`address` 和 `bank-card`，上层也可注册与具体业务
类型无关的通用 policy。

### 密钥轮换

轮换时先把新版本设为 current，旧版本保留为 readable。`ProtectedFieldReprotection` 可识别旧版本信封并返回需要重写
的逻辑值；`valuesNeedingPlaintextMigration(...)` 还可在独立旧明文列与目标密文列之间做逐行幂等判断。上层按稳定主键
游标分页读取后，通过普通 `FormClient.update(WriteSpec)` 写回。这样密文、EXACT/SUFFIX 和 CONTAINS 侧索引仍由
同一事务维护。该协作者不自动扫描数据库、不保存进度，也不静默改写历史数据。

加密主密钥与稳定唯一搜索密钥应分开轮换：先保留旧加密密钥为 readable，完成密文重保护，再移除旧版本；稳定唯一
搜索密钥只有在单独完成全量唯一 token 重建并经过冲突检查后才能更换。

## Schema 与版本

受保护字段的物理密文列、HMAC 隐藏列和 CONTAINS 辅助表由现有 Schema 渲染/同步链生成。启用保护前必须通过明确
迁移把历史明文转换为密文；Schema 同步不会假设历史值已经安全。该能力属于 `2.0.0` 公共 API 基线。
若已有同名列仍是文本类型，即使使用 `FULL_UPDATE`，规划器也会在生成 DDL 前拒绝把它直接改成二进制密文列；必须先完成
显式、可恢复的明文迁移。真实元数据已经报告为 BLOB/BYTEA/VARBINARY/RAW 等二进制存储时，规划器把它视为保护列的
方言物理表示，重复同步不会反复生成伪类型变更。

对已经存在的业务表首次启用 CONTAINS 时，Schema 规划器会单独读取辅助表元数据：缺表才创建，已存在则按结构差异
迁移，因此重复规划是幂等的。审核计划会把辅助表作为独立回滚段，并在主表回滚前先删除新建的辅助表。
业务表使用 `schema.table` 时，CONTAINS 辅助表保留相同 Schema；30 字符兼容限制只作用于自动生成的本地表名、
索引名和外键名，不会把辅助表静默落到默认 Schema。
