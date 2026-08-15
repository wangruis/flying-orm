# 字段加密、盲索引与结果脱敏设计

## 状态

- 设计状态：已确认
- 日期：2026-08-09
- 适用模块：`flying-orm-core`、`flying-orm-rdb`、`flying-orm-testkit`、`flying-orm-benchmark`

## 目标

让任意敏感文本字段按实体注解或 DynamicForm 显式声明启用加密、搜索和业务结果脱敏。手机号、身份证号只是使用示例，
实现不能按字段名猜测类型，也不能把策略写死为两个业务字段。

上层服务只提供一个版本化主密钥环。flying-orm 固定密码协议、用途隔离、密文格式、搜索令牌、事务一致性和安全日志；
不依赖 KMS、Vault、HSM 或厂商 SDK。

## 显式启用

实体字段：

```java
@EncryptedField(
        search = {EncryptedSearchMode.EXACT, EncryptedSearchMode.SUFFIX},
        normalizer = "digits",
        suffixLengths = {4})
@MaskedField(
        policy = "partial",
        prefix = 3,
        suffix = 4,
        display = SensitiveDisplayMode.MASKED)
private String contactValue;
```

DynamicForm：

```java
DynamicForm form = DynamicForm.builder("customer", "customer")
        .addField(contactField)
        .encrypted("contactValue", encryptedDefinition)
        .masked("contactValue", maskedDefinition)
        .build();
```

只有 `@EncryptedField` 或 `DynamicForm.Builder.encrypted(...)` 才启用加解密和保护搜索。只有 `@MaskedField` 或
`DynamicForm.Builder.masked(...)` 才启用业务结果脱敏。没有声明的字段保持现有语义。

允许只加密、只脱敏或同时启用。加密声明不会自动推断业务脱敏策略，但加密字段始终进入日志、异常和观测的强制保护。

## 元数据边界

core 新增 `com.flying.orm.core.protection`：

- `EncryptedSearchMode`：`EXACT`、`SUFFIX`、`CONTAINS`。
- `SensitiveDisplayMode`：`DECLARED`、`MASKED`、`FULL`。
- `EncryptedFieldDefinition`：搜索能力、归一化器 ID、后缀长度和长度边界。
- `MaskedFieldDefinition`：policy ID、通用前后保留参数和声明展示模式。
- `FieldProtectionRegistry`：按表单字段保存不可变保护定义。

不增加 `DynamicField` 或 `EntityFieldMetadata` 的 record 组件。DynamicForm 只保存 registry，并把真正的自动标识符命名和
结构指纹计算拆给聚焦协作者，确保类型不突破 400 行硬门禁。

实体元数据编译器把注解翻译成同一 `FieldProtectionRegistry`；实体和 DynamicForm 不建立两套运行时语义。

## 主密钥环

普通接入：

```java
ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", masterKey);
```

轮换接入：

```java
ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.builder()
        .current("v2", masterKeyV2)
        .readable("v1", masterKeyV1)
        .build();
```

约束：

- 主密钥必须正好 32 字节，并在构建时防御复制。
- 版本为 1 到 16 个 ASCII 字母、数字、点、短横线或下划线，不是基础设施别名。
- 只能有一个 current 版本；readable 版本总数最多 4 个。
- 新写入只使用 current；旧版本只用于解密、搜索和迁移。
- 上层决定如何获得密钥，主项目不读取环境变量、不解析 YAML、不连接外部密钥系统。
- 客户端关闭时清零 ORM 持有的密钥副本；上层仍负责自己原始密钥对象的生命周期。

## 密码协议

- 使用 Java 21 JCA，不引入第三方密码实现。
- 密文使用 AES-256-GCM、随机 96-bit nonce 和 128-bit authentication tag。
- 密文信封保存协议版本、密钥版本、nonce、ciphertext 和 tag；解析必须有固定长度上限。
- AAD 包含协议版本、表单 ID、字段 ID 和租户隔离标识，防止跨字段或跨租户搬移密文。
- 主密钥通过 HKDF-SHA-256 按 `encryption`、`exact`、`suffix`、`contains` 用途派生独立子密钥。
- 派生上下文包含表单、字段、租户和搜索长度；不同字段和用途不能得到相同子密钥。
- 解密认证失败、密钥版本未知、信封损坏或长度超限统一返回稳定安全错误，不能回显密文或驱动消息。

不使用 ECB、静态 IV、可预测 nonce、确定性密文或自行发明的加密算法。

## 精确和后缀搜索

### EXACT

对归一化后的 UTF-8 文本计算字段/租户专用 HMAC-SHA-256，写入隐藏 32 字节索引列。查询时为 current 与全部 readable
版本分别计算令牌，并使用参数化 `IN` 匹配，因此轮换期间旧数据仍可检索。

### SUFFIX

只为声明的 `suffixLengths` 生成隐藏列。例如 `{4, 8}` 只允许后四位和后八位查询，不建立所有可能后缀。

- 长度以 Unicode code point 计算。
- 查询长度必须精确命中声明长度。
- 空白、过短或未声明长度在 SQL 前失败。

### 归一化器

`ProtectedValueNormalizerRegistry` 提供稳定 ID：

- `identity`：只做 Unicode 规范化，不改变大小写或内部空白。
- `case-fold`：Unicode 规范化后做 Locale.ROOT 大小写折叠。
- `digits`：只保留 Unicode 数字并转为 ASCII 十进制数字。

允许上层在启动时注册自定义纯函数归一化器。未知 ID、返回 null、输出超限或非确定性行为由启动验证和契约测试拒绝。

## 业务结果展示

字段声明默认展示方式：

```java
@MaskedField(policy = "partial", display = SensitiveDisplayMode.MASKED)
```

也允许：

```java
@MaskedField(policy = "partial", display = SensitiveDisplayMode.FULL)
```

查询级覆盖：

```java
query.declaredDisplay();
query.masked();
query.showSensitive();
```

优先级为“查询显式设置 > 注解或 DynamicForm 声明 > MASKED 默认值”。覆盖只作用于已经声明 masked 的字段。

- `DECLARED`：使用字段声明。
- `MASKED`：强制调用该字段的 masking policy。
- `FULL`：向可信业务代码返回完整解密值。

`.showSensitive()` 只能由可信后端代码调用，授权属于上层服务；前端结构化条件、请求参数和普通 bind 不能控制该模式。

即使选择 FULL，SQL 日志、参数日志、异常、批量结果、恢复 token、观测和审计信息也必须继续完全隐藏保护值。

## Masking policy

`MaskingPolicyRegistry` 使用稳定 policy ID。内置策略包括：

- `full`：全部替换。
- `partial`：保留配置的前后 code point。
- `email`：保留受控邮箱结构。
- `person-name`、`address`、`bank-card`：提供常用默认行为。

自定义策略必须无状态、并发安全、输出有界。未知策略在客户端装配时失败。null 保持 null，不把空值伪造成星号文本。

## 存储类型

密文使用专用 protected binary 类型，不复用现有泛化 `BINARY` 的大对象映射：

- H2：`VARBINARY(n)` / `BLOB`
- MySQL：`VARBINARY(n)` / `BLOB`
- PostgreSQL：`BYTEA`
- Oracle：`RAW(n)` / `BLOB`
- SQL Server：`VARBINARY(n)` / `VARBINARY(MAX)`

固定 HMAC 索引使用 H2 `VARBINARY(32)`、MySQL `BINARY(32)`、PostgreSQL `BYTEA`、Oracle `RAW(32)`、SQL Server
`BINARY(32)`。小密文是否能使用行内二进制按字段最大 UTF-8 长度和信封开销计算；超过 Oracle 标准 RAW 的 2000 字节
共同边界时使用大对象类型。

隐藏列名称由专用稳定命名器生成，限制在 30 个 ASCII 字符内。隐藏列不进入普通字段投影、DynamicRow、实体映射或
前端可见元数据。

## 写入与读取

写入顺序：字段验证和业务 codec -> 归一化搜索值 -> 生成盲索引 -> AES-GCM 加密 -> 绑定密文与隐藏列。

读取顺序：驱动值/LOB 物化 -> 解析信封 -> 解密认证 -> 业务值转换 -> masking policy 或 FULL -> DynamicRow/实体/DTO。

解密与展示在统一结果转换器完成，使用 `DynamicRow.withValues(...)` 一类稀疏替换能力，不能为每行提前物化普通 Map。

## 多租户隔离

- 共享表租户使用 TenantScope 的稳定租户值参与 AAD 和子密钥派生。
- schema/database 路由使用事务开始前已确定的 routing identity。
- 租户表上的保护搜索必须有唯一租户上下文；无租户或跨租户查询在 SQL 前拒绝。
- 这样同一个明文在不同租户、不同字段的盲索引不同，降低跨范围关联泄露。

## 限制与泄露模型

- 盲索引会泄露同一字段、同一租户范围内的相等频率；这是支持数据库索引查询的明确取舍。
- SUFFIX 和 CONTAINS 比 EXACT 暴露更多相等模式，必须逐字段显式启用。
- 加密字段不能直接排序、范围比较、group、聚合或作为 JOIN ON。
- `like` 不自动改写为保护搜索；调用方使用明确的 exact、suffix 或 contains API。
- 密码字段不属于本功能，密码必须使用不可逆 password hashing。

## 验证范围

1. 密钥长度、版本、数量、防御复制和清零。
2. HKDF 官方 test vector、用途/字段/租户隔离。
3. nonce 唯一性、密文不确定性、篡改、错误密钥和未知版本。
4. EXACT/SUFFIX 写入、轮换期多版本查询和参数顺序。
5. 注解与 DynamicForm 元数据完全对称。
6. MASKED/FULL 优先级和自定义 policy。
7. 日志、异常、观测、错误报告和恢复结果无明文。
8. JDBC/R2DBC、普通 CRUD、Repository、批量和 JOIN 投影。
9. 四库 DDL、索引使用、LOB 取消和资源释放。
