# 统一条件值策略设计

## 状态

- 所属主线：R-007 参数驱动动态条件增强，并服务于 R-020 安全 Scope
- 设计状态：已确认
- 日期：2026-07-31

## 目标

flying-orm 要在核心层统一处理条件值，不能把空值判断推给上层 Spring 服务。无论条件来自参数 Map、前端结构化 JSON、Java DSL，还是 TenantScope、DataScope、TimeScope，都先按同一套规则整理值，再进入条件 AST 和 SQL 渲染。

这里的“空值”包括 `null`、空字符串、只含空白的字符串、空集合、空数组，以及集合中清理后没有剩余元素的情况。字符串使用 Java 21 的 `strip()` 去掉首尾 Unicode 空白。

本次覆盖所有内置 term 和业务自定义 term，不靠逐个判断 `=`、`<>`、`like`。每个 term 声明自己需要的值形状，统一组件据此清理、校验、忽略或拒绝。

## 当前问题

- `ParameterConditionCompiler` 已能忽略 `null`、空白字符串和空集合，但不会把非空字符串 `strip()`，也不会清理集合里的空元素。
- `StructuredConditionCompiler` 会拒绝空值并返回路径，但值形状判断仍散落在编译器中，新增 term 容易漏掉。
- `ConditionGroup.Builder.where(...)` 目前会保留 `null`，因此 `where("deleted_at", "=", null)` 仍可能进入 SQL 参数。
- 空值规则和 SQL term handler 分离，业务 term 没有明确声明自己需要标量、集合还是区间。

## 方案比较

### 方案一：按操作符硬编码

在各入口维护 `switch (operator)`，分别判断 `=`, `like`, `in`, `between` 等。

优点是改动直观。缺点是每增加一个 term 都要同步修改多个入口，自定义 term 仍然只能靠猜，长期一定出现行为分叉，因此不采用。

### 方案二：统一归一化器 + term 值形状

term 注册时声明值形状，所有入口调用同一个无状态归一化器，并由入口策略决定空值是忽略还是拒绝。

优点是内置和自定义 term 走同一路径，规则集中，容易测试；归一化只做 O(n) 的内存操作，不碰数据库。缺点是需要给 term 元数据增加一个小扩展，并处理一次公开行为迁移。

采用此方案。

### 方案三：交给 SQL handler 校验

让每个 SQL handler 在渲染时自行处理空值。

优点是 handler 能看到具体语义。缺点是失败发生得太晚，前端错误无法稳定定位，安全 Scope 还有被静默丢弃的风险，也会把同一规则重复到不同方言和业务 handler 中，因此不采用。

## 核心模型

### ConditionValueShape

term 通过值形状表达输入契约：

| 形状 | 含义 | 典型 term |
| --- | --- | --- |
| `NONE` | 不接收值 | `is-null`、`is-not-null` |
| `SCALAR` | 必须是一个有效标量 | `=`、`!=`、`<>`、`>`、`>=`、`<`、`<=`、`like`、`not-like`、`user-in-org` |
| `COLLECTION` | 必须是清理后仍非空的集合 | `in`、`not-in` |
| `RANGE` | 必须是两个有效且顺序正确的值 | `between`、`not-between` |
| `SCALAR_OR_COLLECTION` | 明确允许单值或非空集合 | 同时支持一个或多个关联值的扩展 term |

扩展 term 由 SQL handler 同时声明 id、值形状和参数化渲染规则。`RelationExistsTermHandler` 这类允许一个或多个关联值的 term 明确使用 `SCALAR_OR_COLLECTION`；没有声明形状的 term 按 `SCALAR` 处理，不再用模糊的兼容模式猜测输入。

### ConditionValuePolicy

入口策略只决定“值为空时怎么办”，不改变 term 的值形状：

- `IGNORE_EMPTY`：清理后为空时不产生条件。
- `REJECT_EMPTY`：清理后为空时立即失败，不生成 SQL。

不提供通用 `PRESERVE_EMPTY`。查询数据库空值必须使用 `is-null` 或 `is-not-null`，不能继续用 `= null`、`<> null` 表达。

### ConditionValueNormalizer

归一化器是 core 中的无状态纯组件，输入 term 元数据、原始值和入口策略，输出以下两种结果之一：

- `Present(value)`：值已整理并符合形状，可以创建 `TermCondition`。
- `Ignored`：仅在 `IGNORE_EMPTY` 下出现，调用方不添加条件。

非法值不包装成结果，直接抛出 `ConditionValueException`；异常携带 `ConditionValueError` 枚举，不要求调用方解析消息。前端编译器负责把它转换成已有的稳定错误码和精确路径。

## 归一化规则

### 字符串

- 使用 `String.strip()`。
- `"  张三  "` 变成 `"张三"`。
- `"   "` 变成空值，再按入口策略忽略或拒绝。
- 不修改字符串内部空白，不擅自改变大小写或通配符。

### 集合和数组

- 数组、`Iterable` 统一整理为不可变 `List`。
- 每个元素按标量规则清理，删除 `null` 和空白字符串；严格入口也允许清掉这些空元素，但清理后整个集合为空时必须拒绝。
- 例如 `[" ", null, " u1 ", "u2"]` 变成 `["u1", "u2"]`。
- 清理后为空时按入口策略处理。
- `COLLECTION` 不接受普通标量；`SCALAR` 不接受集合或数组。

### 区间

- `RANGE` 接受长度恰好为 2 的数组或集合。
- 两端都按标量规则清理。任何一端为空时，`IGNORE_EMPTY` 忽略整个条件，`REJECT_EMPTY` 立即失败。
- 两端必须是同一种可比较类型，并符合 term 的顺序要求。
- `between` 和 `not-between` 允许两端相等，表示单点区间；反向区间在生成 SQL 前失败。
- 输入非空但长度不是 2 时属于形状错误，即使入口使用 `IGNORE_EMPTY` 也不能静默忽略。

### 无值 term

- `is-null`、`is-not-null` 使用 `NONE`。
- Java DSL 提供明确入口，如 `whereNull(field)`、`whereNotNull(field)`；结构化条件允许 value 缺失。
- 如果 `NONE` term 显式携带非空值，立即拒绝，避免调用方误以为参数会生效。

## 各入口策略

| 入口 | 默认策略 | 原因 |
| --- | --- | --- |
| 参数驱动条件 | `IGNORE_EMPTY` | 适合动态查询表单，没填的筛选项不生成条件 |
| 后续实体 Query-by-Example | `IGNORE_EMPTY` | 实体空属性不应自动成为查询条件 |
| 前端结构化条件 | `REJECT_EMPTY` | 客户端已明确提交条件，错误必须可见且可定位 |
| Java DSL `where(...)` | `REJECT_EMPTY` | 显式调用不能悄悄少一个条件 |
| Java DSL `whereIfPresent(...)` | `IGNORE_EMPTY` | 调用方明确选择可选条件语义 |
| TenantScope、DataScope、TimeScope | `REJECT_EMPTY` | 安全范围不能因为空值而消失 |

`whereIfPresent(...)` 只影响当前 term，不改变整个 builder 的策略。嵌套 AND/OR 组在可选 term 被忽略后如果为空，不加入父组，避免渲染空括号。

## API 方向

目标 API 保持简单：

```java
ConditionGroup where = ConditionGroup.and()
        .where("name", "like", "  张三  ")
        .whereIfPresent("status", "=", request.status())
        .whereNull("deleted_at")
        .build();
```

第一项保存值 `张三`；第二项为空时不加入 AST；第三项渲染为 `deleted_at is null`，不绑定参数。

值形状直接放在 core `TermHandler` 中，标准注册表登记全部内置 term。SQL 层的 `SqlTermHandler` 同时声明扩展 term 的 id、值形状和参数化渲染规则；`SqlRenderer.conditions()` 从同一份 SQL 注册表创建条件构建器，避免调用方重复维护第二份值形状元数据。完全未知的 term 按 `SCALAR` 校验，并在渲染时明确报告未注册。

## 自定义业务 term

`user-in-org` 等业务 term 与标准 term 使用同一份值契约：

```java
TermHandler userInOrg = TermHandler.simple("user-in-org", ConditionValueShape.SCALAR);
```

如果业务 term 需要集合或区间，注册时改为对应形状。自定义 handler 仍负责把业务语义渲染为参数化 SQL，但不需要重复处理空白、空集合和形状错误。

term 注册表构建完成后保持不可变，可在高并发请求间安全共享，不增加运行时锁。

## 前端错误

前端结构化条件继续使用已有 `StructuredConditionException`、稳定错误码和 path。归一化失败映射规则如下：

- 空标量：`VALUE_NULL` 或 `VALUE_BLANK`，path 为 `conditions[n].value`。
- 值形状不符：`VALUE_SHAPE_NOT_ALLOWED`。
- 空集合：`VALUE_COLLECTION_EMPTY`。
- 集合元素类型或形状错误：path 精确到 `conditions[n].value[i]`；被清理的空元素不单独报错。
- 区间长度错误：`VALUE_RANGE_SIZE_INVALID`，path 指向 `conditions[n].value`。
- 区间两端类型不一致或不可比较：`VALUE_RANGE_TYPE_MISMATCH`，path 指向具体端点。
- 区间顺序错误：`VALUE_RANGE_ORDER_INVALID`，path 指向 `conditions[n].value`。
- `NONE` term 携带值：值形状错误。

Java DSL、参数驱动和安全 Scope 使用 core 条件值异常，消息采用自然、能直接看懂的英文；上层可以按异常类型统一映射，不依赖消息文本。

## 安全边界

- TenantScope、DataScope 和 TimeScope 永远使用严格策略；空租户、空组织、空时间边界必须在 SQL 前失败。
- 前端仍不能传租户字段、逻辑删除字段和受保护字段；值归一化不会绕过现有字段与 operator 白名单。
- 所有非 `NONE` 值继续使用 SQL 参数绑定，不进入 SQL 文本。
- 可选参数全部被忽略后，查询是否允许无业务条件由现有执行保护或表单策略决定，本次不偷偷增加全表查询规则。

## 行为收口

这是一次有意的公开行为收紧：

- 原来依赖 `.where(field, "=", null)` 的代码改为 `.whereNull(field)`。
- 原来把 `.where(...)` 当可选条件使用的代码改为 `.whereIfPresent(...)`。
- 参数驱动入口仍保持空值忽略，但会多做 `strip()` 和集合元素清理。
- 没有声明形状的自定义 term 默认按 `SCALAR` 工作；需要集合或区间时必须明确声明对应形状。

文档和版本说明要给出迁移示例，不能让行为变化只藏在实现里。

## 并发与性能

- 归一化器无状态、无锁，不使用反射、缓存或数据库访问。
- 标量为 O(1)，集合和区间为 O(n)，只在条件建立阶段执行一次。
- 注册表和 term 描述保持不可变，允许线程间共享。
- 清理后的集合复制为不可变列表，避免调用方在 SQL 渲染或异步执行期间修改参数。
- 不在这个功能里引入 Caffeine；元数据缓存和条件值整理没有同一种生命周期。

## 测试范围

继续采用少量聚焦测试，不逐个复制所有操作符用例：

1. 归一化器按 `NONE`、`SCALAR`、`COLLECTION`、`RANGE` 各覆盖一个成功和一个失败代表。
2. 参数驱动测试覆盖字符串 `strip()`、集合元素清理和清理后忽略。
3. 结构化条件测试覆盖稳定错误码、集合元素 path、range 错误。
4. DSL 测试覆盖严格 `where(...)`、可选 `whereIfPresent(...)`、`whereNull(...)` 和 `whereNotNull(...)`。
5. 自定义 `user-in-org` 测试证明显式形状与默认 `SCALAR` 都走统一规则。
6. Scope 测试证明空租户、空数据范围和空时间边界不能被忽略。
7. SQL renderer 只验证整理后的参数顺序和 `is null`/`is not null` 无参数，不重复测试归一化内部细节。

## 非目标

- 不在本次实现实体 Query-by-Example，只给它预留一致策略。
- 不把 Spring 或当前用户上下文放进主项目。
- 不把空字符串自动转换成数据库 `NULL`。
- 不自动拆分逗号字符串为集合。
- 不自动转义 `like` 通配符；这是单独的匹配语义策略。
- 不改变事务、批量结果、执行观测和缓存模型。

## 实施顺序

1. 先按已提交计划完成 TimeScope 和 `>=`、`<=` 标准 term。
2. 再实现统一条件值模型、归一化器和内置 term 形状。
3. 接入参数驱动、前端结构化条件、Java DSL 和安全 Scope。
4. 更新迁移文档，运行聚焦测试与全模块编译。
5. 完成后继续 R-020 的 Repository/Operator 组合边界复核及业务授权协作文档。
