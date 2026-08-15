# TimeScope 设计

## 状态

- 所属主线：R-020 租户隔离与 DataScope 安全内核
- 设计状态：已确认
- 日期：2026-07-31

## 目标

`TimeScope` 用来承接上层服务算出的可信时间范围，并把它参数化合并进 flying-orm 的查询条件。它和 `TenantScope`、普通 `DataScope`、`FieldScope` 一起使用时只能继续收窄结果，不能覆盖或放宽已有范围。

本次只解决关系型数据库里的单字段时间窗口，不处理时区换算、自然语言时间、周期表达式和跨字段时间规则。这些业务语义应由上层先计算成明确边界值。

## 方案比较

### 方案一：独立 TimeScope 模型，编译成 ConditionGroup

`TimeScope` 是 core 中的不可变值对象，通过工厂方法表达不同边界，再由 `DataScope.time(...)` 转成普通条件。

优点：API 语义清楚；不侵入 R2DBC、同步桥接和方言；自动复用现有 Scope 合并链路。缺点：需要补齐 `>=` 和 `<=` 两个标准 term handler。

### 方案二：只给 DataScope 增加 time(field, start, end)

优点：代码最少。缺点：边界语义不明显，后续增加全闭区间和单边窗口时方法会迅速膨胀，也无法单独传递和复用时间范围。

### 方案三：把 TimeScope 保存在 DataScope 中，由 RDB 层单独渲染

优点：能保留更多时间元数据。缺点：引入 core 到 RDB 的额外分支，Repository、Operator 和 FormClient 都要理解新类型，当前没有必要。

采用方案一。它最符合现有 `TenantScope.toCondition()` 和 `DataScope.and(...)` 的结构，也不会增加模块。

## API 设计

在 `com.flying.orm.core.scope` 中新增 `TimeScope`。它使用嵌套边界枚举表达包含或排除，避免再增加独立公共类型。

```java
TimeScope window = TimeScope.between("created_at", start, end); // [start, end)
TimeScope closed = TimeScope.closed("created_at", start, end);  // [start, end]
TimeScope recent = TimeScope.from("created_at", start);         // [start, +infinity)
TimeScope history = TimeScope.before("created_at", end);        // (-infinity, end)

DataScope scope = DataScope.tenant("tenant_id", tenantId)
        .and(DataScope.time(window))
        .withFields(FieldScope.readable("id", "name", "created_at"));
```

`between(...)` 是默认推荐入口，采用左闭右开区间。`closed(...)` 显式兼容全闭区间。`from(...)` 和 `before(...)` 支持开放的一侧，不需要调用方传空值。

不提供含糊的 `range(...)`，也不让调用方传 SQL 操作符字符串。

## 条件与 SQL

时间边界统一编译成普通 `TermCondition`：

| API | 条件 |
| --- | --- |
| `between(field, start, end)` | `field >= ? AND field < ?` |
| `closed(field, start, end)` | `field >= ? AND field <= ?` |
| `from(field, start)` | `field >= ?` |
| `before(field, end)` | `field < ?` |

`SqlTermHandler` 增加标准 `>=` 和 `<=` 处理器，并加入默认 term 集合。它们是 SQL 标准比较操作，不需要增加数据库方言分支。

参数继续走现有 `SqlRequest` 绑定和 `ValueCodecRegistry`，任何时间值都不能拼进 SQL 文本。

## 校验规则

- 字段名不能为空，继续使用现有标识符安全链路。
- 工厂方法要求对应边界值非空。
- 双边窗口的开始值和结束值必须使用同一种可比较类型。
- `between(...)` 要求开始值严格小于结束值，避免产生空窗口。
- `closed(...)` 允许开始值等于结束值，用于表达一个精确时间点；开始值大于结束值时拒绝。
- 所有错误都在生成 SQL 前以 `IllegalArgumentException` 返回，不访问数据库。

常用的 `Instant`、`LocalDate`、`LocalDateTime`、`OffsetDateTime`、`java.sql.Timestamp` 和数字时间戳都可以按同类型边界使用。时区选择和转换由上层完成，ORM 不猜测系统默认时区。

## 数据流

1. 上层业务根据当前用户、租户和业务规则计算明确时间边界。
2. `TimeScope` 校验字段和边界，再生成参数化 `ConditionGroup`。
3. `DataScope.time(...)` 把时间条件包装成普通服务端范围。
4. `DataScope.and(...)` 将时间、租户、组织和其他服务端条件统一 AND。
5. FormClient、Repository 和 Operator 继续走现有渲染与执行链路。

同步 API 是 R2DBC 的桥接门面，因此不需要第二套实现，也不会改变响应式执行的非阻塞路径。

## 并发与性能

`TimeScope` 和 `DataScope` 都保持不可变，不使用锁，也不引入缓存。每次创建只产生最多两个条件节点，开销与手写两个普通 where 条件相同。执行时仍由数据库索引决定范围查询性能，文档示例会提醒业务表为常用时间字段建立合适索引。

## 测试范围

继续采用少量聚焦测试：

1. core 测试覆盖四类工厂生成的操作符、参数顺序和非法区间校验。
2. RDB 表单测试覆盖 `TenantScope + TimeScope + FieldScope` 的最终 SQL 与参数顺序，证明现有 AND 合并链路已自动接入。

不在本次增加真实数据库测试；`>=`、`<=`、`>`、`<` 都是现有目标数据库支持的标准比较操作。

## 非目标

- 不把 Spring、当前用户或租户上下文放进主项目。
- 不在 ORM 中计算“今天”“本周”“最近七天”。
- 不自动转换时区或数据库时间类型。
- 不增加按月、Cron、滑动窗口等调度概念。
- 不改变事务、批量写入、缓存和执行观测模型。
