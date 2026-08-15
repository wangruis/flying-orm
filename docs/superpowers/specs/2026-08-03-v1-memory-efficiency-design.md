# V1.0.0 动态结果与内存治理设计

## 1. 目标

V1.0.0 仍处在优化阶段，本次允许调整公开 API。目标是在不牺牲简单易用、安全和稳定的前提下，解决三类长期问题：

1. 动态表单查询不再为每一行、每一列创建大量 Map 节点。
2. 批量写入无论输入多少数据，都不能在 ORM 内部无界积压行、分片或结果明细。
3. 元数据、SQL 计划和条件编译缓存必须有容量、淘汰、失效和指标，不能依靠永久 Map 无限制增长。

本次只治理 JVM 内存和对象生命周期，不改变 ATOMIC、INDEPENDENT、UNKNOWN 等事务语义，也不把监控框架或上层容器引入主项目。

## 2. 总体原则

- `flying-orm-core` 继续保持纯 Java，不依赖 Caffeine。
- 长期缓存统一放在 `flying-orm-rdb`，由同一套策略和统计模型管理。
- 缓存只保存不含本次参数值的稳定结构，不能缓存租户值、用户值、查询结果或请求上下文。
- 大批量必须从 Publisher 按需拉取，内存上限由分片、并发、预取和字节预算共同决定。
- 对外发布的数据结构必须不可修改；内部可以在发布前完成解码，但发布后不能再变化。
- 所有上限都有安全默认值，也允许在统一组装入口覆盖；不能使用“0 表示无限”作为默认配置。
- 超限必须在继续拉取上游、申请更多连接或执行更多分片前失败，并返回稳定错误分类。

## 3. 紧凑动态行

### 3.1 公开模型

动态查询默认返回 `DynamicRow`：

```java
Flux<DynamicRow> rows = forms.select(form, where);
Mono<PageResult<DynamicRow>> page = forms.page(form, where, pageQuery);
List<DynamicRow> syncRows = forms.sync().select(form, where);
```

`DynamicRow` 是只读 Map，同时提供按序号和类型读取：

```java
public final class DynamicRow implements Map<String, Object> {
    Object get(String column);
    <T> T get(String column, Class<T> type);
    Object value(int index);
    String columnName(int index);
    int columnCount();
    Map<String, Object> toMap();
}
```

普通调用仍然使用 `row.get("name")`。只有确实要获得独立普通 Map 时才调用 `toMap()`，这个方法会显式产生一次物化成本。

### 3.2 内部布局

```text
RowLayout
  String[] columnNames
  Map<String, Integer> indexes

DynamicRow
  RowLayout layout
  Object[] values
```

- 同一个 R2DBC Result 的所有行共享一个 `RowLayout`。
- `RowLayout` 只活到结果流及其行对象不再被引用，不进入全局缓存。
- 结果列不超过 8 个时直接在线性数组中查找；列更多时才建立一次共享索引，避免小结果为了索引再创建 HashMap。
- 每行只分配一个 `Object[]` 和一个 `DynamicRow`，不创建 `LinkedHashMap` 节点和只读包装器。
- `get`、`containsKey` 和类型读取通过共享索引完成。
- `forEach` 直接按数组遍历；`entrySet`、`keySet`、`values` 返回轻量只读视图，不复制整行。
- 所有修改方法统一抛出 `UnsupportedOperationException`。

### 3.3 列名规则

- 保留驱动返回的原始列标签和顺序。
- 名称访问保持精确匹配，不偷偷改变大小写规则。
- 同一结果集中出现重复列标签时直接抛出稳定的“列标签冲突”错误，要求 SQL 使用别名，不能像 Map 那样静默覆盖前一列。
- 按序号访问仍需检查越界，并返回包含列数和请求下标的明确错误。

### 3.4 解码和对象映射

- R2DBC 回调先把驱动值读入 `Object[]`。
- JSON、数组、Vector、时间和 LOB 在行发布前写入新的值数组，不再复制 Map。
- `RowMapper<T>` 改为接收 `DynamicRow`，record、Bean 和自定义映射直接读取数组布局。
- 别名映射编译成列序号转换，不再为每行创建改名后的 Map。
- `ReactiveSqlExecutor`、FormClient、Operator、Native SQL 和分页结果统一使用 `DynamicRow`，不保留第二套 Map 查询内核。

## 4. 批量写入内存边界

### 4.1 默认预算

`BatchWriteOptions` 增加明确的内存字段，并取消默认无限行数：

```text
chunkSize              默认 500
concurrency            ATOMIC 固定 1，INDEPENDENT 默认 1
maxRows                默认 100000
maxBufferedBytes       默认 32 MiB
maxResultChunks        默认 4096
```

这些是单次批量任务的预算。单次调用可以进一步收紧或调大，但不能超过客户端启动时确定的硬上限。

客户端级 `BatchMemoryLimits` 提供最后一道边界：

```text
maxChunkSize           默认 10000
maxConcurrency         默认 32
maxRows                默认 10000000
maxBufferedBytes       默认 256 MiB
maxResultChunks        默认 65536
```

硬上限可以在创建 `FlyingOrmClients` 时调整，运行中的单次请求不能覆盖它。这样普通调用有保守默认值，大任务也可以在启动配置中明确获得更大预算，而不是通过某次请求临时放开整个进程。

### 4.2 输入和预取

- List 入口只复制外层引用列表，不在调用开始时复制每一行 Map。
- 行在 Publisher 被请求并进入当前分片时才校验、编码并快照成 `Object[]`。
- `buffer(chunkSize)` 后的并发执行显式使用预取 `1`，不能沿用 Reactor 的隐藏默认预取。
- INDEPENDENT 在途行数最多接近 `chunkSize * concurrency`，再加一个正在组装的分片。
- 每个参数值按稳定规则估算重量；字符串、字节数组、ByteBuffer、数组和常见容器按内容长度计入。
- 单行已经超过 `maxBufferedBytes` 时，在创建 Statement 前直接拒绝。
- 分片执行完成、失败或取消后立即归还对应字节预算。

字节预算是保护阈值，不宣称等于 JVM 精确对象大小。它的目的，是阻止明显的大字符串、大二进制和高并发分片同时留在堆里。

### 4.3 RECEIPT 模式

当前 ATOMIC + RECEIPT 会先 `collectList()` 全部分片，这个行为必须删除。

新流程：

1. 先按 operationId 和计划摘要查询已有回执。
2. 没有回执时，在同一事务中预留 operationId，边消费、边执行、边增量计算 payload 摘要。
3. 所有分片成功后写入最终摘要和汇总，再提交事务。
4. 已有完成回执时，只流式消费输入并计算摘要，不执行写入；摘要一致才返回已有结果。
5. 摘要不一致返回稳定的幂等冲突错误。

整个流程最多保留当前在途分片，不再为了摘要保存整批参数。

### 4.4 结果内存

- `writeBatchChunks()` 保持逐分片流式返回，适合超大批量。
- `writeBatch()` 可以汇总结果，但最多保存 `maxResultChunks` 条分片明细。
- List 输入按实际行数校验；Publisher 输入根据必填的 `maxRows` 和 `chunkSize` 计算最坏分片数。最坏分片数超过 `maxResultChunks` 时，`writeBatch()` 在订阅输入和执行 SQL 前拒绝，使用方改用 `writeBatchChunks()`。
- 不允许静默截断失败或成功分片；使用方需要超过汇总上限的明细时必须选择流式入口。
- ATOMIC 的汇总可压缩为连续范围，但 UNKNOWN、失败和冲突信息必须完整保留。

### 4.5 取消和异常

- 取消后停止向上游请求新行，释放当前缓冲和连接。
- 内存超限、行数超限和结果明细超限使用不同稳定错误码。
- 观测事件只记录预算、峰值、行数和分片数，不记录参数值。

## 5. 统一缓存治理

### 5.1 缓存区域

V1.0.0 统一管理以下区域：

| 区域 | 保存内容 | 默认状态 | 最大权重 | 单条最大权重 | 空闲过期 |
| --- | --- | --- | ---: | ---: | --- |
| metadata | DynamicForm、TableMetadata | 保持显式开启 | 16384 | 1024 | 5 分钟 |
| sql-plan | 不含参数值的 SQL、参数顺序和列布局计划 | 有界开启 | 32768 | 2048 | 10 分钟 |
| condition-plan | 不含参数值的结构化条件形状计划 | 默认关闭，可显式开启 | 16384 | 1024 | 10 分钟 |
| entity-mapping | 实体映射、回调和读写计划 | 跟随 Class 生命周期并补有界子缓存 | 每个 Class 64 | 16 | 跟随 Class |

条件形状通常来自前端，基数不可预测，因此默认不为了理论命中率增加老年代压力。只有压测证明有稳定重复形状时才显式打开。

### 5.2 策略模型

`flying-orm-rdb` 提供统一策略：

```java
OrmCachePolicy policy = OrmCachePolicy.builder()
        .metadata(CacheRegionPolicy.metadataDefaults())
        .sqlPlans(CacheRegionPolicy.sqlPlanDefaults())
        .conditionPlans(CacheRegionPolicy.disabled())
        .build();
```

每个区域至少包含：

```text
enabled
maximumWeight
maximumEntryWeight
expireAfterAccess
recordStats
```

- 使用 Caffeine W-TinyLFU 做准入和淘汰。
- 每个条目重量最少为 1，`maximumWeight` 同时形成条目数的硬上界。
- 超过 `maximumEntryWeight` 的单个计划不进入缓存，直接执行本次请求。
- 禁止同时维护另一份永久 Map 作为旁路索引。

### 5.3 权重规则

- 元数据：基础权重 + 字段数 + 索引、外键和约束数量的加权值。
- SQL 计划：基础权重 + SQL 字符长度分段 + 参数槽数量 + 投影列数量。
- 条件计划：基础权重 + 节点数 + 深度 + 参数槽数量。
- 实体映射：字段写入器、构造参数和别名索引数量。

权重是稳定的逻辑单位，便于不同 JVM 上得到一致行为；不使用昂贵且不可靠的运行时对象大小扫描。

### 5.4 缓存键安全

- SQL 和条件缓存键只包含规范化结构，不包含参数值。
- 缓存键包含方言、操作类型、表单结构指纹、投影、排序、分页形状、Scope 形状和 term 注册版本。
- 原生 SQL 默认不进入动态 SQL 计划缓存。
- 前端不能提供任意缓存 key；字段数、节点数、深度和 key 长度继续受安全策略限制。
- 同一个形状的不同租户值、用户值和查询值复用计划，但绝不能互相复用数据。
- 条件计划只保存节点规则、参数槽顺序和取值路径；每次请求仍创建自己的值数组，再生成不可变 `ConditionGroup`，缓存对象不能持有上一次请求的输入树。

### 5.5 失效

- createOrAlter 成功后精确失效表元数据及该表相关 SQL 计划。
- term、codec、方言或条件策略版本变化时，旧 key 自然隔离；显式替换运行时组件时同步清理相关区域。
- 手动元数据失效必须传播到依赖它的 SQL 计划区域。
- 条件计划不依赖具体表结构时不跟随 DDL 全量清理。
- 所有区域支持单 key、按表和全量清理。

### 5.6 指标

主项目暴露框架无关的 `OrmCacheSnapshot`：

```text
estimatedSize
estimatedWeight
maximumWeight
hitCount
missCount
hitRate
loadSuccessCount
loadFailureCount
totalLoadTime
evictionCount
evictionWeight
rejectedOversizedCount
```

上层可以把快照接入任意监控系统。flying-orm 不依赖 Micrometer，也不创建后台上报线程。

### 5.7 现有类级缓存

- `ClassValue` 继续用于实体元数据、生命周期回调和固定映射计划，类卸载时一起释放。
- `MappingPlan` 不能继续用无界强引用 Map 保存任意 codec 注册表实例。
- codec 维度改成有界缓存或弱身份键；普通默认注册表走单独快路径。
- 缓存中不能保存请求对象、动态行、Publisher、连接或订阅状态。

## 6. 统一组装入口

`FlyingOrmClients.Builder` 统一接收：

```java
.cachePolicy(OrmCachePolicy.safeDefaults())
.batchMemoryLimits(BatchMemoryLimits.defaults())
.batchWriteOptions(BatchWriteOptions.defaults())
```

- 使用方不需要分别寻找三个缓存实现类。
- 单次批量选项只能收紧或显式覆盖单次预算，不能绕开进程级硬上限。
- 主项目仍然不包含上层框架自动配置。

## 7. 验证

### 7.1 DynamicRow

- Map 读取语义、顺序、null、只读行为和重复列标签测试。
- 同一结果集共享布局测试。
- JSON、数组、Vector、时间和 LOB 解码不复制 Map 的测试。
- record、Bean、自定义 RowMapper 和别名映射测试。
- JMH 对比 LinkedHashMap 与 DynamicRow 的分配率、吞吐和读取延迟。

### 7.2 批量

- Publisher 请求量、预取和最大在途分片测试。
- 字节预算、单行超限、取消释放和错误码测试。
- ATOMIC、INDEPENDENT、UNKNOWN 和 RECEIPT 原有事务语义回归。
- RECEIPT 大输入不再 collectList 的契约测试。
- 大批量峰值堆内存和 GC 分配率基准。

### 7.3 缓存

- 每个区域的容量、权重、TTL、准入、淘汰和 oversized 拒绝测试。
- DDL 精确失效和 term/codec 版本隔离测试。
- 参数值不进入 key、Scope 值不串用测试。
- 命中率、加载失败、淘汰权重和快照一致性测试。
- 高基数一次性条件压力测试，确认容量始终不超过上限。

真实数据库兼容和长时间压力验证仍放在代码能力稳定后统一执行；本阶段先用契约测试、H2 和 JMH 验证结构与内存边界。

## 8. 完成标准

- 动态查询公开返回 `DynamicRow`，默认路径不再创建逐行 LinkedHashMap。
- 类型映射和字段解码不再依赖中间 Map 复制。
- 批量写入的行、字节、在途分片、结果明细和 Reactor 预取全部有界。
- RECEIPT 模式不再保留整批参数。
- 所有长期缓存均有最大权重、淘汰、失效和统计，且缓存键不包含参数值。
- 条件计划缓存默认关闭，其他缓存采用文档规定的安全默认值。
- V1.0.0 公开 API 基线、示例、Javadoc 和性能基准全部同步更新。
- core、rdb 和 testkit 的关键测试通过，未引入新的 Spring 依赖或第二套执行内核。
