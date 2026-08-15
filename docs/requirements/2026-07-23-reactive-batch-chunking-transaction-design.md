# 响应式批量分片与事务设计

## 状态

已实现首版。设计已纠偏为 R2DBC-first；传统 JDBC 风格只做阻塞桥接，不再作为独立 JDBC 执行内核推进。当前代码已覆盖默认 ATOMIC、显式 INDEPENDENT、任务限制、超时包装、事务状态清理、UNKNOWN 回执确认和相同操作编号安全重放。

## 背景

早期批量入口会一次性保存全部参数，而且没有明确的分片、事务和部分失败语义。数据量增大后，会遇到参数过多、内存占用升高、连接边界不清楚等问题，因此 V1 已统一改为可流式消费的 `BatchWriteRequest`。

本设计在现有批量插入上补齐分片和事务能力。默认保证整批原子性，同时提供显式的独立分片模式，供吞吐量优先且允许部分成功的业务使用。批量选项、分片结果、汇总结果和恢复令牌都以 R2DBC/Reactor 内核为准；同步调用方通过阻塞桥接获得同一份结果，而不是走另一套 JDBC 事务。

## 已确认的设计结论

- 默认使用 `ATOMIC`：整批共用一个连接和一个事务，任一分片失败就停止执行并回滚整批。
- 显式提供 `INDEPENDENT`：每个分片使用独立事务，普通分片失败不会影响已经提交的分片。
- `INDEPENDENT` 同时提供逐分片 `Flux` 和最终汇总 `Mono` 两种使用方式。
- 传统 JDBC 风格提供同步汇总结果入口，但内部桥接同一个 R2DBC 批量执行链路；逐分片实时结果优先保留给 R2DBC `Flux`，同步桥接后续可提供阻塞迭代式 API。
- 分片大小默认 `500`，独立模式并发数默认 `1`，使用方可以显式调整。
- `UNKNOWN` 不做盲目重试；关键业务可以显式开启事务回执，通过操作编号确认或安全重放。
- 不新增 Maven 模块，代码继续放在 `flying-orm-rdb`，通用 SQL 请求模型仍放在 `flying-orm-core`。
- R2DBC 必须保持真实非阻塞，不用 JDBC 包装 Reactor；传统 JDBC 风格入口只是同步阻塞门面，内部仍走 R2DBC，不单独维护 `DataSource`/`java.sql.Connection` 事务路径。
- 第一版不做自适应分片和自动重试，先把事务语义、背压和结果可信度做好。

## API

### 默认原子写入

```java
Mono<BatchWriteResult> insertBatch(
    FormMetadata form,
    Publisher<Map<String, Object>> rows
);
```

这个入口等价于 `BatchWriteOptions.atomic(500)`。

### 可配置写入并返回汇总

```java
Mono<BatchWriteResult> insertBatch(
    FormMetadata form,
    Publisher<Map<String, Object>> rows,
    BatchWriteOptions options
);
```

这个入口同时支持 `ATOMIC` 和 `INDEPENDENT`。独立模式出现普通分片失败时，`Mono` 正常返回 `PARTIAL` 结果，让使用方统一查看成功、失败和状态未知的分片。

### 独立分片实时结果

```java
Flux<BatchChunkResult> insertBatchChunks(
    FormMetadata form,
    Publisher<Map<String, Object>> rows,
    BatchWriteOptions options
);
```

这个入口只接受 `INDEPENDENT`。`ATOMIC` 在整批提交前没有任何一个分片可以被称为“已提交”，因此不通过这个入口伪造实时成功结果。

### 配置模型

`BatchWriteOptions` 保持紧凑，提交模式作为内部枚举，避免拆出过多零散类型。

```java
public record BatchWriteOptions(
    Mode mode,
    int chunkSize,
    int concurrency,
    long maxRows,
    Duration timeout,
    Recovery recovery
) {
    public enum Mode {
        ATOMIC,
        INDEPENDENT
    }

    public record Recovery(
        RecoveryMode mode,
        String operationId,
        Duration confirmTimeout
    ) {
    }

    public enum RecoveryMode {
        NONE,
        RECEIPT
    }
}
```

提供以下工厂方法：

- `atomic(int chunkSize)`
- `independent(int chunkSize)`
- `independent(int chunkSize, int concurrency)`
- `withMaxRows(long maxRows)`
- `withTimeout(Duration timeout)`
- `withReceipt(String operationId)`
- `withReceipt(String operationId, Duration confirmTimeout)`

`maxRows` 和 `timeout` 是稳定性保护项，`0` 表示不限制。第一版不擅自给长任务设置硬上限，由应用按业务场景显式配置。

恢复模式默认是 `NONE`，不会增加额外 SQL。`RECEIPT` 需要稳定且全局唯一的 `operationId`；关键业务应由调用方使用业务请求号生成，保证应用崩溃后仍能用同一个编号恢复。框架可以为普通调用生成编号，但自动生成的编号只适合当前调用内恢复。

## 结果模型

### 分片结果

`BatchChunkResult` 至少包含：

- `chunkIndex`：从 `0` 开始的分片编号。
- `startOffset`：该分片第一行在输入流中的位置。
- `inputCount`：分片接收的行数。
- `affectedRows`：数据库返回的影响行数。
- `status`：最终状态。
- `failure`：失败类型、消息、SQL state 和数据库错误码；成功时为空。
- `recoveryToken`：状态为 `UNKNOWN` 且启用了回执时，用于后续确认或安全重放。

状态使用能直接表达数据库事实的名称：

```java
COMMITTED
ROLLED_BACK
FAILED
UNKNOWN
```

- `COMMITTED`：数据库已确认提交。
- `ROLLED_BACK`：该分片本身执行成功，但因为同一原子事务中的其他分片失败而被确认回滚。
- `FAILED`：该分片执行失败，并且事务已确认没有提交；结果同时携带失败信息。
- `UNKNOWN`：提交阶段连接中断等原因导致客户端无法确认最终状态。

结果不复制整行数据，也不记录完整 SQL 参数，避免大批量写入时放大内存占用或泄露业务数据。使用方通过 `chunkIndex + startOffset + inputCount` 定位原始数据。

### 汇总结果

`BatchWriteResult` 包含提交模式、总输入数、总影响行数、汇总状态和分片结果。汇总状态包括：

```java
COMMITTED
PARTIAL
ROLLED_BACK
UNKNOWN
```

- 原子模式全部成功时为 `COMMITTED`。
- 原子模式执行失败并确认回滚时为 `ROLLED_BACK`。
- 独立模式同时存在成功和失败分片时为 `PARTIAL`。
- 任意提交结果无法确认时，汇总状态至少为 `UNKNOWN`，不能用成功或失败掩盖不确定性。

### 异常通道

- `ATOMIC` 的执行失败通过 `BatchWriteException` 进入 Reactor 错误通道，异常携带回滚后的 `BatchWriteResult`。
- `INDEPENDENT` 的普通 SQL 或数据约束错误作为 `FAILED` 分片返回，并继续执行后续分片。
- 原子模式失败时，失败分片标记为 `FAILED`，之前执行成功但被整批回滚的分片标记为 `ROLLED_BACK`。
- 数据源报错、无法创建连接、连接已不可用等导致任务无法继续的问题进入错误通道。
- 独立模式汇总入口遇到全局故障时，`BatchWriteException` 携带终止前已经生成的分片结果。
- 内核不自动重试写入，尤其不重试 `UNKNOWN`，避免产生重复数据。
- `FAILED` 和 `UNKNOWN` 分片的 `failure.kind()` 使用统一 `RdbErrorKind`。上层可以区分约束、死锁、锁等待、取消等情况，但是否重试仍必须结合幂等性和最终事务状态决定。

## UNKNOWN 恢复

`UNKNOWN` 不是普通 SQL 错误。它表示提交命令可能已经到达数据库，但客户端没有收到明确结果。数据库不可访问时，ORM 无法凭空判断事务结果，因此必须先如实返回 `UNKNOWN`。

### 恢复模式

- `NONE`：性能优先，不增加内部表写入；结果保留 `UNKNOWN`，使用方通过业务唯一键自行确认。
- `RECEIPT`：稳定性优先，在同一事务中写入操作回执，可以确认已提交事务，并让相同操作编号的重放保持幂等。

回执模式使用内部表 `flying_orm_batch_receipt`，至少保存：

- `operation_id`
- `chunk_index`，原子整批使用固定值，独立模式使用真实分片编号。
- `plan_hash`
- `payload_hash`
- `row_count`
- `created_at`

`operation_id + chunk_index` 是唯一键。表名允许配置，但一个数据源内必须保持稳定。框架提供 MySQL、PostgreSQL 和 H2 的建表 SQL，不在应用启动时偷偷建表；使用方开启回执前需要通过迁移或动态结构客户端完成建表。

### 写入过程

1. 事务开始后先插入一条回执占位记录，利用唯一键占住操作编号。
2. 执行业务分片，同时按规范化后的参数顺序计算摘要和行数。
3. 提交前更新回执的 `plan_hash`、`payload_hash` 和 `row_count`。
4. 业务数据和完整回执在同一个事务中提交或回滚。

未提交的回执对其他连接不可见，但唯一键冲突会让相同操作编号的并发重放等待原事务完成。原事务提交后，重放会看到唯一键冲突并读取已提交回执；原事务回滚后，占位记录消失，重放可以继续执行。

摘要使用确定性的类型标记、长度和值编码计算，不能直接依赖 `Map.toString()` 或对象默认 `hashCode()`。同一操作编号对应的执行计划、参数值、行数或分片方式不一致时，框架拒绝重放。

### 确认与安全重放

`UNKNOWN` 结果携带操作编号、分片编号、回执表名和摘要组成的恢复令牌，并提供确认入口：

```java
Mono<BatchResolution> resolveUnknown(BatchRecoveryToken token);
```

- 在主库找到匹配回执时返回 `COMMITTED`。
- 数据库不可访问或暂时没有可见回执时仍返回 `UNKNOWN`，不能仅凭一次“查不到”就断言已经回滚。
- 确认查询只访问主库，不能从可能有复制延迟的只读节点判断结果。

使用方需要恢复写入时，使用原来的 `operationId`、分片配置和数据重新调用批量入口。框架先走回执唯一键：已经提交则校验摘要后直接返回原结果；原事务已回滚则重新执行；原事务仍在处理则等待唯一键结果。数据库仍不可用时继续返回 `UNKNOWN`，不会无条件重复写入。

回执需要按业务保留周期定期清理。清理时间必须晚于业务可能发生的最迟重试时间，避免旧操作在回执删除后被再次执行。

## 执行计划

第一行决定字段布局，保持当前批量插入规则：后续行允许 `Map` 顺序不同，但规范化后的字段集合必须完全一致，也不能用大小写别名重复提交同一字段。

首个分片生成一次调用内可复用的 `BatchInsertPlan`，内容包括：

- 渲染后的 SQL。
- 排好顺序的字段和列。
- 参数数量和绑定顺序。
- 参数标记风格。
- 已知字段类型。

后续分片复用执行计划，不再重复整理字段和渲染 SQL。每行数据按计划转换成紧凑参数数组，执行结束后释放该分片数据。

第一版只做单次调用内复用，不做跨调用全局缓存。动态表结构会变化，全局缓存需要可靠的表单版本和淘汰策略，等元数据版本机制稳定并有基准数据后再加入。

## ATOMIC 数据流

1. 从上游收集第一个完整分片；空数据直接返回空的成功结果，不申请连接。
2. 根据第一行编译 `BatchInsertPlan`。
3. 获取一个 R2DBC `Connection` 并开启事务。
4. 使用同一个连接顺序执行所有分片。
5. 全部分片成功后提交事务，再返回 `COMMITTED` 汇总结果。
6. 任一分片失败时停止消费后续数据，回滚整个事务。
7. 下游取消订阅时回滚事务。
8. 成功、失败和取消路径最后都关闭连接。

原子模式不并行执行分片。同一个连接并行创建和执行多个 `Statement` 的驱动行为不统一，也会让回滚和错误定位变得不可信。

如果 SQL 执行失败，回滚成功后可以明确返回 `ROLLED_BACK`。如果提交命令发出后连接断开，客户端无法判断数据库最终是否提交，此时返回 `UNKNOWN`，不能声称已经全部回滚。

提交开始后不把普通下游取消直接解释成回滚成功。执行器会先等待提交得到明确结果；确认响应丢失时进入 `UNKNOWN`，避免提交和取消竞争导致错误状态。

## INDEPENDENT 数据流

1. 首个分片编译 `BatchInsertPlan`，后续分片复用。
2. 每个分片单独获取连接、开启事务、执行并提交。
3. 执行失败时回滚当前分片，不影响已经提交的其他分片。
4. 每个分片完成后立即发出 `BatchChunkResult`。
5. 并发执行时按实际完成顺序发出结果，不等待编号更小的慢分片。
6. 使用方通过 `chunkIndex` 恢复输入顺序；汇总入口会按分片编号整理结果。
7. 取消订阅时，已经提交的分片保持不变，正在执行的分片尝试回滚，尚未消费的数据不再申请连接。

独立模式的并发始终有上限。默认并发数为 `1`；提高并发后，同时占用的连接数不会超过配置值。具体上限应结合连接池容量设置。

## 背压与内存

- 输入使用 `Publisher`，按 `chunkSize` 有界缓冲，不提前收集整批数据。
- 每个分片只在执行期间保留自己的参数数组。
- `ATOMIC` 只预取有限分片，并保持顺序执行。
- `INDEPENDENT` 最多保留“并发数 + 少量预取”范围内的分片，不允许无界 `flatMap`。
- `maxRows` 超限属于整个任务无法继续的错误。原子模式回滚；独立模式停止继续消费；两种模式都通过 `BatchWriteException` 进入错误通道，并携带当时已有的结果。
- `timeout` 覆盖整个批量任务，而不是单条 SQL。超时后的事务处理遵循取消规则。

## 类型与代码边界

- 公开的批量配置、分片结果、汇总结果和异常放在 `flying-orm-rdb` 的批量写入包中。
- `ReactiveFormClient` 负责 API、表单校验和调用编排，不直接管理连接。
- `FormDataSqlRenderer` 负责把表单和字段布局编译成可复用执行计划。
- `R2dbcSqlExecutor` 负责连接、事务、分片调度、参数绑定和资源关闭。
- 通用 SQL 请求模型继续留在 `flying-orm-core`，但不会为了该功能新增 Maven 模块。

## 精简验证范围

本阶段只保留能保护事务边界的关键测试：

1. H2 真实集成：`ATOMIC` 的后续分片违反唯一约束后，整批数据为零。
2. H2 真实集成：`INDEPENDENT` 的中间分片失败，前后成功分片已提交，分片结果和数据库数据一致。
3. 执行器契约：成功提交一次；失败和取消会回滚；所有路径只关闭一次连接；独立并发不超过配置值。
4. H2 真实集成：回执模式下模拟提交确认丢失后，使用相同操作编号重放不会产生重复数据。

字段布局、PostgreSQL 参数标记和 `null` 类型推断沿用现有测试，不重复扩张测试组合。性能提升比例在后续 JMH 或专项压测完成前不做数字承诺。

## 暂不处理

- 批量 upsert。
- 自动重试和失败分片自动拆分。
- 自适应分片大小。
- 跨调用执行计划缓存。
- 数据库生成主键逐行回填。
- Oracle 和 SQL Server 的专项批处理优化。

这些能力都可以建立在本次事务和结果模型上，不需要改变默认 `ATOMIC` 语义。
