# Metadata Cache Benchmark Design

## Goal

给 Caffeine 元数据缓存补一层可运行的性能和并发证据。目标不是一次性证明“最终性能最强”，而是先把企业级缓存最关键的几件事测起来：高并发热表读取、容量淘汰、DDL 失效和命中率统计。

## Scope

- 在 `flying-orm-benchmark` 增加元数据缓存 JMH 基准入口。
- 在 `flying-orm-rdb` 增加一个轻量并发测试，确认同一 key 的并发读取不会把底层 metadata reader 打爆。
- 更新需求索引和功能矩阵，标记 Caffeine 缓存已经有 benchmark 入口。

不做真实数据库压测；这个阶段先用内存 delegate 控制变量。真实库 information_schema / 数据字典表压测后续单独做。

## Benchmark Scenarios

1. Hot form read
   - 多线程反复读取同一个 form/table。
   - 观察吞吐、命中率和 delegate 实际读取次数。

2. Many table read
   - 在一组表名之间轮询读取。
   - 用较小 `maxEntries` 触发 Caffeine 容量淘汰。

3. Read with invalidation
   - 读取一批表，并按固定频率 invalidate。
   - 验证失效后能重新加载，同时不会出现异常或明显阻塞。

## Components

- `MetadataCacheBenchmark`
  - 放在 `flying-orm-benchmark/src/main/java/com/flying/orm/benchmark`。
  - 使用 `@State(Scope.Benchmark)` 管理 `CachedReactiveFormMetadataReader` 和计数 delegate。
  - 使用已有 `BenchmarkRunner` 入口统一运行。

- `CachedReactiveFormMetadataReaderTest`
  - 补一个小并发测试。
  - 验证并发读取同一个 form/table 后，delegate 读取次数保持在 1。

## Error Handling

基准 delegate 不访问数据库，避免把驱动波动混入缓存结果。缓存加载失败的真实语义已经在生产类里处理：loader 报错时移除当前缓存值，下一次读取可以重新加载。

## Verification

- `mvn -pl flying-orm-rdb -Dtest=CachedReactiveFormMetadataReaderTest test`
- `mvn -pl flying-orm-benchmark -am -DskipTests compile`
- 可选运行一次短 JMH：`mvn -pl flying-orm-benchmark -DskipTests package` 后通过 `BenchmarkRunner` 跑单个 benchmark。

## Open Decisions

- JMH 默认参数保持轻量，避免日常验证太慢。
- 真实库压测不放在本次任务里。
