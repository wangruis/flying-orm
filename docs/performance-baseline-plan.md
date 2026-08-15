# flying-orm Performance Baseline Plan

flying-orm 的性能结论必须来自可复现的 JMH 或压测结果，不能只凭代码结构判断。

## Current Runner

`flying-orm-benchmark` 提供 `com.flying.orm.benchmark.BenchmarkRunner` 作为固定入口。

默认配置：

- include: `com.flying.orm.benchmark.*Benchmark`
- forks: `1`
- threads: `1`
- warmup iterations: `3`
- warmup time per iteration: `1s`
- measurement iterations: `5`
- measurement time per iteration: `1s`
- result: `target/benchmark-results/flying-orm-jmh.json`
- result format: JSON

可选参数：

- `--include <regex>`：只跑匹配的基准
- `--result <file>`：指定结果文件
- `--forks <n>`：指定 fork 次数
- `--threads <n>`：固定并发线程数
- `--warmup <n>`：指定预热次数
- `--warmup-time <seconds>`：指定每轮预热秒数
- `--measurement <n>`：指定采样次数
- `--measurement-time <seconds>`：指定每轮测量秒数
- `--mode <throughput|average|sample|single-shot>`：覆盖基准类声明的统计模式；需要 P95/P99 时使用 `sample`

## Current flying-orm Benchmarks

- batch insert plan creation
- batch insert row mapping
- batch upsert plan creation
- batch upsert row mapping
- frontend structured-condition compilation
- SQL where rendering
- record/Bean row mapping
- record/Bean entity value extraction
- Caffeine 元数据热读、多表读取和失效
- 响应式执行器直接调用、默认执行保护包装和执行观测包装开销

比较候选结果和历史基线时，两轮必须使用相同的 JDK、堆参数、fork、线程数、预热时间和测量时间。吞吐使用 `throughput`，延迟分位使用 `sample`；JSON 原始结果应和汇总报告一起保留，不能只摘一个最好看的数字。

## Baseline Rule

现在的 benchmark 只是测量入口。只有在同一机器、同一 JVM、同一参数下重复运行等价场景，并保留原始结果后，才能写性能改善结论。

### RC 回归判定

正式候选版和基线版在同一台安静机器上交替执行，每边至少三轮。建议使用 2 forks、5 次 2 秒预热、
8 次 2 秒测量；比较三轮 score 的中位数，不能拿最好一轮互比。

- 中位数变慢不足 5%：通过本项，仍保留原始 JSON。
- 中位数变慢达到 5% 但不足 10%：告警，清理后台负载后重跑；连续两组仍同方向才进入分析。
- 中位数变慢达到 10%：阻断 RC，除非 profiling 证明是已审核的等价交换并在版本说明中记录。
- 三组配对结果有两组以上同方向，才把变化当成稳定趋势；方向反复时按环境噪声处理并重跑。
- 任一核心场景低于 1 ns/op、结果出现 NaN/Infinity、JDK/JVM/JMH 参数不一致，或运行期间机器有明显后台负载，本轮作废。
- throughput 报告中的 scorePercentiles 不是请求延迟；P95/P99 必须另用 `--mode sample` 运行后判断。
- 数据库场景只要出现非预期错误、连接泄漏、超时或取消，即使吞吐更高也不能通过。

阈值是相对同机历史基线的门禁，不是跨机器的绝对性能承诺。正式性能声明还必须带 CPU、内存、OS、
Git commit、完整命令和原始 JSON。

## 真实数据库长跑入口

JMH 继续测纯 Java 热路径；真实 R2DBC 执行由 `RealDatabasePerformanceRunner` 测量。本地认证环境需要固定
MySQL/PostgreSQL 容器、16 连接池、业务并发和批量大小，并记录 ops/s、rows/s、P50/P95/P99、错误分类、
负载器 CPU、堆峰值和连接池峰值。两个驱动各自在独立 JVM 中运行，避免传递依赖和客户端资源互相干扰。
Docker 编排和 PowerShell 入口属于单独维护的本地认证资产，不随源码仓库分发；复跑时必须把脚本版本和
完整命令一起写进证据目录。

首轮固定参数结果见 [2026-08-02 本机真实数据库性能基线](performance-database-baseline-2026-08-02.md)。
这份结果是历史起点，不是 RC 三轮门禁的替代品。

## 无数据库对比入口

候选版本和基线版本分别运行 JMH 并保留 JSON。表示同一场景的 benchmark 方法名必须一致，例如都使用 `renderWhere`；包名和类名可以不同。然后运行：

```bash
java -cp "target/classes;<benchmark运行时classpath>" \
  com.flying.orm.benchmark.BenchmarkComparisonRunner \
  --flying target/benchmark-results/flying.json \
  --baseline target/benchmark-results/baseline.json \
  --output target/benchmark-results/comparison.md
```

比较器会拒绝 JDK、线程数、fork、预热、测量配置、模式或单位不一致的同名场景。吞吐模式按“越高越好”计算改善率，其余耗时模式按“越低越好”计算，并在 sample 结果存在时同时输出 P50/P95/P99。

正式报告使用 [performance-comparison-template.md](performance-comparison-template.md)。真实数据库的有界并发可以用 `ReactiveConcurrencyProbe` 运行，它会返回请求数、完成数、失败数、取消数、峰值并发、耗时和异常类型汇总；探针不创建或接管连接池。

## Latest Local Smoke

2026-08-01 使用 JDK 21.0.10、JMH 1.37、单线程、1 次 1 秒预热和 2 次 1 秒采样，执行了不连接数据库的 `ReactiveExecutorOverheadBenchmark`：

| 场景 | mean | P50 | P95 | P99 |
| --- | ---: | ---: | ---: | ---: |
| direct update | 55.950 ns/op | 100 ns | 100 ns | 100 ns |
| default execution protection | 54.278 ns/op | 100 ns | 100 ns | 100 ns |
| execution observation | 233.800 ns/op | 200 ns | 300 ns | 300 ns |

运行命令使用 `--mode sample --forks 1 --threads 1 --warmup 1 --measurement 2`。这是验证 runner、采样分位和执行包装热路径的短 smoke；预热和测量时间不足以形成正式性能结论，direct 与 protection 的小幅倒挂属于测量噪声。正式结论仍必须按模板在同机、同参数下重新执行。

同日还执行了两轮全套 16 场景短基线，并归档原始 JSON。14 个可用场景的同机变化范围为
`-7.64%` 到 `+7.98%`；direct/protected 两项因亚纳秒结果排除。完整环境、数据和判断见
[2026-08-01 本机无数据库短基线](performance-local-baseline-2026-08-01.md)。
