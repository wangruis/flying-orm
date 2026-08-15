# MySQL 更新尾延迟与依赖审计实施计划

> 设计依据：`docs/superpowers/specs/2026-08-03-mysql-tail-latency-dependency-audit-design.md`

## 目标

这一批只解决两个发布后问题：先把 MySQL `updateById` 的尾延迟拆成连接获取、SQL 执行及提交、连接归还三个阶段，再补一个可重复、会留证据、失败会返回非零退出码的正式依赖漏洞审计入口。

阶段诊断只属于 `flying-orm-benchmark`，必须显式开启。不开启时，性能 runner 继续直接把连接池交给 `R2dbcSqlExecutor`，主项目运行时和 ORM 公共 API 都不增加诊断开销。

## Task 1：实现单次操作的阶段计时

**文件**

- 新增：`flying-orm-benchmark/src/main/java/com/flying/orm/benchmark/database/DatabaseOperationPhaseRecorder.java`
- 新增：`flying-orm-benchmark/src/main/java/com/flying/orm/benchmark/database/PhaseTimingConnectionFactory.java`
- 修改：`flying-orm-benchmark/src/main/java/com/flying/orm/benchmark/database/ReactiveDatabaseLoadProbe.java`
- 新增：`flying-orm-benchmark/src/test/java/com/flying/orm/benchmark/database/DatabaseOperationPhaseRecorderTest.java`

**步骤**

1. 先写一个内存假连接的测试，证明同一次响应式订阅能够得到 `acquire`、`executeAndCommit`、`release`、`total` 四组数据，并且满足 `total >= acquire + release`。
2. 用 Reactor Context 保存每次订阅自己的计时样本，不能使用 `ThreadLocal`，避免响应式线程切换造成串样。
3. 用连接工厂包装器记录连接池 `create()` 的完成时间，并代理 `Connection.close()` 记录归还时间。只包装性能 runner 的连接池，不进入 `flying-orm-rdb`。
4. 每条操作结束后先算出自己的 `executeAndCommit = max(0, total - acquire - release)`，然后分别写入线程安全的 HdrHistogram `Recorder`。不能拿几个 P99 相减来推算执行时间。
5. `ReactiveDatabaseLoadProbe` 只在正式测量阶段启用阶段记录，预热数据不能混进报告。
6. 运行：`mvn -pl flying-orm-benchmark -Dtest=DatabaseOperationPhaseRecorderTest,ReactiveDatabaseLoadProbeTest test`。

## Task 2：把诊断接入 runner 和报告

**文件**

- 修改：`flying-orm-benchmark/src/main/java/com/flying/orm/benchmark/database/RealDatabasePerformanceRunner.java`
- 修改：`flying-orm-benchmark/src/main/java/com/flying/orm/benchmark/database/DatabasePerformanceReport.java`
- 修改：`flying-orm-benchmark/src/main/java/com/flying/orm/benchmark/database/DatabasePerformanceReportWriter.java`
- 修改：`flying-orm-benchmark/src/test/java/com/flying/orm/benchmark/database/RealDatabasePerformanceRunnerTest.java`
- 修改：`flying-orm-benchmark/src/test/java/com/flying/orm/benchmark/database/DatabasePerformanceReportWriterTest.java`
- 修改：`certification/Invoke-Performance.ps1`
- 修改：`certification/README.md`

**步骤**

1. 先补参数和报告测试：`--phase-diagnostics true` 能被解析；关闭时 `phaseLatency` 为空；开启时 JSON 和 Markdown 都包含四阶段的样本数及 P50/P95/P99/最大值。
2. runner 仅在显式开启时使用 `PhaseTimingConnectionFactory`，每个场景使用独立 recorder，场景结束后一次性取快照。
3. 报告新增可选 `phaseLatency`。旧的总延迟、吞吐、错误率和资源指标保持原口径，不改 `schemaVersion`，因为只新增可选字段。
4. PowerShell 性能入口新增 `-PhaseDiagnostics` 开关，并只在开启时向 Java runner 传 `--phase-diagnostics true`。
5. 文档说明阶段口径和诊断用途，明确专项诊断不能代替完整三轮性能门禁。
6. 运行：`mvn -pl flying-orm-benchmark -Dtest=RealDatabasePerformanceRunnerTest,DatabasePerformanceReportWriterTest,DatabaseOperationPhaseRecorderTest,ReactiveDatabaseLoadProbeTest test`。

## Task 3：补正式依赖漏洞审计入口

**文件**

- 修改：`pom.xml`
- 新增：`certification/Invoke-DependencyAudit.ps1`
- 修改：`certification/README.md`

**步骤**

1. 在现有 `audit` profile 中通过 `nvdApiKeyEnvironmentVariable` 读取 `NVD_API_KEY`，密钥不放在命令行、POM、日志或结果清单里。
2. 脚本先检查 Java 21、Maven 3.9+、Git 和 `NVD_API_KEY`，任何前置条件缺失都立即返回非零退出码。
3. 执行 `mvn -Paudit -DskipTests verify`。OWASP 数据更新失败、扫描失败、CVSS 7.0 及以上漏洞或许可证报告失败都不能被脚本吞掉。
4. 把 HTML、JSON、许可证报告、Git 提交、起止时间、工具版本和每个证据文件的 SHA-256 放进 `target/audit-results/<run-id>`。结果目录继续由 Git 忽略。
5. 当前机器没有 `NVD_API_KEY` 时，只验证脚本会明确失败；不能把这次前置检查写成“漏洞审计通过”。

## Task 4：定向验证与结果判断

1. 编译：`mvn -pl flying-orm-benchmark -am -DskipTests compile`。
2. 运行本计划列出的少量测试，不扩大到全项目重复回归。
3. 若本机 MySQL Docker 环境可用，运行：`certification/Invoke-Performance.ps1 -Mode Smoke -Database MySql -Scenario UpdateById -PhaseDiagnostics`。该短跑只验证采样链路，不作为正式 P99 结论。
4. 运行依赖审计脚本。没有 `NVD_API_KEY` 时记录为“正式审计被密钥前置条件阻塞”；拿到密钥后再执行一次完整审计并保存证据。
5. 汇报哪一阶段主导 P99，并给出对应优化方向；在没有阶段证据前不修改连接池、事务或 MySQL 持久化参数。

## Task 5：完成更新提交策略对照验证

1. 新增固定的 `transactionalUpdateBatch` benchmark 场景：一条连接、一个显式事务、顺序更新 8 行、一次提交。
2. 保留原 `updateById` 单条自动提交基线，新增 `UpdateComparison` 脚本预设同时运行两个场景。
3. 两个场景都输出阶段延迟；批量事务的业务吞吐按 8 行一次操作换算成 `rows/s`，不能只比较 `ops/s`。
4. 在机器负载稳定时连续运行三轮 MySQL Formal 对照，以三轮中位数判断，不挑最好的一轮。
5. 不修改 `innodb_flush_log_at_trx_commit`、`sync_binlog` 等持久化设置；若事务组明显改善，只把它记录为业务显式选择，不改变单条更新默认语义。
