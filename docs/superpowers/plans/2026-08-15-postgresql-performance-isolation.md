# PostgreSQL R2DBC 性能隔离实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在不修改 Core/RDB 生产语义的前提下，建立可验证的性能产物身份，并用单变量实验确定 PostgreSQL P95/P99 阻断来自驱动、环境还是 ORM 执行路径。

**Architecture:** 第一批只修改 `flying-orm-benchmark` 的报告与运行证据，报告必须记录实际生效的 fetchSize、驱动版本和已加载关键 class 的 SHA-256。随后使用同一源码、同一参数和独立 JVM，以 ABBA 顺序隔离 PostgreSQL 驱动版本；只有得到重复一致的根因证据后，才另行设计生产优化。

**Tech Stack:** Java 21、Maven 3.9.16、JUnit Jupiter、R2DBC PostgreSQL、HdrHistogram、Docker PostgreSQL。

## Global Constraints

- 生命线顺序固定为：简单易用、开箱即用、稳定安全，然后才是高性能、高并发、高吞吐和低延迟。
- 本计划不修改 `flying-orm-core` 或 `flying-orm-rdb` 的生产代码，也不回退既有资源、取消、LOB、超时和事务保护。
- 不执行 Git 提交、推送、切换分支或历史改写。
- 仅运行 benchmark 聚焦测试；真实库实验完成前不运行全量 Maven 门禁。
- 性能实验一次只改变一个变量，原始 JSON、完整命令和环境证据必须保留在 `target/benchmark-results`。

---

### Task 1: 让报告证明实际运行产物和有效参数

**Files:**
- Create: `flying-orm-benchmark/src/main/java/com/flying/orm/benchmark/database/BenchmarkRunIdentity.java`
- Modify: `flying-orm-benchmark/src/main/java/com/flying/orm/benchmark/database/DatabasePerformanceReport.java`
- Modify: `flying-orm-benchmark/src/main/java/com/flying/orm/benchmark/database/DatabasePerformanceReportWriter.java`
- Modify: `flying-orm-benchmark/src/main/java/com/flying/orm/benchmark/database/ReactivePerformanceArguments.java`
- Modify: `flying-orm-benchmark/src/main/java/com/flying/orm/benchmark/database/RealDatabasePerformanceRunner.java`
- Modify: `flying-orm-benchmark/src/main/java/com/flying/orm/benchmark/database/ReactivePerformanceDatabaseRunner.java`
- Test: `flying-orm-benchmark/src/test/java/com/flying/orm/benchmark/database/DatabasePerformanceReportWriterTest.java`
- Test: `flying-orm-benchmark/src/test/java/com/flying/orm/benchmark/database/RealDatabasePerformanceRunnerTest.java`

**Interfaces:**
- Consumes: `SqlExecutionOptions.fetchSize()`、运行时已加载 class 资源、Git 当前 HEAD 和 tracked diff。
- Produces: package-private `BenchmarkRunIdentity.capture(...)`；报告中的实际 fetchSize、显式覆盖标记、驱动实现版本、源码状态和关键 class SHA-256。

- [ ] **Step 1: 先写失败测试**

  测试必须证明：未提供 `--fetch-size` 时报告写出生产配置的实际数值，而不是“生产默认”；任意 `--git-commit` 标签不能替代自动采集的 HEAD；报告包含 RDB、benchmark 和 driver class SHA-256 以及驱动实现版本。

- [ ] **Step 2: 运行聚焦测试并确认 RED**

  ```powershell
  & 'D:\apache-maven-3.9.16\bin\mvn.cmd' '-Dmaven.repo.local=D:\MavenRepository' '-pl' 'flying-orm-benchmark' '-am' '-Dtest=DatabasePerformanceReportWriterTest,RealDatabasePerformanceRunnerTest' '-Dsurefire.failIfNoSpecifiedTests=false' test
  ```

  Expected: FAIL，原因是现有报告只有调用方提供的 git 标签和 `fetchSizeOverride`，没有运行产物身份。

- [ ] **Step 3: 最小实现身份采集**

  `BenchmarkRunIdentity` 仅执行本地只读采集：`git rev-parse HEAD`、tracked diff SHA-256、dirty 标记、classpath SHA-256、关键 class 字节 SHA-256、VM/GC 名称。报告不得写入绝对路径、数据库 URL、用户名、密码、SQL 或参数值；Git 不可用或关键 class 无法读取时直接拒绝生成正式性能报告。

- [ ] **Step 4: 写入实际 fetchSize 和驱动版本**

  `ReactivePerformanceArguments.reportParameters()` 必须记录 `SqlExecutionOptions` 最终生效值；`ReactivePerformanceDatabaseRunner` 从已实例化驱动 class 的 package/module 元数据提取实现版本，不能只写 metadata 名称。

- [ ] **Step 5: 运行聚焦测试并确认 GREEN**

  重复 Step 2 命令，Expected: PASS。

### Task 2: 冻结 PostgreSQL 单变量实验协议

**Files:**
- Modify: `flying-orm-benchmark/src/test/java/com/flying/orm/benchmark/database/RealDatabasePerformanceRunnerTest.java`
- Output only: `flying-orm-benchmark/target/benchmark-results/postgresql-driver-ab-20260815/`

**Interfaces:**
- Consumes: Task 1 的身份字段和现有 `--scenarios queryById --fetch-size 0 --phase-diagnostics true`。
- Produces: 六个独立 JVM 的 JSON/Markdown、命令清单和 SHA-256 清单。

- [ ] **Step 1: 增加参数契约测试**

  验证 `queryById`、显式 fetchSize 0、阶段诊断和 30/60 秒计时可同时解析并写入报告。

- [ ] **Step 2: 运行测试并确认现有或补强契约 GREEN**

  使用 Task 1 的聚焦测试命令，不连接数据库。

- [ ] **Step 3: 构建一次候选 class**

  使用 Maven 3.9.16 和 `D:\MavenRepository`，只构建 Core、RDB、benchmark 所需 class；不运行全量 quality。

- [ ] **Step 4: 按 A-B / B-A / A-B 运行六个新 JVM**

  A=`r2dbc-postgresql 1.1.1.RELEASE`，B=`1.1.2.RELEASE`；其余参数固定：fetchSize 0、池/并发 16、seed 10000、预热 30 秒、测量 60 秒、只运行 `queryById`。每次开始前确认仅 PostgreSQL 容器运行且连接池计数归零。

- [ ] **Step 5: 校验实验身份和有效性**

  六份报告的源码 HEAD、tracked diff hash、RDB/benchmark class SHA 必须一致；每个 A/B 对只有 driver version/class SHA 不同；错误数、预热错误、最终 active/pending 连接必须为零，否则该轮作废。

### Task 3: 形成根因判断，不修改生产代码

**Files:**
- Output only: `flying-orm-benchmark/target/benchmark-results/postgresql-driver-ab-20260815/analysis.md`

**Interfaces:**
- Consumes: 六份有效 JSON 的 throughput、P50/P95/P99、acquire/execute/release、CPU、heap 和 driver identity。
- Produces: 驱动、环境或 ORM 路径三选一的证据结论，以及下一次唯一允许改变的变量。

- [ ] **Step 1: 计算三个配对块的比值**

  分别比较 A/B 的 throughput、P95、P99 和阶段延迟；不得跨小时直接拿绝对值代替配对结果。

- [ ] **Step 2: 应用裁决规则**

  三对中至少两对同方向且幅度达到 10%，才把变化归为稳定趋势；A/B 同时随时间恶化则判为环境状态；execute-only 随驱动版本变化才升级为驱动候选。

- [ ] **Step 3: 输出结论与下一步**

  结论只描述证据，不修改 timeout、LOB、取消、连接或 fetch 生产策略。若仍不稳定，下一步只能是同版本 A/A 长跑；若驱动差异稳定，再决定驱动版本；若 ORM 固定开销被证明，再单独设计安全的标量 fast path。

### Task 4: 原生 R2DBC 与 ORM 查询路径隔离

**Files:**
- Modify: `flying-orm-benchmark/src/main/java/com/flying/orm/benchmark/database/ReactivePerformanceScenario.java`
- Modify: `flying-orm-benchmark/src/main/java/com/flying/orm/benchmark/database/ReactivePerformanceScenarioRunner.java`
- Modify: `flying-orm-benchmark/src/main/java/com/flying/orm/benchmark/database/ReactivePerformanceDatabaseRunner.java`
- Test: `flying-orm-benchmark/src/test/java/com/flying/orm/benchmark/database/RealDatabasePerformanceRunnerTest.java`
- Output only: `flying-orm-benchmark/target/benchmark-results/postgresql-raw-orm-ab-20260815/`

**Interfaces:**
- Consumes: 同一个 `ConnectionPool`、`PhaseTimingConnectionFactory`、目标 SQL/bind marker、fetchSize 0 和 load probe。
- Produces: 固定场景 `rawQueryById`；它完整消费一行并通过 `usingWhen` 归还连接，不绕过测试工具的超时、错误计数和池泄漏检查。

- [ ] **Step 1: 写 raw 查询失败契约**

  先验证场景解析、bind/fetchSize、单行消费、成功/失败/取消连接关闭；生产 benchmark 尚无该场景时测试必须编译失败或断言失败。

- [ ] **Step 2: 实现最小 raw 场景并复跑聚焦测试**

  raw 路径只使用 R2DBC SPI；不得复制 ORM 资源状态机或进入 Core/RDB 生产代码。

- [ ] **Step 3: 按 ORM-RAW / RAW-ORM / ORM-RAW 运行六个独立 JVM**

  驱动固定 `1.1.2.RELEASE`，其余参数与 Task 2 一致。三个配对中至少两对同方向且达到 10%，才能把差异升级为 ORM 路径候选。

## Self-Review

- Spec coverage: 报告身份、实际 fetch、驱动隔离、配对规则和禁止修改生产语义均有明确任务。
- Placeholder scan: 无 TBD/TODO；每步有文件、命令、预期结果或裁决标准。
- Type consistency: `BenchmarkRunIdentity` 仅属于 benchmark 包；Core/RDB 公共 API 不变。
