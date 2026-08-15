# 本机无数据库性能基线实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 不修改现有 benchmark/testkit 源码，复用已有 JMH Runner 和 JSON 比较器，建立一套能重复执行、能判断噪声和回归的本机无数据库基线。

**Architecture:** 先编译现有 benchmark 模块并生成运行时 classpath，再用完全相同的 JDK、线程、fork、预热和测量参数连续执行两轮短基线。原始 JSON 留在 `target`，仓库只提交环境、命令、汇总值、波动说明和正式 RC 阈值规则。

**Tech Stack:** Java 21、Maven 3.9、JMH 1.37、现有 `BenchmarkRunner`、现有 `BenchmarkComparisonRunner`。

**Execution Status:** 本计划已在同一批次完成；下方步骤保留为复现记录。

## Global Constraints

- 不新增或修改 `flying-orm-benchmark`、`flying-orm-testkit` 源码。
- 不连接 MySQL、PostgreSQL、H2、Oracle 或 SQL Server。
- 不形成任何与其他项目绑定的公开对比文档。
- 短 JMH 只验证链路和同机波动，不写“已经达到正式性能目标”。
- 主项目继续保持零 Spring 依赖。
- 不提交用户修改的 `AGENTS.md`。
- 本批工作统一提交，不为每个小步骤单独提交。

---

### Task 1: 验证已有工具链

**Files:**
- Verify only: `flying-orm-benchmark/src/main/java/com/flying/orm/benchmark/BenchmarkRunner.java`
- Verify only: `flying-orm-benchmark/src/main/java/com/flying/orm/benchmark/BenchmarkComparisonRunner.java`
- Test: `flying-orm-benchmark/src/test/java/com/flying/orm/benchmark/BenchmarkRunnerTest.java`
- Test: `flying-orm-benchmark/src/test/java/com/flying/orm/benchmark/BenchmarkComparisonRunnerTest.java`

- [ ] 运行 Runner、比较器和 benchmark smoke 测试。
- [ ] 确认 JSON 输出、参数一致性校验和分位值报告入口可用。
- [ ] 确认 benchmark/testkit 工作区无源码差异。

### Task 2: 执行两轮短基线

**Files:**
- Generated only: `flying-orm-benchmark/target/benchmark-results/local-run-a.json`
- Generated only: `flying-orm-benchmark/target/benchmark-results/local-run-b.json`
- Generated only: `flying-orm-benchmark/target/benchmark-results/local-repeatability.md`

- [ ] 编译 benchmark 模块并生成完整运行时 classpath。
- [ ] 用 `forks=1`、`threads=1`、`warmup=1x1s`、`measurement=2x1s` 执行第一轮全部无数据库场景。
- [ ] 使用完全相同参数执行第二轮。
- [ ] 用现有比较器校验两份 JSON 的环境和 JMH 参数一致，并生成波动报告。

### Task 3: 固化回归规则和本机记录

**Files:**
- Create: `docs/performance-local-baseline-2026-08-01.md`
- Modify: `docs/performance-baseline-plan.md`
- Modify: `docs/v1-roadmap.md`

- [ ] 记录 Git commit、JDK、Maven、OS、CPU 标识、逻辑处理器数、JMH 参数和运行命令。
- [ ] 汇总两轮结果及波动，明确短跑不能作为正式性能承诺。
- [ ] 定义正式 RC 的提醒、阻断和结果作废条件。
- [ ] 明确真实数据库吞吐、P95/P99 和连接占用仍在最后阶段验证。

### Task 4: 交付验证

**Files:**
- Verify: all changed docs

- [ ] 运行 `git diff --check`。
- [ ] 运行 `mvn -P release-artifacts -DskipTests clean verify`。
- [ ] 只暂存性能文档和计划，排除 `AGENTS.md`、benchmark/testkit 源码。
- [ ] 使用自然中文提交说明并推送 `main`。
