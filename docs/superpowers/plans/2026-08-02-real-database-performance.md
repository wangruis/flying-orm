# 真实数据库性能长跑实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 建立并执行 MySQL/PostgreSQL 查询、更新、ATOMIC 和 INDEPENDENT 的可复跑真实性能长跑。

**Architecture:** 保留现有 JMH 测纯 Java 热路径，在 benchmark 模块内新增独立响应式负载器。负载器使用真实 R2DBC Pool、HDR Histogram 和 flying-orm 执行器，认证脚本负责固定 Docker 环境、独立 Java 进程和脱敏证据归档。

**Tech Stack:** Java 21、Reactor 3.8、R2DBC Pool 1.0.2、HDR Histogram、Jackson、MySQL 8.4、PostgreSQL 17、Maven、Docker Compose、PowerShell。

## Global Constraints

- 不修改 flying-orm 公共 API，不给 core/rdb 增加 benchmark 依赖。
- 主项目继续零 Spring 依赖。
- benchmark/testkit 可以增加运行和验证需要的依赖，普通测试不能连接外部数据库。
- 报告不保存 URL、密码、SQL 参数或业务行。
- 正式结果只代表记录中的本机和固定环境，不形成跨机器绝对承诺。
- 注释和文档使用自然、能直接看懂的中文。
- 本批统一提交，保持用户的 `AGENTS.md` 未暂存。

---

### Task 1: 通用负载探针和指标模型

**Files:**
- Modify: `pom.xml`
- Modify: `flying-orm-benchmark/pom.xml`
- Create: `flying-orm-benchmark/src/main/java/com/flying/orm/benchmark/database/ReactiveDatabaseLoadProbe.java`
- Test: `flying-orm-benchmark/src/test/java/com/flying/orm/benchmark/database/ReactiveDatabaseLoadProbeTest.java`

- [x] 在父 POM 管理 HDR Histogram 版本，benchmark 引入 HDR Histogram、R2DBC Pool 和 R2DBC SPI。
- [x] 用失败契约锁定并发上限、预热/测量分离、完成/失败计数和延迟分位。
- [x] 实现固定 worker 的真响应式持续负载，不创建每请求线程，不保存每次延迟对象。
- [x] 对整体超时、空 Publisher、同步抛错和错误分类提供稳定统计。
- [x] 运行负载探针测试。

### Task 2: 报告模型和脱敏输出

**Files:**
- Create: `flying-orm-benchmark/src/main/java/com/flying/orm/benchmark/database/DatabasePerformanceReport.java`
- Create: `flying-orm-benchmark/src/main/java/com/flying/orm/benchmark/database/DatabasePerformanceReportWriter.java`
- Test: `flying-orm-benchmark/src/test/java/com/flying/orm/benchmark/database/DatabasePerformanceReportWriterTest.java`

- [x] 定义环境、运行参数、场景结果和整体状态记录。
- [x] 输出稳定 JSON 和中文 Markdown 摘要。
- [x] 拒绝 NaN/Infinity、负计数、越界连接峰值和互相矛盾的状态。
- [x] 测试报告不包含 URL、密码、SQL 参数或业务值。

### Task 3: 两库四类真实性能场景

**Files:**
- Create: `flying-orm-benchmark/src/main/java/com/flying/orm/benchmark/database/RealDatabasePerformanceRunner.java`
- Test: `flying-orm-benchmark/src/test/java/com/flying/orm/benchmark/database/RealDatabasePerformanceRunnerTest.java`
- Modify: `flying-orm-benchmark/pom.xml`

- [x] 增加 MySQL/PostgreSQL 可选驱动 profile，默认构建不加载真实驱动。
- [x] 实现命令行参数校验和数据库选择，凭据只进入连接工厂。
- [x] 建表并准备固定查询/更新种子数据。
- [x] 实现 queryById、updateById、atomicBatchInsert、independentBatchInsert。
- [x] 每个场景采集进程 CPU、峰值堆内存和池峰值；结束后检查零借出、零等待。
- [x] 写 JSON/Markdown 后再根据整体状态决定退出成功或失败。

### Task 4: 性能执行与证据归档

**Files:**
- Create: `certification/Invoke-Performance.ps1`
- Modify: `certification/README.md`
- Modify: `docs/performance-baseline-plan.md`

- [x] 复用 `.env` 和 Compose，增加 Smoke/Formal 两档固定参数。
- [x] 编译 benchmark、为两个驱动分别生成运行时 classpath、使用独立 Java 进程执行。
- [x] 归档 environment、JSON、Markdown 和运行日志，清单不含凭据。
- [x] 先执行 Smoke，修复真实驱动或指标缺口。
- [x] 执行一轮 Formal，记录固定环境的本机结果。

### Task 5: 回归、路线和提交

**Files:**
- Create: `docs/performance-database-baseline-2026-08-02.md`
- Modify: `docs/real-database-certification.md`
- Modify: `docs/v1-roadmap.md`
- Modify: `docs/v1-release-checklist.md`

- [x] 记录正式一轮的完整参数、结果和边界，不摘取单个最好数字。
- [x] 明确候选版门禁还需要相同参数至少三轮中位数。
- [x] 运行 benchmark 聚焦测试、全项目 Maven 回归和 `git diff --check`。
- [x] 清理无用 import、变量、重复逻辑和凭据痕迹。
- [x] 批量提交并推送，保持 `AGENTS.md` 未暂存。
