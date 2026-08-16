# Core/RDB Production Re-audit Cycle 3 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. This run is inline because the active collaboration rules forbid creating additional subagents.

**Goal:** 再次完整审核并独立 Code Review 当前 Core/RDB 生产 Java，在不新增生产类型、不改变公共 API、不反向取消既有有效修复的前提下，修复全部可复现问题并通过完整质量门禁。

**Architecture:** 以当前脏工作区为不可破坏基线，先用代码图谱定位入口、状态机和高风险调用链，再用排序后的物理文件清单补齐逐文件覆盖。审核与 Code Review 使用不同检查矩阵；只有具备当前生产路径、明确影响和可复现 RED 的候选才允许修改现有生产类型。

**Tech Stack:** Java 21、Maven 3.9.16、JUnit Jupiter、Reactor/R2DBC、JDBC、JaCoCo、SpotBugs、Checkstyle、codebase-memory-mcp。

## Global Constraints

- 范围仅限 `flying-orm-core/src/main/java` 与 `flying-orm-rdb/src/main/java`；测试只作为契约证据。
- 不新增生产类型，不删除、改名、改签名、改返回类型、改可见性或新增 public/protected API。
- 不回退或覆盖工作区已有变更，不使用破坏性 Git 命令，不暂存、提交或推送。
- 既有修复只有同时出现当前生产路径、可复现反例和不与现有绿色契约互斥的新 RED 时才能重开。
- 生产类型不得达到 400 行或 30 个可调用方法；修改后必须重新核对。
- Maven 固定使用 `D:\apache-maven-3.9.16` 与 `D:\MavenRepository`。

---

### Task 1: 冻结当前源码与契约基线

**Files:**
- Read: `AGENTS.md`
- Read: `.superpowers/sdd/2026-08-12-core-rdb-production-audit/accepted-contracts.md`
- Create: `.superpowers/sdd/2026-08-12-core-rdb-production-reaudit-cycle3/progress.md`

**Interfaces:**
- Consumes: 当前分支、HEAD、脏工作区、Core/RDB 生产源码、V2.0.0 API 快照。
- Produces: 可复算的文件清单、哈希、行数、最大类型和本轮反振荡规则。

- [ ] **Step 1:** 记录分支、HEAD、状态和已有未提交文件，不修改 Git 状态。
- [ ] **Step 2:** 生成 Core/RDB 排序物理清单、逐文件内容摘要、行数和大类门禁统计。
- [ ] **Step 3:** 刷新或验证代码图谱，并记录图谱覆盖不足时的物理补充范围。
- [ ] **Step 4:** 导入上一轮接受契约，固定本轮 finding 门槛。

### Task 2: 全量生产代码审核

**Files:**
- Read: `flying-orm-core/src/main/java/**/*.java`
- Read: `flying-orm-rdb/src/main/java/**/*.java`
- Create: `.superpowers/sdd/2026-08-12-core-rdb-production-reaudit-cycle3/audit-report.md`

**Interfaces:**
- Consumes: Task 1 清单和图谱入口。
- Produces: 每个生产文件都被纳入的审核记录，以及带生产调用路径和影响的候选清单。

- [ ] **Step 1:** 审核 Core 的条件 AST、Scope、元数据、分页、Join、codec 与 SQL 模型。
- [ ] **Step 2:** 审核 RDB 的渲染、模板、映射、字段保护、缓存、元数据、Schema 与 Repository。
- [ ] **Step 3:** 审核 JDBC/R2DBC 连接、事务、批量、取消、超时、清理、fatal 和观测矩阵。
- [ ] **Step 4:** 对热点、循环、无界状态、阻塞调用、错误脱敏和公共 API 做横向扫描。
- [ ] **Step 5:** 拒绝没有当前可达路径、影响或失败契约的猜测，不把替代设计升级为 defect。

### Task 3: 独立 Code Review 与反振荡复核

**Files:**
- Read: Task 2 涉及的生产文件及直接测试。
- Create: `.superpowers/sdd/2026-08-12-core-rdb-production-reaudit-cycle3/code-review-report.md`

**Interfaces:**
- Consumes: Task 2 候选、上一轮接受契约、当前 public API baseline。
- Produces: 独立确认、降级或拒绝结论；任何重开项必须附新的非互斥 RED 设计。

- [ ] **Step 1:** 从调用方、被调用方、四库方言和 JDBC/R2DBC 对称性反向复核候选。
- [ ] **Step 2:** 检查候选是否实际推翻已有绿色契约；若是且无新反例，必须拒绝。
- [ ] **Step 3:** 对确认 finding 给出精确生产入口、失败结果、允许修改的现有文件和直接测试类。
- [ ] **Step 4:** 确认没有新增 public/protected 声明和生产类型。

### Task 4: 测试先行修复确认问题

**Files:**
- Modify: 仅确认 finding 所在的现有生产 Java 类型。
- Modify: 仅与 finding 直接对应的现有测试类。

**Interfaces:**
- Consumes: Task 3 确认 finding。
- Produces: 每项都具备 RED 失败证据、最小兼容实现和 GREEN 证据的修复。

- [ ] **Step 1:** 在现有测试类写一条能从公开或真实内部生产入口复现问题的精确契约。
- [ ] **Step 2:** 单独运行该测试，确认旧实现因目标原因失败；若测试已绿，撤销测试并拒绝候选。
- [ ] **Step 3:** 只修改现有内部实现，不改变 API、成功路径、事务终态或错误分类之外的行为。
- [ ] **Step 4:** 运行直接测试类和相邻对称矩阵，确认 GREEN 且无回归。
- [ ] **Step 5:** 再次独立阅读修复差异，检查异常图、资源释放、参数顺序和并发边界。

### Task 5: 完整验证与交付证据

**Files:**
- Create: `.superpowers/sdd/2026-08-12-core-rdb-production-reaudit-cycle3/final-report.md`
- Update: `.superpowers/sdd/2026-08-12-core-rdb-production-reaudit-cycle3/progress.md`

**Interfaces:**
- Consumes: 当前完整工作区和所有本轮修复。
- Produces: 可复查的测试、API、覆盖率、静态分析、规模和工作区边界结论。

- [ ] **Step 1:** 运行聚焦回归并读取失败/错误/跳过数量。
- [ ] **Step 2:** 运行 `-Pquality -pl flying-orm-core,flying-orm-rdb -am verify`。
- [ ] **Step 3:** 核对 PublicApiClosure、PublicApiBaseline、JaCoCo、SpotBugs、Checkstyle 和 `git diff --check`。
- [ ] **Step 4:** 重新计算最终 628 文件清单、行数和 400 行门禁。
- [ ] **Step 5:** 报告已验证内容、未运行的实库边界和保留的既有工作区变更；不执行 Git 操作。
