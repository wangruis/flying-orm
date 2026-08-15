# Core/RDB Production Re-audit Cycle 4 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. The active collaboration rule forbids creating additional subagents, so review checkpoints are executed inline with separate evidence matrices.

**Goal:** 再次全面审查并独立 code-review 当前 Core/RDB 全部生产 Java，在不新增生产类型、不改变现有 API、不反向取消有效修复的前提下，修复所有当前可复现问题并通过完整质量门禁。

**Architecture:** 以当前脏工作区为不可破坏基线，代码图谱优先定位入口、调用链和热点，排序后的物理文件清单保证 628 个生产类逐项覆盖。审查和 code-review 使用不同矩阵；只有具备当前生产路径、明确影响、旧实现 RED 且不冲突现有绿色契约的候选才能进入修复。

**Tech Stack:** Java 21、Maven 3.9.16、JUnit Jupiter、JDBC、R2DBC/Reactor、JaCoCo、SpotBugs、Checkstyle、codebase-memory-mcp。

## Global Constraints

- 范围只包含 `flying-orm-core/src/main/java` 和 `flying-orm-rdb/src/main/java`；测试只作契约证据。
- 不新增生产类型，不删除、重命名、新增或改变任何 public/protected API。
- 不回退、覆盖或格式化无关工作区修改；不执行 Git 暂存、提交、推送。
- 已接受修复只有出现新的当前可达反例和不互斥 RED 才能重开。
- 修改只能落在现有生产类和现有直接测试类，并必须完成 RED、GREEN、相邻回归和修改后复审。
- Core/RDB 生产类不得达到 400 行或 30 个可调用方法。
- Maven 固定使用 `D:\apache-maven-3.9.16` 和 `D:\MavenRepository`。

---

### Task 1: 冻结清单、契约和历史疑点

**Files:**
- Read: `AGENTS.md`
- Read: `.superpowers/sdd/2026-08-12-core-rdb-production-audit/accepted-contracts.md`
- Create: `.superpowers/sdd/2026-08-12-core-rdb-production-reaudit-cycle4/progress.md`

**Interfaces:**
- Consumes: 当前 branch、HEAD、dirty status、Core/RDB 生产文件。
- Produces: 文件数、行数、内容摘要、大类门禁、反振荡规则和历史疑点清单。

- [x] **Step 1:** 记录当前 Git 和物理生产清单，不修改工作区状态。
- [x] **Step 2:** 验证图谱索引，并用物理清单补齐图谱未覆盖文件。
- [x] **Step 3:** 重新验证历史未决候选：FieldScope/DataScope、custom codec 二次转换、codec 错误脱敏、public vector renderer Scope 边界。

### Task 2: 628 个生产类全面审查

**Files:**
- Read: `flying-orm-core/src/main/java/**/*.java`
- Read: `flying-orm-rdb/src/main/java/**/*.java`
- Create: `.superpowers/sdd/2026-08-12-core-rdb-production-reaudit-cycle4/audit-report.md`

**Interfaces:**
- Consumes: Task 1 冻结清单。
- Produces: 逐文件覆盖证据、确认候选和拒绝候选。

- [x] **Step 1:** 审查 Core 条件、Scope、Form、Join、分页、元数据、codec 和 SQL 模型。
- [x] **Step 2:** 审查 RDB 渲染、保护、模板、映射、metadata、schema、Repository 和缓存。
- [x] **Step 3:** 审查 JDBC/R2DBC 事务、批量、超时、取消、资源和 fatal 矩阵。
- [x] **Step 4:** 横向扫描边界算术、无界状态、错误回显、阻塞、背压、数组所有权和公共 API。

### Task 3: 独立 Code Review 与测试先行修复

**Files:**
- Modify: 仅确认 finding 所在的现有生产类。
- Test: 仅对应的现有直接测试类。
- Create: `.superpowers/sdd/2026-08-12-core-rdb-production-reaudit-cycle4/code-review-report.md`

**Interfaces:**
- Consumes: Task 2 候选和当前绿色契约。
- Produces: 确认、降级或拒绝结论，以及每项 RED/GREEN 证据。

- [x] **Step 1:** 对候选反向追踪调用方、被调用方、同步/响应式和四方言语义。
- [x] **Step 2:** 先在现有测试类写精确 RED 并运行；旧实现不红则撤回候选。
- [x] **Step 3:** 只修改现有内部实现，运行直接类和相邻矩阵。
- [x] **Step 4:** 重新检查生产差异、public/protected 声明、参数顺序、异常图和规模门禁。

### Task 4: 完整验证与报告

**Files:**
- Create: `.superpowers/sdd/2026-08-12-core-rdb-production-reaudit-cycle4/final-report.md`
- Update: `.superpowers/sdd/2026-08-12-core-rdb-production-reaudit-cycle4/progress.md`

**Interfaces:**
- Consumes: 当前完整工作区和本轮修改。
- Produces: 测试、API、覆盖率、静态分析、清单、未验证边界和工作区说明。

- [x] **Step 1:** 运行聚焦及关联回归并读取失败/错误/跳过数。
- [x] **Step 2:** 运行 `-Pquality -pl flying-orm-core,flying-orm-rdb -am verify`。
- [x] **Step 3:** 核对 API、JaCoCo、SpotBugs、Checkstyle、`git diff --check`。
- [x] **Step 4:** 复算最终 628 文件、行数、摘要和 400 行门禁。
- [x] **Step 5:** 明确本轮未执行的实库和 Git 操作，不把自动化门禁表述成绝对无缺陷证明。
