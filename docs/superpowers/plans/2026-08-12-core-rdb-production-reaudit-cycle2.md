# Core/RDB Production Re-Audit Cycle 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:systematic-debugging and superpowers:test-driven-development before every production repair. This execution remains inline because the active task does not authorize new subagents.

**Goal:** 对当前工作树中的 Core/RDB 全部生产 Java 再完成一轮独立审核、独立 code-review、必要修复和统一验证，同时保持 V2.0.0 公共 API 集合不变，并禁止无新反例地推翻上一轮已验证修复。

**Architecture:** 审核与 code-review 使用同一物理清单但不同检查矩阵；第二阶段不继承第一阶段候选结论。候选只有在当前源码存在生产可达路径、稳定 RED 契约且不与 accepted-contracts 冲突时才进入修复。修复后重新检查完整调用链、公共 API、同步/响应式对称性和全量质量门禁。

**Tech Stack:** Java 21、Maven 3.9.16、JUnit Jupiter、JDBC、R2DBC、Reactor、JaCoCo、Checkstyle、SpotBugs、codebase-memory-mcp。

## Global Constraints

- 逐文件审查范围只包含 `flying-orm-core/src/main/java/**/*.java` 与 `flying-orm-rdb/src/main/java/**/*.java`。
- benchmark、testkit、示例和文档不进入生产代码 finding；测试只作为契约证据或确认缺陷的回归夹具。
- 不新增生产类型，不新增、删除、改名或改变公开/受保护 API 的签名、类型、可见性、枚举常量或注解元素。
- 上一轮 accepted-contracts 默认有效；重新否决必须同时给出当前生产调用链、可复现反例和不互斥的 RED 测试。
- 不删除、覆盖或回退工作区已有变更；不创建分支，不暂存，不提交，不推送。
- 每项修复必须先观察 RED，再实施根因修复并观察 GREEN；没有 RED 的理论风险只记录裁决，不修改生产代码。
- 最终生产类型继续满足 `<400` 物理行和 `<30` 可调用方法硬门禁。

---

### Task 1: Freeze Current Source And Accepted Contracts

**Files:**
- Read: `flying-orm-core/src/main/java/**/*.java`
- Read: `flying-orm-rdb/src/main/java/**/*.java`
- Read: `.superpowers/sdd/2026-08-12-core-rdb-production-audit/accepted-contracts.md`
- Record: `.superpowers/sdd/2026-08-12-core-rdb-production-reaudit-cycle2/progress.md`

**Interfaces:**
- Consumes: 当前 HEAD、dirty worktree、上一轮最终清单及 accepted-contracts。
- Produces: 新一轮路径/内容哈希、文件数量、公共 API 和质量基线。

- [ ] 读取 Git 状态、HEAD、分支和上一轮证据，不改变 Git。
- [ ] 更新知识图谱并以 `rg --files` 复核 Core/RDB 生产 Java 物理清单。
- [ ] 复算路径+内容 SHA-256、最大类行数和当前生产 diff。
- [ ] 运行当前 Core/RDB quality 基线；任何失败先分类为环境、已有回归或本轮候选。

### Task 2: Full Production Audit

**Files:**
- Read: `flying-orm-core/src/main/java/**/*.java`
- Read: `flying-orm-rdb/src/main/java/**/*.java`
- Record: `.superpowers/sdd/2026-08-12-core-rdb-production-reaudit-cycle2/audit-report.md`

**Interfaces:**
- Consumes: Task 1 冻结清单和图谱。
- Produces: 628 个文件的覆盖证据、候选及拒绝理由。

- [ ] 逐文件检查不可变性、输入边界、Scope、SQL/参数、codec、映射、缓存与安全。
- [ ] 逐文件检查 JDBC/R2DBC 事务状态、批量、取消、超时、资源、fatal、UNKNOWN 与观测。
- [ ] 用调用图复核每个候选的真实入口；不能到达生产入口的候选直接拒绝。
- [ ] 与 accepted-contracts 对照；冲突候选没有新反例时不得升级。

### Task 3: Independent Production Code Review

**Files:**
- Read: `flying-orm-core/src/main/java/**/*.java`
- Read: `flying-orm-rdb/src/main/java/**/*.java`
- Record: `.superpowers/sdd/2026-08-12-core-rdb-production-reaudit-cycle2/code-review-report.md`

**Interfaces:**
- Consumes: Task 1 清单，不继承 Task 2 finding 结论。
- Produces: 从公开入口、热点、跨内核对称性和失败矩阵重新得到的独立结论。

- [ ] 从公共入口向内追踪高入度、复杂度和状态机热点。
- [ ] 检查当前生产 diff 与邻接调用方，确认上一轮修复没有产生反向回归。
- [ ] 对 Task 2 候选独立复现；相同根因合并，不以审查次数或投票决定。
- [ ] 只保留含文件位置、影响、调用链和 failing-now 契约的 Critical/Important finding。

### Task 4: TDD Repair Confirmed Findings

**Files:**
- Modify: 仅 Task 2/3 交叉确认 finding 的根因生产文件。
- Test: 仅相应既有测试类。
- Record: `.superpowers/sdd/2026-08-12-core-rdb-production-reaudit-cycle2/progress.md`

**Interfaces:**
- Consumes: 确认 finding、调用图和现有绿色契约。
- Produces: API 零变化的生产修复和 RED→GREEN 证据。

- [ ] 为每个 finding 写最小直接测试并运行，确认因目标缺陷稳定 RED。
- [ ] 若测试在旧实现即绿或只能与 accepted-contracts 冲突，撤回候选而不改生产代码。
- [ ] 修复根因，运行直接测试、邻接矩阵和公共 API 契约确认 GREEN。
- [ ] 重新审查修复点及上下游；没有新反例不得撤销已接受修复。

### Task 5: Final Verification And Report

**Files:**
- Read: `flying-orm-core/src/main/java/**/*.java`
- Read: `flying-orm-rdb/src/main/java/**/*.java`
- Record: `.superpowers/sdd/2026-08-12-core-rdb-production-reaudit-cycle2/final-report.md`

**Interfaces:**
- Consumes: 最终工作树和全部裁决。
- Produces: 可复核的最终验收证据。

- [ ] 复算最终 128/500 清单、哈希和大类门禁。
- [ ] 运行 `D:\apache-maven-3.9.16\bin\mvn.cmd -Dmaven.repo.local=D:\MavenRepository -Pquality -pl flying-orm-core,flying-orm-rdb -am verify`。
- [ ] 确认 `PublicApiClosureTest`、`PublicApiBaselineTest` 和 `git diff --check` 全绿。
- [ ] 报告已修复、已拒绝、未验证外部边界和未触碰的范围外 dirty worktree。
