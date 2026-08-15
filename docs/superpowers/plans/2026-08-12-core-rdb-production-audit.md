# Core/RDB Production Audit And Repair Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development for scoped read-only audits, superpowers:systematic-debugging for confirmed defects, and superpowers:test-driven-development for every production repair.

**Goal:** 对 `flying-orm-core` 与 `flying-orm-rdb` 当前工作树中的全部生产 Java 代码完成一次全量审核、一次独立 code-review、必要修复和统一验证，同时保持公共 API 集合完全不变，并防止后续复审以互斥标准推翻已经验证的修复。

**Architecture:** 审查按 core、RDB 执行内核、RDB 其余表层三个互不重叠的生产代码域并行进行；候选问题只读报告给主代理，主代理完成调用图、现有契约和可复现性裁决。只有当前代码能够稳定 RED 的真实缺陷才进入逐项修复，修复后执行局部复审、全量复审和质量门禁。

**Tech Stack:** Java 21、Maven 3.9.16、JUnit Jupiter、Reactor、R2DBC SPI、JDBC、JaCoCo、Checkstyle、SpotBugs、codebase-memory-mcp。

## Global Constraints

- 审查对象仅限 `flying-orm-core/src/main/java/**/*.java` 与 `flying-orm-rdb/src/main/java/**/*.java`；benchmark、testkit、文档和示例不进入逐类审查。
- 测试代码只可作为现有契约证据，或为已确认缺陷添加最小回归测试；测试改动不扩大产品功能。
- 修复前后公开类型、公开/受保护方法、构造器、字段、枚举常量、注解元素及其签名和可见性集合必须完全一致；不得新增、删除、重命名或改变公共 API。
- 不删除、不覆盖、不回退工作区已有变更；与本轮无关的 benchmark、testkit 和其他文件保持原状。
- 不提交、不推送、不创建分支；用户另行明确要求前只保留工作区变更和审查证据。
- 每个生产修复必须先完成根因追踪，再写直接回归测试并观察预期 RED，之后才允许最小 GREEN；不得凭代码风格、偏好或理论风险机械修改。
- 已接受修复的稳定性规则：后续复审若要否决既有修复，必须同时给出当前源码上的可复现反例、与既有契约不互斥的新 RED、明确调用链和风险；仅因另一种设计也合理、假设性兼容、性能猜测或改变审查口径，不得判为不通过。
- 若新候选与既有绿色契约冲突，先登记双方契约并裁决；无法证明既有契约错误时保留原修复，不制造“修复—撤回—再修复”循环。
- 涉及 SQL、参数顺序、事务状态、UNKNOWN、取消、背压、资源释放、缓存分区、Scope、安全和错误脱敏的修复，必须核对 JDBC/R2DBC 或同步/响应式对称性。
- 任何生产类不得因本轮达到 400 行或 30 个可调用方法；本轮不以拆分类作为无关清理目标。

---

### Task 1: Freeze The Baseline And Anti-Oscillation Ledger

**Files:**
- Read: `flying-orm-core/src/main/java/**/*.java`
- Read: `flying-orm-rdb/src/main/java/**/*.java`
- Read: `flying-orm-core/src/test/java/**/*PublicApi*Test.java`
- Read: `flying-orm-rdb/src/test/java/**/*PublicApi*Test.java`
- Record: `.superpowers/sdd/2026-08-12-core-rdb-production-audit/progress.md`

**Interfaces:**
- Consumes: 当前 HEAD、工作树 diff、知识图谱和 Maven 公共 API 契约。
- Produces: 生产文件清单及哈希、未提交改动清单、公共 API 基线、已接受修复契约与本轮判定规则。

- [x] 记录 `git status --short`、分支、HEAD 和 Git 工作树类型，不改变 Git 状态。
- [x] 更新 codebase-memory-mcp 索引，并分别导出 core、RDB 生产类型和高入度/高复杂度入口。
- [x] 用 `rg --files`/PowerShell 仅核对图谱无法保证的物理生产文件数量、路径和 SHA-256 清单。
- [x] 运行公共 API 直接契约和当前质量基线；失败只作为调查输入，不立即修改生产代码。
- [x] 将现有测试明确锁定的已接受修复契约写入 ledger，后续候选必须与其一并裁决。

### Task 2: Full Production Audit

**Files:**
- Read: `flying-orm-core/src/main/java/**/*.java`
- Read: `flying-orm-rdb/src/main/java/**/*.java`
- Record: `.superpowers/sdd/2026-08-12-core-rdb-production-audit/*-audit-report.md`

**Interfaces:**
- Consumes: Task 1 的冻结清单、图谱调用链和契约 ledger。
- Produces: 每个生产文件的覆盖记录，以及只含可复现候选的审计报告。

- [x] Core 审计逐文件检查不可变性、条件/Scope、codec、元数据、SQL 模型、错误边界和公共契约。
- [x] RDB 执行内核审计逐文件检查 JDBC/R2DBC、事务、批量、取消、超时、背压、资源、UNKNOWN 和异常图。
- [x] RDB 表层审计逐文件检查渲染、映射、Repository、Form、Schema、Metadata、缓存、保护字段和方言一致性。
- [x] 每个候选必须包含具体源码位置、真实入口、数据/调用路径、现有测试关系和可失败的直接契约；不满足者记录为已否决而非 finding。

### Task 3: Independent Full Code Review

**Files:**
- Read: `flying-orm-core/src/main/java/**/*.java`
- Read: `flying-orm-rdb/src/main/java/**/*.java`
- Record: `.superpowers/sdd/2026-08-12-core-rdb-production-audit/*-code-review-report.md`

**Interfaces:**
- Consumes: 与 Task 2 相同的冻结生产清单，但不继承其候选结论。
- Produces: 独立 code-review 结果，以及与审计结果的交叉裁决表。

- [x] 从公开入口、热点、复杂度、失败路径和跨内核对称性重新检查全部生产文件。
- [x] 对 Task 2 finding 独立复现；相同根因合并，意见差异写明证据，不以投票决定正确性。
- [x] 对所有可能推翻既有修复的意见执行反循环规则；没有新 RED 和调用链则否决撤回建议。

### Task 4: Triage And Repair Confirmed Defects

**Files:**
- Modify: 仅 ledger 中已经登记具体路径和 RED 契约的生产文件。
- Test: 仅与相应缺陷直接相关的既有测试类。
- Record: `.superpowers/sdd/2026-08-12-core-rdb-production-audit/progress.md`

**Interfaces:**
- Consumes: Task 2/3 交叉确认的 defect，及其精确调用图、根因和回归测试契约。
- Produces: 公共 API 零变化的最小生产修复和 RED→GREEN 证据。

- [x] 对每个候选先完成 systematic-debugging 四阶段并将根因写入 ledger。
- [x] 在修改生产代码前添加一个直接测试，运行并确认因目标缺陷而 RED；假阳性立即撤回测试并登记否决理由。
- [x] 只修改根因位置，运行直接测试确认 GREEN，再运行相关模块测试确认没有邻接回归。
- [x] 每个修复由独立 reviewer 检查需求符合性、代码质量、公共 API、原契约和反循环规则；有效问题进入限定修复轮，未经证据的新范围问题不扩张该轮。

### Task 5: Post-Fix Re-Audit And Final Verification

**Files:**
- Read: `flying-orm-core/src/main/java/**/*.java`
- Read: `flying-orm-rdb/src/main/java/**/*.java`
- Record: `.superpowers/sdd/2026-08-12-core-rdb-production-audit/final-report.md`

**Interfaces:**
- Consumes: 当前最终工作树、冻结 API 基线、全部修复与裁决 ledger。
- Produces: 全量复审、API 等同性和 Maven 质量门禁证据。

- [x] 对所有本轮生产 diff 做一次局部复审，确认修复没有扩大语义或破坏既有契约。
- [x] 重新逐文件覆盖 core/RDB 当前生产清单，重点回查每个修改点上下游和 JDBC/R2DBC 对称路径。
- [x] 运行 `D:\apache-maven-3.9.16\bin\mvn.cmd -Dmaven.repo.local=D:\MavenRepository -Pquality -pl flying-orm-core,flying-orm-rdb -am verify`。
- [x] 运行公共 API 契约、`git diff --check`、生产大类门禁和生产清单哈希复核。
- [x] 最终报告分别列出已修复、已否决候选、未验证外部边界、与本轮无关的既有工作区改动；没有新证据不得把已接受修复重新判失败。
