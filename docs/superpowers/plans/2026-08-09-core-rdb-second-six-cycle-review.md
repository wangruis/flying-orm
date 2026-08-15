# Core/RDB 第二组六轮全量审查实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:dispatching-parallel-agents to execute the independent partitions; every production fix must use superpowers:test-driven-development.

**Goal:** 对 `flying-orm-core` 与 `flying-orm-rdb` 当前生产 Java 连续执行六次完整循环；每次循环先逐文件全面审查和修复，再对修复后的同一完整快照执行独立全面 code-review 和修复。

**Architecture:** 物理清单分为 core、RDB 执行内核、RDB 表层三个互斥分区，三分区合计即当轮全部生产 Java。审查与 code-review 顺序串行，但分区内部可并行；code-review 轮换审查者以提供新视角，所有 finding 由主代理做调用链复核、RED→GREEN 验证与跨分区集成。

**Tech Stack:** Java 21、Maven 3.9.16、JUnit Jupiter、Reactor/R2DBC、JDBC、JaCoCo、SpotBugs、Checkstyle、codebase-memory-mcp。

## Global Constraints

- 仅审查和修改 `flying-orm-core/src/main/java/**/*.java` 与 `flying-orm-rdb/src/main/java/**/*.java`；测试只用于证明真实修复。
- 不新增功能，不删除或改写既有功能，不扩张公共 API，不改变 SQL、参数顺序、Scope、事务或缓存语义，除非修复已证实的契约缺陷。
- 当前三个互斥分区基线为 core 112、RDB engine 101、RDB surface 341，总计 554；每个阶段必须重新生成物理清单和哈希，不复用上一阶段结论。
- RDB engine 只包括一级包 `batch|execution|jdbc|reactive|sync|transaction|lifecycle`；`internal/sync` 属于 RDB surface。
- 每轮 audit 和后置 code-review 都必须完整读取三个分区的全部当前生产 Java；不能用抽样、包级专题或上轮报告替代。
- finding 在修改生产代码前必须先写直接行为测试并实跑确认 RED；RED 原因不正确时撤销或修正测试，不能修改生产代码制造绿色。
- 每个 GREEN 先跑直接测试，再跑受影响模块；每轮 code-review 结束后执行 core+rdb 全量测试。
- 不回退工作区已有变更，不格式化无关文件，不创建分支、不提交、不推送。
- 新生产类型控制在 300 行/20 可调用方法内；禁止新增或扩张到 400 行/30 可调用方法的生产类型。

---

### Task 1: 建立第二组六轮基线

**Files:**
- Create: `.superpowers/sdd/2026-08-09-core-rdb-second-six-cycle-review/progress.md`
- Create: `.superpowers/sdd/2026-08-09-core-rdb-second-six-cycle-review/baseline-report.md`

**Interfaces:**
- Consumes: 当前工作区生产源码、上一组六轮已完成后的最终快照。
- Produces: 当前物理清单、分区哈希、测试基线和六轮账本。

- [ ] **Step 1: 刷新知识图并记录架构/热点**

调用 `index_repository(mode=moderate)`，再读取 core 与 rdb 的 architecture、dependencies、hotspots、boundaries。

- [ ] **Step 2: 生成三个互斥清单和 SHA-256**

```powershell
$core = @(rg --files flying-orm-core/src/main/java -g '*.java')
$rdb = @(rg --files flying-orm-rdb/src/main/java -g '*.java')
$engine = @($rdb | Where-Object { $_ -match 'rdb[\\/](batch|execution|jdbc|reactive|sync|transaction|lifecycle)[\\/]' })
```

断言 `core + engine + surface == all core/rdb production Java`，并把排序路径及内容哈希写入 baseline report。

- [ ] **Step 3: 实跑基线全量测试**

```powershell
& 'D:\apache-maven-3.9.16\bin\mvn.cmd' '-Dmaven.repo.local=D:\MavenRepository' '-pl' 'flying-orm-rdb' '-am' test
```

从 Surefire XML 记录 tests/failures/errors/skipped，失败时先按 systematic-debugging 定位，不跳过测试。

### Task 2: 循环 1——全面审查修复，再独立全面 code-review 修复

**Files:**
- Create: `.superpowers/sdd/2026-08-09-core-rdb-second-six-cycle-review/cycle1-*-audit-report.md`
- Create: `.superpowers/sdd/2026-08-09-core-rdb-second-six-cycle-review/cycle1-*-code-review-report.md`
- Modify: 仅经 RED 证实的直接生产/测试文件。

**Interfaces:**
- Consumes: Task 1 基线快照。
- Produces: 循环 1 修复后的完整可测试快照及六份分区报告。

- [ ] **Step 1: 全量 audit** — 三个审查者分别完整读取 core、RDB engine、RDB surface，记录清单哈希、逐类风险、调用链及 finding。
- [ ] **Step 2: audit RED→GREEN** — 主代理逐项复核；每项先运行直接 RED，再允许最小 GREEN，随后运行直接测试。
- [ ] **Step 3: audit 集成验证** — 运行 `mvn -pl flying-orm-rdb -am test`，并执行 `git diff --check`。
- [ ] **Step 4: 独立全量 code-review** — 轮换分区审查者，在 audit 修复后的当前快照重新读取 554/554，不复用 audit 结论。
- [ ] **Step 5: review RED→GREEN** — 对独立 review finding 重复严格 TDD 和主代理影响复核。
- [ ] **Step 6: 循环收口** — 再次运行全量 core+rdb 测试、文件/大类门禁、`git diff --check`，更新 progress。

### Task 3: 循环 2——全面审查修复，再独立全面 code-review 修复

**Files:** `cycle2-*-audit-report.md`、`cycle2-*-code-review-report.md`，以及仅经 RED 证实的直接生产/测试文件。

- [ ] **Step 1:** 在循环 1 最终快照上重建三个清单与内容哈希，完整 audit 554/554。
- [ ] **Step 2:** 对 audit finding 执行调用链复核、直接 RED、最小 GREEN、直接测试。
- [ ] **Step 3:** 全量测试与 `git diff --check` 后，轮换审查者重新完整 code-review 554/554。
- [ ] **Step 4:** 对 review finding 执行独立 RED→GREEN，并再次跑全量测试、大小门禁和 diff-check。
- [ ] **Step 5:** 写入六份循环 2 报告并更新 progress；没有高置信 finding 时明确记录零修改，不制造问题。

### Task 4: 循环 3——全面审查修复，再独立全面 code-review 修复

**Files:** `cycle3-*-audit-report.md`、`cycle3-*-code-review-report.md`，以及仅经 RED 证实的直接生产/测试文件。

- [ ] **Step 1:** 重建清单/哈希并完整 audit 554/554；聚焦事务、取消、资源、Scope、安全、不可变性和极值。
- [ ] **Step 2:** audit finding 严格 RED→GREEN，完成全量测试与 diff-check。
- [ ] **Step 3:** 轮换审查者重新完整 code-review 554/554；从公开契约、跨类一致性和异常矩阵反向审查。
- [ ] **Step 4:** review finding 严格 RED→GREEN，完成全量测试、大小门禁、报告和 progress。

### Task 5: 循环 4——全面审查修复，再独立全面 code-review 修复

**Files:** `cycle4-*-audit-report.md`、`cycle4-*-code-review-report.md`，以及仅经 RED 证实的直接生产/测试文件。

- [ ] **Step 1:** 重建清单/哈希并完整 audit 554/554；复核 JDBC/R2DBC 双内核对称与失败关闭。
- [ ] **Step 2:** audit finding 严格 RED→GREEN，完成全量测试与 diff-check。
- [ ] **Step 3:** 轮换审查者重新完整 code-review 554/554；复核缓存/路由、DDL/schema、批量/背压和观测。
- [ ] **Step 4:** review finding 严格 RED→GREEN，完成全量测试、大小门禁、报告和 progress。

### Task 6: 循环 5——全面审查修复，再独立全面 code-review 修复

**Files:** `cycle5-*-audit-report.md`、`cycle5-*-code-review-report.md`，以及仅经 RED 证实的直接生产/测试文件。

- [ ] **Step 1:** 重建清单/哈希并完整 audit 554/554；对前四轮修复做反向回归审查但不代替逐文件覆盖。
- [ ] **Step 2:** audit finding 严格 RED→GREEN，完成全量测试与 diff-check。
- [ ] **Step 3:** 轮换审查者重新完整 code-review 554/554；检查 public API、SQL/参数、事务状态和 Throwable 图。
- [ ] **Step 4:** review finding 严格 RED→GREEN，完成全量测试、大小门禁、报告和 progress。

### Task 7: 循环 6——全面审查修复，再独立全面 code-review 修复

**Files:** `cycle6-*-audit-report.md`、`cycle6-*-code-review-report.md`，以及仅经 RED 证实的直接生产/测试文件。

- [ ] **Step 1:** 重建清单/哈希并完整 audit 554/554；任何“上一轮已审”不能作为跳过依据。
- [ ] **Step 2:** audit finding 严格 RED→GREEN，完成全量测试与 diff-check。
- [ ] **Step 3:** 轮换审查者重新完整 code-review 554/554，并由主代理做最终跨分区调用链复核。
- [ ] **Step 4:** review finding 严格 RED→GREEN，完成全量测试、大小门禁、六份报告和 progress。

### Task 8: 最终封版验证

**Files:**
- Modify: `.superpowers/sdd/2026-08-09-core-rdb-second-six-cycle-review/progress.md`

- [ ] **Step 1: 全量测试**

```powershell
& 'D:\apache-maven-3.9.16\bin\mvn.cmd' '-Dmaven.repo.local=D:\MavenRepository' '-pl' 'flying-orm-rdb' '-am' test
```

- [ ] **Step 2: 质量门禁**

```powershell
& 'D:\apache-maven-3.9.16\bin\mvn.cmd' '-Dmaven.repo.local=D:\MavenRepository' '-Pquality' '-pl' 'flying-orm-core,flying-orm-rdb' '-am' verify
```

- [ ] **Step 3: 发布制品门禁**

```powershell
& 'D:\apache-maven-3.9.16\bin\mvn.cmd' '-Dmaven.repo.local=D:\MavenRepository' '-Prelease-artifacts' '-pl' 'flying-orm-core,flying-orm-rdb' '-am' verify
```

- [ ] **Step 4: 静态收口** — 断言报告 36 份、生产清单完整、无生产文件达到 400 行、`git diff --check` 为 0，并记录未运行的实库认证边界。

## Self-Review

- Spec coverage：六个循环均显式包含完整 audit、修复、独立完整 code-review、修复及两次集成验证。
- Placeholder scan：所有命令、目录、分区规则和验收证据均已给出；未知 finding 只能经运行时证据进入修改，不以占位任务代替。
- Type/contract consistency：全程不新增预设 API；发现缺陷时以现有公开契约和直接测试确定修复结果。
