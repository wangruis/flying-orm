# core 与 rdb 六循环全面审查执行计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to execute this plan task-by-task. Each cycle must complete both the full production audit and the full code-review before advancing.

**Goal:** 对 `flying-orm-core` 与 `flying-orm-rdb` 的全部生产 Java 代码执行六个完整循环；每个循环先全面审查、修复，再全面 code-review、修复，保持现有功能、API 与运行语义不变。

**Architecture:** 当前基线包含 core 111 个、rdb 441 个生产 Java 文件，共 552 个。每个循环按互不重叠的写入边界并行覆盖 core、rdb 执行内核和 rdb 表层，主代理负责跨模块调用链、公共契约、变更合并与最终验收；审查视角可以轮换，但任何视角都不能缩小每轮的全文件覆盖范围。

**Tech Stack:** Java 21、Maven 3.9.16、JDBC、R2DBC、Reactor、JUnit Jupiter、JaCoCo、SpotBugs、Checkstyle、codebase-memory-mcp。

## Global Constraints

- 每个循环必须先完成 552 个生产 Java 文件的全面审查与必要修复，再对同一 552 个文件完成全面 code-review 与必要修复。
- 只修改 `flying-orm-core`、`flying-orm-rdb` 的生产代码及证明修复所必需的直接契约测试；不新增功能，不删除功能，不改坏现有功能。
- 不改变公共 API、SQL 与参数顺序、Scope 收窄规则、事务单控制者、`UNKNOWN`、缓存失效、取消、背压、资源释放和错误脱敏语义，除非修复被测试证明的既有缺陷所必需。
- 任何修复先建立能够复现缺陷的聚焦测试；确认测试失败后实施最小修复，再执行聚焦测试与相关模块测试。
- 生产类型继续遵守 300 行/20 方法目标及 400 行/30 方法硬门禁；不得用无职责转发层规避门禁。
- 使用 `D:\apache-maven-3.9.16` 和 `D:\MavenRepository`；不在 C 盘创建项目、缓存或测试输出。
- 当前工作区已有用户认可的未提交审查修复，必须纳入审查并保留；不得 reset、checkout、覆盖或回退。
- 用户未要求本轮 Git 操作，因此不创建分支、不提交、不推送。

---

## 固定覆盖分区

- **core：** `flying-orm-core/src/main/java/**/*.java`，111 个文件。
- **rdb 执行内核：** `batch`、`execution`、`jdbc`、`reactive`、`sync`、`transaction`、`lifecycle`，100 个文件。
- **rdb 表层与共享实现：** rdb 其余生产包，341 个文件。
- **跨模块复核：** core AST/SQL/Scope/codec/metadata 到 rdb JDBC/R2DBC、Form、Repository、Operator、Schema、Cache 的调用链与行为对称性。

## 每个循环的固定执行协议

1. 从文件清单首项重新开始，逐类记录已读覆盖，不继承上一循环的“已审”结论。
2. 使用知识图谱追踪高扇入类型、调用方和跨模块数据流；字符串、配置和完整文件清单才回退到 `rg`。
3. 第一遍为全面审查：实现正确性、失败路径、边界值、资源与协议契约均逐类检查。
4. 发现高置信缺陷时，先写聚焦失败测试，再做最小生产修复；不得夹带重构或功能扩展。
5. 聚焦测试通过后，从同一 552 文件清单再次开始第二遍完整 code-review，审查当前最终源码而非旧 diff。
6. code-review 发现问题时重复失败测试、最小修复、聚焦验证流程。
7. 每循环结束执行 `git diff --check`，记录覆盖计数、修复、测试和未验证边界；存在未解决高风险问题时不得进入下一循环。

### Task 1: 循环一——执行正确性与资源基线

**Files:** 固定覆盖分区中的全部 552 个生产 Java 文件；测试仅限实际修复对应文件。

- [ ] 全面审查全部文件，重点验证异常传播、JDBC/R2DBC 资源对称、SQL/参数顺序和输入快照。
- [ ] 修复全面审查发现的真实缺陷并完成聚焦验证。
- [ ] 从首文件重新执行全部文件 code-review，重点反查遗漏的回滚、关闭、失效与公开错误泄漏。
- [ ] 修复 code-review 发现的真实缺陷，执行聚焦测试和差异检查。

### Task 2: 循环二——公共契约、安全与不可变性

**Files:** 固定覆盖分区中的全部 552 个生产 Java 文件；测试仅限实际修复对应文件。

- [ ] 全面审查全部文件，重点验证公共 API、防御复制、脱敏、标识符与结构化条件安全。
- [ ] 修复全面审查发现的真实缺陷并完成聚焦验证。
- [ ] 从首文件重新执行全部文件 code-review，重点反查 Scope 只能收窄、参数化绑定和安全上限。
- [ ] 修复 code-review 发现的真实缺陷，执行聚焦测试和差异检查。

### Task 3: 循环三——并发、订阅、取消与背压

**Files:** 固定覆盖分区中的全部 552 个生产 Java 文件；测试仅限实际修复对应文件。

- [ ] 全面审查全部文件，重点验证每订阅状态、共享对象线程安全、请求量、取消和同步重入。
- [ ] 修复全面审查发现的真实缺陷并完成聚焦验证。
- [ ] 从首文件重新执行全部文件 code-review，重点反查连接获取、Publisher 协议、缓存并发与观察者隔离。
- [ ] 修复 code-review 发现的真实缺陷，执行聚焦测试和差异检查。

### Task 4: 循环四——事务、UNKNOWN、Scope 与缓存一致性

**Files:** 固定覆盖分区中的全部 552 个生产 Java 文件；测试仅限实际修复对应文件。

- [ ] 全面审查全部文件，重点验证事务单控制者、外部事务参与、提交/回滚未确认和 recovery token。
- [ ] 修复全面审查发现的真实缺陷并完成聚焦验证。
- [ ] 从首文件重新执行全部文件 code-review，重点反查 DDL 后缓存失效、动态路由锁定、Scope 与逻辑删除。
- [ ] 修复 code-review 发现的真实缺陷，执行聚焦测试和差异检查。

### Task 5: 循环五——极值、容量、方言与双内核对称

**Files:** 固定覆盖分区中的全部 552 个生产 Java 文件；测试仅限实际修复对应文件。

- [ ] 全面审查全部文件，重点验证 Duration/计数溢出、内存预算、行/分片上限和极端输入。
- [ ] 修复全面审查发现的真实缺陷并完成聚焦验证。
- [ ] 从首文件重新执行全部文件 code-review，重点反查四库方言、JDBC/R2DBC 结果与错误分类对称性。
- [ ] 修复 code-review 发现的真实缺陷，执行聚焦测试和差异检查。

### Task 6: 循环六——封版对抗性全面复审

**Files:** 固定覆盖分区中的全部 552 个生产 Java 文件；测试仅限实际修复对应文件。

- [ ] 不采信前五轮结论，从首文件重新进行最终全面审查，覆盖所有既定安全、正确性和生命周期契约。
- [ ] 修复全面审查发现的真实缺陷并完成聚焦验证。
- [ ] 从首文件重新执行最终完整 code-review，审查最终工作区源码、全部现有 diff 和跨模块行为。
- [ ] 修复 code-review 发现的真实缺陷，执行聚焦测试、模块全量测试、质量门禁和差异检查。
- [ ] 复核 552 文件覆盖证据、生产类型门禁、未提交文件边界和未验证事项，形成封版报告。

## 最终验证

- `D:\apache-maven-3.9.16\bin\mvn.cmd -Dmaven.repo.local=D:\MavenRepository -pl flying-orm-core,flying-orm-rdb -am test`
- `D:\apache-maven-3.9.16\bin\mvn.cmd -Dmaven.repo.local=D:\MavenRepository -Pquality -pl flying-orm-core,flying-orm-rdb -am verify`
- `git diff --check`
- `git status --short`
