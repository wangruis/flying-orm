# Docker Certification Infrastructure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 建立可重复的 Docker 真实数据库认证环境、统一执行入口和脱敏证据格式。

**Architecture:** Compose 只管理数据库生命周期，PowerShell 入口负责选择数据库、拼装现有 Maven Profile 和归档证据。认证代码与 ORM 内核隔离，不新增运行时依赖，也不改 testkit/benchmark 实现。

**Tech Stack:** Docker Compose、PowerShell 7/Windows PowerShell、Maven、JUnit 6、R2DBC、Java 21。

## Global Constraints

- 主项目保持零 Spring 依赖。
- 本批不执行正式真实库兼容、并发或性能结论。
- 不修改 `flying-orm-testkit`、`flying-orm-benchmark` 和用户自己的 `AGENTS.md`。
- 本地密码和完整连接串不能进入 Git 或认证报告。
- 注释和使用说明使用自然、具体的中文。

---

### Task 1: 固定数据库环境

**Files:**
- Create: `certification/docker-compose.yml`
- Create: `certification/.env.example`
- Modify: `.gitignore`

- [x] 固定 MySQL、PostgreSQL、Oracle 和 SQL Server 镜像版本。
- [x] 配置互不冲突的本机端口、持久卷和健康检查。
- [x] 用 Compose profile 区分核心、预览、全部和单库启动。
- [x] 忽略本机 `.env`，只提交可以公开的示例值。

### Task 2: 建立统一执行入口

**Files:**
- Create: `certification/Invoke-Certification.ps1`

- [x] 支持 Start、Verify、Status、Stop 四个动作。
- [x] 自动检查工具和环境变量，首次生成本机 `.env`。
- [x] 按数据库选择 Compose profile、Maven Profile 和 R2DBC 系统属性。
- [x] 归档脱敏环境、镜像摘要、Maven 输出和 JSON 结果清单。
- [x] 失败时保留证据并返回非零退出码。

### Task 3: 补齐使用与认证口径

**Files:**
- Create: `certification/README.md`
- Create: `docs/real-database-certification.md`
- Modify: `docs/database-support-matrix.md`
- Modify: `docs/v1-roadmap.md`

- [x] 写清核心组、预览组、单库运行和数据卷清理命令。
- [x] 写清版本、驱动、字符集、功能、并发、故障和性能证据格式。
- [x] 明确基础设施就绪不等于真实数据库已经认证。

### Task 4: 静态验证与交付

- [x] PowerShell 解析脚本通过，并用 Windows PowerShell 5.1 完成只读 Status 冒烟。
- [x] `docker compose config` 对核心组和全部组都能展开。
- [x] Maven 编译和现有核心测试通过。
- [x] Git 边界检查确认没有提交 `.env`、结果目录、AGENTS.md 或被冻结模块改动。
- [x] 批量提交并推送本轮文件。
