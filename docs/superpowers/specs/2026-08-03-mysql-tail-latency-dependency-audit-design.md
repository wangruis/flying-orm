# MySQL 更新尾延迟与依赖漏洞审计设计

## 目标

V1 发布后先处理两个仍未放行的门禁：定位 MySQL `updateById` P99 长尾到底花在拿连接、执行并提交还是归还连接；使用新鲜 NVD 数据完成可重复、可归档的正式依赖漏洞审计。

本批不调整 ORM 公共 API，不降低 MySQL 持久化等级，不把缺少 NVD 数据的扫描写成审计通过，也不发布 Maven 仓库制品。

## 已有证据

- 单独运行 `updateById` 时，三轮 P99 中位数为 `5.464 ms`，优于最初 `6.001 ms` 基线。
- 完整四场景连续写入后，更新 P99 中位数上升到 `8.741 ms`，但吞吐、错误率和连接池回收正常。
- 长尾与 redo/binlog 文件等待峰值同时出现，现有证据更支持严格同步提交下的存储抖动，不支持连接泄漏或无界排队结论。
- 当前 `audit` Profile 会在 CVSS 7.0 及以上漏洞或扫描错误时失败，但没有显式从环境变量读取 NVD API Key，也没有统一归档入口。

## 方案选择

### 方案一：在 ORM 公共执行器增加性能阶段 API

数据最直接，但会扩大 1.x 公共兼容面，并让诊断职责进入运行时内核。本批不采用。

### 方案二：在 benchmark 内包装连接池并用 Reactor Context 关联一次操作

连接获取和归还由包装后的 `ConnectionFactory`、`Connection` 计时；总耗时由操作跟踪器计时；同一次订阅通过 Reactor Context 共享样本，得到 `acquire`、`executeAndCommit`、`release` 和 `total` 四段延迟。它不改变运行时 API，诊断开关关闭时不进入正式基线热路径。本批采用这一方案。

### 方案三：只依赖 MySQL `performance_schema`

能看到数据库等待，却无法区分客户端连接池和 Publisher 生命周期，也难以和单次 ORM 操作一一对应。保留为数据库侧旁证，不单独使用。

## 性能诊断结构

### 操作样本

每次订阅创建一个仅在该订阅内使用的可变样本，记录：

- 连接池 `create()` 从订阅到拿到连接的耗时。
- SQL 操作完成后，池化连接 `close()` 从订阅到完成的耗时。
- 整次 Publisher 从订阅到终止的总耗时。
- `executeAndCommit = total - acquire - release`，最小为 0。

样本只经 Reactor Context 传递，不使用 `ThreadLocal`，避免事件循环切换后串数据。

### 聚合结果

使用 HdrHistogram `Recorder` 并发记录四个阶段，场景结束后生成不可变快照，输出 count、P50、P95、P99 和 max。报告 JSON 与 Markdown 增加可选 `phaseLatency`；诊断未开启时该字段为空，不改变原有正式结果语义。

### 开关与入口

Runner 增加受控布尔参数 `--phase-diagnostics`，PowerShell 入口增加 `-PhaseDiagnostics`。只有显式开启时才包装连接池和跟踪操作。首轮只运行 MySQL `UpdateById`、`QueryThenUpdate` 和完整四场景顺序，不把专项结果替代正式门禁。

## 漏洞审计结构

- Maven `audit` Profile 通过 `nvdApiKeyEnvironmentVariable` 从 `NVD_API_KEY` 读取密钥，不把密钥放进命令行、POM、日志或报告。
- 新增 `certification/Invoke-DependencyAudit.ps1` 作为正式入口。入口先检查 Java、Maven 和 `NVD_API_KEY`，再执行 `mvn -Paudit -DskipTests verify`。
- 成功后把 HTML、JSON、第三方许可证报告、Git 提交号、执行时间和文件 SHA-256 复制到 `target/audit-results/<run-id>`。
- 缺少 API Key、NVD 更新失败、报告缺失、CVSS 达到 7.0 或 Maven 失败时，脚本非零退出，不能生成“通过”结论。
- 当前不添加项目许可证；许可证报告只检查第三方依赖，不改变源码授权状态。

## 错误与安全边界

- 性能诊断失败不能吞掉原 SQL 异常，也不能改变连接关闭顺序。
- 取消、失败和成功都记录总耗时；只有拿到连接或完成归还时才填写对应阶段。
- 诊断报告不记录 SQL 参数、数据库 URL、用户名、密码、NVD Key 或业务行。
- `performance_schema` 仍由独立诊断账号读取，业务账号权限不扩大。

## 测试与验收

- 单元测试覆盖连接获取、执行、归还和总耗时的同订阅关联，以及失败、取消和未开启诊断的行为。
- 报告测试覆盖可选阶段字段的 JSON/Markdown 输出。
- PowerShell 脚本至少验证缺少 `NVD_API_KEY` 时明确失败，不打印任何密钥内容。
- 性能定位以三轮同环境中位数判断：若 `acquire` 或 `release` P99 抬升，处理连接池；若 `executeAndCommit` 抬升并与 redo/binlog 等待一致，处理 MySQL 存储和事务提交；若 Java 侧剩余阶段异常，再回到 ORM 热路径优化。
- 正式性能放行仍要求严格持久化、完整四场景三轮、零业务错误、连接全部归还，并且 `updateById` P99 中位数不超过 `6.601 ms`。

## 不在本批范围

- 不通过降低 `innodb_flush_log_at_trx_commit` 或 `sync_binlog` 取得正式通过结果。
- 不增加自动写重试，不扩大连接池掩盖磁盘饱和。
- 不修改 Repository、FormClient、Scope、动态表单或事务公共契约。
- 不创建 GitHub Release，不上传二进制制品。
