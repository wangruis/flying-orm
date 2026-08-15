# 真实数据库认证方法

真实数据库认证分批执行，不能用一次 smoke 代替全部结论。所有批次都要在同一个 Git 提交、明确的数据库镜像和驱动版本上完成。

## 固定环境

源码仓库保留固定环境、场景和历史结果，便于复核认证口径；包含凭据、本机 Docker 编排和执行脚本的认证资产
单独维护，不随源码仓库分发。当前固定环境是：

| 数据库 | 镜像 | R2DBC 驱动 |
| --- | --- | --- |
| MySQL | `mysql:8.4.10` | `io.asyncer:r2dbc-mysql:1.4.1` |
| PostgreSQL | `postgres:17.10` | `org.postgresql:r2dbc-postgresql:1.1.2.RELEASE` |
| Oracle | `gvenzl/oracle-free:23.26.0-slim-faststart` | `com.oracle.database.r2dbc:oracle-r2dbc:1.3.0` |
| SQL Server | `mcr.microsoft.com/mssql/server:2022-CU22-GDR1-ubuntu-22.04` | `io.r2dbc:r2dbc-mssql:1.0.4.RELEASE` |
| H2 | 不使用容器 | `io.r2dbc:r2dbc-h2:1.1.0.RELEASE` |

版本标签用于稳定复跑，最终报告还要记录本机实际拉取的镜像 ID。镜像或驱动升级后必须重新执行相关批次，旧结果不能自动继承。

MySQL 8.4 使用 `caching_sha2_password`。R2DBC 认证使用 `sslMode=REQUIRED`，避免容器重启后进入完整认证阶段时
因没有 TLS 而被驱动拒绝；生产环境应进一步使用受信任证书校验。`allowPublicKeyRetrieval` 是 MySQL JDBC Connector
在明确关闭 TLS 时的本地认证选项，不是 Asyncer R2DBC URL 的通用能力，也不能代替生产 TLS。

## 当前认证结果

2026-08-01 已在本机 Docker 完成 MySQL/PostgreSQL 第一轮真实驱动功能认证、两批真实事务与故障认证，
以及连接池和响应式并发稳定性认证。认证基于提交 `f8401ef` 加本批测试，当时通过本地维护的
`Invoke-Certification.ps1 -Action Verify -Database Core` 入口执行；该脚本不属于当前源码仓库。

| 数据库 | 通过场景 | 结果 |
| --- | ---: | --- |
| MySQL 8.4.10（镜像 `sha256:8dbcf531a03a...`） | 15 | 功能、批量事务、故障恢复、小池耗尽恢复、慢消费者取消和持续有界并发通过 |
| PostgreSQL 17.10（镜像 `sha256:a426e44bac0b...`） | 16 | MySQL 同类场景通过，另含原生 Array 与数组条件 |

本轮共执行 33 个外部场景，31 个通过，Oracle 和 SQL Server 的 2 个场景因为没有启动目标数据库而按设计跳过，
没有失败。新增的 6 个并发场景覆盖小池占满后的统一连接获取超时、等待申请取消和释放后恢复；零初始 demand、
分段 request、中途 cancel 和取消后连接复用；以及 4 连接上限下 96 次持续查询全部完成、峰值不越界、最终零借出
和零等待。这里证明的是 flying-orm 对下游 demand 和取消的响应，以及连接池最终资源状态，不把驱动内部实现
是否预取写成认证结论。脱敏原始证据位于本机
`target/certification-results/20260801T160844Z-f8401efab988`，该目录不提交
仓库，避免把本机连接信息和大体积测试输出混入源码历史。

真实终止 MySQL 会话时，r2dbc-mysql 会把 `Connection unexpectedly closed` 和一次强制关闭提示写到 stderr。
这是受测故障已经到达驱动的证据，不代表 Maven 失败；认证结论以 Surefire 的失败数和 `manifest.json` 状态为准。

2026-08-02 完成 Oracle 和 SQL Server 第一轮预览真实库认证。两库分别执行 7 个目标场景且全部通过：动态表单
DDL/CRUD/MERGE、JSON、BLOB/CLOB、乐观锁、主外键元数据、32 请求/8 并发的有界执行，以及默认 ATOMIC
整批回滚、显式 INDEPENDENT 分片隔离和重复键分类。Oracle 证据在
`target/certification-results/20260801T175500Z-1b64e3ff320a`，SQL Server 证据在
`target/certification-results/20260801T175711Z-1b64e3ff320a`；证据目录仍只保留在本机。

Oracle 真实 MERGE 暴露了大字段源参数没有目标列类型上下文的问题。当前会保留可稳定哈希的 BLOB/CLOB 参数，
在 bind 前转换成非阻塞 R2DBC LOB；含 LOB 的驱动批次逐行复用同一连接，事务型批量仍留在原事务中，普通
批次继续走原生批处理。
SQL Server 使用旧的 `r2dbc-mssql:1.0.2.RELEASE` 复跑时，驱动曾在初始化和短连接关闭后报告 Netty ByteBuf
泄漏与 `onErrorDropped: Connection closed`，因此认证驱动升级到当前维护版 `1.0.4.RELEASE` 并重新执行同一批次。
升级后 ByteBuf 泄漏告警消失，短连接主动关闭时的少量 `onErrorDropped` 仍存在，但没有进入业务结果或造成测试
失败，继续作为预览驱动观测残项。

同日补完第二轮 Preview 深化认证。Oracle 和 SQL Server 各新增 6 个真实场景：两连接小池耗尽后的获取超时与恢复、
慢消费者分段 request 后取消并归还连接、四连接上限下 96 次持续查询、UPDATE 交叉锁真实死锁、管理员终止受测
会话后的连接分类与新连接恢复，以及提交确认丢失后的 UNKNOWN 查询和幂等重放。加上第一轮场景，两库各有
13 个目标场景通过。本批最终执行 57 个测试，其中 26 个属于当前两库实际执行，0 失败、0 错误；脱敏证据位于
`target/certification-results/20260801T203433Z-d40d2489b6f5`。

本轮真实 Oracle 还发现回执预留阶段把空字符串写入 `payload_hash` 的兼容问题。Oracle 会把空字符串当成 NULL，
因此在业务事务执行前触发 ORA-01400。回执仓库已改用非空 `RESERVED` 占位，提交前仍由真实 payload hash 覆盖；
Oracle 的 UNKNOWN 确认和重放随后通过。Oracle 会话终止由独立 SYSTEM 认证连接完成，业务账号没有获得
`ALTER SYSTEM`。SQL Server 的死锁使用两事务交叉 UPDATE，稳定返回并翻译 1205。

`r2dbc-mssql:1.0.4.RELEASE` 在连接池主动关闭短连接时仍会把少量 `onErrorDropped: Connection closed` 写到 stderr，
但池指标最终归零，业务 Publisher 没有收到额外失败，Surefire 和认证清单均通过。该日志继续作为预览驱动限制，
不会通过全局吞错钩子掩盖。

同日完成第三轮 Preview 故障边界认证。Oracle 使用 `FOR UPDATE WAIT 1` 在真实行锁竞争下返回 ORA-30006，
SQL Server 在竞争会话设置 `LOCK_TIMEOUT 500` 后返回 1222；两者都被稳定翻译为 `LOCK_TIMEOUT`，回滚并关闭
两条连接后，新连接可以立即读回原表。至此两库各有 14 个目标场景通过。本批执行 59 个测试，其中 28 个属于
当前两库实际执行，31 个因目标数据库未启动而按设计跳过，0 失败、0 错误；脱敏证据位于
`target/certification-results/20260801T204247Z-bb2ae8b1898b`。显式锁超时缺口已经关闭，SQL Server 主动关闭
连接时的 dropped 日志仍作为正式支持范围内的已知驱动日志边界保留。

2026-08-02 又完成了一轮固定资源正式性能长跑。MySQL/PostgreSQL 的查询、更新、ATOMIC 和 INDEPENDENT 共
8 个场景全部零错误，连接池最终零借出、零等待；吞吐、P50/P95/P99、CPU、堆和连接峰值均已归档。完整结果见
[本机真实数据库性能基线](performance-database-baseline-2026-08-02.md)。这是一轮初始基线，企业版 V1 的 RC
性能门禁仍要求相同参数至少三轮并比较中位数。

真实驱动认证发现并修复了方言标识符引用、元数据参数标记、PostgreSQL Array 类型推断、PostgreSQL JSON
驱动包装值、MySQL 主键元数据重复/数字布尔值、MySQL TLS 连接配置差异、MySQL 8 的
`ER_LOCK_NOWAIT(3572)` 分类缺口，以及真实断链时 PostgreSQL `57P0x` 和无 SQLState R2DBC 资源异常的
连接分类缺口；预览认证又修复了 Oracle MERGE 的 BLOB/CLOB 非阻塞绑定和 SQL Server 认证 URL 在 Windows
`mvn.cmd` 下的参数传递问题。

## V1.0.1 最终认证

2026-08-07 使用 MySQL 8.4.10 和 PostgreSQL 17.8 完成 V1.0.1 最终门禁。功能、事务、故障、取消、UNKNOWN、
连接池恢复、慢消费者和有界并发共实际执行 33 个外部场景，0 失败、0 错误。平级 Spring 示例另有两种数据库
各 1 个外部事务用例通过，覆盖普通提交/回滚、外部 ATOMIC 最终提交/回滚和 INDEPENDENT 预执行拒绝。

两库随后完成固定参数三轮真实性能认证。每轮所有场景错误率为 0，结束时连接池借出和等待均为 0。MySQL
完整序列受宿主机同步写抖动影响，updateById 又按同口径执行三轮单场景复核，吞吐、P95 和 P99 与既有稳定
多轮参考持平。完整环境、结果和边界见
[V1.0.1 真实数据库与性能认证](database-certification-2026-08-07-v1.0.1.md)。

## V2.0.0 当前认证

2026-08-08 使用 V2 当前代码完成五库验证，2026-08-09 在修复确定性 SQL 错误的连接归还分类后完成三轮复核。
H2 同时覆盖原生 JDBC 和 R2DBC 契约；MySQL 8.4.10、PostgreSQL 17.8、Oracle Free 23.26.0、
SQL Server 2022 CU22 GDR1 完成 JDBC 与 R2DBC 真实驱动认证。每轮执行 65 项，失败、错误和跳过均为 0；三轮合计
195 项。Oracle 的 15 个 R2DBC 目标场景全部通过，包括慢消费者取消后的连接归还。

MySQL、PostgreSQL 又完成 JDBC/R2DBC 固定参数三轮性能认证。12 份报告、54 个场景轮次全部 `PASSED`，
错误、预热失败、结束时连接借出和连接等待均为 0。完整版本、参数和三轮中位数见
[V2.0.0 真实数据库与性能认证](database-certification-2026-08-08-v2.0.0.md)。

## V2.0.0 当前基线四库复核

2026-08-10 使用当前候选版源码和固定 Docker 镜像再次完成四库真实认证。MySQL 8.4.10 与 PostgreSQL 17.10
同批执行 37 个目标场景；Oracle Free 23.26.0 和 SQL Server 2022 CU22 GDR1 分别执行 16 个目标场景。
四库合计实际执行 69 个目标场景，失败和错误均为 0。未配置目标库的方法按测试设计跳过，不计入 69 个实际场景。

同日对新增能力完成全面审查并修复后再次复跑：MySQL/PostgreSQL 同批实际执行 37 项，Oracle 与 SQL Server
分别实际执行 16 项，合计仍为 69 项，失败、错误和目标场景跳过均为 0。复跑前的全项目质量门禁实际执行
1192 项本地测试，覆盖率、Checkstyle、SpotBugs 以及发布制品检查全部通过。本次修复覆盖保护写在 R2DBC
BEGIN/COMMIT 回执超时后的 UNKNOWN 分类与连接隔离、派生客户端共享密钥的引用生命周期、同步和响应式装饰器对
保护批量入口的完整转发，以及 Oracle 12c 可移植的 JOIN 结果别名长度；四库认证确认这些修复未改变现有 JDBC、
R2DBC、事务、JOIN、加密搜索或脱敏行为。

本轮继续覆盖 DDL、CRUD、JSON、LOB、元数据、乐观锁、ATOMIC/INDEPENDENT、死锁、锁超时、连接中断、
UNKNOWN 恢复、连接池耗尽恢复、慢消费者取消和持续有界并发；同时新增 JDBC/R2DBC 双执行链的
`join/leftJoin/rightJoin` 与字段保护真实数据认证，验证密文入库、精确/后缀/包含搜索、默认脱敏和可信完整显示。

真实认证发现并修复了两个启动与执行缺口：组合后的响应式执行器装饰层未继续委托原子字段保护写入；Oracle 官方
R2DBC 驱动的 metadata 名称 `Oracle Database` 未被自动方言解析器识别。修复后对应聚焦测试与四库完整外部套件
均重新通过。SQL Server 主动终止连接时仍可能由驱动向 stderr 输出少量 `Connection closed/onErrorDropped`，
它只出现在故障注入边界，Surefire、业务结果与后续连接恢复均通过，不使用全局丢错钩子掩盖。

2026-08-11 在继续完成固定范围全面审查、根因修复和全量质量门禁后，再次使用同一组固定镜像复跑四库认证。
MySQL 8.4.10 与 PostgreSQL 17.10 同批实际执行 37 项，Oracle Free 23.26.0 与 SQL Server 2022 CU22 GDR1
分别实际执行 16 项，合计 69 项；失败、错误和目标场景跳过均为 0。复跑前全项目质量门禁实际执行 1288 项测试，
失败和错误为 0，另有 69 项未配置外部数据库的场景按普通构建设计跳过；覆盖率、Checkstyle、SpotBugs 和发布制品
门禁全部通过。本轮修复包括标准回执表 `operation_id varchar(128)` 与公开操作 ID 契约一致、JDBC 受保护多语句写入
共用一次总超时、单条及批量 side-index 插入必须确认影响一行，以及字段保护 Schema 显式索引重名的稳定拒绝。
SQL Server 故障注入仍可能输出上述驱动级断链日志，但认证结果、连接恢复和资源归还均通过。

2026-08-13 对当前 V2.0.0 候选源码再次执行发布前四库认证。固定镜像分别为 MySQL 8.4.10
（镜像 ID `8dbcf531a03a`）、PostgreSQL 17.10（`7958605b474b`）、Oracle Free 23.26.0
（`6ace9029608f`）和 SQL Server 2022 CU22 GDR1（`bf438d7104f8`）。MySQL/PostgreSQL 同批实际执行
37 项，Oracle 和 SQL Server 各实际执行 16 项，四库合计 69 项，失败和错误均为 0；未配置目标库的方法按设计
跳过，不计入实际场景。认证完成后对同一源码运行全项目 `quality` 门禁，实际执行 1435 项测试，失败和错误为 0，
其中 69 项未配置外部数据库的场景按普通构建设计跳过；覆盖率、Checkstyle、SpotBugs 均通过。随后执行
`release-artifacts` 门禁，源码包、Javadoc、依赖分析和全部发布制品构建通过。本轮 SQL Server 故障注入仍可能输出
少量驱动级 `Connection closed/onErrorDropped`，但 Surefire、业务结果、连接恢复和资源归还均通过。

## 批次和通过条件

| 批次 | 验证内容 | 通过条件 |
| --- | --- | --- |
| 环境 | 容器健康、版本、字符集、时区、驱动加载 | 所有信息完整，连接可建立，报告不含密码 |
| 功能 | DDL、CRUD、分页、批量、JSON、LOB、元数据、乐观锁；PostgreSQL 加 Array | 结果值和影响行数正确，无跳过的目标场景 |
| 事务与故障 | ATOMIC/INDEPENDENT、冲突、回滚、取消、死锁、锁超时、连接中断、UNKNOWN 恢复 | 事务结果可解释，连接释放，稳定错误分类正确 |
| 并发 | 有界并发、连接池接近耗尽、慢消费者、取消和回压 | 无泄漏、无无限排队、错误率和峰值连接可解释 |
| 性能 | 吞吐、P50/P95/P99、内存、CPU、连接占用 | 同机多轮结果稳定，并满足候选版门槛 |

MySQL、PostgreSQL 必须完成全部功能、故障和多轮性能阻断批次。Oracle、SQL Server 必须完成当前承诺能力的
功能、事务、故障和并发认证；专门吞吐基线不作为 V2 阻断项。

## 证据结构

每次执行至少保留以下内容：

```text
run-id/
  manifest.json
  environment.txt
  maven-output.log                 # 功能/故障认证
  mysql-metrics.json               # 性能认证
  mysql-summary.md
  postgresql-metrics.json
  postgresql-summary.md
```

`manifest.json` 必须包含 Git 提交、数据库选择、数据库和驱动版本、开始与结束时间、状态和证据文件名。连接串、用户名之外的凭据、生产地址和业务数据不得进入报告。

## 执行顺序

1. 先运行 `Core` smoke，修复 MySQL/PostgreSQL 功能和驱动差异。
2. 再分别运行 Oracle、SQL Server，避免两个重型容器互相影响排查。
3. 功能稳定后执行真实故障和事务批次。
4. 最后在固定资源和安静机器上运行并发、吞吐与延迟认证。
5. 修复认证发现的问题并重跑受影响批次。
6. 全部阻断证据齐全后，才进入发版交付收口。
