# 2026-08-02 最终真实数据库认证复核

## 结论

本轮已经完成 PostgreSQL pgvector、Schema/RLS 连接池清理与隔离、五种数据库兼容、并发稳定性、真实库长跑和 JVM 热路径基准。

- 功能兼容、事务、故障恢复、并发边界和连接池资源回收：通过。
- PostgreSQL pgvector 写入、读取、距离排序：通过。
- PostgreSQL Schema/RLS 在同一条池化物理连接上的切换、清理和无上下文隔离：通过。
- MySQL/PostgreSQL 三轮正式性能运行：所有场景零错误，所有连接均归还。
- RC 性能门禁：仍然阻断。MySQL `updateById` 的三轮 P99 中位数约为 `9.110 ms`，相对最初 `6.001 ms` 基线仍超过 10% 硬线。

因此，这一轮可以确认 ORM 主链路没有功能、并发或连接泄漏阻断，但不能把企业版 V1 的性能门禁写成已经通过。按照已有决定，不再追加原生 Linux 或独立磁盘交叉复核；该限制保留到发版交付收口时处理。

## 认证环境

| 数据库 | 镜像 | 本机镜像 ID | 认证定位 |
| --- | --- | --- | --- |
| MySQL | `mysql:8.4.10` | `sha256:8dbcf531a03a...` | V1 核心支持 |
| PostgreSQL + pgvector | `pgvector/pgvector:0.8.1-pg17` | `sha256:3e8b3adfd27b...` | V1 核心支持与向量增强 |
| Oracle | `gvenzl/oracle-free:23.26.0-slim-faststart` | `sha256:6ace9029608f...` | 预览支持 |
| SQL Server | `mcr.microsoft.com/mssql/server:2022-CU22-GDR1-ubuntu-22.04` | `sha256:bf438d7104f8...` | 预览支持 |
| H2 | 内嵌驱动，不使用容器 | `r2dbc-h2:1.1.0.RELEASE` | 开发和测试 |

运行环境为 Oracle JDK 21.0.10、Windows 10 amd64、20 个逻辑处理器。认证基于提交 `83468326a970` 加当前工作区变更；最终发版必须在这些变更提交后重新生成一份干净提交证据，本报告不代替最后的发版认证。

## PostgreSQL 高级认证

认证容器从普通 PostgreSQL 镜像改为固定 pgvector 版本镜像。新增真实库用例覆盖：

1. 创建 `vector` 扩展和 `vector(3)` 列。
2. 使用 flying-orm 参数绑定写入两个三维向量。
3. 从驱动返回值读取向量并还原为 `float[]`。
4. 使用 `<->` 执行最近邻排序并核对距离与第一条结果。
5. 把连接池大小固定为 1，让两个 schema、两个租户和无上下文请求必然复用同一条物理连接。
6. 以非超级用户角色执行 RLS，避免 PostgreSQL 超级用户绕过策略造成假通过。
7. 归还连接前清理 `search_path`、RLS 会话变量和临时数据库角色，再从池中直接借出连接核对清理结果。

PostgreSQL 单库认证执行 61 个测试，其中 18 个属于当前数据库实际执行，43 个因目标数据库不匹配按设计跳过，失败 0、错误 0。证据目录：

```text
target/certification-results/20260802T061849Z-83468326a970
```

## 数据库兼容与并发

### MySQL 与 PostgreSQL

核心双库统一执行 61 个测试，实际执行 33 个，失败 0、错误 0。覆盖 DDL、CRUD、分页、JSON、LOB、PostgreSQL Array、pgvector、乐观锁、ATOMIC/INDEPENDENT 批量、死锁、锁超时、断连恢复、UNKNOWN、慢消费者、取消、小池耗尽和持续有界并发。

证据目录：

```text
target/certification-results/20260802T061929Z-83468326a970
```

### H2

H2 内嵌真实驱动执行 31 个测试，全部通过且无跳过。覆盖动态表单批量与 upsert、元数据读取、JSON、LOB、带偏移时间、同步桥接和结构变更。

### Oracle 与 SQL Server

预览双库统一执行 61 个测试，实际执行 28 个，失败 0、错误 0。覆盖真实 DDL/CRUD/MERGE、类型映射、事务、死锁、锁超时、会话终止、连接恢复和 UNKNOWN。

SQL Server 驱动在主动终止连接的故障场景仍会向 stderr 写少量 `onErrorDropped: Connection closed`。业务 Publisher、后续连接恢复、池最终状态和测试断言都正常，因此保留为预览驱动限制，不用全局吞错钩子掩盖。

证据目录：

```text
target/certification-results/20260802T062547Z-83468326a970
```

## 三轮真实库性能

固定参数：连接池 16，查询/更新并发 16，批量外层并发 4，每批 32 行，每个场景预热 5 秒、测量 15 秒，种子数据 10,000 行。

三轮目录：

```text
target/performance-results/20260802T063034Z-83468326a970-formal
target/performance-results/20260802T063405Z-83468326a970-formal
target/performance-results/20260802T063742Z-83468326a970-formal
```

| 数据库 | 场景 | ops/s 中位数 | rows/s 中位数 | P95 ms 中位数 | P99 ms 中位数 |
| --- | --- | ---: | ---: | ---: | ---: |
| MySQL | queryById | 13,613.28 | 13,613.28 | 1.459 | 1.753 |
| MySQL | updateById | 3,983.01 | 3,983.01 | 5.063 | 9.110 |
| MySQL | atomicBatchInsert | 274.98 | 8,799.23 | 16.663 | 19.087 |
| MySQL | independentBatchInsert | 422.97 | 13,535.14 | 11.166 | 13.017 |
| PostgreSQL | queryById | 26,269.48 | 26,269.48 | 0.798 | 0.952 |
| PostgreSQL | updateById | 12,031.36 | 12,031.36 | 1.580 | 1.850 |
| PostgreSQL | atomicBatchInsert | 342.98 | 10,975.26 | 13.885 | 16.515 |
| PostgreSQL | independentBatchInsert | 631.43 | 20,205.78 | 7.062 | 9.052 |

全部 24 个数据库场景均为零预热错误、零测量错误。六次数据库负载结束时都是 `finalAcquiredConnections=0`、`finalPendingAcquires=0`，没有发现持续连接增长或池等待残留。

MySQL 单行更新吞吐和 P95 基本稳定，但 P99 相对最初基线仍明显偏高。现有证据更符合 Windows Docker Desktop 严格持久化写入的尾延迟抖动，没有发现 ORM 连接未归还、无限排队或业务错误；在没有交叉环境证据前，门禁仍按阻断处理。

## JVM 热路径基线

JMH 1.37 使用 1 fork、1 线程、3 次预热、5 次测量完成 16 个基准方法。完整 JSON：

```text
target/performance-results/flying-orm-jmh-final.json
```

本轮有代表性的结果：

| 热路径 | 结果 |
| --- | ---: |
| 条件编译 | 595.887 ops/ms |
| WHERE 渲染 | 907.714 ops/ms |
| insert 批量计划 | 394.764 ops/ms |
| upsert 批量计划 | 291.819 ops/ms |
| Bean 映射 | 3,584.419 ops/ms |
| Record 映射 | 2,949.630 ops/ms |
| Caffeine 热表读取 | 16,774,159.053 ops/s |
| Caffeine 多表读取 | 7,441,323.145 ops/s |

这些值只作为当前机器、JDK 和 JMH 参数下的后续回归基线。执行器的亚纳秒空 Publisher 数值可能被 JIT 消除影响，不能拿来宣传真实调用耗时；真实执行性能以数据库长跑结果为准。

## 后续位置

真实数据库认证主任务已经完成，当前唯一明确的性能阻断仍是 MySQL 更新 P99。下一步回到 V1 后置开发任务；发版交付收口继续放在最后，并在代码提交、版本号和候选版本确定后重新跑干净提交认证。
