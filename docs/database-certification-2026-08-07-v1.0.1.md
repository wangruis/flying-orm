# V1.0.1 真实数据库与性能认证

## 结论

2026-08-07 在本机 Docker MySQL 8.4.10 和 PostgreSQL 17.8 上完成 V1.0.1 最终认证。功能、事务、故障、
取消、连接池恢复和有界并发共实际执行 33 个外部场景，0 失败、0 错误；Oracle、SQL Server 的 28 个
非目标场景按设计跳过。

平级 `flying-orm-example` 另用 Spring 的 `R2dbcTransactionManager` 在两种真实数据库上验证外部事务。
普通提交/回滚、外部 ATOMIC 的 `ENLISTED -> COMMITTED/ROLLED_BACK`、外部 INDEPENDENT 预执行拒绝和
最终数据行数全部通过。示例代码不进入主项目仓库，core/rdb 仍保持零 Spring 依赖。

## 固定环境

| 项目 | 值 |
| --- | --- |
| JDK | Oracle JDK 21.0.10 |
| MySQL | 8.4.10，`r2dbc-mysql:1.4.1`，TLS REQUIRED |
| PostgreSQL | 17.8，`r2dbc-postgresql:1.1.1.RELEASE` |
| 连接池 | r2dbc-pool 1.0.2.RELEASE，固定 16 连接 |
| 查询/更新并发 | 16 |
| 批量外层并发 | 4 |
| 每批行数 | 32 |
| 预热/测量 | 每场景 5 秒 / 15 秒 |
| 种子数据 | 10,000 行 |

连接凭据只在认证子进程环境中使用，没有写入报告。认证没有启动或停止其他数据库，也没有操作
`tdengine-tsdb`。

## 三轮中位数

### MySQL

| 场景 | ops/s | rows/s | P50 ms | P95 ms | P99 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| queryById | 13,481.259 | 13,481.259 | 1.151 | 1.502 | 2.111 |
| updateById | 3,014.751 | 3,014.751 | 4.047 | 5.333 | 9.085 |
| transactionalUpdateBatch | 1,712.536 | 13,700.288 | 9.183 | 11.362 | 14.213 |
| atomicBatchInsert | 266.441 | 8,526.116 | 14.377 | 18.858 | 23.609 |
| independentBatchInsert | 437.457 | 13,998.620 | 8.937 | 10.871 | 13.222 |

完整序列三轮都是零错误、零最终借出和零等待，但宿主机 I/O 抖动明显：第二轮出现一次约 5.3 秒停顿，
第三轮 `updateById` P99 达到 22.495 ms。因此又按相同并发、预热和测量参数执行三轮 updateById 单场景复核：

| 轮次 | ops/s | P50 ms | P95 ms | P99 ms | max ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 3,852.735 | 4.004 | 5.648 | 9.568 | 26.886 |
| 2 | 3,837.875 | 4.063 | 5.509 | 9.658 | 25.199 |
| 3 | 3,801.362 | 4.086 | 5.489 | 9.576 | 107.610 |

单场景中位数为 `3837.875 ops/s / P95 5.509 ms / P99 9.576 ms`，与已有稳定三轮参考
`3803.18 ops/s / P95 5.890 ms / P99 9.593 ms` 持平或略好。结合 JMH 默认执行路径无回退，可以确认
本轮没有引入 ORM updateById 性能回归；完整序列长尾继续作为 Windows Docker 严格同步写的环境波动记录。

### PostgreSQL

| 场景 | ops/s | rows/s | P50 ms | P95 ms | P99 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| queryById | 31,371.320 | 31,371.320 | 0.479 | 0.753 | 0.926 |
| updateById | 12,471.955 | 12,471.955 | 1.255 | 1.628 | 2.345 |
| transactionalUpdateBatch | 2,583.620 | 20,668.960 | 5.530 | 9.421 | 17.842 |
| atomicBatchInsert | 366.825 | 11,738.402 | 10.961 | 13.312 | 17.203 |
| independentBatchInsert | 610.290 | 19,529.267 | 6.255 | 7.897 | 9.773 |

三轮全部零错误、零最终借出和零等待。认证过程中发现性能 runner 的显式事务对照场景把 `?` 直接交给
PostgreSQL 原生 Statement；修正为 `$1` 后，短测和三轮正式结果全部通过。该问题只位于 benchmark，
生产执行器的统一参数标记改写没有受影响。

## 原始证据

- 功能与故障认证：Maven Surefire 61 个测试，MySQL/PostgreSQL 实际场景 33 个通过，其他数据库 28 个跳过。
- Spring 外部事务真实库：2 个测试通过。
- 本地 JMH：[阶段 9 回归说明](performance-results/2026-08-07-v101-phase9-local-regression.md)。
- 真实库每轮 JSON 与摘要：`docs/performance-results/2026-08-07-v101-phase9-*-round-*`。
- MySQL 更新专项：`docs/performance-results/2026-08-07-v101-phase9-mysql-update-only-round-*`。

所有正式性能 JSON 都包含环境、固定参数、错误率、CPU、堆峰值和连接池峰值，不包含数据库连接串。
