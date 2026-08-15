# flying-orm 真实数据库性能结果

- 运行编号：`v101-phase9-postgresql-round-2`
- Git commit：`151ad3477d9d-working-tree`
- 状态：**PASSED**
- 时间：2026-08-07T07:13:16.050623400Z 至 2026-08-07T07:14:59.575487800Z

## 固定参数

连接池 16，查询/更新并发 16，批量外层并发 4，每批 32 行，预热 5 秒，测量 15 秒。

## PostgreSQL

数据库版本：PostgreSQL 17.8 (Debian 17.8-1.pgdg12+1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 12.2.0-14+deb12u1) 12.2.0, 64-bit；驱动：PostgreSQL；连接池：r2dbc-pool 1.0.2.RELEASE。

| 场景 | 状态 | ops/s | rows/s | P50 ms | P95 ms | P99 ms | 错误率 | 峰值连接 | CPU | 峰值堆 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| queryById | PASSED | 31814.257 | 31814.257 | 0.467 | 0.746 | 0.905 | 0.0000% | 16 | 20.909% | 465.43 MiB |
| updateById | PASSED | 12471.955 | 12471.955 | 1.255 | 1.628 | 2.345 | 0.0000% | 16 | 7.042% | 497.74 MiB |
| transactionalUpdateBatch | PASSED | 1983.905 | 15871.238 | 5.530 | 21.987 | 54.624 | 0.0000% | 16 | 8.329% | 491.13 MiB |
| atomicBatchInsert | PASSED | 385.157 | 12325.009 | 11.043 | 13.312 | 16.474 | 0.0000% | 4 | 3.543% | 486.93 MiB |
| independentBatchInsert | PASSED | 600.481 | 19215.392 | 6.255 | 8.880 | 16.392 | 0.0000% | 16 | 8.897% | 486.92 MiB |

最终池状态：allocated=16，acquired=0，pending=0。

> CPU 是负载器 Java 进程平均占用，不包含 Docker 数据库进程；堆内存是 JVM 内存池峰值，不等于整个进程 RSS。本结果只代表记录中的本机与固定环境。
