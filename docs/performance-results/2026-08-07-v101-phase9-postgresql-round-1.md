# flying-orm 真实数据库性能结果

- 运行编号：`v101-phase9-postgresql-round-1`
- Git commit：`151ad3477d9d-working-tree`
- 状态：**PASSED**
- 时间：2026-08-07T07:09:45.317144700Z 至 2026-08-07T07:11:28.799035800Z

## 固定参数

连接池 16，查询/更新并发 16，批量外层并发 4，每批 32 行，预热 5 秒，测量 15 秒。

## PostgreSQL

数据库版本：PostgreSQL 17.8 (Debian 17.8-1.pgdg12+1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 12.2.0-14+deb12u1) 12.2.0, 64-bit；驱动：PostgreSQL；连接池：r2dbc-pool 1.0.2.RELEASE。

| 场景 | 状态 | ops/s | rows/s | P50 ms | P95 ms | P99 ms | 错误率 | 峰值连接 | CPU | 峰值堆 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| queryById | PASSED | 31371.320 | 31371.320 | 0.479 | 0.753 | 0.926 | 0.0000% | 16 | 19.179% | 467.33 MiB |
| updateById | PASSED | 12492.569 | 12492.569 | 1.270 | 1.604 | 2.033 | 0.0000% | 16 | 7.694% | 499.02 MiB |
| transactionalUpdateBatch | PASSED | 2583.620 | 20668.960 | 5.771 | 9.421 | 17.842 | 0.0000% | 16 | 8.884% | 489.88 MiB |
| atomicBatchInsert | PASSED | 357.345 | 11435.041 | 10.961 | 17.089 | 20.972 | 0.0000% | 4 | 3.790% | 487.61 MiB |
| independentBatchInsert | PASSED | 610.290 | 19529.267 | 6.484 | 7.897 | 9.773 | 0.0000% | 16 | 9.461% | 487.61 MiB |

最终池状态：allocated=16，acquired=0，pending=0。

> CPU 是负载器 Java 进程平均占用，不包含 Docker 数据库进程；堆内存是 JVM 内存池峰值，不等于整个进程 RSS。本结果只代表记录中的本机与固定环境。
