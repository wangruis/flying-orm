# flying-orm 真实数据库性能结果

- 运行编号：`v101-phase9-postgresql-round-3`
- Git commit：`151ad3477d9d-working-tree`
- 状态：**PASSED**
- 时间：2026-08-07T07:16:47.796191500Z 至 2026-08-07T07:18:35.542992900Z

## 固定参数

连接池 16，查询/更新并发 16，批量外层并发 4，每批 32 行，预热 5 秒，测量 15 秒。

## PostgreSQL

数据库版本：PostgreSQL 17.8 (Debian 17.8-1.pgdg12+1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 12.2.0-14+deb12u1) 12.2.0, 64-bit；驱动：PostgreSQL；连接池：r2dbc-pool 1.0.2.RELEASE。

| 场景 | 状态 | ops/s | rows/s | P50 ms | P95 ms | P99 ms | 错误率 | 峰值连接 | CPU | 峰值堆 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| queryById | PASSED | 20529.590 | 20529.590 | 0.559 | 1.953 | 3.926 | 0.0000% | 16 | 14.612% | 357.98 MiB |
| updateById | PASSED | 12239.474 | 12239.474 | 1.250 | 1.628 | 3.582 | 0.0000% | 16 | 6.319% | 390.00 MiB |
| transactionalUpdateBatch | PASSED | 3083.386 | 24667.086 | 4.821 | 6.738 | 7.717 | 0.0000% | 16 | 10.378% | 372.57 MiB |
| atomicBatchInsert | PASSED | 366.825 | 11738.402 | 10.600 | 12.968 | 17.203 | 0.0000% | 4 | 3.200% | 372.16 MiB |
| independentBatchInsert | PASSED | 675.169 | 21605.411 | 5.964 | 6.992 | 8.446 | 0.0000% | 16 | 9.287% | 372.67 MiB |

最终池状态：allocated=16，acquired=0，pending=0。

> CPU 是负载器 Java 进程平均占用，不包含 Docker 数据库进程；堆内存是 JVM 内存池峰值，不等于整个进程 RSS。本结果只代表记录中的本机与固定环境。
