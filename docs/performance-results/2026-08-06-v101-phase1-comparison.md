# flying-orm 无数据库基准对比

- flying-orm 结果：`docs\performance-results\2026-08-06-v101-phase1-final.json`
- 基线结果：`docs\performance-results\2026-08-06-v101-phase1-baseline.json`
- 改善率：吞吐模式数值越高越好，耗时模式数值越低越好。

| 场景 | 模式 | 单位 | 基线 | flying-orm | 改善率 |
| --- | --- | --- | ---: | ---: | ---: |
| directUpdate | avgt | ns/op | 0.527 | 0.510 | +3.28% |
| observedUpdate | avgt | ns/op | 244.034 | 238.705 | +2.18% |
| protectedUpdate | avgt | ns/op | 0.977 | 0.955 | +2.22% |
| renderSelectHotPlanHit | thrpt | ops/ms | 892.072 | 970.442 | +8.79% |
| renderSelectWithoutPlanCache | thrpt | ops/ms | 270.557 | 292.187 | +7.99% |
| renderWhere | thrpt | ops/ms | 674.225 | 690.211 | +2.37% |

## 延迟分位值

| 场景 | 基线 P50 | flying P50 | 基线 P95 | flying P95 | 基线 P99 | flying P99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| directUpdate | 0.512 | 0.510 | 0.595 | 0.510 | 0.595 | 0.510 |
| observedUpdate | 241.491 | 238.530 | 255.258 | 239.130 | 255.258 | 239.130 |
| protectedUpdate | 0.951 | 0.956 | 1.075 | 0.959 | 1.075 | 0.959 |
| renderSelectHotPlanHit | 911.710 | 969.528 | 957.983 | 976.423 | 957.983 | 976.423 |
| renderSelectWithoutPlanCache | 284.930 | 293.720 | 287.075 | 294.151 | 287.075 | 294.151 |
| renderWhere | 696.130 | 690.571 | 698.741 | 692.212 | 698.741 | 692.212 |

> 只有两边在同一机器、JDK、JVM 参数和 JMH 配置下运行，结果才可用于性能结论。
