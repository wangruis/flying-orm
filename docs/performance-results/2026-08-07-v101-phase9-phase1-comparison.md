# flying-orm 无数据库基准对比

- flying-orm 结果：`D:\new_code\flying-orm\docs\performance-results\2026-08-07-v101-phase9-local-jmh.json`
- 基线结果：`D:\new_code\flying-orm\docs\performance-results\2026-08-06-v101-phase1-final.json`
- 改善率：吞吐模式数值越高越好，耗时模式数值越低越好。

| 场景 | 模式 | 单位 | 基线 | flying-orm | 改善率 |
| --- | --- | --- | ---: | ---: | ---: |
| directUpdate | avgt | ns/op | 0.510 | 0.515 | -0.89% |
| observedUpdate | avgt | ns/op | 238.705 | 244.073 | -2.25% |
| protectedUpdate | avgt | ns/op | 0.955 | 0.953 | +0.14% |
| renderSelectHotPlanHit | thrpt | ops/ms | 970.442 | 963.142 | -0.75% |
| renderSelectWithoutPlanCache | thrpt | ops/ms | 292.187 | 299.756 | +2.59% |
| renderWhere | thrpt | ops/ms | 690.211 | 714.473 | +3.52% |

## 延迟分位值

| 场景 | 基线 P50 | flying P50 | 基线 P95 | flying P95 | 基线 P99 | flying P99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| directUpdate | 0.510 | 0.514 | 0.510 | 0.521 | 0.510 | 0.521 |
| observedUpdate | 238.530 | 243.341 | 239.130 | 247.847 | 239.130 | 247.847 |
| protectedUpdate | 0.956 | 0.953 | 0.959 | 0.957 | 0.959 | 0.957 |
| renderSelectHotPlanHit | 969.528 | 966.910 | 976.423 | 968.291 | 976.423 | 968.291 |
| renderSelectWithoutPlanCache | 293.720 | 300.022 | 294.151 | 300.378 | 294.151 | 300.378 |
| renderWhere | 690.571 | 710.931 | 692.212 | 721.290 | 692.212 | 721.290 |

> 只有两边在同一机器、JDK、JVM 参数和 JMH 配置下运行，结果才可用于性能结论。
