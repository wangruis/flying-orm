# flying-orm 无数据库基准对比

- flying-orm 结果：`docs\performance-results\2026-08-07-v101-phase9-batch-plan-baseline-compatible.json`
- 基线结果：`docs\performance-results\2026-08-06-v101-phase2-batch-plan.json`
- 改善率：吞吐模式数值越高越好，耗时模式数值越低越好。

| 场景 | 模式 | 单位 | 基线 | flying-orm | 改善率 |
| --- | --- | --- | ---: | ---: | ---: |
| compileInsertPlan | thrpt | ops/ms | 271.581 | 278.825 | +2.67% |
| compileUpsertPlan | thrpt | ops/ms | 215.846 | 204.040 | -5.47% |
| mapBatchRows | thrpt | ops/ms | 16.105 | 18.027 | +11.94% |
| mapUpsertRows | thrpt | ops/ms | 16.684 | 16.130 | -3.32% |

## 延迟分位值

| 场景 | 基线 P50 | flying P50 | 基线 P95 | flying P95 | 基线 P99 | flying P99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| compileInsertPlan | 271.581 | 278.825 | 274.362 | 281.430 | 274.362 | 281.430 |
| compileUpsertPlan | 215.846 | 204.040 | 217.027 | 205.603 | 217.027 | 205.603 |
| mapBatchRows | 16.105 | 18.027 | 16.175 | 18.121 | 16.175 | 18.121 |
| mapUpsertRows | 16.684 | 16.130 | 17.169 | 16.252 | 17.169 | 16.252 |

> 只有两边在同一机器、JDK、JVM 参数和 JMH 配置下运行，结果才可用于性能结论。
