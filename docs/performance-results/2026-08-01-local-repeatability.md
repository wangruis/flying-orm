# flying-orm 无数据库基准对比

- flying-orm 结果：`target\benchmark-results\local-run-b.json`
- 基线结果：`target\benchmark-results\local-run-a.json`
- 改善率：吞吐模式数值越高越好，耗时模式数值越低越好。

| 场景 | 模式 | 单位 | 基线 | flying-orm | 改善率 |
| --- | --- | --- | ---: | ---: | ---: |
| compileInsertPlan | thrpt | ops/ms | 326.131 | 324.835 | -0.40% |
| compileStructuredConditions | thrpt | ops/ms | 582.853 | 562.919 | -3.42% |
| compileUpsertPlan | thrpt | ops/ms | 252.478 | 258.765 | +2.49% |
| directUpdate | avgt | ns/op | 0.531 | 0.518 | +2.40% |
| hotFormRead | thrpt | ops/s | 10955738.882 | 10982802.358 | +0.25% |
| manyTableRead | thrpt | ops/s | 6939043.900 | 6722655.826 | -3.12% |
| mapBatchRows | thrpt | ops/ms | 17.397 | 17.123 | -1.57% |
| mapBean | thrpt | ops/ms | 3685.048 | 3713.269 | +0.77% |
| mapRecord | thrpt | ops/ms | 2922.450 | 2978.976 | +1.93% |
| mapUpsertRows | thrpt | ops/ms | 17.899 | 16.532 | -7.64% |
| observedUpdate | avgt | ns/op | 205.156 | 205.402 | -0.12% |
| protectedUpdate | avgt | ns/op | 0.967 | 0.964 | +0.30% |
| readBeanValues | thrpt | ops/ms | 21088.153 | 22770.566 | +7.98% |
| readRecordValues | thrpt | ops/ms | 21706.186 | 23089.111 | +6.37% |
| readWithInvalidation | thrpt | ops/s | 3031519.270 | 3049033.011 | +0.58% |
| renderWhere | thrpt | ops/ms | 727.176 | 688.832 | -5.27% |

## 延迟分位值

| 场景 | 基线 P50 | flying P50 | 基线 P95 | flying P95 | 基线 P99 | flying P99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| compileInsertPlan | 326.131 | 324.835 | 326.208 | 325.713 | 326.208 | 325.713 |
| compileStructuredConditions | 582.853 | 562.919 | 583.219 | 562.932 | 583.219 | 562.932 |
| compileUpsertPlan | 252.478 | 258.765 | 252.544 | 260.286 | 252.544 | 260.286 |
| directUpdate | 0.531 | 0.518 | 0.540 | 0.528 | 0.540 | 0.528 |
| hotFormRead | 10955738.882 | 10982802.358 | 10997813.557 | 11046751.587 | 10997813.557 | 11046751.587 |
| manyTableRead | 6939043.900 | 6722655.826 | 6958569.914 | 6773076.542 | 6958569.914 | 6773076.542 |
| mapBatchRows | 17.397 | 17.123 | 17.419 | 17.232 | 17.419 | 17.232 |
| mapBean | 3685.048 | 3713.269 | 3690.726 | 3719.078 | 3690.726 | 3719.078 |
| mapRecord | 2922.450 | 2978.976 | 2950.168 | 2979.036 | 2950.168 | 2979.036 |
| mapUpsertRows | 17.899 | 16.532 | 17.985 | 16.654 | 17.985 | 16.654 |
| observedUpdate | 205.156 | 205.402 | 205.352 | 205.733 | 205.352 | 205.733 |
| protectedUpdate | 0.967 | 0.964 | 0.979 | 0.975 | 0.979 | 0.975 |
| readBeanValues | 21088.153 | 22770.566 | 21318.481 | 23000.088 | 21318.481 | 23000.088 |
| readRecordValues | 21706.186 | 23089.111 | 21761.548 | 23124.029 | 21761.548 | 23124.029 |
| readWithInvalidation | 3031519.270 | 3049033.011 | 3034690.428 | 3066149.614 | 3034690.428 | 3066149.614 |
| renderWhere | 727.176 | 688.832 | 731.550 | 695.434 | 731.550 | 695.434 |

> 只有两边在同一机器、JDK、JVM 参数和 JMH 配置下运行，结果才可用于性能结论。
