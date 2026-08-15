# 2026-08-01 本机无数据库短基线

这次短跑只确认三件事：现有 JMH 能完整执行、JSON 能归档比较、同一提交在同机短跑时大概会抖多少。
它不是正式 RC 性能结论，也没有连接任何数据库。

## 环境

| 项目 | 值 |
| --- | --- |
| Git commit | `7f086e67306f547041fd31bc9af9da216d41122c` |
| JDK | Oracle JDK 21.0.10，HotSpot 64-Bit Server VM |
| Maven | 3.9.16 |
| JMH | 1.37 |
| OS | Windows 10 Education 22H2，build 19045，AMD64 |
| CPU | Intel Core i5-14600K，20 个逻辑处理器 |
| JVM 参数 | 未额外设置 |
| JMH 参数 | 1 fork，1 thread，1 次 1 秒预热，2 次 1 秒测量 |

运行了两轮完全相同的 16 个场景。原始结果和比较报告保存在：

- [第一轮 JSON](performance-results/2026-08-01-local-run-a.json)
- [第二轮 JSON](performance-results/2026-08-01-local-run-b.json)
- [两轮比较报告](performance-results/2026-08-01-local-repeatability.md)

## 结果

吞吐模式的差异为“第二轮相对第一轮吞吐变化”，平均耗时模式的差异为“第二轮相对第一轮耗时改善”。
正数更好，负数更慢。

| 场景 | 模式 | 第一轮 | 第二轮 | 差异 |
| --- | --- | ---: | ---: | ---: |
| compileInsertPlan | ops/ms | 326.131 | 324.835 | -0.40% |
| compileUpsertPlan | ops/ms | 252.478 | 258.765 | +2.49% |
| mapBatchRows | ops/ms | 17.397 | 17.123 | -1.57% |
| mapUpsertRows | ops/ms | 17.899 | 16.532 | -7.64% |
| mapBean | ops/ms | 3685.048 | 3713.269 | +0.77% |
| mapRecord | ops/ms | 2922.450 | 2978.976 | +1.93% |
| readBeanValues | ops/ms | 21088.153 | 22770.566 | +7.98% |
| readRecordValues | ops/ms | 21706.186 | 23089.111 | +6.37% |
| hotFormRead | ops/s | 10955738.882 | 10982802.358 | +0.25% |
| manyTableRead | ops/s | 6939043.900 | 6722655.826 | -3.12% |
| readWithInvalidation | ops/s | 3031519.270 | 3049033.011 | +0.58% |
| renderWhere | ops/ms | 727.176 | 688.832 | -5.27% |
| compileStructuredConditions | ops/ms | 582.853 | 562.919 | -3.42% |
| observedUpdate | ns/op | 205.156 | 205.402 | -0.12% |

`directUpdate` 和 `protectedUpdate` 分别只有约 0.5 ns/op、1 ns/op，已经低到可能被 JIT 常量折叠，
本轮不把这两个数字纳入基线或回归门禁。比较器报告里的“分位值”对 throughput 场景表示每轮吞吐分布，
不是请求延迟 P95/P99；真正的延迟分位必须使用 `--mode sample` 单独执行。

## 判断

- 两轮 14 个可用场景的变化范围是 `-7.64%` 到 `+7.98%`。
- 1 次预热和 2 次测量的噪声足以跨过 5%，因此单次 5% 变慢只能提醒复跑，不能直接判回归。
- 10% 可以作为短跑的临时硬线；正式 RC 仍按三组配对运行的中位数和方向一致性判断。
- 元数据缓存热读两轮约 1096 万、1098 万 ops/s；这只说明内存热路径可重复，不代表数据库字典读取吞吐。
- 观测包装两轮约 205 ns/op，重复性较好；仍要在正式参数下再定绝对预算。

真实数据库吞吐、P95/P99、连接占用、慢消费者和 ATOMIC/INDEPENDENT 批量压力测试继续放在最后阶段。
