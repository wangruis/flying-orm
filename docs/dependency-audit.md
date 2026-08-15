# 依赖漏洞与许可证审计

依赖审计使用显式 Maven Profile，不影响日常编译和测试。正式候选版必须在能访问漏洞数据源的 CI 或发布环境执行：

```bash
mvn -Paudit verify
```

该命令会执行两项检查：

- OWASP Dependency-Check 聚合扫描运行时依赖，CVSS 达到 `7.0` 时构建失败。
- License Maven Plugin 生成所有模块的第三方许可证报告。

输出默认放在根项目 `target` 目录：

- `target/dependency-check-report.html`
- `target/dependency-check-report.json`
- `target/reports/licenses/aggregate-third-party-report.html`

## 数据源要求

Dependency-Check 第一次运行需要下载并建立漏洞数据库，耗时可能较长。企业 CI 应配置 NVD API Key 或内部镜像，并缓存插件数据目录。扫描服务不可用、数据更新失败或报告没有生成时，本轮审计算失败，不能按“未发现漏洞”处理。

## 范围

漏洞门禁只扫描发布物会携带的 compile/runtime 依赖，测试驱动和 JUnit 不进入漏洞门禁。许可证报告保留测试依赖，方便研发侧完整追踪，但排除 `com.flying.orm` 自身模块。主项目采用什么开源许可证属于发布决策，不能由第三方依赖报告代替。

## 最近一次入口验证

2026-08-01 使用 `mvn -Paudit -Ddependency-check.skip=true -DskipTests verify` 验证 Profile 和许可证聚合报告，构建成功，报告没有未知第三方许可证。该命令明确跳过了漏洞数据库，因此不算漏洞扫描通过；正式候选版仍必须执行不带 skip 的完整命令。

同日尝试执行不带 skip 的完整扫描，首次漏洞库同步在本地限定等待窗口内没有完成，因此主动终止，没有生成漏洞报告。发布 CI 需要先配置 NVD Key 或内部镜像与缓存，再重新执行并归档结果。
