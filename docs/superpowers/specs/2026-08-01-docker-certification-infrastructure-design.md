# Docker 真实数据库认证基础设施设计

## 目标

为 flying-orm 建立一套能在本机 Docker Desktop 上重复运行的真实数据库认证环境。环境负责固定数据库版本、等待服务真正可用、调用现有 R2DBC 兼容测试，并留下不含密码的执行证据。本阶段只准备入口，不提前产生“已经认证”或“性能达标”的结论。

## 边界

- 主项目继续保持零 Spring 依赖，认证工具不进入 ORM 运行时依赖。
- 不修改 `flying-orm-testkit` 和 `flying-orm-benchmark` 的现有代码，只调用它们已经提供的入口。
- H2 继续由模块测试以内嵌方式运行，不额外启动容器。
- MySQL、PostgreSQL 是 V1 正式版必须完成的核心认证组。
- Oracle、SQL Server 是资源更重的预览认证组，能够单独启动和验证。
- 本地认证密码只放在被 Git 忽略的 `.env` 中；报告不得记录密码或完整 R2DBC URL。

## 结构

`certification/docker-compose.yml` 固定四个外部数据库的镜像、端口、卷和健康检查。服务使用 profile 分组，默认执行不会意外启动全部重型数据库。

`certification/Invoke-Certification.ps1` 是唯一人工入口。它提供 `Start`、`Verify`、`Status` 和 `Stop` 四个动作，并支持 `Core`、`Preview`、`All` 或单库选择。`Verify` 会先确保容器健康，再按数据库选择 Maven Profile 和系统属性。

`target/certification-results/<run-id>` 保存本轮环境信息、镜像摘要、Maven 原始输出和 JSON 清单。这个目录由现有 `target/` 忽略规则排除，不会误提交凭据或本机噪声。

## 数据流

1. 脚本检查 Java、Maven、Docker 和 Compose 是否可用。
2. 首次运行从 `.env.example` 生成本机 `.env`，使用方可以在启动前修改端口和本地密码。
3. Compose 启动所选数据库并等待健康检查通过。
4. 脚本根据同一份 `.env` 生成 R2DBC URL，只把 URL 作为 Maven 系统属性传给现有外部兼容测试。
5. 测试结束后写入脱敏清单。失败也保留证据并返回非零退出码。

## 认证口径

容器健康只证明数据库可以接受连接；兼容 smoke 通过只证明当前测试场景通过。正式认证还必须补齐故障、事务、并发、资源释放和性能批次，并记录镜像摘要、数据库版本、驱动版本、Git 提交和执行时间。

同一数据库的功能、并发和性能结果分别记录。任何非预期异常、连接泄漏、UNKNOWN 无法恢复或结果缺少环境信息，都不能标记为认证通过。

## 失败处理

- Docker 不可用、环境文件缺项或容器不健康时，在调用 Maven 前失败。
- Maven 返回非零退出码时保留日志和失败清单，再把非零退出码返回给调用方。
- `Stop` 默认保留数据卷，便于复查；只有显式传 `-RemoveVolumes` 才删除认证数据。
- 报告只记录数据库选择、镜像、版本、提交和结果，不记录密码或完整连接串。
