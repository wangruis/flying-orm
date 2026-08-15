# 受保护 CONTAINS 搜索、事务维护与数据迁移设计

## 状态

- 设计状态：已确认
- 日期：2026-08-09
- 前置设计：`2026-08-09-protected-fields-design.md`

## 目标

在不解密数据库全表、不使用确定性密文、不向数据库传递主密钥的前提下，为显式启用的保护字段提供有界
`contains` 搜索，并保证基础行、精确/后缀盲索引和 contains 令牌在 JDBC/R2DBC、普通写入和批量写入中的事务一致性。

同时提供显式的历史明文迁移与密钥轮换能力，不允许 Schema 同步静默把已有明文改成密文。

## 令牌模型

归一化文本按 Unicode code point 生成连续 trigram。每个 trigram 使用字段、租户和 contains 用途子密钥计算
HMAC-SHA-256。数据库只保存 32 字节令牌，不保存明文片段。

- 默认最小查询长度为 3 个 code point。
- 字段可以在定义中提高最小长度，但不能低于 3。
- 重复 trigram 在同一记录中去重，避免无意义令牌膨胀。
- 最大规范化长度、单字段最大令牌数和单行全部保护字段令牌数都有固定上限。

## 辅助表

每张业务表最多创建一个 contains 令牌表，包含：

- `field_tag`：受控的短字段标识，不是调用方原字段名。
- `token_hash`：32 字节 HMAC。
- 业务表全部主键列。

索引：

1. 查询索引：`(field_tag, token_hash, owner_pk...)`。
2. 唯一索引：`(owner_pk..., field_tag, token_hash)`。

辅助表和索引名称使用稳定哈希命名并限制在 30 个 ASCII 字符。业务表有复合主键时复制全部主键列。没有主键、主键
类型无法安全映射或实体无法取得数据库生成键时，启动阶段拒绝启用 CONTAINS。

辅助表使用到业务表主键的级联删除外键；物理删除即使来自受控原生 SQL，也不会留下孤儿。逻辑删除不移除令牌，
因为基础行查询仍受逻辑删除 Scope 约束；最终物理删除由外键清理。

## 候选查询与复核

数据库候选查询按全部 trigram 取交集：

```sql
select owner_pk...
from protected_token_table
where field_tag = ? and token_hash in (?, ...)
group by owner_pk...
having count(distinct token_hash) = ?
```

轮换期为 current 与 readable 密钥版本生成令牌集合。查询计划必须按版本分组，不能把不同密钥版本的局部匹配拼成一个
错误候选。

候选返回后：

1. 按 owner PK 有界读取密文字段。
2. 解密并执行同一归一化器。
3. 在应用内验证真实 substring。
4. 只把验证通过的 owner PK 交给最终业务查询。

HMAC 碰撞概率极低，但 trigram 集合仍可能出现排列导致的 false positive，因此复核不可省略。

## 容量和分页

- 默认最大候选数 1000，可在框架无关高级配置中降低或提高到受控硬上限。
- 超过候选上限直接返回稳定 `PROTECTED_SEARCH_CANDIDATE_LIMIT`，不能截断后伪装完整结果。
- 响应式链路按背压读取和解密候选；需要精确 count/page 时只收集最多候选上限内的主键。
- 最终 count、offset page 和 cursor page 都以复核后的主键集合为准。
- 查询超时覆盖候选 SQL、密文读取、解密复核和最终业务查询的整个预算。

第一版 CONTAINS 只支持单 DynamicForm/实体查询。JOIN 查询明确拒绝 CONTAINS，直到有能证明外连接与分页精确性的独立设计。

## 事务工作单元

现有 `SqlExecutionSequence` 只保证同连接顺序执行，不作为字段保护事务。rdb 新增包内聚焦事务工作单元：

- JDBC 使用当前 `JdbcTransactionParticipant` 和 `JdbcConnectionProvider`。
- R2DBC 使用当前 `R2dbcTransactionParticipant`、连接生命周期和 `usingWhen` 清理规则。
- 外部事务存在时只复用连接，不 begin、commit、rollback 或 close 外部连接。
- 无外部事务时由工作单元 begin；全部 SQL 成功后 commit；任一步失败先确认 rollback。
- commit/rollback 回执不确定时返回现有 UNKNOWN 分类并隔离连接。
- RuntimeException、Error、VirtualMachineError、取消和超时不能绕过回滚与资源清理。

工作单元不是新的公开事务 API，只服务需要多条 SQL 原子提交的保护写入和迁移。

## 写入流程

### Insert

1. 生成业务主键或执行基础 insert 并取得数据库生成键。
2. 在同一连接和事务写入基础密文与 EXACT/SUFFIX 隐藏列。
3. 批量插入去重后的 contains 令牌。
4. 全部成功后提交或等待外部事务完成。

### Update/Upsert

1. 使用现有 Scope、逻辑删除和乐观锁执行基础写入。
2. 影响行数为 0 时按现有乐观锁/条件失败处理，不能改令牌。
3. 删除该 owner/field 的旧令牌。
4. 插入新令牌。
5. 基础写入与令牌维护任一步失败都回滚。

### Delete

- 物理删除依赖级联外键清理令牌。
- 逻辑删除保留令牌，但后续搜索的基础行阶段必须应用逻辑删除 Scope。

### Batch

- ATOMIC：整个批次的基础行与令牌在一个事务中提交或回滚。
- INDEPENDENT：每个 chunk 的基础行与令牌原子提交；结果继续保留 COMMITTED、FAILED、PARTIAL、UNKNOWN。
- 输入仍按有界 chunk 消费，不提前收集完整 Publisher。
- 单个业务行对应的令牌数组在收到时立即快照，不能保留调用方可变容器。

## 历史数据迁移

Schema 同步只创建经审核的新列、索引和辅助表，不自动读取或覆盖历史明文。迁移使用独立、可恢复的程序化任务：

1. 审核并创建目标密文列、隐藏索引列和辅助表。
2. 按稳定主键游标分片读取历史行。
3. 在事务内加密、写盲索引和令牌，并记录迁移版本。
4. 重读抽样或按摘要验证，不记录明文。
5. 切换读取到密文列。
6. 经独立危险 DDL 批准后删除旧明文列或完成列交换。

迁移任务必须幂等：已经使用目标协议和密钥版本的行直接跳过；部分失败从最后确认游标继续。迁移进度、失败分类和行数
进入稳定结果对象，不复制底层异常消息。

## 密钥轮换

- 部署新 key ring 后，新写入立即使用 current。
- 读取按密文信封版本选择 readable 密钥。
- EXACT/SUFFIX/CONTAINS 查询同时计算全部 readable 版本令牌。
- 后台 `reprotect` 任务按主键游标解密旧版本并用 current 重写密文、盲索引和令牌。
- 全量完成并验证后，上层才从 key ring 移除旧版本。
- 删除仍被数据引用的旧密钥会导致明确启动或运行期错误，不能回退到其他密钥猜测解密。

## 故障与安全测试

故障注入点至少覆盖：

1. 基础 SQL 前后失败。
2. 数据库生成键取得失败。
3. 删除旧令牌后失败。
4. 插入部分新令牌后失败。
5. commit 回执丢失。
6. rollback RuntimeException/Error/VME。
7. 外部事务最终 rollback。
8. 响应式取消和慢消费者。
9. JDBC 线程中断和 Statement cancel。
10. 迁移中途进程终止后的幂等恢复。

所有失败都要证明基础行和辅助令牌没有可观察的不一致，或明确报告 UNKNOWN 并提供恢复入口。

## 性能与认证

- JMH 覆盖 AES-GCM、HKDF、EXACT/SUFFIX HMAC、trigram 生成和结果脱敏的吞吐与分配。
- 真实数据库记录候选查询 P95/P99、索引命中、令牌数量、吞吐、超时、错误和连接泄漏。
- MySQL 8.4、PostgreSQL、Oracle Free 23、SQL Server 2022 分别验证 JDBC/R2DBC。
- 大密文字段必须覆盖 Oracle R2DBC LOB 慢消费者取消和资源释放。
- 最终认证连续运行三轮；任何一轮失败都不能用另外两轮绿色覆盖。

## 非目标

- 不让数据库执行解密或持有主密钥。
- 不支持任意 `%pattern%` 自动解析。
- 不保证盲索引隐藏同一字段/租户内部的相等频率。
- 不在第一版支持 CONTAINS + JOIN、任意短字符串 contains 或无界候选扫描。
- 不把迁移脚本、真实密钥或 Docker 凭据提交到源码仓库。
