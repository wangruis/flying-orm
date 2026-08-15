# 字段加密、保护搜索与脱敏实施计划

> 实施状态（2026-08-10）：显式元数据、密钥环、密码协议、EXACT/SUFFIX/CONTAINS、通用脱敏、单条/批量
> JDBC/R2DBC 原子维护、历史明文迁移与密钥轮换协作者、公共 API 和文档均已完成，并通过质量/发布门禁。
> MySQL/PostgreSQL/Oracle/SQL Server 的 V2.0.0 新能力实库认证已经完成，证据见 `docs/real-database-certification.md`。

> **执行要求：** 使用 `superpowers:executing-plans` 逐项实施；每个生产改动必须先有可观察的 RED。

**目标：** 只有实体注解或 `DynamicForm` 显式声明的字段才启用加解密、保护搜索和业务结果脱敏；上层只提供版本化 32 字节主密钥环。

**架构：** core 保存注解和不可变保护元数据；rdb 保存 JCA 密码协议、字段转换、隐藏索引列、查询改写和 JDBC/R2DBC 事务编排。实体注解与 DynamicForm 最终编译成同一 `FieldProtectionRegistry`，不建立两套执行语义。

## 阶段 1：保护元数据与显式启用

- [ ] 新增 `EncryptedSearchMode`、`SensitiveDisplayMode`、`EncryptedFieldDefinition`、`MaskedFieldDefinition` 和 `FieldProtectionRegistry`。
- [ ] 新增 `@EncryptedField`、`@MaskedField`；未标注字段必须保持当前语义。
- [ ] 为 `DynamicForm.Builder` 增加 `encrypted(...)`、`masked(...)`，并在 build 时验证字段存在、定义唯一且不可变。
- [ ] 实体元数据编译器把注解翻译成同一 registry；覆盖继承字段、物理列名和未持久化字段边界。
- [ ] 运行 core 直接契约、实体映射契约和公共 API 快照。

## 阶段 2：主密钥环与密码协议

- [ ] 先写密钥长度、版本格式、最多四个 readable 版本、防御复制和关闭清零 RED。
- [ ] 实现 `ProtectedFieldKeyRing`，上层只传主密钥；不读取环境变量、不连接 KMS/Vault/HSM。
- [ ] 用 RFC 5869 向量锁定 HKDF-SHA-256；按 encryption/exact/suffix/contains、表单、字段、租户和长度派生用途隔离子密钥。
- [ ] 实现有界版本信封与 AES-256-GCM（96-bit 随机 nonce、128-bit tag）；篡改、未知版本和损坏统一稳定错误。

## 阶段 3：EXACT、SUFFIX 与读写转换

- [ ] 实现 `identity`、`case-fold`、`digits` 规范化器注册表及自定义纯函数校验。
- [ ] 写入链按“业务 codec → 规范化 → HMAC 索引 → GCM 密文”处理；读取链按相反方向解密。
- [ ] 为 current 与 readable 版本生成 EXACT/SUFFIX 查询 token，保持稳定参数顺序；未声明模式和非法后缀在 SQL 前拒绝。
- [ ] 统一接入普通 CRUD、Repository、批量、JDBC 与 R2DBC；加密字段拒绝排序、范围、聚合和 JOIN ON。
- [ ] 隐藏列使用 30 ASCII 字符内稳定名称，并按四库方言渲染受保护 binary/HMAC 类型。

## 阶段 4：业务脱敏与展示覆盖

- [ ] 实现 `MaskingPolicyRegistry` 和通用 `full`、`partial`、`email`、`person-name`、`address`、`bank-card` 策略。
- [ ] 默认优先级固定为“查询设置 > 字段声明 > MASKED”；提供 `declaredDisplay()`、`masked()`、`showSensitive()`。
- [ ] 只对声明 `masked` 的字段改变业务结果；FULL 仍不得进入 SQL 日志、异常、观测和恢复结果。
- [ ] `DynamicRow` 使用稀疏值替换，不为每行主动物化 Map；实体/DTO 映射复用同一结果转换器。

## 阶段 5：CONTAINS 与原子事务维护

- [ ] 实现 Unicode code point trigram、去重、单值/单行 token 上限和版本分组查询。
- [ ] 为每张业务表生成一个辅助 token 表；要求稳定主键，建立候选和唯一索引以及级联删除外键。
- [ ] 候选查询后必须解密复核真实 substring；超过候选上限稳定失败，不能截断伪装完整结果。
- [ ] 新增包内 JDBC/R2DBC 保护写入工作单元，正确参与外部事务；自有事务覆盖失败、取消、超时、UNKNOWN 与连接隔离。
- [ ] 第一版 JOIN 明确拒绝 CONTAINS；EXACT/SUFFIX 只允许 WHERE，不允许 JOIN ON。

## 阶段 6：迁移、轮换、文档与认证

- [ ] 提供显式、幂等、可恢复的历史明文迁移和 `reprotect` 轮换任务；Schema 同步不得静默覆盖历史数据。
- [ ] 更新 README、需求索引、公共 API 基线、错误码、数据库矩阵和已知限制。
- [ ] 运行聚焦测试、`mvn -Pquality clean verify`、`mvn -Prelease-artifacts verify`。
- [ ] MySQL 8.4、PostgreSQL、Oracle Free 23、SQL Server 2022 的 JDBC/R2DBC 认证连续三轮；记录版本、驱动、用例、性能和资源泄漏证据。
