package com.flying.orm.rdb.schema;

/** 上线审核使用的结构迁移风险级别，级别越高，越需要维护窗口、备份或人工确认。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public enum SchemaMigrationRiskLevel {
    /** 只有可直接执行、可完整回滚的普通结构调整。 */
    LOW,
    /** 计划里有跳过项，需要发布人员确认目标结构是否符合预期。 */
    MEDIUM,
    /** 可能长时间锁表，或者回滚仍需要人工处理索引等数据库对象。 */
    HIGH,
    /** 可能丢数据，或者涉及主键、外键等无法自动保证恢复正确性的变更。 */
    CRITICAL
}
