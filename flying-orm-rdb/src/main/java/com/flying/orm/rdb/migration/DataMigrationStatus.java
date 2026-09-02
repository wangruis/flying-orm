package com.flying.orm.rdb.migration;

/** 数据迁移最终状态。回滚失败必须单独暴露，不能伪装成已经恢复。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public enum DataMigrationStatus {
    SUCCEEDED,
    ROLLED_BACK,
    ROLLBACK_FAILED,
    /** 当前正向步骤可能已经生效，但执行器没有收到可确认结果；必须人工核对。 */
    OUTCOME_UNKNOWN
}
