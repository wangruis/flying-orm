package com.flying.orm.rdb.observation;

/**
 * 批量观测事件的类型。批量写入不是只有“成功/失败”，分片和 UNKNOWN 恢复也要单独看。
 *
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
public enum BatchExecutionEventType {

    /** 单个分片已经产出结果。 */
    CHUNK,
    /** 整次批量写入已经汇总完成。 */
    SUMMARY,
    /** UNKNOWN 恢复令牌已经查询出一个确认结果。 */
    RECOVERY
}
