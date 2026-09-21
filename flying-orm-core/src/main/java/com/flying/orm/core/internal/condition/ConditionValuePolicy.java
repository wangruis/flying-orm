package com.flying.orm.core.internal.condition;

/**
 * 条件值清理后为空时，是忽略这个条件，还是直接告诉调用方输入有问题。
 *
 * @author wangr
 * @date 2026-07-31
 * @version v1.0
 */
public enum ConditionValuePolicy {
    /** 清理后没有有效值就不生成这个条件，适合搜索表单的可选项。 */
    IGNORE_EMPTY,

    /** 清理后没有有效值立即报错，避免 update/delete 因条件丢失而扩大范围。 */
    REJECT_EMPTY
}
