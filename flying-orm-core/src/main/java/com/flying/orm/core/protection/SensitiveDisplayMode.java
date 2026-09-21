package com.flying.orm.core.protection;

/**
 * 已声明脱敏字段的业务结果展示方式。
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
public enum SensitiveDisplayMode {

    /** 使用字段声明的展示方式。 */
    DECLARED,

    /** 强制使用字段声明的 masking policy。 */
    MASKED,

    /** 向可信后端代码返回完整业务值；日志与观测仍保持隐藏。 */
    FULL
}
