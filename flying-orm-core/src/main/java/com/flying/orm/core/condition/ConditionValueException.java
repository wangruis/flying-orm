package com.flying.orm.core.condition;

import java.util.Objects;

/**
 * 条件值不符合 term 约定时抛出的稳定异常。上层应判断 error，不要解析消息文本。
 *
 * @author wangr
 * @date 2026-07-31
 * @version v1.0
 */
public final class ConditionValueException extends IllegalArgumentException {

    private final Error error;

    public ConditionValueException(Error error, String message) {
        super(message);
        this.error = Objects.requireNonNull(error, "condition value error must not be null");
    }

    public Error error() {
        return error;
    }

    /**
     * 不同入口可以把这些 core 错误映射成自己的公开错误码。
     */
    public enum Error {
        NULL_VALUE,
        BLANK_VALUE,
        COLLECTION_EMPTY,
        /** 集合超过条件入口允许的默认硬上限。 */
        COLLECTION_TOO_LARGE,
        /** 字符串值超过条件入口允许的资源上限。 */
        STRING_TOO_LONG,
        SHAPE_NOT_ALLOWED,
        RANGE_SIZE_INVALID,
        RANGE_TYPE_MISMATCH,
        RANGE_ORDER_INVALID
    }
}
