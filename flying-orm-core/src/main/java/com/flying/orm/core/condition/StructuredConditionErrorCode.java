package com.flying.orm.core.condition;

/**
 * 前端结构化条件编译失败时的原因码，上层可以拿它做提示或业务分支。
 *
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
public enum StructuredConditionErrorCode {

    /** 条件树嵌套超过安全策略允许的最大层级。 */
    DEPTH_EXCEEDED,

    /** 一次请求里的条件节点总数超过安全上限。 */
    NODE_COUNT_EXCEEDED,

    /** 节点同时声明了互斥属性，或者缺少成为条件或分组所需的信息。 */
    INVALID_NODE_SHAPE,

    /** 字段不存在、没有放行，或者属于租户与逻辑删除等受保护字段。 */
    FIELD_NOT_ALLOWED,

    /** 操作符没有注册，或者不在当前安全策略的白名单中。 */
    OPERATOR_NOT_ALLOWED,

    /** 操作符本身可用，但不能用于当前字段。 */
    FIELD_OPERATOR_NOT_ALLOWED,

    /** AND/OR 分组里没有任何有效子条件。 */
    EMPTY_GROUP,

    /** 分组使用了安全策略不允许的逻辑关系。 */
    LOGIC_NOT_ALLOWED,

    /** 当前操作符要求有值，但收到的是 {@code null}。 */
    VALUE_NULL,

    /** 当前操作符要求有效文本，但清理前后空白后没有内容。 */
    VALUE_BLANK,

    /** 文本值长度超过安全策略允许的上限。 */
    VALUE_TOO_LONG,

    /** 值应该是标量、集合或区间中的另一种形态。 */
    VALUE_SHAPE_NOT_ALLOWED,

    /** 集合条件没有任何有效元素。 */
    VALUE_COLLECTION_EMPTY,

    /** 集合元素数量超过安全策略允许的上限。 */
    VALUE_COLLECTION_TOO_LARGE,

    /** 区间条件没有且只有两个边界值。 */
    VALUE_RANGE_SIZE_INVALID,

    /** 区间的两个边界无法转换成同一种字段类型。 */
    VALUE_RANGE_TYPE_MISMATCH,

    /** 区间起始值大于结束值。 */
    VALUE_RANGE_ORDER_INVALID,

    /** 输入值的 Java 形态与字段或操作符要求不一致。 */
    VALUE_TYPE_MISMATCH,

    /** 输入值形态正确，但无法转换成字段声明的逻辑类型。 */
    VALUE_CONVERSION_FAILED
}
