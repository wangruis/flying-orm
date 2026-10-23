package com.flying.orm.core.condition;

/**
 * 可信服务端的结构化条件值映射；不属于前端可提交的数据。
 *
 * <p>字段授权和操作符别名解析后，编译器在同一次有界遍历中逐个调用本接口。
 * 集合和区间按元素调用，无值条件不调用；返回值仍接受形状、长度和字段类型检查。
 * 实现应无副作用，并能随不可变策略一起被并发复用。</p>
 *
 * @author wangr
 * @date 2026-10-05
 * @version v1.0
 */
@FunctionalInterface
public interface StructuredConditionValueMapper {

    /** 将一个标量映射为持久值；field 是表单字段名，operator 是解析后的内部操作符。 */
    Object map(String field, String operator, Object value);
}
