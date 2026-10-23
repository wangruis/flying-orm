package com.flying.orm.core.sql.render;

import com.flying.orm.core.condition.TermCondition;

import java.util.function.UnaryOperator;

/**
 * 关联条件的扩展入口。外层字段解析器由查询所有者提供，保留真实表名、JOIN 别名或聚合表达式。
 * 实现应复用参数化查询，不把条件值拼接到 SQL 中。
 *
 * @author wangr
 * @version v4.1.2
 */
public interface CorrelatedSqlTermHandler extends SqlTermHandler {

    /** 渲染关联条件；两个解析器分别返回外层字段表达式及其所属关系限定符。 */
    SqlFragment render(TermCondition term, SqlRenderContext context,
                       UnaryOperator<String> outerFields, UnaryOperator<String> outerQualifiers);

    /** 没有外层归属时不能把未限定列误解析成子查询自己的列。 */
    @Override
    default SqlFragment render(TermCondition term, SqlRenderContext context) {
        throw new IllegalArgumentException("correlated query term requires a qualified outer query context");
    }
}
