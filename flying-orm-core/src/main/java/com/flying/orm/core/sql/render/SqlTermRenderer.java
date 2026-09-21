package com.flying.orm.core.sql.render;

import com.flying.orm.core.condition.TermCondition;

/**
 * SQL term 渲染函数，将结构化条件 term 转换为参数化 SQL 片段。
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
@FunctionalInterface
public interface SqlTermRenderer {

    /**
     * 渲染 term 条件。返回片段作为一个完整布尔操作数组合，由 SqlRenderer 添加整体括号；
     * 扩展可以返回包含 AND/OR 的参数化条件，无需自行处理外层条件的优先级。
     *
     * @param term    term 条件
     * @param context SQL 渲染上下文
     * @return 参数化 SQL 片段
     */
    SqlFragment render(TermCondition term, SqlRenderContext context);
}
