package com.flying.orm.core.join;

import com.flying.orm.core.form.DynamicForm;

import java.util.Objects;

/**
 * JOIN 查询中的稳定数据源。
 *
 * <p>序号由查询构建器按加入顺序分配，也是内部 SQL 别名和默认结果别名的稳定来源。
 * 业务 API 不允许调用方直接提供 SQL 别名。</p>
 *
 * @param ordinal 数据源序号，根源固定为零
 * @param form 数据源对应的动态表单
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
public record JoinSource(int ordinal, DynamicForm form) {

    /** 完成序号和表单的基础校验。 */
    public JoinSource {
        if (ordinal < 0) {
            throw new IllegalArgumentException("join source ordinal must not be negative");
        }
        form = Objects.requireNonNull(form, "join source form must not be null");
    }
}
