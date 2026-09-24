package com.flying.orm.core.join;

import com.flying.orm.core.sql.render.SqlIdentifiers;

import java.util.Objects;

/**
 * JOIN 查询的显式投影。
 *
 * @param field 投影字段
 * @param alias 唯一结果列别名
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
public record JoinProjection(JoinFieldRef field, String alias) {

    /** 仅用于自动生成别名；显式别名的长度由目标数据库决定。 */
    static final int MAX_PORTABLE_ALIAS_LENGTH = 30;

    /** 校验字段和普通结果别名，不对开发者指定的别名设置跨库长度上限。 */
    public JoinProjection {
        field = Objects.requireNonNull(field, "join projection field must not be null");
        alias = SqlIdentifiers.requireIdentifier(alias, "join projection alias");
        if (alias.indexOf('.') >= 0) {
            throw new IllegalArgumentException("join projection alias must be a plain identifier");
        }
    }
}
