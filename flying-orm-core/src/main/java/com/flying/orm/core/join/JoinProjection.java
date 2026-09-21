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

    static final int MAX_PORTABLE_ALIAS_LENGTH = 30;

    /** 校验字段和跨方言安全的普通结果别名。 */
    public JoinProjection {
        field = Objects.requireNonNull(field, "join projection field must not be null");
        alias = SqlIdentifiers.requireIdentifier(alias, "join projection alias");
        if (alias.indexOf('.') >= 0) {
            throw new IllegalArgumentException("join projection alias must be a plain identifier");
        }
        if (alias.length() > MAX_PORTABLE_ALIAS_LENGTH) {
            throw new IllegalArgumentException("join projection alias exceeds the portable identifier limit");
        }
    }
}
