package com.flying.orm.core.join;

import java.util.Objects;

/**
 * 指向某个 JOIN 数据源已注册字段的引用。
 *
 * @param source 字段所属数据源
 * @param field 字段规范名称
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
public record JoinFieldRef(JoinSource source, String field) {

    /** 查找并保存表单发布的规范字段名。 */
    public JoinFieldRef {
        source = Objects.requireNonNull(source, "join field source must not be null");
        field = source.form().field(field).name();
    }
}
