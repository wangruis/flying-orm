package com.flying.orm.rdb.aggregate;

import com.flying.orm.core.field.FieldIdentity;

/**
 * 聚合结果中一项分组字段及其业务别名。
 *
 * @param field 规范源字段
 * @param alias 结果别名
 * @author wangr
 * @version v3.2
 */
public record GroupSelection(String field, String alias) {

    public GroupSelection {
        field = FieldIdentity.of(field).name();
        alias = FieldIdentity.of(alias).name();
    }

    public static GroupSelection of(String field, String alias) {
        return new GroupSelection(field, alias);
    }
}
