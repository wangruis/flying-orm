package com.flying.orm.core.metadata;

import com.flying.orm.core.internal.Names;

import java.util.Objects;

/**
 * 命名 CHECK 约束。谓词必须来自 {@link CheckPredicate} 的封闭结构，不能携带原始 SQL。
 *
 * @param name 约束名
 * @param predicate 结构化约束谓词
 * @author wangr
 * @version v3.2
 */
public record CheckConstraintDefinition(String name, CheckPredicate predicate) {

    public CheckConstraintDefinition {
        name = Names.requireText(name, "check constraint name");
        predicate = Objects.requireNonNull(predicate, "check constraint predicate must not be null");
    }

    public static CheckConstraintDefinition of(String name, CheckPredicate predicate) {
        return new CheckConstraintDefinition(name, predicate);
    }
}
