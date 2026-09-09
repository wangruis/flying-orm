package com.flying.orm.core.metadata;

import com.flying.orm.core.internal.Names;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/**
 * 命名唯一约束。复合列的声明顺序会原样保留，供差异比较和 DDL 渲染使用。
 *
 * @param name 约束名
 * @param columns 约束列，顺序与声明一致
 * @param nullPolicy NULL 参与唯一性判断的规则
 * @author wangr
 * @version v3.2
 */
public record UniqueConstraintDefinition(String name, List<String> columns, UniqueNullPolicy nullPolicy) {

    /** 保留普通唯一约束的构造合同，NULL 规则沿用数据库默认值。 */
    public UniqueConstraintDefinition(String name, List<String> columns) {
        this(name, columns, UniqueNullPolicy.DEFAULT);
    }

    public UniqueConstraintDefinition {
        name = Names.requireText(name, "unique constraint name");
        nullPolicy = Objects.requireNonNull(nullPolicy, "unique constraint null policy must not be null");
        if (columns == null) {
            throw new IllegalArgumentException("unique constraint columns must not be null");
        }
        columns = columns.stream()
                .map(column -> Names.requireText(column, "unique constraint column name"))
                .toList();
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("unique constraint columns must not be empty");
        }
        // 同一物理列重复出现不会形成新的复合键语义，只会把非法或不可移植的结构拖到 DDL 执行阶段。
        if (new HashSet<>(columns).size() != columns.size()) {
            throw new IllegalArgumentException("unique constraint columns must not contain duplicates");
        }
    }

    /** 创建命名唯一约束，参数顺序就是复合约束的列顺序。 */
    public static UniqueConstraintDefinition of(String name, String... columns) {
        return new UniqueConstraintDefinition(name, Arrays.asList(columns));
    }
}
