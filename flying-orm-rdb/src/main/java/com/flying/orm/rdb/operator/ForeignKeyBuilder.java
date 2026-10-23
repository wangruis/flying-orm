package com.flying.orm.rdb.operator;

import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;

/**
 * 外键 builder 只描述目标关系，是否真的迁移交给上层看计划后决定。
 *
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
public final class ForeignKeyBuilder<T> {

    private final T table;
    private final DdlStructureDraft draft;
    private final ForeignKeyDefinition.Builder foreignKey;

    ForeignKeyBuilder(T table, DdlStructureDraft draft, String name) {
        this.table = table;
        this.draft = draft;
        this.foreignKey = ForeignKeyDefinition.builder(DdlStructureDraft.requireText(name, "foreign key name"));
    }

    /**
     * 按顺序追加当前表的一个外键列。
     *
     * @param name 当前表列名
     * @return 当前外键构建器
     */
    public ForeignKeyBuilder<T> column(String name) {
        foreignKey.addColumn(DdlStructureDraft.requireText(name, "foreign key column"));
        return this;
    }

    /**
     * 一次追加多个当前表列，顺序必须与引用列一一对应。
     *
     * @param first 第一列
     * @param rest 其余列
     * @return 当前外键构建器
     */
    public ForeignKeyBuilder<T> columns(String first, String... rest) {
        column(first);
        for (String column : rest) {
            column(column);
        }
        return this;
    }

    /**
     * 设置被引用的物理表。
     *
     * @param name 引用表名
     * @return 当前外键构建器
     */
    public ForeignKeyBuilder<T> referenceTable(String name) {
        foreignKey.reference(RelationIdentity.table(name));
        return this;
    }

    /**
     * 按顺序追加一个引用列。
     *
     * @param name 引用表列名
     * @return 当前外键构建器
     */
    public ForeignKeyBuilder<T> referenceColumn(String name) {
        foreignKey.addReferenceColumn(DdlStructureDraft.requireText(name, "foreign key reference column"));
        return this;
    }

    /**
     * 一次追加多个引用列，数量和顺序必须与当前表外键列一致。
     *
     * @param first 第一列
     * @param rest 其余列
     * @return 当前外键构建器
     */
    public ForeignKeyBuilder<T> referenceColumns(String first, String... rest) {
        referenceColumn(first);
        for (String column : rest) {
            referenceColumn(column);
        }
        return this;
    }

    /**
     * 校验并把外键加入表级迁移目标。真正是否执行由迁移安全计划决定。
     *
     * @return 所属表构建器
     */
    public T commit() {
        draft.addForeignKey(foreignKey.build());
        return table;
    }
}
