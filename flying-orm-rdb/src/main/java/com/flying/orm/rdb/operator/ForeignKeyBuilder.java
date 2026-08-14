package com.flying.orm.rdb.operator;

import com.flying.orm.core.metadata.ForeignKeyMetadata;

/**
 * 外键 builder 只描述目标关系，是否真的迁移交给上层看计划后决定。
 *
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
public final class ForeignKeyBuilder {

    private final CreateOrAlterTableBuilder table;

    private final ForeignKeyMetadata.Builder foreignKey;

    ForeignKeyBuilder(CreateOrAlterTableBuilder table, String name) {
        this.table = table;
        this.foreignKey = ForeignKeyMetadata.builder(CreateOrAlterTableBuilder.requireText(name, "foreign key name"));
    }

    /**
     * 按顺序追加当前表的一个外键列。
     *
     * @param name 当前表列名
     * @return 当前外键构建器
     */
    public ForeignKeyBuilder column(String name) {
        foreignKey.addColumn(CreateOrAlterTableBuilder.requireText(name, "foreign key column"));
        return this;
    }

    /**
     * 一次追加多个当前表列，顺序必须与引用列一一对应。
     *
     * @param first 第一列
     * @param rest 其余列
     * @return 当前外键构建器
     */
    public ForeignKeyBuilder columns(String first, String... rest) {
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
    public ForeignKeyBuilder referenceTable(String name) {
        foreignKey.referenceTable(CreateOrAlterTableBuilder.requireText(name, "foreign key reference table"));
        return this;
    }

    /**
     * 按顺序追加一个引用列。
     *
     * @param name 引用表列名
     * @return 当前外键构建器
     */
    public ForeignKeyBuilder referenceColumn(String name) {
        foreignKey.addReferenceColumn(CreateOrAlterTableBuilder.requireText(name, "foreign key reference column"));
        return this;
    }

    /**
     * 一次追加多个引用列，数量和顺序必须与当前表外键列一致。
     *
     * @param first 第一列
     * @param rest 其余列
     * @return 当前外键构建器
     */
    public ForeignKeyBuilder referenceColumns(String first, String... rest) {
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
    public CreateOrAlterTableBuilder commit() {
        table.addForeignKey(foreignKey.build());
        return table;
    }
}
