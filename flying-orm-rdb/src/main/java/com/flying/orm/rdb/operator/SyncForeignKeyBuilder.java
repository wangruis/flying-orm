package com.flying.orm.rdb.operator;

import com.flying.orm.core.metadata.ForeignKeyMetadata;

/**
 * 同步外键定义 builder。
 *
 * <p>当前表列和引用表列按调用顺序一一对应。commit 只把不可变外键描述加入表草稿，危险的关系迁移
 * 仍由 Schema 计划、审核和批准流程决定是否可以真正执行。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v2.0
 */
public final class SyncForeignKeyBuilder {

    private final SyncCreateOrAlterTableBuilder table;
    private final ForeignKeyMetadata.Builder foreignKey;

    SyncForeignKeyBuilder(SyncCreateOrAlterTableBuilder table, String name) {
        this.table = table;
        this.foreignKey = ForeignKeyMetadata.builder(
                DdlStructureDraft.requireText(name, "foreign key name"));
    }

    /** 追加当前表的一个外键列。 */
    public SyncForeignKeyBuilder column(String name) {
        foreignKey.addColumn(DdlStructureDraft.requireText(name, "foreign key column"));
        return this;
    }

    /** 一次追加多个当前表外键列。 */
    public SyncForeignKeyBuilder columns(String first, String... rest) {
        column(first);
        for (String column : rest) {
            column(column);
        }
        return this;
    }

    /** 设置被引用的物理表名。 */
    public SyncForeignKeyBuilder referenceTable(String name) {
        foreignKey.referenceTable(
                DdlStructureDraft.requireText(name, "foreign key reference table"));
        return this;
    }

    /** 追加一个被引用的列。 */
    public SyncForeignKeyBuilder referenceColumn(String name) {
        foreignKey.addReferenceColumn(
                DdlStructureDraft.requireText(name, "foreign key reference column"));
        return this;
    }

    /** 一次追加多个被引用列，顺序必须与当前表列完全一致。 */
    public SyncForeignKeyBuilder referenceColumns(String first, String... rest) {
        referenceColumn(first);
        for (String column : rest) {
            referenceColumn(column);
        }
        return this;
    }

    /** 冻结当前外键描述并加入表级草稿。 */
    public SyncCreateOrAlterTableBuilder commit() {
        table.draft().addForeignKey(foreignKey.build());
        return table;
    }
}
