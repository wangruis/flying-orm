package com.flying.orm.rdb.operator;

import com.flying.orm.core.metadata.IndexMetadata;

/**
 * 同步索引定义 builder。
 *
 * <p>索引只在 commit 后加入当前表的目标结构；这一步不会直接下发 CREATE INDEX。这样 DDL 客户端可以
 * 先读取现有结构，再统一判断应创建、跳过还是要求人工审核。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v2.0
 */
public final class SyncIndexBuilder {

    private final SyncCreateOrAlterTableBuilder table;
    private final IndexMetadata.Builder index;

    SyncIndexBuilder(SyncCreateOrAlterTableBuilder table, String name) {
        this.table = table;
        this.index = IndexMetadata.builder(DdlStructureDraft.requireText(name, "index name"));
    }

    /** 标记为唯一索引。 */
    public SyncIndexBuilder unique() {
        index.unique();
        return this;
    }

    /** 按调用顺序追加一个索引列。 */
    public SyncIndexBuilder column(String name) {
        index.addColumn(DdlStructureDraft.requireText(name, "index column"));
        return this;
    }

    /** 一次追加多个索引列，顺序为 first 后接 rest。 */
    public SyncIndexBuilder columns(String first, String... rest) {
        column(first);
        for (String column : rest) {
            column(column);
        }
        return this;
    }

    /** 冻结当前索引描述并加入表级草稿。 */
    public SyncCreateOrAlterTableBuilder commit() {
        table.draft().addIndex(index.build());
        return table;
    }
}
