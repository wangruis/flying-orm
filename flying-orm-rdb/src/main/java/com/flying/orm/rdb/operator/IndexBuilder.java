package com.flying.orm.rdb.operator;

import com.flying.orm.core.metadata.IndexMetadata;

/**
 * 索引 builder。这里只描述目标索引长什么样，真正是否需要创建由 createOrAlter 读库后决定。
 *
 * @author wangr
 * @date 2026-07-28
 * @version v1.0
 */
public final class IndexBuilder {

    private final CreateOrAlterTableBuilder table;

    private final IndexMetadata.Builder index;

    IndexBuilder(CreateOrAlterTableBuilder table, String name) {
        this.table = table;
        this.index = IndexMetadata.builder(CreateOrAlterTableBuilder.requireText(name, "index name"));
    }

    /**
     * 把目标索引标记为唯一索引。
     *
     * @return 当前索引构建器
     */
    public IndexBuilder unique() {
        index.unique();
        return this;
    }

    /**
     * 按调用顺序追加一个索引列，复合索引的列顺序会原样保留。
     *
     * @param name 列名
     * @return 当前索引构建器
     */
    public IndexBuilder column(String name) {
        index.addColumn(CreateOrAlterTableBuilder.requireText(name, "index column"));
        return this;
    }

    /**
     * 一次追加多个索引列，顺序为 first 后接 rest。
     *
     * @param first 第一列
     * @param rest 其余列
     * @return 当前索引构建器
     */
    public IndexBuilder columns(String first, String... rest) {
        column(first);
        for (String column : rest) {
            column(column);
        }
        return this;
    }

    /**
     * 校验并把索引加入表级迁移目标。这里不立即执行 CREATE INDEX。
     *
     * @return 所属表构建器
     */
    public CreateOrAlterTableBuilder commit() {
        table.addIndex(index.build());
        return table;
    }
}
