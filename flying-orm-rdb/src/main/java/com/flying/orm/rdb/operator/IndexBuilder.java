package com.flying.orm.rdb.operator;

import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.IndexKeyPart;

/**
 * 索引 builder。这里只描述目标索引长什么样，真正是否需要创建由 createOrAlter 读库后决定。
 *
 * @author wangr
 * @date 2026-07-28
 * @version v1.0
 */
public final class IndexBuilder<T> {

    private final T table;
    private final DdlStructureDraft draft;
    private final IndexDefinition.Builder index;

    IndexBuilder(T table, DdlStructureDraft draft, String name) {
        this.table = table;
        this.draft = draft;
        this.index = IndexDefinition.builder(DdlStructureDraft.requireText(name, "index name"));
    }

    /**
     * 把目标索引标记为唯一索引。
     *
     * @return 当前索引构建器
     */
    public IndexBuilder<T> unique() {
        index.unique();
        return this;
    }

    /**
     * 按调用顺序追加一个索引列，复合索引的列顺序会原样保留。
     *
     * @param name 列名
     * @return 当前索引构建器
     */
    public IndexBuilder<T> column(String name) {
        index.addColumn(DdlStructureDraft.requireText(name, "index column"));
        return this;
    }

    /** 按指定方向追加索引列，复用关系 Schema 的方向枚举。 */
    public IndexBuilder<T> column(String name, IndexKeyPart.Direction direction) {
        index.addKey(new IndexKeyPart(name, direction));
        return this;
    }

    /**
     * 一次追加多个索引列，顺序为 first 后接 rest。
     *
     * @param first 第一列
     * @param rest 其余列
     * @return 当前索引构建器
     */
    public IndexBuilder<T> columns(String first, String... rest) {
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
    public T commit() {
        draft.addIndex(index.build());
        return table;
    }
}
