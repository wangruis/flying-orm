package com.flying.orm.core.metadata;

import com.flying.orm.core.internal.Names;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/**
 * 命名索引定义。构建器只负责收集，发布的对象会复制键集合，因此后续修改构建器不会污染快照。
 *
 * @author wangr
 * @version v3.2
 */
public final class IndexDefinition {

    private final String name;
    private final boolean unique;
    private final List<IndexKeyPart> keys;

    private IndexDefinition(String name, boolean unique, List<IndexKeyPart> keys) {
        this.name = name;
        this.unique = unique;
        this.keys = List.copyOf(keys);
        // 排序方向不同也不能让同一物理列成为两个索引键；在元数据边界拒绝，避免生成方言相关的坏 DDL。
        HashSet<String> columns = new HashSet<>();
        for (IndexKeyPart key : this.keys) {
            if (!columns.add(key.column())) {
                throw new IllegalArgumentException("index key columns must not contain duplicates");
            }
        }
    }

    public static Builder builder(String name) {
        return new Builder(name);
    }

    public String name() {
        return name;
    }

    public boolean unique() {
        return unique;
    }

    /** 返回只读索引键，顺序与声明顺序一致。 */
    public List<IndexKeyPart> keys() {
        return keys;
    }

    public static final class Builder {

        private final String name;
        private boolean unique;
        private final List<IndexKeyPart> keys = new ArrayList<>();

        private Builder(String name) {
            this.name = Names.requireText(name, "index name");
        }

        /** 将索引标记为唯一索引。 */
        public Builder unique() {
            return unique(true);
        }

        /** 明确设置唯一性，便于适配已有元数据。 */
        public Builder unique(boolean unique) {
            this.unique = unique;
            return this;
        }

        /** 添加一个结构化索引键；调用顺序就是复合索引顺序。 */
        public Builder addKey(IndexKeyPart key) {
            keys.add(Objects.requireNonNull(key, "index key must not be null"));
            return this;
        }

        public IndexDefinition build() {
            if (keys.isEmpty()) {
                throw new IllegalArgumentException("index keys must not be empty");
            }
            return new IndexDefinition(name, unique, keys);
        }
    }
}
