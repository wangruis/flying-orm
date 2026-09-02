package com.flying.orm.core.metadata;

import com.flying.orm.core.internal.Names;

import java.util.ArrayList;
import java.util.List;

/**
 * 索引元数据描述表索引的名称、唯一性和索引列顺序，供 SQL 规划和 DDL 渲染复用。
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
public final class IndexMetadata {

    private final String name;

    private final String normalizedName;

    private final boolean unique;

    private final List<String> columns;

    private IndexMetadata(String name,
                          String normalizedName,
                          boolean unique,
                          List<String> columns) {
        this.name = name;
        this.normalizedName = normalizedName;
        this.unique = unique;
        this.columns = List.copyOf(columns);
    }

    /**
     * 创建索引元数据构建器。
     *
     * @param name 索引名称
     * @return 索引元数据构建器
     */
    public static Builder builder(String name) {
        return new Builder(name);
    }

    /**
     * 返回原始索引名称。
     *
     * @return 原始索引名称
     */
    public String name() {
        return name;
    }

    /**
     * 返回规范化索引名称。
     *
     * @return 小写且去除首尾空白后的索引名称
     */
    public String normalizedName() {
        return normalizedName;
    }

    /**
     * 返回索引是否唯一。
     *
     * @return 唯一索引返回 true
     */
    public boolean unique() {
        return unique;
    }

    /**
     * 返回只读索引列名集合，顺序与声明顺序一致。
     *
     * @return 只读索引列名集合
     */
    public List<String> columns() {
        return columns;
    }

    /**
     * 索引元数据构建器，用于在发布只读索引前收集索引列。
     *
     * @author wangr
     * @date 2026-07-21
     * @version v1.0
     */
    public static final class Builder {

        private final String name;

        private final String normalizedName;

        private boolean unique;

        private final List<String> columns = new ArrayList<>();

        private Builder(String name) {
            this.name = Names.requireText(name, "index name");
            this.normalizedName = Names.key(this.name, "index name");
        }

        /**
         * 标记当前索引为唯一索引。
         *
         * @return 当前构建器
         */
        public Builder unique() {
            unique = true;
            return this;
        }

        /**
         * 添加索引列。
         *
         * @param columnName 索引列名
         * @return 当前构建器
         */
        public Builder addColumn(String columnName) {
            columns.add(Names.requireText(columnName, "index column name"));
            return this;
        }

        /**
         * 构建只读索引元数据。
         *
         * @return 索引元数据
         */
        public IndexMetadata build() {
            if (columns.isEmpty()) {
                throw new IllegalArgumentException("index columns must not be empty");
            }
            return new IndexMetadata(name, normalizedName, unique, columns);
        }
    }
}
