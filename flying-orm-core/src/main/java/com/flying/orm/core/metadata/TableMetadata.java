package com.flying.orm.core.metadata;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 表元数据是 ORM 热路径的只读快照，集中保存列、主键、索引和外键，并以规范化名称提供稳定的 O(1) 查找。
 *
 * <p>构造时完成集合复制、重复名称检查和查找索引建立。发布后的对象没有可变状态，可以安全放进
 * Caffeine 元数据缓存并被并发查询复用。这里描述“数据库应该是什么样”，不负责执行 DDL。</p>
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
public final class TableMetadata {

    private final String name;

    private final String normalizedName;

    private final List<ColumnMetadata> columns;

    private final Map<String, ColumnMetadata> columnsByName;

    private final List<ColumnMetadata> primaryKeyColumns;

    private final List<IndexMetadata> indexes;

    private final Map<String, IndexMetadata> indexesByName;

    private final List<ForeignKeyMetadata> foreignKeys;

    private final Map<String, ForeignKeyMetadata> foreignKeysByName;

    private TableMetadata(String name,
                          List<ColumnMetadata> columns,
                          List<IndexMetadata> indexes,
                          List<ForeignKeyMetadata> foreignKeys) {
        this.name = MetadataNames.requireText(name, "table name");
        this.normalizedName = MetadataNames.normalize(name, "table name");

        // 先复制再建立派生索引，保证列表视图和按名称查找看到的是同一批对象。
        List<ColumnMetadata> copiedColumns = List.copyOf(columns);
        Map<String, ColumnMetadata> indexedColumns = new LinkedHashMap<>(MetadataNames.mapCapacity(copiedColumns.size()));
        List<ColumnMetadata> primaryKeys = new ArrayList<>();

        for (ColumnMetadata column : copiedColumns) {
            ColumnMetadata safeColumn = Objects.requireNonNull(column, "column must not be null");
            ColumnMetadata previous = indexedColumns.putIfAbsent(safeColumn.normalizedName(), safeColumn);
            if (previous != null) {
                throw new IllegalArgumentException("duplicate column name");
            }
            if (safeColumn.primaryKey()) {
                primaryKeys.add(safeColumn);
            }
        }

        this.columns = copiedColumns;
        this.columnsByName = Map.copyOf(indexedColumns);
        this.primaryKeyColumns = List.copyOf(primaryKeys);

        // 重复名称必须在进入缓存前暴露，否则不同调用入口可能拿到不一致的定义。
        List<IndexMetadata> copiedIndexes = List.copyOf(indexes);
        Map<String, IndexMetadata> indexedIndexes = new LinkedHashMap<>(MetadataNames.mapCapacity(copiedIndexes.size()));
        for (IndexMetadata index : copiedIndexes) {
            IndexMetadata safeIndex = Objects.requireNonNull(index, "index must not be null");
            IndexMetadata previous = indexedIndexes.putIfAbsent(safeIndex.normalizedName(), safeIndex);
            if (previous != null) {
                throw new IllegalArgumentException("duplicate index name");
            }
        }
        this.indexes = copiedIndexes;
        this.indexesByName = Map.copyOf(indexedIndexes);

        List<ForeignKeyMetadata> copiedForeignKeys = List.copyOf(foreignKeys);
        Map<String, ForeignKeyMetadata> indexedForeignKeys = new LinkedHashMap<>(MetadataNames.mapCapacity(copiedForeignKeys.size()));
        for (ForeignKeyMetadata foreignKey : copiedForeignKeys) {
            ForeignKeyMetadata safeForeignKey = Objects.requireNonNull(foreignKey, "foreign key must not be null");
            ForeignKeyMetadata previous = indexedForeignKeys.putIfAbsent(safeForeignKey.normalizedName(), safeForeignKey);
            if (previous != null) {
                throw new IllegalArgumentException("duplicate foreign key name");
            }
        }
        this.foreignKeys = copiedForeignKeys;
        this.foreignKeysByName = Map.copyOf(indexedForeignKeys);
    }

    /**
     * 创建表元数据构建器。
     *
     * @param name 表名
     * @return 表元数据构建器
     */
    public static Builder builder(String name) {
        return new Builder(name);
    }

    /**
     * 返回原始表名。
     *
     * @return 原始表名
     */
    public String name() {
        return name;
    }

    /**
     * 返回规范化表名，用于缓存键和跨方言查找。
     *
     * @return 小写且去除首尾空白后的表名
     */
    public String normalizedName() {
        return normalizedName;
    }

    /**
     * 返回表的只读列集合。
     *
     * @return 只读列集合
     */
    public List<ColumnMetadata> columns() {
        return columns;
    }

    /**
     * 按规范化列名查找列。
     *
     * @param name 调用方传入的列名，可包含大小写差异和首尾空白
     * @return 匹配列；不存在时返回空
     */
    public Optional<ColumnMetadata> findColumn(String name) {
        return Optional.ofNullable(columnsByName.get(MetadataNames.normalize(name, "column name")));
    }

    /**
     * 按规范化列名获取列，不存在时抛出确定性异常。
     *
     * @param name 调用方传入的列名
     * @return 匹配列
     * @throws IllegalArgumentException 列不存在时抛出
     */
    public ColumnMetadata column(String name) {
        return findColumn(name).orElseThrow(() -> new IllegalArgumentException(
                "column does not exist"));
    }

    /**
     * 返回按声明顺序排列的主键列集合。
     *
     * @return 只读主键列集合
     */
    public List<ColumnMetadata> primaryKeyColumns() {
        return primaryKeyColumns;
    }

    /**
     * 返回表的只读索引集合。
     *
     * @return 只读索引集合
     */
    public List<IndexMetadata> indexes() {
        return indexes;
    }

    /**
     * 按规范化索引名查找索引。
     *
     * @param name 调用方传入的索引名
     * @return 匹配索引；不存在时返回空
     */
    public Optional<IndexMetadata> findIndex(String name) {
        return Optional.ofNullable(indexesByName.get(MetadataNames.normalize(name, "index name")));
    }

    /**
     * 按规范化索引名获取索引，不存在时抛出确定性异常。
     *
     * @param name 调用方传入的索引名
     * @return 匹配索引
     * @throws IllegalArgumentException 索引不存在时抛出
     */
    public IndexMetadata index(String name) {
        return findIndex(name).orElseThrow(() -> new IllegalArgumentException(
                "index does not exist"));
    }

    public List<ForeignKeyMetadata> foreignKeys() {
        return foreignKeys;
    }

    /**
     * 按规范化名称查找外键。
     *
     * @param name 外键名，可包含大小写差异和首尾空白
     * @return 匹配外键；不存在时返回空
     */
    public Optional<ForeignKeyMetadata> findForeignKey(String name) {
        return Optional.ofNullable(foreignKeysByName.get(MetadataNames.normalize(name, "foreign key name")));
    }

    /**
     * 获取指定外键，不存在时给出不回显调用方输入的确定性错误。
     *
     * @param name 外键名
     * @return 匹配外键
     */
    public ForeignKeyMetadata foreignKey(String name) {
        return findForeignKey(name).orElseThrow(() -> new IllegalArgumentException(
                "foreign key does not exist"));
    }

    /**
     * 表元数据构建器，用于在发布只读元数据前收集列、索引和外键定义。
     * 构建器本身可变，只应在当前配置线程使用。
     *
     * @author wangr
     * @date 2026-07-21
     * @version v1.0
     */
    public static final class Builder {

        private final String name;

        private final List<ColumnMetadata> columns = new ArrayList<>();

        private final List<IndexMetadata> indexes = new ArrayList<>();

        private final List<ForeignKeyMetadata> foreignKeys = new ArrayList<>();

        private Builder(String name) {
            this.name = MetadataNames.requireText(name, "table name");
        }

        /**
         * 添加列定义。
         *
         * @param column 列元数据
         * @return 当前构建器
         */
        public Builder addColumn(ColumnMetadata column) {
            columns.add(Objects.requireNonNull(column, "column must not be null"));
            return this;
        }

        /**
         * 添加索引定义。
         *
         * @param index 索引元数据
         * @return 当前构建器
         */
        public Builder addIndex(IndexMetadata index) {
            indexes.add(Objects.requireNonNull(index, "index must not be null"));
            return this;
        }

        /**
         * 添加外键定义。引用列是否真实存在由后续 schema 校验负责，这里只收集声明。
         *
         * @param foreignKey 外键元数据
         * @return 当前构建器
         */
        public Builder addForeignKey(ForeignKeyMetadata foreignKey) {
            foreignKeys.add(Objects.requireNonNull(foreignKey, "foreign key must not be null"));
            return this;
        }

        /**
         * 构建只读表元数据。
         *
         * @return 表元数据
         */
        public TableMetadata build() {
            return new TableMetadata(name, columns, indexes, foreignKeys);
        }
    }
}
