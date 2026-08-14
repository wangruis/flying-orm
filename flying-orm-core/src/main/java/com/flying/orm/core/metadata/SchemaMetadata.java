package com.flying.orm.core.metadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Schema 元数据聚合一组表定义，提供规范化表名索引以支撑高频路由和 SQL 规划。
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
public final class SchemaMetadata {

    private final String name;

    private final String normalizedName;

    private final List<TableMetadata> tables;

    private final MetadataNameIndex<TableMetadata> tablesByName;

    private SchemaMetadata(String name, List<TableMetadata> tables) {
        this.name = MetadataNames.requireText(name, "schema name");
        this.normalizedName = MetadataNames.normalize(name, "schema name");

        List<TableMetadata> copiedTables = List.copyOf(tables);
        this.tables = copiedTables;
        this.tablesByName = MetadataNameIndex.of(copiedTables, TableMetadata::name, "table");
    }

    /**
     * 创建 Schema 元数据构建器。
     *
     * @param name Schema 名称
     * @return Schema 元数据构建器
     */
    public static Builder builder(String name) {
        return new Builder(name);
    }

    /**
     * 返回原始 Schema 名称。
     *
     * @return 原始 Schema 名称
     */
    public String name() {
        return name;
    }

    /**
     * 返回规范化 Schema 名称。
     *
     * @return 小写且去除首尾空白后的 Schema 名称
     */
    public String normalizedName() {
        return normalizedName;
    }

    /**
     * 返回只读表集合。
     *
     * @return 只读表集合
     */
    public List<TableMetadata> tables() {
        return tables;
    }

    /**
     * 按物理名称查找表。精确名称优先；仅当忽略大小写后的名称仍唯一时才宽松匹配。
     *
     * @param name 调用方传入的表名
     * @return 匹配表；不存在时返回空
     */
    public Optional<TableMetadata> findTable(String name) {
        return tablesByName.find(name, "table name");
    }

    /**
     * 按物理名称获取表，不存在或忽略大小写后存在歧义时抛出确定性异常。
     *
     * @param name 调用方传入的表名
     * @return 匹配表
     * @throws IllegalArgumentException 表不存在时抛出
     */
    public TableMetadata table(String name) {
        return findTable(name).orElseThrow(() -> new IllegalArgumentException(
                "table does not exist"));
    }

    /**
     * Schema 元数据构建器，用于在发布只读 Schema 前收集表定义。
     *
     * @author wangr
     * @date 2026-07-21
     * @version v1.0
     */
    public static final class Builder {

        private final String name;

        private final List<TableMetadata> tables = new ArrayList<>();

        private Builder(String name) {
            this.name = MetadataNames.requireText(name, "schema name");
        }

        /**
         * 添加表定义。
         *
         * @param table 表元数据
         * @return 当前构建器
         */
        public Builder addTable(TableMetadata table) {
            tables.add(Objects.requireNonNull(table, "table must not be null"));
            return this;
        }

        /**
         * 构建只读 Schema 元数据。
         *
         * @return Schema 元数据
         */
        public SchemaMetadata build() {
            return new SchemaMetadata(name, tables);
        }
    }
}
