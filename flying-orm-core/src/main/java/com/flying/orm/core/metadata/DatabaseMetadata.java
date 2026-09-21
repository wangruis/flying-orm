package com.flying.orm.core.metadata;

import com.flying.orm.core.internal.Names;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * 数据库元数据是 ORM 运行时的顶层只读目录，负责 Schema 与表的高频规范化定位。
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
public final class DatabaseMetadata {

    private final String name;

    private final List<SchemaMetadata> schemas;

    private final MetadataNameIndex<SchemaMetadata> schemasByName;

    private final SchemaMetadata currentSchema;

    private DatabaseMetadata(String name, List<SchemaMetadata> schemas, String currentSchemaName) {
        this.name = name;
        this.schemas = List.copyOf(schemas);
        this.schemasByName = MetadataNameIndex.ofOwned(this.schemas,
                                                       SchemaMetadata::name,
                                                       SchemaMetadata::normalizedName,
                                                       "schema");
        this.currentSchema = resolveCurrentSchema(currentSchemaName, this.schemas);
    }

    /**
     * 创建数据库元数据构建器。
     *
     * @param name 数据库名称
     * @return 数据库元数据构建器
     */
    public static Builder builder(String name) {
        return new Builder(name);
    }

    /**
     * 返回原始数据库名称。
     *
     * @return 原始数据库名称
     */
    public String name() {
        return name;
    }

    /**
     * 返回只读 Schema 集合。
     *
     * @return 只读 Schema 集合
     */
    public List<SchemaMetadata> schemas() {
        return schemas;
    }

    /**
     * 按物理名称查找 Schema。精确名称优先；仅当忽略大小写后的名称仍唯一时才宽松匹配。
     *
     * @param name 调用方传入的 Schema 名称
     * @return 匹配 Schema；不存在时返回空
     */
    public Optional<SchemaMetadata> findSchema(String name) {
        return schemasByName.find(name, "schema name");
    }

    /**
     * 按物理名称获取 Schema，不存在或忽略大小写后存在歧义时抛出确定性异常。
     *
     * @param name 调用方传入的 Schema 名称
     * @return 匹配 Schema
     * @throws IllegalArgumentException Schema 不存在时抛出
     */
    public SchemaMetadata schema(String name) {
        return findSchema(name).orElseThrow(() -> new IllegalArgumentException(
                "schema does not exist"));
    }

    /**
     * 返回当前 Schema。
     *
     * @return 当前 Schema
     * @throws IllegalStateException 未配置且没有可用 Schema 时抛出
     */
    public SchemaMetadata currentSchema() {
        if (currentSchema == null) {
            throw new IllegalStateException("database has no current schema");
        }
        return currentSchema;
    }

    /**
     * 在指定 Schema 中按物理表名查找表；大小写折叠后有歧义时不猜测目标。
     *
     * @param schemaName Schema 名称
     * @param tableName  表名
     * @return 匹配表；不存在时返回空
     */
    public Optional<TableMetadata> findTable(String schemaName, String tableName) {
        return findSchema(schemaName).flatMap(schema -> schema.findTable(tableName));
    }

    /**
     * 在指定 Schema 中按物理表名获取表；大小写折叠后有歧义时按不存在处理。
     *
     * @param schemaName Schema 名称
     * @param tableName  表名
     * @return 匹配表
     * @throws IllegalArgumentException Schema 或表不存在时抛出
     */
    public TableMetadata table(String schemaName, String tableName) {
        return schema(schemaName).table(tableName);
    }

    private SchemaMetadata resolveCurrentSchema(String currentSchemaName, List<SchemaMetadata> copiedSchemas) {
        if (currentSchemaName == null || currentSchemaName.isBlank()) {
            return copiedSchemas.isEmpty() ? null : copiedSchemas.getFirst();
        }
        return schema(currentSchemaName);
    }

    /**
     * 数据库元数据构建器，用于在发布只读数据库目录前收集 Schema 定义。
     *
     * @author wangr
     * @date 2026-07-21
     * @version v1.0
     */
    public static final class Builder {

        private final String name;

        private final List<SchemaMetadata> schemas = new ArrayList<>();

        private String currentSchemaName;

        private Builder(String name) {
            this.name = Names.requireText(name, "database name");
        }

        /**
         * 添加 Schema 定义。
         *
         * @param schema Schema 元数据
         * @return 当前构建器
         */
        public Builder addSchema(SchemaMetadata schema) {
            schemas.add(Objects.requireNonNull(schema, "schema must not be null"));
            return this;
        }

        /**
         * 指定当前 Schema 名称。
         *
         * @param name 当前 Schema 名称
         * @return 当前构建器
         */
        public Builder currentSchema(String name) {
            currentSchemaName = Names.requireText(name, "current schema name");
            return this;
        }

        /**
         * 构建只读数据库元数据。
         *
         * @return 数据库元数据
         */
        public DatabaseMetadata build() {
            return new DatabaseMetadata(name, schemas, currentSchemaName);
        }
    }
}
