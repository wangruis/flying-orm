package com.flying.orm.rdb.operator;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.metadata.MetadataCacheInvalidator;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;

import java.util.List;
import java.util.Objects;

/**
 * 原生同步元数据入口，直接调用同步 reader，不等待响应式结果、不保存第二份缓存。
 *
 * @author wangr
 * @date 2026-07-28
 * @version v1.0
 */
public final class SyncMetadataOperator {

    private final JdbcFormMetadataReader jdbcMetadata;

    private final MetadataCacheInvalidator cacheInvalidator;

    /** 原生 JDBC 元数据入口，读取时不创建 Mono，也不等待 R2DBC。 */
    SyncMetadataOperator(JdbcFormMetadataReader metadata, MetadataCacheInvalidator cacheInvalidator) {
        this.jdbcMetadata = Objects.requireNonNull(metadata, "sync metadata reader must not be null");
        this.cacheInvalidator = Objects.requireNonNull(
                cacheInvalidator, "metadata cache invalidator must not be null");
    }

    /** 直读当前 Schema 可见的表和视图名称，不缓存、不加载逐表结构。 */
    public List<String> readTableNames() {
        return jdbcMetadata.readTableNames();
    }

    /** 直读指定 Schema 的表和视图名称；null 使用连接的当前 Schema。 */
    public List<String> readTableNames(String schema) {
        return jdbcMetadata.readTableNames(schema);
    }

    /** 批量直读当前 Schema 的全部表结构。 */
    public List<RelationalTableDefinition> readTables() {
        return jdbcMetadata.readTables();
    }

    /** 批量直读指定 Schema 的全部表结构；null 使用当前 Schema。 */
    public List<RelationalTableDefinition> readTables(String schema) {
        return jdbcMetadata.readTables(schema);
    }

    /**
     * 按默认 schema 读取动态表单。
     *
     * @param formId 动态表单业务标识
     * @param table 物理表名
     * @return 动态表单
     */
    public DynamicForm readForm(String formId, String table) {
        return jdbcMetadata.readForm(formId, table);
    }

    /**
     * 按明确 schema 读取动态表单。
     *
     * @param formId 动态表单业务标识
     * @param schema schema 名
     * @param table 物理表名
     * @return 动态表单
     */
    public DynamicForm readForm(String formId, String schema, String table) {
        return jdbcMetadata.readForm(formId, schema, table);
    }

    /**
     * 按默认 schema 读取表元数据。
     *
     * @param table 物理表名
     * @return 表元数据
     */
    public RelationalTableDefinition readTable(String table) {
        return jdbcMetadata.readTable(table);
    }

    /**
     * 按明确 schema 读取表元数据。
     *
     * @param schema schema 名
     * @param table 物理表名
     * @return 表元数据
     */
    public RelationalTableDefinition readTable(String schema, String table) {
        return jdbcMetadata.readTable(schema, table);
    }

    /**
     * 把失效命令转给共享失效协调器，本身不保存第二份缓存。
     *
     * @param table 物理表名，可以是 table 或 schema.table
     */
    public void invalidate(String table) {
        cacheInvalidator.invalidate(table);
    }

    /**
     * 清掉指定 schema 和表的共享元数据缓存，没有启用缓存时什么也不做。
     *
     * @param schema schema 名
     * @param table 物理表名
     */
    public void invalidate(String schema, String table) {
        cacheInvalidator.invalidate(schema, table);
    }

    /** 清空共享 reader 管理的全部元数据缓存，没有启用缓存时什么也不做。 */
    public void invalidateAll() {
        cacheInvalidator.invalidateAll();
    }
}
