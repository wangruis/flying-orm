package com.flying.orm.rdb.operator;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.rdb.metadata.MetadataCacheInvalidator;
import com.flying.orm.rdb.metadata.SyncFormMetadataReader;

import java.util.Objects;

/**
 * {@link MetadataOperator} 的同步包装，只在读取调用边界等待响应式结果，不保存第二份缓存。
 *
 * @author wangr
 * @date 2026-07-28
 * @version v1.0
 */
public final class SyncMetadataOperator {

    private final SyncFormMetadataReader jdbcMetadata;

    private final MetadataCacheInvalidator cacheInvalidator;

    /** 原生 JDBC 元数据入口，读取时不创建 Mono，也不等待 R2DBC。 */
    SyncMetadataOperator(SyncFormMetadataReader metadata, MetadataCacheInvalidator cacheInvalidator) {
        this.jdbcMetadata = Objects.requireNonNull(metadata, "sync metadata reader must not be null");
        this.cacheInvalidator = Objects.requireNonNull(
                cacheInvalidator, "metadata cache invalidator must not be null");
    }

    /**
     * 按默认 schema 读取动态表单并等待结果。
     *
     * @param formId 动态表单业务标识
     * @param table 物理表名
     * @return 动态表单
     */
    public DynamicForm readForm(String formId, String table) {
        return jdbcMetadata.readForm(formId, table);
    }

    /**
     * 按明确 schema 读取动态表单并等待结果。
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
     * 按默认 schema 读取表元数据并等待结果。
     *
     * @param table 物理表名
     * @return 表元数据
     */
    public TableMetadata readTable(String table) {
        return jdbcMetadata.readTable(table);
    }

    /**
     * 按明确 schema 读取表元数据并等待结果。
     *
     * @param schema schema 名
     * @param table 物理表名
     * @return 表元数据
     */
    public TableMetadata readTable(String schema, String table) {
        return jdbcMetadata.readTable(schema, table);
    }

    /**
     * 同步侧只负责把失效命令转给 reactive operator，本身不保存第二份缓存。
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
