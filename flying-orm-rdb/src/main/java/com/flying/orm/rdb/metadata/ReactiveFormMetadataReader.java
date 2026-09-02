package com.flying.orm.rdb.metadata;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.TableMetadata;
import reactor.core.publisher.Mono;

/**
 * 从真实数据库把表结构读回动态表单。它只负责读取和转换，不负责建表、迁移或缓存。
 *
 * @author wangr
 * @date 2026-07-28
 * @version v1.0
 */
public interface ReactiveFormMetadataReader {

    /**
     * 按物理表名读取动态表单结构。
     *
     * @param formId 表单 ID，由调用方决定怎么命名
     * @param table  物理表名，可以是 table 或 schema.table
     * @return 数据库里当前能看到的表单结构
     */
    Mono<DynamicForm> readForm(String formId, String table);

    /**
     * 读取表元数据。默认只包含字段；具体数据库实现可以覆盖它，把索引也读回来。
     *
     * @param table 物理表名，可以是 table 或 schema.table
     * @return 表元数据
     */
    default Mono<TableMetadata> readTable(String table) {
        return readForm(table, table).map(DynamicForm::toTableMetadata);
    }

    /**
     * 按 schema 和物理表名读取动态表单结构。
     *
     * @param formId 表单 ID，由调用方决定怎么命名
     * @param schema 数据库 schema
     * @param table  物理表名
     * @return 数据库里当前能看到的表单结构
     */
    Mono<DynamicForm> readForm(String formId, String schema, String table);

    /**
     * 按 schema 和物理表名读取表元数据。默认只包含字段；具体数据库实现可以覆盖它，把索引也读回来。
     *
     * @param schema 数据库 schema
     * @param table  物理表名
     * @return 表元数据
     */
    default Mono<TableMetadata> readTable(String schema, String table) {
        return readForm(table, schema, table).map(DynamicForm::toTableMetadata);
    }

    /**
     * 精确清理一个物理表的元数据缓存。无缓存 reader 默认什么也不做，Caffeine 包装器会覆盖这个方法。
     *
     * @param table 迁移成功的物理表名，可以是 table 或 schema.table
     */
    default void invalidate(String table) {
    }
}
