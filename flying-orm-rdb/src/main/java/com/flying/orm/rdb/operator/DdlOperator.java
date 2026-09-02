package com.flying.orm.rdb.operator;

import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import com.flying.orm.rdb.schema.ReactiveSchemaClient;

import java.util.Objects;

/**
 * 动态表结构的响应式入口。
 *
 * <p>本对象本身不保存建表状态，每次调用 {@link #createOrAlter(String)} 都会创建一个新的迁移构建器；
 * 它只共享已经组装好的 schema client 和元数据 reader，因此可以作为轻量入口使用。</p>
 *
 * @author wangr
 * @date 2026-07-27
 * @version v1.0
 */
public final class DdlOperator {

    private final ReactiveSchemaClient schemaClient;

    private final ReactiveFormMetadataReader metadataReader;

    DdlOperator(ReactiveSchemaClient schemaClient, ReactiveFormMetadataReader metadataReader) {
        this.schemaClient = Objects.requireNonNull(schemaClient, "schema client must not be null");
        this.metadataReader = Objects.requireNonNull(metadataReader, "reactive form metadata reader must not be null");
    }

    /**
     * 开始描述一张表的目标结构。真正读取元数据、生成迁移计划和执行 DDL 都在后续 commit/plan 时发生。
     *
     * @param table 物理表名，只接受安全标识符
     * @return 新的表结构构建器
     */
    public CreateOrAlterTableBuilder createOrAlter(String table) {
        return new CreateOrAlterTableBuilder(schemaClient, metadataReader, table);
    }
}
