package com.flying.orm.rdb.metadata;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.rdb.schema.SchemaSnapshot;
import com.flying.orm.rdb.schema.SchemaSnapshotCoverage;
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
     * 声明该 reader 能稳定回读的结构事实。默认保守为无覆盖；只有确实返回完整规范快照的自定义
     * reader 才应显式返回 {@link SchemaSnapshotCoverage#complete()}。
     */
    default SchemaSnapshotCoverage snapshotCoverage() {
        return SchemaSnapshotCoverage.none();
    }

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
     * 读取不会伪造未知属性的 Schema 快照。旧的自定义 reader 通过默认适配继续二进制兼容；内建 reader
     * 会覆盖本方法，直接区分表不存在和字典属性未知。
     */
    default Mono<SchemaSnapshot> readSnapshot(String table) {
        return readTable(table).map(metadata -> SchemaSnapshot.fromLegacy(
                RelationIdentity.table(table), metadata));
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

    /** 按明确 schema 读取三态 Schema 快照。 */
    default Mono<SchemaSnapshot> readSnapshot(String schema, String table) {
        return readTable(schema, table).map(metadata -> SchemaSnapshot.fromLegacy(
                RelationIdentity.of(null, schema, table), metadata));
    }

    /**
     * 精确清理一个物理表的元数据缓存。无缓存 reader 默认什么也不做，Caffeine 包装器会覆盖这个方法。
     *
     * @param table 迁移成功的物理表名，可以是 table 或 schema.table
     */
    default void invalidate(String table) {
    }
}
