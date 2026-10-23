package com.flying.orm.rdb.metadata;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.schema.SchemaSnapshot;
import com.flying.orm.rdb.schema.SchemaSnapshotCoverage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * 响应式元数据端口。读取保留分段关系身份，订阅时访问数据库。
 * 表单读取只需列事实；表结构读取不通过表单反向还原。
 *
 * @author wangr
 * @version v4.1.2
 */
public interface ReactiveFormMetadataReader extends MetadataCacheInvalidator {

    default SchemaSnapshotCoverage snapshotCoverage() {
        return SchemaSnapshotCoverage.none();
    }

    default Flux<String> readTableNames() {
        return readTableNames(null);
    }

    default Flux<String> readTableNames(String schema) {
        return Flux.error(new UnsupportedOperationException(
                "metadata reader does not support table name enumeration"));
    }

    default Flux<RelationalTableDefinition> readTables() {
        return readTables(null);
    }

    default Flux<RelationalTableDefinition> readTables(String schema) {
        return Flux.error(new UnsupportedOperationException("metadata reader does not support schema enumeration"));
    }

    default Mono<DynamicForm> readForm(String formId, String table) {
        return Mono.defer(() -> readForm(formId, RelationIdentity.parse(table)));
    }

    default Mono<DynamicForm> readForm(String formId, String schema, String table) {
        return Mono.defer(() -> readForm(formId, RelationIdentity.of(null, schema, table)));
    }

    Mono<DynamicForm> readForm(String formId, RelationIdentity relation);

    default Mono<RelationalTableDefinition> readTable(String table) {
        return Mono.defer(() -> readTable(RelationIdentity.parse(table)));
    }

    default Mono<RelationalTableDefinition> readTable(String schema, String table) {
        return Mono.defer(() -> readTable(RelationIdentity.of(null, schema, table)));
    }

    Mono<RelationalTableDefinition> readTable(RelationIdentity relation);

    /** 不使用或填充普通元数据缓存；缓存实现应委托给底层直读入口。 */
    default Mono<RelationalTableDefinition> readTableForSchema(RelationIdentity relation) {
        return Mono.defer(() -> readTable(relation));
    }

    default Mono<SchemaSnapshot> readSnapshot(String table) {
        return Mono.defer(() -> readSnapshot(RelationIdentity.parse(table)));
    }

    default Mono<SchemaSnapshot> readSnapshot(String schema, String table) {
        return Mono.defer(() -> readSnapshot(RelationIdentity.of(null, schema, table)));
    }

    /** 不把未读取的事实伪造为不存在。 */
    default Mono<SchemaSnapshot> readSnapshot(RelationIdentity relation) {
        return Mono.defer(() -> readTableForSchema(relation)).map(SchemaSnapshot::fromPartial);
    }

    @Override
    default void invalidate(RelationIdentity relation) {
    }

    @Override
    default void invalidateAll() {
    }
}
