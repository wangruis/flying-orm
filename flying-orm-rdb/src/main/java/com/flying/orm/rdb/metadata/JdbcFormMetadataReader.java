package com.flying.orm.rdb.metadata;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.schema.SchemaSnapshot;
import com.flying.orm.rdb.schema.SchemaSnapshotCoverage;

import java.util.List;

/**
 * 同步元数据端口。实现接收分段关系身份，字符串便利入口只负责解析一次。
 * 表单读取只需列事实；表结构读取独立提供，不通过表单反向还原。
 * 缓存实现须覆盖 Schema 直读入口，完整快照须如实声明覆盖范围。
 *
 * @author wangr
 * @version v4.1.2
 */
public abstract class JdbcFormMetadataReader implements MetadataCacheInvalidator {

    protected JdbcFormMetadataReader() {
    }

    public SchemaSnapshotCoverage snapshotCoverage() {
        return SchemaSnapshotCoverage.none();
    }

    public List<String> readTableNames() {
        return readTableNames(null);
    }

    public List<String> readTableNames(String schema) {
        throw new UnsupportedOperationException("metadata reader does not support table name enumeration");
    }

    public List<RelationalTableDefinition> readTables() {
        return readTables(null);
    }

    public List<RelationalTableDefinition> readTables(String schema) {
        throw new UnsupportedOperationException("metadata reader does not support schema enumeration");
    }

    public DynamicForm readForm(String formId, String table) {
        return readForm(formId, RelationIdentity.parse(table));
    }

    public DynamicForm readForm(String formId, String schema, String table) {
        return readForm(formId, RelationIdentity.of(null, schema, table));
    }

    public abstract DynamicForm readForm(String formId, RelationIdentity relation);

    public RelationalTableDefinition readTable(String table) {
        return readTable(RelationIdentity.parse(table));
    }

    public RelationalTableDefinition readTable(String schema, String table) {
        return readTable(RelationIdentity.of(null, schema, table));
    }

    public abstract RelationalTableDefinition readTable(RelationIdentity relation);

    /** 不使用或填充普通元数据缓存。 */
    public RelationalTableDefinition readTableForSchema(RelationIdentity relation) {
        return readTable(relation);
    }

    public SchemaSnapshot readSnapshot(String table) {
        return readSnapshot(RelationIdentity.parse(table));
    }

    public SchemaSnapshot readSnapshot(String schema, String table) {
        return readSnapshot(RelationIdentity.of(null, schema, table));
    }

    /** 不把未读取的事实伪造为不存在。 */
    public SchemaSnapshot readSnapshot(RelationIdentity relation) {
        return SchemaSnapshot.fromPartial(readTableForSchema(relation));
    }

    @Override
    public void invalidate(RelationIdentity relation) {
    }

    @Override
    public void invalidateAll() {
    }
}
