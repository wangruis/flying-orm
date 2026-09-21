package com.flying.orm.rdb.mapping;

import com.flying.orm.core.annotation.TableCatalog;
import com.flying.orm.core.annotation.TableColumn;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.RelationalFormDefinition;
import com.flying.orm.core.metadata.RelationalMetadataFingerprint;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EntityDynamicFormRelationalParityTest {

    @Test
    void publishesLegacyAndCanonicalViewsFromOneEntityCompilation() {
        var descriptor = EntitySchemaDescriptor.builder(OrderEntity.class).build();
        var form = descriptor.form();
        var table = descriptor.table();

        // 旧 table() 访问器保持 schema.table 字符串，CRUD 内部同时保留三个独立身份段。
        assertEquals("app.orders", descriptor.metadata().table());
        assertEquals("orders", form.table());
        assertEquals(table.identity(), form.relationIdentity().orElseThrow());
        assertEquals(
                "insert into \"tenant\".\"app\".\"orders\" (\"code\") values (?)",
                FormDataSqlRenderer.create(
                        SqlRenderer.builder().addDefaultTerms().build(),
                        RdbDialect.postgresql())
                        .insert(form, Map.of("code", "A-1"))
                        .sql());
        assertEquals("tenant", table.identity().catalog().orElseThrow());
        assertEquals("app", table.identity().schema().orElseThrow());
        assertEquals("orders", table.identity().table());

        assertEquals(form.fields().stream().map(field -> field.name()).toList(),
                     table.columns().stream().map(column -> column.name()).toList());
        assertEquals(form.fields().stream().map(field -> field.databaseType()).toList(),
                     table.columns().stream().map(column -> column.databaseType()).toList());

        RelationalFormDefinition.Builder equivalent = RelationalFormDefinition.builder(
                form.id(), table.identity());
        for (int index = 0; index < form.fields().size(); index++) {
            equivalent.addField(form.fields().get(index), table.columns().get(index));
        }
        table.primaryKey().ifPresent(equivalent::primaryKey);

        assertEquals(RelationalMetadataFingerprint.of(table),
                     RelationalMetadataFingerprint.of(equivalent.build().table()));
    }

    @Test
    void repositoryCrudPreservesExplicitSchemaAndLiteralTableSegments() {
        DynamicForm form;
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            form = models.metadata(SchemaQualifiedEntity.class).toDynamicForm();
        }

        assertEquals("order.items", form.table());
        assertEquals("sales.data", form.relationIdentity().orElseThrow().schema().orElseThrow());
        assertEquals("order.items", form.relationIdentity().orElseThrow().table());
        assertEquals(
                "insert into \"sales.data\".\"order.items\" (\"code\") values (?)",
                FormDataSqlRenderer.create(
                        SqlRenderer.builder().addDefaultTerms().build(),
                        RdbDialect.postgresql())
                        .insert(form, Map.of("code", "A-1"))
                        .sql());
    }

    @Test
    void repositoryCrudKeepsLegacyDottedTableBehavior() {
        DynamicForm form;
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            form = models.metadata(LegacyQualifiedEntity.class).toDynamicForm();
        }

        assertTrue(form.relationIdentity().isEmpty());
        assertEquals(
                "insert into \"sales\".\"orders\" (\"code\") values (?)",
                FormDataSqlRenderer.create(
                        SqlRenderer.builder().addDefaultTerms().build(),
                        RdbDialect.postgresql())
                         .insert(form, Map.of("code", "A-1"))
                         .sql());
    }

    @Test
    void relationalDescriptorKeepsLegacyDottedTableBehavior() {
        var descriptor = EntitySchemaDescriptor.builder(LegacyQualifiedEntity.class).build();
        var form = descriptor.form();

        assertEquals("sales", descriptor.table().identity().schema().orElseThrow());
        assertEquals("orders", descriptor.table().identity().table());
        assertEquals(descriptor.table().identity(), form.relationIdentity().orElseThrow());
        assertEquals(
                "insert into \"sales\".\"orders\" (\"code\") values (?)",
                FormDataSqlRenderer.create(
                        SqlRenderer.builder().addDefaultTerms().build(),
                        RdbDialect.postgresql())
                        .insert(form, Map.of("code", "A-1"))
                        .sql());
    }

    @Test
    void entityStructureFingerprintIncludesExplicitRelationIdentity() {
        var first = EntitySchemaDescriptor.builder(FirstSchemaEntity.class).build().metadata();
        var second = EntitySchemaDescriptor.builder(SecondSchemaEntity.class).build().metadata();

        assertNotEquals(first.structureFingerprint(), second.structureFingerprint());
    }

    @TableCatalog("tenant")
    @TableName(value = "orders", schema = "app")
    private static final class OrderEntity {

        @TableId
        @TableColumn(databaseTypeId = "BIGINT", nullable = TableColumn.Nullability.NOT_NULL)
        private Long id;

        @TableColumn(databaseTypeId = "VARCHAR", length = 64,
                nullable = TableColumn.Nullability.NOT_NULL)
        private String code;
    }

    @TableName(value = "order.items", schema = "sales.data")
    private static final class SchemaQualifiedEntity {

        @TableColumn(databaseTypeId = "VARCHAR")
        private String code;
    }

    @TableName("sales.orders")
    private static final class LegacyQualifiedEntity {

        @TableColumn(databaseTypeId = "VARCHAR")
        private String code;
    }

    @TableName(value = "orders", schema = "first")
    private static final class FirstSchemaEntity {

        @TableColumn(databaseTypeId = "VARCHAR")
        private String code;
    }

    @TableName(value = "orders", schema = "second")
    private static final class SecondSchemaEntity {

        @TableColumn(databaseTypeId = "VARCHAR")
        private String code;
    }
}
