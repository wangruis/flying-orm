package com.flying.orm.rdb.mapping;

import com.flying.orm.core.annotation.TableCatalog;
import com.flying.orm.core.annotation.TableColumn;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.RelationalFormDefinition;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationalMetadataFingerprint;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.List;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EntityDynamicFormRelationalParityTest {

    @Test
    void entityAndFormShareTheirCompiledField() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            EntityMetadata<OrderEntity> metadata = models.metadata(OrderEntity.class);
            EntityFieldMetadata field = metadata.field("code");
            assertSame(metadata.toDynamicForm().field("code"), field.field());
            assertFalse(metadata.field("id").nullable());
        }
    }

    @Test
    void entityFormAndSchemaShareTheCompleteColumnDeclaration() {
        EntitySchemaDescriptor<CompleteColumnsEntity> descriptor =
                EntitySchemaDescriptor.builder(CompleteColumnsEntity.class).build();
        for (ColumnDefinition column : descriptor.table().columns()) {
            assertSame(column, descriptor.form().field(column.name()).column());
            assertSame(column, descriptor.metadata().field(column.name()).field().column());
        }
        ColumnDefinition code = descriptor.form().field("code").column();
        assertEquals("VARCHAR", code.codecId());
        assertEquals("display code", code.comment());
        assertEquals("utf8mb4", code.charset());
        assertEquals("utf8mb4_bin", code.collation());
        ColumnDefinition createdAt = descriptor.form().field("created_at").column();
        assertEquals(ColumnDefault.currentTimestamp(), createdAt.defaultValue());
        assertEquals(0, createdAt.temporalPrecision());
    }

    @Test
    void ordinaryCrudCompilesTheSameDeclaredColumnsAsSchema() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            EntityMetadata<CompleteColumnsEntity> ordinary = models.metadata(CompleteColumnsEntity.class);
            EntitySchemaDescriptor<CompleteColumnsEntity> schema = models.schemaDescriptor(CompleteColumnsEntity.class);
            for (EntityFieldMetadata field : ordinary.fields()) {
                assertEquals(schema.table().column(field.columnName()), field.field().column());
            }
        }
    }

    @Test
    void temporalPrecisionAliasHasTheSameMeaningInCrudAndSchema() {
        EntitySchemaDescriptor<TemporalPrecisionEntity> descriptor =
                EntitySchemaDescriptor.builder(TemporalPrecisionEntity.class).build();
        assertEquals(0, descriptor.form().field("created_at").precision());
        assertEquals(0, descriptor.table().column("created_at").temporalPrecision());
    }

    @Test
    void compiledFieldRetainsDeclaredGenericTypeInBothEntryPoints() throws Exception {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            EntityFieldMetadata ordinary = models.metadata(GenericColumnsEntity.class).field("values");
            EntityFieldMetadata schema = models.schemaDescriptor(GenericColumnsEntity.class).metadata().field("values");
            assertEquals(GenericColumnsEntity.class.getDeclaredField("values").getGenericType(), ordinary.javaType());
            assertEquals(ordinary.javaType(), schema.javaType());
        }
    }

    @Test
    void publishesCrudAndSchemaFromOneEntityIdentity() {
        var descriptor = EntitySchemaDescriptor.builder(OrderEntity.class).build();
        var form = descriptor.form();
        var table = descriptor.table();

        assertEquals("orders", descriptor.metadata().table());
        assertEquals("tenant.app.orders", form.table());
        assertEquals(table.identity(), form.relationIdentity());
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
            equivalent.addField(form.fields().get(index));
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

        assertEquals("sales.data.order.items", form.table());
        assertEquals("sales.data", form.relationIdentity().schema().orElseThrow());
        assertEquals("order.items", form.relationIdentity().table());
        assertEquals(
                "insert into \"sales.data\".\"order.items\" (\"code\") values (?)",
                FormDataSqlRenderer.create(
                        SqlRenderer.builder().addDefaultTerms().build(),
                        RdbDialect.postgresql())
                        .insert(form, Map.of("code", "A-1"))
                        .sql());
    }

    @Test
    void qualifiedNamesUseTheSameIdentityForCrudAndSchema() {
        DynamicForm form;
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            form = models.metadata(LegacyQualifiedEntity.class).toDynamicForm();
        }

        assertEquals(EntitySchemaDescriptor.builder(LegacyQualifiedEntity.class).build().table().identity(),
                form.relationIdentity());
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
        assertEquals(descriptor.table().identity(), form.relationIdentity());
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

    private static final class CompleteColumnsEntity {
        @TableColumn(length = 64, comment = "display code", charset = "utf8mb4", collation = "utf8mb4_bin")
        private String code;

        @TableColumn(temporalPrecision = 0, defaultId = "CURRENT_TIMESTAMP")
        private LocalDateTime createdAt;
    }

    private static final class TemporalPrecisionEntity {
        @TableColumn(precision = 0)
        private LocalDateTime createdAt;
    }

    private static final class GenericColumnsEntity {
        private Map<String, List<Long>> values;
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
