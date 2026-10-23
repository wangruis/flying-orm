package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import com.flying.orm.rdb.protection.ProtectedContainsLayout;
import com.flying.orm.rdb.protection.ProtectedFormLayout;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemaMetadataFailureBoundaryTest {
    @Test
    void jdbcPrimarySnapshotFailureIsNotAbsence() { assertSnapshotFailure(false, false); }

    @Test
    void reactivePrimarySnapshotFailureIsNotAbsence() { assertSnapshotFailure(true, false); }

    @Test
    void jdbcAuxiliarySnapshotFailureIsNotAbsence() { assertSnapshotFailure(false, true); }

    @Test
    void reactiveAuxiliarySnapshotFailureIsNotAbsence() { assertSnapshotFailure(true, true); }

    private void assertSnapshotFailure(
            boolean reactive, boolean auxiliary) {
        DynamicForm logical = DynamicForm.builder("items", "items")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR").withLength(30))
                .encrypted("secret", EncryptedFieldDefinition.builder()
                        .searchModes(EncryptedSearchMode.CONTAINS).build()).build();
        DynamicForm target = auxiliary ? ProtectedContainsLayout.resolve(logical).orElseThrow().table()
                : DynamicForm.builder("items", "items")
                        .addField(DynamicField.of("name", "VARCHAR").withLength(30)).build();
        RelationalTableDefinition.Builder source = RelationalTableDefinition.builder(target.table());
        target.fields().forEach(field -> source.addColumn(
                "VARCHAR".equalsIgnoreCase(field.dataType()) ? field.withLength(29).column() : field.column()));
        RelationalTableDefinition current = source.build();
        RelationalTableDefinition owner = ProtectedFormLayout.physical(logical).toTableDefinition();
        IllegalArgumentException missing = new IllegalArgumentException("table metadata not found: " + target.table());
        JdbcFormMetadataReader jdbc = new JdbcFormMetadataReader() {
            public DynamicForm readForm(String id, RelationIdentity relation) {
                throw new AssertionError("use physical metadata");
            }

            @Override
            public RelationalTableDefinition readTable(RelationIdentity relation) {
                return readForm(relation.table(), relation).toTableDefinition(relation);
            }

            public RelationalTableDefinition readTableForSchema(RelationIdentity relation) {
                String table = relation.qualifiedName();
                return table.equals(target.table()) ? current : owner;
            }
            public SchemaSnapshot readSnapshot(RelationIdentity relation) {
                throw missing;
            }
        };
        ReactiveFormMetadataReader async = new ReactiveFormMetadataReader() {
            public Mono<DynamicForm> readForm(String id, RelationIdentity relation) {
                return Mono.error(new AssertionError("use physical metadata"));
            }

            @Override
            public Mono<RelationalTableDefinition> readTable(RelationIdentity relation) {
                return readForm(relation.table(), relation).map(form -> form.toTableDefinition(relation));
            }

            public Mono<RelationalTableDefinition> readTableForSchema(RelationIdentity relation) {
                String table = relation.qualifiedName();
                return Mono.just(table.equals(target.table()) ? current : owner);
            }
            public Mono<SchemaSnapshot> readSnapshot(RelationIdentity relation) {
                return Mono.error(missing);
            }
        };
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.mysql());
        SchemaMigrationPlanner planner = renderer.migrationPlanner();
        SchemaMigrationOptions options = SchemaMigrationOptions.safe().allowColumnChange();
        DynamicForm requested = auxiliary ? logical : target;

        assertSame(missing, assertThrows(IllegalArgumentException.class, () -> {
            if (reactive) {
                planner.plan(requested, List.of(), List.of(), async, options).block();
            } else {
                planner.planJdbc(requested, List.of(), List.of(), jdbc, options);
            }
        }));
    }
}
