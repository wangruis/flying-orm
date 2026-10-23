package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReaders;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReaders;
import com.flying.orm.rdb.protection.ProtectedContainsLayout;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FormSchemaIdentityTest {
    @Test
    void bothPlannersBindLiteralNamesWithoutReparsingOrEagerReads() {
        RdbDialect dialect = RdbDialect.postgresql();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(dialect);
        SchemaMigrationPlanner planner = new SchemaMigrationPlanner(renderer);
        List<SqlRequest> reads = new ArrayList<>();
        SyncSqlExecutor jdbc = new com.flying.orm.rdb.support.ListQueryFixture() {
            public List<DynamicRow> query(SqlRequest request) { reads.add(request); return List.of(); }
            public long rowsUpdated(SqlRequest request) { throw new AssertionError("planning must not execute DDL"); }
            public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String column) {
                throw new AssertionError("planning must not write");
            }
        };
        ReactiveSqlExecutor reactive = new ReactiveSqlExecutor() {
            public Flux<DynamicRow> query(SqlRequest request) { reads.add(request); return Flux.empty(); }
            public Mono<Long> rowsUpdated(SqlRequest request) { throw new AssertionError("planning must not execute DDL"); }
        };
        JdbcFormMetadataReader syncReader = JdbcFormMetadataReaders.create(jdbc, dialect);
        ReactiveFormMetadataReader reader = ReactiveFormMetadataReaders.cached(
                ReactiveFormMetadataReaders.create(reactive, dialect));
        for (RelationIdentity identity : List.of(RelationIdentity.table("sales.items"),
                RelationIdentity.of(null, "sales.eu", "items.v2"), RelationIdentity.parse("sales.items"))) {
            DynamicForm form = DynamicForm.relationalBuilder("items", identity)
                    .addField(DynamicField.primaryKey("id", "BIGINT")).build();
            SchemaMigrationPlan sync = planner.planJdbc(form, List.of(), List.of(), syncReader, SchemaMigrationOptions.safe());
            assertEquals(identity, sync.target().identity());
            List<Object> bound = new ArrayList<>();
            bound.add(identity.table());
            identity.schema().ifPresent(bound::add);
            assertEquals(bound, reads.getLast().parameters());
            reads.clear();
            Mono<SchemaMigrationPlan> pending = planner.plan(form, List.of(), List.of(), reader, SchemaMigrationOptions.safe());
            assertTrue(reads.isEmpty());
            for (int subscription = 0; subscription < 2; subscription++) {
                assertEquals(sync.requests(), pending.block().requests());
                assertEquals(bound, reads.getLast().parameters());
            }
            assertEquals(2, reads.size());
            reads.clear();
        }
    }

    @Test
    void protectedAuxiliaryTablesKeepNamespaceInPlanAndInvalidation() {
        RelationIdentity identity = RelationIdentity.of(null, "sales.eu", "items.v2");
        DynamicForm form = DynamicForm.relationalBuilder("items", identity)
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("note", "VARCHAR"))
                .encrypted("note", EncryptedFieldDefinition.builder()
                        .searchModes(EncryptedSearchMode.CONTAINS).build()).build();
        RelationIdentity side = ProtectedContainsLayout.resolve(form).orElseThrow().table().relationIdentity();
        SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(RdbDialect.postgresql()).createTablePlan(form, List.of());
        assertEquals(List.of(side), plan.additionalCreatedTables());
        assertEquals(List.of(identity, side), SchemaMigrationSupport.metadataRelations(plan));
        assertEquals(List.of(identity, side), SchemaMigrationSupport.metadataTables(form));
        assertTrue(plan.operations().stream().allMatch(operation -> operation.relation().schema().equals(identity.schema())));
    }

    @Test
    void createAndAlterUseTheSameSegmentedIdentityAsCrud() {
        RdbDialect dialect = RdbDialect.postgresql();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(dialect);
        for (RelationIdentity identity : List.of(RelationIdentity.table("sales.items"),
                RelationIdentity.of(null, "sales.eu", "items.v2"))) {
            DynamicForm source = DynamicForm.relationalBuilder("items", identity)
                    .addField(DynamicField.primaryKey("id", "BIGINT")).build();
            DynamicForm target = DynamicForm.relationalBuilder("items", identity)
                    .addField(DynamicField.primaryKey("id", "BIGINT"))
                    .addField(DynamicField.of("note", "VARCHAR")).build();
            String table = dialect.schema().relationIdentifier(identity);
            SchemaMigrationPlan create = renderer.createTablePlan(source, List.of());
            assertEquals(identity, create.target().identity());
            assertTrue(create.requests().getFirst().sql().startsWith("create table " + table + " ("));
            assertTrue(renderer.migrate(source.diffTo(target)).getFirst().sql()
                    .startsWith("alter table " + table + " add column "));
            SchemaMigrationPlan alter = renderer.migrateSafelyPlan(source.toTableDefinition(), target, List.of());
            assertEquals(identity, alter.target().identity());
            assertTrue(alter.requests().getFirst().sql().startsWith("alter table " + table + " add column "));
        }
    }
}
