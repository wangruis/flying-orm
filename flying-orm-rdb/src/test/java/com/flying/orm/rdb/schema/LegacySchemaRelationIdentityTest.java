package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReaders;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LegacySchemaRelationIdentityTest {

    private final FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.postgresql());

    @Test
    void createEntrypointsRejectLossyStructuredIdentities() {
        assertAll(lossyIdentities().stream().map(identity -> () -> {
            DynamicForm form = form(identity, false);
            assertAll(identity.toString(),
                    () -> assertRelationalRequired(() -> renderer.createTable(form)),
                    () -> assertRelationalRequired(() -> renderer.createTablePlan(form, List.of())));
        }));
    }

    @Test
    void migrationEntrypointsRejectLossyStructuredIdentities() {
        assertAll(lossyIdentities().stream().map(identity -> () -> {
            DynamicForm source = form(identity, false);
            DynamicForm target = form(identity, true);
            assertAll(identity.toString(),
                    () -> assertRelationalRequired(() -> renderer.migrate(source.diffTo(target))),
                    () -> assertRelationalRequired(() -> renderer.migrateSafelyPlan(
                            source.toTableMetadata(), target, List.of())));
        }));
    }

    @Test
    void reactivePlanAndReviewRejectBeforeReadingMetadata() {
        // 覆盖 ReactiveSchemaClient.planCreateOrAlter/reviewCreateOrAlter 共用的规划入口。
        SchemaMigrationPlanner planner = new SchemaMigrationPlanner(renderer);
        assertAll(lossyIdentities().stream().map(identity -> () -> {
            DynamicForm form = form(identity, false);
            MissingReactiveReader reader = new MissingReactiveReader();
            assertAll(identity.toString(),
                    () -> assertRelationalRequired(() -> planner.plan(form, List.of(), List.of(),
                            reader, SchemaMigrationOptions.safe()).block()),
                    () -> assertRelationalRequired(() -> planner.review(form, List.of(), List.of(), reader,
                            SchemaMigrationOptions.safe(), SchemaMigrationReviewPolicy.allowBlocking()).block()),
                    () -> assertEquals(List.of(), reader.tables));
        }));
    }

    @Test
    void jdbcPlanAndReviewRejectBeforeReadingMetadata() {
        assertAll(lossyIdentities().stream().map(identity -> () -> {
            DynamicForm form = form(identity, false);
            EmptyMetadataExecutor executor = new EmptyMetadataExecutor();
            JdbcFormMetadataReader reader = JdbcFormMetadataReaders.create(executor, RdbDialect.postgresql());
            JdbcSchemaClient client = JdbcSchemaClient.create(executor, renderer);
            assertAll(identity.toString(),
                    () -> assertRelationalRequired(() -> client.planCreateOrAlter(form, List.of(), reader)),
                    () -> assertRelationalRequired(() -> client.reviewCreateOrAlter(form, List.of(), List.of(),
                            reader, SchemaMigrationOptions.safe(), SchemaMigrationReviewPolicy.allowBlocking())),
                    () -> assertEquals(0, executor.queries));
        }));
    }

    @Test
    void rendererPreservesLegacyQualifiedNamesAndLosslessStructuredNames() {
        for (DynamicForm source : losslessForms(false)) {
            DynamicForm target = source.relationIdentity()
                    .map(identity -> form(identity, true))
                    .orElseGet(() -> legacyForm(true));
            String table = source.relationIdentity().isPresent() ? "\"items\"" : "\"tenant_schema\".\"items\"";
            assertAll(source.table(),
                    () -> assertTrue(renderer.createTable(source).getFirst().sql()
                            .startsWith("create table " + table + " (")),
                    () -> assertTrue(renderer.createTablePlan(source, List.of()).requests().getFirst().sql()
                            .startsWith("create table " + table + " (")),
                    () -> assertTrue(renderer.migrate(source.diffTo(target)).getFirst().sql()
                            .startsWith("alter table " + table + " add column ")),
                    () -> assertTrue(renderer.migrateSafelyPlan(source.toTableMetadata(), target, List.of())
                            .requests().getFirst().sql().startsWith("alter table " + table + " add column ")));
        }
    }

    @Test
    void plannersStillReadMetadataForLosslessForms() {
        SchemaMigrationPlanner reactive = new SchemaMigrationPlanner(renderer);
        for (DynamicForm form : losslessForms(false)) {
            MissingReactiveReader reactiveReader = new MissingReactiveReader();
            assertTrue(reactive.plan(form, List.of(), List.of(), reactiveReader,
                    SchemaMigrationOptions.safe()).block().hasExecutableSql());
            assertTrue(reactive.review(form, List.of(), List.of(), reactiveReader, SchemaMigrationOptions.safe(),
                    SchemaMigrationReviewPolicy.allowBlocking()).block().migration().hasExecutableSql());
            assertEquals(List.of(form.table(), form.table()), reactiveReader.tables);

            EmptyMetadataExecutor executor = new EmptyMetadataExecutor();
            JdbcFormMetadataReader jdbcReader = JdbcFormMetadataReaders.create(executor, RdbDialect.postgresql());
            JdbcSchemaClient jdbc = JdbcSchemaClient.create(executor, renderer);
            assertTrue(jdbc.planCreateOrAlter(form, List.of(), jdbcReader).hasExecutableSql());
            assertTrue(jdbc.reviewCreateOrAlter(form, List.of(), List.of(), jdbcReader,
                    SchemaMigrationOptions.safe(), SchemaMigrationReviewPolicy.allowBlocking())
                    .migration().hasExecutableSql());
            assertEquals(2, executor.queries);
        }
    }

    private static void assertRelationalRequired(Executable action) {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, action);
        assertTrue(failure.getMessage().contains("relational"), failure::getMessage);
    }

    private static List<RelationIdentity> lossyIdentities() {
        return List.of(RelationIdentity.of(null, "tenant_schema", "items"),
                RelationIdentity.of("tenant_catalog", null, "items"),
                RelationIdentity.table("tenant_schema.items"));
    }

    private static List<DynamicForm> losslessForms(boolean additionalColumn) {
        return List.of(legacyForm(additionalColumn), form(RelationIdentity.table("items"), additionalColumn));
    }

    private static DynamicForm legacyForm(boolean additionalColumn) {
        return fields(DynamicForm.builder("items", "tenant_schema.items"), additionalColumn);
    }

    private static DynamicForm form(RelationIdentity identity, boolean additionalColumn) {
        return fields(DynamicForm.relationalBuilder("items", identity), additionalColumn);
    }

    private static DynamicForm fields(DynamicForm.Builder builder, boolean additionalColumn) {
        builder.addField(DynamicField.primaryKey("id", "BIGINT"));
        if (additionalColumn) {
            builder.addField(DynamicField.of("note", "VARCHAR"));
        }
        return builder.build();
    }

    private static final class MissingReactiveReader implements ReactiveFormMetadataReader {
        private final List<String> tables = new ArrayList<>();

        @Override
        public Mono<DynamicForm> readForm(String formId, String table) {
            tables.add(table);
            return Mono.error(new IllegalArgumentException("table metadata not found"));
        }

        @Override
        public Mono<DynamicForm> readForm(String formId, String schema, String table) {
            throw new AssertionError("legacy planner must use its String metadata entrypoint");
        }
    }

    private static final class EmptyMetadataExecutor implements SyncSqlExecutor {
        private int queries;

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            queries++;
            return List.of();
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            throw new AssertionError("planning must not execute DDL");
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
            throw new AssertionError("planning must not execute DDL");
        }
    }
}
