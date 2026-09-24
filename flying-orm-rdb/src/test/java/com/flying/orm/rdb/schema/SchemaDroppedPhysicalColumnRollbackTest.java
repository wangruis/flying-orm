package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ColumnMetadata;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaDroppedPhysicalColumnRollbackTest {

    @TestFactory
    Stream<DynamicTest> dropOnlyReviewLoadsThePhysicalDefaultForEveryDialect() {
        return Stream.of(RdbDialect.postgresql(), RdbDialect.mysql(), RdbDialect.h2(),
                        RdbDialect.oracle(), RdbDialect.sqlServer())
                .map(dialect -> DynamicTest.dynamicTest(dialect.name(), () -> {
                    SchemaMigrationPlanner planner = FormSchemaSqlRenderer.create(dialect).migrationPlanner();
                    DynamicForm source = DynamicForm.builder("items", "items")
                            .addField(DynamicField.of("id", "INTEGER"))
                            .addField(DynamicField.of("code", "INTEGER")).build();
                    DynamicForm target = DynamicForm.builder("items", "items")
                            .addField(DynamicField.of("id", "INTEGER")).build();
                    SchemaSnapshot snapshot = SchemaSnapshot.builder(RelationIdentity.table("items"))
                            .tablePresent().physicalColumns(List.of(
                                    ColumnDefinition.builder("id", "INTEGER").build(),
                                    ColumnDefinition.builder("code", "INTEGER")
                                            .defaultValue(ColumnDefault.literal(7)).build())).build();
                    AtomicInteger reads = new AtomicInteger();
                    ReactiveFormMetadataReader reader = reader(source, snapshot, reads);
                    SchemaMigrationOptions options = SchemaMigrationOptions.safe().allowDropColumn();

                    planner.plan(target, List.of(), List.of(), reader, options).block();
                    assertEquals(0, reads.get(), "plain forward plans need no rollback snapshot");
                    ReviewedSchemaMigrationPlan reviewed = planner.review(target, List.of(), List.of(), reader,
                            options, SchemaMigrationReviewPolicy.allowBlocking()).block();
                    assertTrue(reviewed.rollback().requests().stream().map(SqlRequest::sql)
                            .map(sql -> sql.toLowerCase(Locale.ROOT)).anyMatch(sql -> sql.contains("default 7")),
                            reviewed.rollback().requests().toString());
                    assertEquals(1, reads.get());
                    assertThrows(IllegalStateException.class, () -> planner.review(target, List.of(), List.of(),
                            reader(source, null, reads), options, SchemaMigrationReviewPolicy.allowBlocking()).block());
                }));
    }

    private static ReactiveFormMetadataReader reader(DynamicForm source, SchemaSnapshot snapshot,
                                                     AtomicInteger reads) {
        return new ReactiveFormMetadataReader() {
            public Mono<DynamicForm> readForm(String id, String table) { return Mono.just(source); }
            public Mono<DynamicForm> readForm(String id, String schema, String table) { return Mono.just(source); }
            public Mono<SchemaSnapshot> readSnapshot(String table) {
                reads.incrementAndGet();
                return Mono.justOrEmpty(snapshot);
            }
        };
    }

    @Test
    void restoresOracleCharacterLengthUnits() {
        for (String unit : List.of("CHAR", "BYTE")) {
            List<String> rollback = review(RdbDialect.oracle(),
                    ColumnDefinition.builder("code", "VARCHAR2(10 " + unit + ")").build(),
                    ColumnMetadata.of("code", "VARCHAR2").withLength(10));
            assertTrue(rollback.stream().anyMatch(sql -> sql.contains("varchar2(10 "
                    + unit.toLowerCase(Locale.ROOT) + ")")), rollback.toString());
        }
    }

    @Test
    void restoresKnownDroppedColumnDefaultCharsetCollationAndComment() {
        ColumnDefinition removed = ColumnDefinition.builder("code", "VARCHAR")
                .length(16).nullable(false).defaultValue(ColumnDefault.literal("pending"))
                .charset("utf8mb4").collation("utf8mb4_bin").comment("keep code").build();

        List<String> rollback = review(RdbDialect.mysql(), removed,
                ColumnMetadata.of("code", "VARCHAR").withLength(16).withNullable(false).withComment("keep code"));

        String restored = rollback.stream().filter(sql -> sql.contains("add column `code`")).findFirst().orElseThrow();
        assertTrue(restored.contains("default 'pending'"), restored);
        assertTrue(restored.contains("character set `utf8mb4`"), restored);
        assertTrue(restored.contains("collate `utf8mb4_bin`"), restored);
        assertTrue(restored.contains("comment 'keep code'"), restored);
    }

    @Test
    void doesNotReinterpretPhysicalTimestampAsLogicalTimestamp() {
        ColumnDefinition removed = ColumnDefinition.builder("recorded", "TIMESTAMP")
                .temporalPrecision(6).defaultValue(ColumnDefault.currentTimestamp()).build();

        List<String> rollback = review(RdbDialect.mysql(), removed,
                ColumnMetadata.of("recorded", "TIMESTAMP").withPrecision(6, null));

        assertTrue(rollback.stream().anyMatch(sql -> sql.contains("add column `recorded` timestamp(6)")
                && sql.contains("default current_timestamp(6)")), rollback.toString());
    }

    @Test
    void restoresKnownDroppedColumnSeparateComment() {
        ColumnDefinition removed = ColumnDefinition.builder("code", "VARCHAR")
                .length(16).defaultValue(ColumnDefault.literal("pending")).comment("keep code").build();

        List<String> rollback = review(RdbDialect.postgresql(), removed,
                ColumnMetadata.of("code", "VARCHAR").withLength(16).withComment("keep code"));

        assertTrue(rollback.stream().anyMatch(sql -> sql.contains("add column \"code\" varchar(16)")
                && sql.contains("default e'pending'")), rollback.toString());
        assertTrue(rollback.contains("comment on column \"items\".\"code\" is e'keep code'"), rollback.toString());
    }

    @Test
    void restoresLogicalOffsetTimeMarkerWithPhysicalTextStorage() {
        ColumnDefinition removed = ColumnDefinition.builder("scheduled", "VARCHAR")
                .length(32).comment("keep schedule").build();

        List<String> rollback = review(RdbDialect.mysql(), removed,
                ColumnMetadata.of("scheduled", "OFFSET_TIME").withComment("keep schedule"));

        assertTrue(rollback.stream().anyMatch(sql -> sql.contains("add column `scheduled` varchar(32)")
                && sql.contains("comment '[[flying-orm:v1:offset_time]]keep schedule'")), rollback.toString());
    }

    @Test
    void restoresSqlServerNamedDefaultAndCollation() {
        ColumnDefinition removed = ColumnDefinition.builder("code", "NVARCHAR")
                .length(16).defaultValue(ColumnDefault.literal("pending"))
                .defaultConstraintName("df_code").collation("Latin1_General_100_BIN2").build();

        List<String> rollback = review(RdbDialect.sqlServer(), removed,
                ColumnMetadata.of("code", "NVARCHAR").withLength(16));

        assertTrue(rollback.stream().anyMatch(sql -> sql.contains("add [code] nvarchar(16)")
                && sql.contains("collate latin1_general_100_bin2")
                && sql.contains("constraint [df_code] default n'pending'")), rollback.toString());
    }

    @Test
    void restoresKnownSeparateCommentWithoutPhysicalSnapshot() {
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.postgresql());
        TableMetadata current = TableMetadata.builder("items")
                .addColumn(ColumnMetadata.of("label", "VARCHAR").withLength(16))
                .addColumn(ColumnMetadata.of("code", "VARCHAR").withLength(16).withComment("keep code"))
                .build();
        DynamicForm target = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("label", "VARCHAR").withLength(16)).build();
        SchemaMigrationPlan migration = renderer.migrateSafelyPlan(current, target, List.of(),
                SchemaMigrationOptions.safe().allowDropColumn());

        List<String> rollback = SchemaMigrationReviewer.create(renderer)
                .review(current, migration, SchemaMigrationReviewPolicy.allowBlocking())
                .rollback().requests().stream().map(SqlRequest::sql).toList();

        assertTrue(rollback.contains("comment on column \"items\".\"code\" is E'keep code'"), rollback.toString());
    }

    private static List<String> review(RdbDialect dialect, ColumnDefinition removed, ColumnMetadata oldColumn) {
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(dialect);
        TableMetadata current = TableMetadata.builder("items")
                .addColumn(ColumnMetadata.of("label", "VARCHAR").withLength(16))
                .addColumn(oldColumn).build();
        DynamicForm target = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("label", "VARCHAR").withLength(32)).build();
        SchemaSnapshot snapshot = SchemaSnapshot.builder(RelationIdentity.of(null, null, "items")).tablePresent()
                .physicalColumns(List.of(ColumnDefinition.builder("label", "VARCHAR").length(16).build(), removed))
                .build();
        SchemaMigrationPlan migration = renderer.migrationPlanner().migrateSafelyPlan(
                current, target, List.of(), List.of(), SchemaMigrationOptions.safe().allowDropColumn(), snapshot);
        return SchemaMigrationReviewer.create(renderer)
                .review(current, migration, SchemaMigrationReviewPolicy.allowBlocking(), snapshot)
                .rollback().requests().stream().map(SqlRequest::sql)
                .map(sql -> sql.toLowerCase(Locale.ROOT)).toList();
    }
}
