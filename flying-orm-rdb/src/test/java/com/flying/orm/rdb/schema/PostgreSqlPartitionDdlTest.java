package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.IndexKeyPart;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.TablePartitionDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PostgreSqlPartitionDdlTest {

    @Test
    void rendersAQuotedRangeClauseWhenEveryUniqueKeyContainsThePartitionColumn() {
        RelationalTableDefinition table = base()
                .primaryKey(PrimaryKeyDefinition.of("pk_job_events", "id", "occurred_at"))
                .addUnique(UniqueConstraintDefinition.of(
                        "uk_job_events_tenant", "tenant_id", "occurred_at"))
                .addIndex(IndexDefinition.builder("ux_job_events_external")
                        .unique()
                        .addKey(IndexKeyPart.asc("external_id"))
                        .addKey(IndexKeyPart.desc("occurred_at"))
                        .build())
                .addIndex(IndexDefinition.builder("ix_job_events_tenant")
                        .addKey(IndexKeyPart.asc("tenant_id"))
                        .build())
                .build();

        List<String> sql = RelationalSchemaSqlRenderer.create(RdbDialect.postgresql().schema())
                .render(create(table)).stream().map(request -> request.sql()).toList();

        String createTable = sql.stream()
                .filter(statement -> statement.startsWith("create table"))
                .findFirst()
                .orElseThrow();
        assertTrue(createTable.contains("partition by range (\"occurred_at\")"));
        assertEquals(4, sql.size());
    }

    @Test
    void rejectsEveryUniqueShapeThatOmitsThePartitionColumnBeforePublishingRequests() {
        List<RelationalTableDefinition> invalid = List.of(
                base().primaryKey(PrimaryKeyDefinition.of("pk_job_events", "id")).build(),
                base().addUnique(UniqueConstraintDefinition.of(
                        "uk_job_events_tenant", "tenant_id")).build(),
                base().addIndex(IndexDefinition.builder("ux_job_events_external")
                        .unique().addKey(IndexKeyPart.asc("external_id")).build()).build());

        for (RelationalTableDefinition table : invalid) {
            Map<String, String> sequences = new LinkedHashMap<>();
            assertThrows(UnsupportedOperationException.class,
                    () -> RelationalSchemaSqlRenderer.create(RdbDialect.postgresql().schema())
                            .render(create(table), sequences));
            assertTrue(sequences.isEmpty());
        }
    }

    @Test
    void everyNonPostgresqlDialectRejectsAPartitionedTable() {
        RelationalTableDefinition table = base()
                .primaryKey(PrimaryKeyDefinition.of("pk_job_events", "id", "occurred_at"))
                .build();

        for (SchemaDialect dialect : List.of(
                SchemaDialect.standard(),
                RdbDialect.h2().schema(),
                RdbDialect.mysql().schema(),
                RdbDialect.oracle().schema(),
                RdbDialect.sqlServer().schema())) {
            assertThrows(UnsupportedOperationException.class,
                    () -> RelationalSchemaSqlRenderer.create(dialect).render(create(table)));
        }
    }

    @Test
    void existingPartitionedParentRejectsInvalidUniqueEvolutionWithoutPublishingSql() {
        RelationalTableDefinition actual = validBase().build();
        List<RelationalTableDefinition> invalid = List.of(
                validBase().addUnique(UniqueConstraintDefinition.of(
                        "uk_job_events_tenant", "tenant_id")).build(),
                validBase().addIndex(IndexDefinition.builder("ux_job_events_external")
                        .unique().addKey(IndexKeyPart.asc("external_id")).build()).build());
        RdbDialect dialect = RdbDialect.postgresql();
        DatabaseDescriptor database = DatabaseDescriptor.of("PostgreSQL", "16", dialect);

        for (RelationalTableDefinition desired : invalid) {
            ReviewedSchemaPlan plan = RelationalSchemaPlanReviewer.create(dialect).review(
                    database,
                    desired,
                    SchemaSnapshot.present(actual),
                    SchemaSnapshotCoverage.complete(),
                    SchemaCompatibilityMode.EXACT);

            assertTrue(plan.requiresManualAction());
            assertTrue(plan.requests().isEmpty());
            assertEquals(List.of(SchemaOperation.Kind.VERIFY_MANUALLY),
                    plan.operations().stream().map(SchemaOperation::kind).toList());
            assertEquals("table-partition", plan.operations().getFirst().objectName());
        }
    }

    private static RelationalTableDefinition.Builder base() {
        return RelationalTableDefinition.builder(RelationIdentity.of(null, "jobs", "job_events"))
                .addColumn(ColumnDefinition.builder("id", "BIGINT")
                        .nullable(false)
                        .generation(ValueGeneration.sequence("jobs.job_event_id_seq"))
                        .build())
                .addColumn(ColumnDefinition.builder("tenant_id", "BIGINT").nullable(false).build())
                .addColumn(ColumnDefinition.builder("external_id", "VARCHAR").length(64).build())
                .addColumn(ColumnDefinition.builder("occurred_at", "TIMESTAMP").nullable(false).build())
                .partition(TablePartitionDefinition.range("occurred_at"));
    }

    private static RelationalTableDefinition.Builder validBase() {
        return base().primaryKey(PrimaryKeyDefinition.of(
                "pk_job_events", "id", "occurred_at"));
    }

    private static SchemaOperation create(RelationalTableDefinition table) {
        return SchemaOperation.of(
                SchemaOperation.Kind.CREATE_TABLE,
                table.identity(),
                table.identity().table(),
                null,
                table,
                SchemaOperation.Compatibility.REQUIRES_REVIEW);
    }
}
