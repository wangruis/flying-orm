package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.IndexKeyPart;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalSchemaDefinition;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.TablePartitionDefinition;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.DialectCapabilities;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MultiTableSchemaDependencyOrderTest {

    @Test
    void createsReferencedTablesBeforeTheirDependents() {
        RelationalTableDefinition customers = table("customers", null);
        RelationalTableDefinition orders = table("orders", "customers");
        RelationalTableDefinition lineItems = table("line_items", "orders");
        DatabaseDescriptor database = database();

        MultiTableSchemaPlanner.Plan plan = new MultiTableSchemaPlanner(
                database, MultiTableSchemaPlanner.ForeignKeyCycleSupport.SUPPORTED)
                .plan(RelationalSchemaDefinition.of(List.of(lineItems, customers, orders)));

        assertSame(database, plan.database());
        assertEquals(List.of("customers", "orders", "line_items"),
                     plan.firstPhase().stream().map(operation -> operation.relation().table()).toList());
        assertEquals(List.of(SchemaOperation.Kind.CREATE_TABLE,
                             SchemaOperation.Kind.CREATE_TABLE,
                             SchemaOperation.Kind.CREATE_TABLE),
                     plan.firstPhase().stream().map(SchemaOperation::kind).toList());
        assertEquals(List.of("fk_orders_customers", "fk_line_items_orders"),
                     plan.secondPhase().stream().map(SchemaOperation::objectName).toList());
    }

    @Test
    void rejectsCrossTableForeignKeyNameCollisionsInSchemaScopedDialects() {
        RelationalTableDefinition parent = table("parents", null);
        RelationalTableDefinition first = dependentTable("first_children", "fk_shared_parent");
        RelationalTableDefinition second = dependentTable("second_children", "fk_shared_parent");
        RelationalSchemaDefinition schema = RelationalSchemaDefinition.of(List.of(parent, first, second));

        for (RdbDialect dialect : List.of(
                RdbDialect.mysql(), RdbDialect.oracle(), RdbDialect.sqlServer())) {
            DatabaseDescriptor database = DatabaseDescriptor.of(dialect.name(), "test", dialect);
            assertThrows(IllegalArgumentException.class, () -> new MultiTableSchemaPlanner(
                    database, MultiTableSchemaPlanner.ForeignKeyCycleSupport.SUPPORTED).plan(schema));
        }
    }

    @Test
    void rejectsCrossTableIndexNameCollisionsInSchemaScopedDialects() {
        RelationalSchemaDefinition schema = RelationalSchemaDefinition.of(List.of(
                indexedTable("first_events", "idx_shared_code"),
                indexedTable("second_events", "idx_shared_code")));

        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.oracle())) {
            DatabaseDescriptor database = DatabaseDescriptor.of(dialect.name(), "test", dialect);
            assertThrows(IllegalArgumentException.class, () -> new MultiTableSchemaPlanner(
                    database, MultiTableSchemaPlanner.ForeignKeyCycleSupport.SUPPORTED).plan(schema));
        }
    }

    @Test
    void preservesTableCommentsInTheFirstPhaseCreateOperation() {
        RelationalTableDefinition customers = RelationalTableDefinition.builder(
                        RelationIdentity.table("customers"))
                .comment("customer master")
                .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
                .primaryKey(PrimaryKeyDefinition.of("pk_customers", "id"))
                .build();

        SchemaOperation create = new MultiTableSchemaPlanner(
                database(), MultiTableSchemaPlanner.ForeignKeyCycleSupport.SUPPORTED)
                .plan(RelationalSchemaDefinition.of(List.of(customers)))
                .firstPhase().getFirst();

        RelationalTableDefinition planned = (RelationalTableDefinition) create.desired();
        assertEquals("customer master", planned.comment());
    }

    @Test
    void createsAllReferencedUniqueIndexesBeforeClosingAForeignKeyCycle() {
        RelationalTableDefinition first = indexedCycleTable("cycle_a", "cycle_b");
        RelationalTableDefinition second = indexedCycleTable("cycle_b", "cycle_a");
        for (List<RelationalTableDefinition> tables : List.of(List.of(first, second), List.of(second, first))) {
            MultiTableSchemaPlanner.Plan plan = new MultiTableSchemaPlanner(
                    postgresDatabase(), MultiTableSchemaPlanner.ForeignKeyCycleSupport.SUPPORTED)
                    .plan(RelationalSchemaDefinition.of(tables));

            assertEquals(List.of(SchemaOperation.Kind.ADD_INDEX, SchemaOperation.Kind.ADD_INDEX,
                            SchemaOperation.Kind.ADD_FOREIGN_KEY, SchemaOperation.Kind.ADD_FOREIGN_KEY),
                    plan.secondPhase().stream().map(SchemaOperation::kind).toList());
            assertTrue(plan.operations().stream().allMatch(operation -> !RelationalSchemaSqlRenderer
                    .create(RdbDialect.postgresql().schema()).render(operation).isEmpty()));
        }
    }

    private static RelationalTableDefinition indexedCycleTable(String name, String dependency) {
        return RelationalTableDefinition.builder(RelationIdentity.table(name))
                .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
                .addColumn(ColumnDefinition.builder("code", "BIGINT").nullable(false).build())
                .addColumn(ColumnDefinition.builder("parent_code", "BIGINT").build())
                .primaryKey(PrimaryKeyDefinition.of("pk_" + name, "id"))
                .addIndex(IndexDefinition.builder("ux_" + name).unique().addKey(IndexKeyPart.asc("code")).build())
                .addForeignKey(ForeignKeyDefinition.builder("fk_" + name)
                        .addColumn("parent_code").reference(RelationIdentity.table(dependency))
                        .addReferenceColumn("code").build())
                .build();
    }

    @Test
    void preservesPartitionMetadataInTheFirstPhaseCreateOperation() {
        RelationalTableDefinition events = RelationalTableDefinition.builder(
                        RelationIdentity.table("job_events"))
                .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
                .addColumn(ColumnDefinition.builder("occurred_at", "TIMESTAMP").nullable(false).build())
                .primaryKey(PrimaryKeyDefinition.of("pk_job_events", "id", "occurred_at"))
                .partition(TablePartitionDefinition.range("occurred_at"))
                .build();

        SchemaOperation create = new MultiTableSchemaPlanner(
                database(), MultiTableSchemaPlanner.ForeignKeyCycleSupport.SUPPORTED)
                .plan(RelationalSchemaDefinition.of(List.of(events)))
                .firstPhase().getFirst();

        RelationalTableDefinition planned = (RelationalTableDefinition) create.desired();
        assertEquals(events.partition(), planned.partition());
    }

    @Test
    void rejectsAnInvalidPartitionedUniqueIndexBeforePublishingAPlan() {
        RelationalTableDefinition events = partitionedEvents()
                .addIndex(IndexDefinition.builder("ux_job_events_external")
                        .unique()
                        .addKey(IndexKeyPart.asc("external_id"))
                        .build())
                .build();

        assertThrows(UnsupportedOperationException.class, () -> new MultiTableSchemaPlanner(
                postgresDatabase(), MultiTableSchemaPlanner.ForeignKeyCycleSupport.SUPPORTED)
                .plan(RelationalSchemaDefinition.of(List.of(events))));
    }

    @Test
    void publishesALegalPartitionedUniqueIndexThatTheSecondPhaseCanRender() {
        RelationalTableDefinition events = partitionedEvents()
                .addIndex(IndexDefinition.builder("ux_job_events_external")
                        .unique()
                        .addKey(IndexKeyPart.asc("external_id"))
                        .addKey(IndexKeyPart.asc("occurred_at"))
                        .build())
                .build();
        MultiTableSchemaPlanner.Plan plan = new MultiTableSchemaPlanner(
                postgresDatabase(), MultiTableSchemaPlanner.ForeignKeyCycleSupport.SUPPORTED)
                .plan(RelationalSchemaDefinition.of(List.of(events)));

        assertEquals(events.partition(),
                ((RelationalTableDefinition) plan.firstPhase().getFirst().desired()).partition());
        assertEquals(List.of(SchemaOperation.Kind.ADD_INDEX),
                plan.secondPhase().stream().map(SchemaOperation::kind).toList());
        assertTrue(RelationalSchemaSqlRenderer.create(RdbDialect.postgresql().schema())
                .render(plan.secondPhase().getFirst()).getFirst().sql()
                .startsWith("create unique index"));
    }

    @Test
    void requiresReviewWhenCreateUsesAnUnsupportedGenerationStrategy() {
        RelationalTableDefinition generated = RelationalTableDefinition.builder(
                        RelationIdentity.table("generated_ids"))
                .addColumn(ColumnDefinition.builder("id", "BIGINT")
                        .nullable(false)
                        .generation(ValueGeneration.identity())
                        .build())
                .primaryKey(PrimaryKeyDefinition.of("pk_generated_ids", "id"))
                .build();

        SchemaOperation create = new MultiTableSchemaPlanner(
                database(), MultiTableSchemaPlanner.ForeignKeyCycleSupport.SUPPORTED)
                .plan(RelationalSchemaDefinition.of(List.of(generated)))
                .firstPhase().getFirst();

        assertEquals(SchemaOperation.Compatibility.REQUIRES_REVIEW, create.compatibility());
    }

    private static RelationalTableDefinition table(String name, String dependency) {
        RelationalTableDefinition.Builder table = RelationalTableDefinition.builder(RelationIdentity.table(name))
                .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
                .addColumn(ColumnDefinition.builder("parent_id", "BIGINT").build())
                .primaryKey(PrimaryKeyDefinition.of("pk_" + name, "id"));
        if (dependency != null) {
            table.addForeignKey(ForeignKeyDefinition.builder("fk_" + name + '_' + dependency)
                    .addColumn("parent_id")
                    .reference(RelationIdentity.table(dependency))
                    .addReferenceColumn("id")
                    .build());
        }
        return table.build();
    }

    private static RelationalTableDefinition dependentTable(String name, String foreignKeyName) {
        return RelationalTableDefinition.builder(RelationIdentity.table(name))
                .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
                .addColumn(ColumnDefinition.builder("parent_id", "BIGINT").build())
                .primaryKey(PrimaryKeyDefinition.of("pk_" + name, "id"))
                .addForeignKey(ForeignKeyDefinition.builder(foreignKeyName)
                        .addColumn("parent_id")
                        .reference(RelationIdentity.table("parents"))
                        .addReferenceColumn("id")
                        .build())
                .build();
    }

    private static RelationalTableDefinition indexedTable(String name, String indexName) {
        return RelationalTableDefinition.builder(RelationIdentity.table(name))
                .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
                .addColumn(ColumnDefinition.builder("code", "BIGINT").build())
                .primaryKey(PrimaryKeyDefinition.of("pk_" + name, "id"))
                .addIndex(IndexDefinition.builder(indexName)
                        .addKey(IndexKeyPart.asc("code"))
                        .build())
                .build();
    }

    private static RelationalTableDefinition.Builder partitionedEvents() {
        return RelationalTableDefinition.builder(RelationIdentity.table("job_events"))
                .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
                .addColumn(ColumnDefinition.builder("external_id", "VARCHAR").build())
                .addColumn(ColumnDefinition.builder("occurred_at", "TIMESTAMP").nullable(false).build())
                .primaryKey(PrimaryKeyDefinition.of("pk_job_events", "id", "occurred_at"))
                .partition(TablePartitionDefinition.range("occurred_at"));
    }

    private static DatabaseDescriptor database() {
        return DatabaseDescriptor.of("test", "1", "test", DialectCapabilities.empty());
    }

    private static DatabaseDescriptor postgresDatabase() {
        RdbDialect dialect = RdbDialect.postgresql();
        return DatabaseDescriptor.of("PostgreSQL", "16", dialect);
    }
}
