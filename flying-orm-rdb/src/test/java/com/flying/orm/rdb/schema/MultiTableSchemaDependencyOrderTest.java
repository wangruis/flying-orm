package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalSchemaDefinition;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.DialectCapabilities;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

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

    private static DatabaseDescriptor database() {
        return DatabaseDescriptor.of("test", "1", "test", DialectCapabilities.empty());
    }
}
