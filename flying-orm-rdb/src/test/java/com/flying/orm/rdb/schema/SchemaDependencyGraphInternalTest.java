package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalSchemaDefinition;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SchemaDependencyGraphInternalTest {

    @Test
    void ordersExplicitTablesByTheirForeignKeyDependencies() {
        RelationalTableDefinition accounts = table("accounts");
        RelationalTableDefinition orders = table("orders", foreignKey("orders", "accounts"));
        RelationalTableDefinition events = table("events", foreignKey("events", "orders"));

        SchemaDependencyGraph graph = SchemaDependencyGraph.of(
                RelationalSchemaDefinition.of(List.of(events, accounts, orders)));

        assertEquals(List.of("accounts", "orders", "events"), names(graph.dependencyOrder()));
    }

    @Test
    void publishesForeignKeyCyclesAsOneStableStronglyConnectedComponent() {
        RelationalTableDefinition owners = table("owners", foreignKey("owners", "accounts"));
        RelationalTableDefinition accounts = table("accounts", foreignKey("accounts", "owners"));

        SchemaStronglyConnectedComponents components = SchemaDependencyGraph.of(
                        RelationalSchemaDefinition.of(List.of(owners, accounts)))
                .stronglyConnectedComponents();

        assertEquals(List.of(List.of("accounts", "owners")),
                     components.components().stream().map(SchemaDependencyGraphInternalTest::names).toList());
        assertEquals(List.of("accounts", "owners"), names(components.dependencyOrder()));
    }

    private static RelationalTableDefinition table(String name, ForeignKeyDefinition... foreignKeys) {
        RelationalTableDefinition.Builder builder = RelationalTableDefinition.builder(RelationIdentity.table(name))
                .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
                .addColumn(ColumnDefinition.builder("owner_id", "BIGINT").build())
                .primaryKey(PrimaryKeyDefinition.of("pk_" + name, "id"));
        for (ForeignKeyDefinition foreignKey : foreignKeys) {
            builder.addForeignKey(foreignKey);
        }
        return builder.build();
    }

    private static ForeignKeyDefinition foreignKey(String owner, String target) {
        return ForeignKeyDefinition.builder("fk_" + owner + '_' + target)
                .addColumn("owner_id")
                .reference(RelationIdentity.table(target))
                .addReferenceColumn("id")
                .build();
    }

    private static List<String> names(List<RelationalTableDefinition> tables) {
        return tables.stream().map(table -> table.identity().table()).toList();
    }
}
