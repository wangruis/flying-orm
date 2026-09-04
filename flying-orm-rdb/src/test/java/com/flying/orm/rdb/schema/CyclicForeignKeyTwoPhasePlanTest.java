package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.IndexKeyPart;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalSchemaDefinition;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.DialectCapabilities;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CyclicForeignKeyTwoPhasePlanTest {

    @Test
    void defersIndexesAndCyclicForeignKeysUntilEveryTableExists() {
        RelationalTableDefinition owners = table("owners", "accounts");
        RelationalTableDefinition accounts = table("accounts", "owners");

        MultiTableSchemaPlanner.Plan plan = new MultiTableSchemaPlanner(
                database(), MultiTableSchemaPlanner.ForeignKeyCycleSupport.SUPPORTED)
                .plan(RelationalSchemaDefinition.of(List.of(owners, accounts)));

        assertEquals(List.of("accounts", "owners"),
                     plan.firstPhase().stream().map(operation -> operation.relation().table()).toList());
        assertTrue(plan.firstPhase().stream()
                .map(operation -> (RelationalTableDefinition) operation.desired())
                .allMatch(table -> table.indexes().isEmpty() && table.foreignKeys().isEmpty()));
        assertEquals(List.of(SchemaOperation.Kind.ADD_INDEX,
                             SchemaOperation.Kind.ADD_FOREIGN_KEY,
                             SchemaOperation.Kind.ADD_INDEX,
                             SchemaOperation.Kind.ADD_FOREIGN_KEY),
                     plan.secondPhase().stream().map(SchemaOperation::kind).toList());
    }

    private static RelationalTableDefinition table(String name, String dependency) {
        return RelationalTableDefinition.builder(RelationIdentity.table(name))
                .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
                .addColumn(ColumnDefinition.builder("peer_id", "BIGINT").build())
                .primaryKey(PrimaryKeyDefinition.of("pk_" + name, "id"))
                .addIndex(IndexDefinition.builder("ix_" + name + "_peer")
                        .addKey(IndexKeyPart.asc("peer_id"))
                        .build())
                .addForeignKey(ForeignKeyDefinition.builder("fk_" + name + '_' + dependency)
                        .addColumn("peer_id")
                        .reference(RelationIdentity.table(dependency))
                        .addReferenceColumn("id")
                        .build())
                .build();
    }

    private static DatabaseDescriptor database() {
        return DatabaseDescriptor.of("test", "1", "test", DialectCapabilities.empty());
    }
}
