package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalSchemaDefinition;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.DialectCapabilities;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class UnsupportedCycleManualActionTest {

    @Test
    void emitsManualOperationsInsteadOfGuessingHowToCloseAnUnsupportedCycle() {
        RelationalTableDefinition owners = table("owners", "accounts");
        RelationalTableDefinition accounts = table("accounts", "owners");

        MultiTableSchemaPlanner.Plan plan = new MultiTableSchemaPlanner(
                database(), MultiTableSchemaPlanner.ForeignKeyCycleSupport.MANUAL_REQUIRED)
                .plan(RelationalSchemaDefinition.of(List.of(owners, accounts)));

        assertEquals(List.of(SchemaOperation.Kind.VERIFY_MANUALLY,
                             SchemaOperation.Kind.VERIFY_MANUALLY),
                     plan.secondPhase().stream().map(SchemaOperation::kind).toList());
        assertEquals(List.of("fk_accounts_owners", "fk_owners_accounts"),
                     plan.secondPhase().stream().map(SchemaOperation::objectName).toList());
    }

    private static RelationalTableDefinition table(String name, String dependency) {
        return RelationalTableDefinition.builder(RelationIdentity.table(name))
                .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
                .addColumn(ColumnDefinition.builder("peer_id", "BIGINT").build())
                .primaryKey(PrimaryKeyDefinition.of("pk_" + name, "id"))
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
