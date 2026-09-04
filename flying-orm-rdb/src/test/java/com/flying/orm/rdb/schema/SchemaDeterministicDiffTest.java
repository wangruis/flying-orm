package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.dialect.DialectCapabilities;
import com.flying.orm.rdb.dialect.DialectCapabilityId;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SchemaDeterministicDiffTest {

    private static final RelationIdentity ACCOUNTS = RelationIdentity.table("accounts");

    @Test
    void declarationAndCapabilityOrderDoNotChangeOperationOrder() {
        RelationalTableDefinition desiredOne = table("id", "delta", "charlie");
        RelationalTableDefinition desiredTwo = table("id", "charlie", "delta");
        RelationalTableDefinition actualOne = table("id", "bravo", "alpha");
        RelationalTableDefinition actualTwo = table("id", "alpha", "bravo");

        SchemaCompatibilityReport first = SchemaDiffer.diff(
                desiredOne,
                SchemaSnapshot.present(actualOne),
                DialectCapabilities.of(DialectCapabilityId.NATIVE_JSON,
                                       DialectCapabilityId.NATIVE_BOOLEAN),
                SchemaCompatibilityMode.SAFE_INCREMENTAL);
        SchemaCompatibilityReport second = SchemaDiffer.diff(
                desiredTwo,
                SchemaSnapshot.present(actualTwo),
                DialectCapabilities.of(DialectCapabilityId.NATIVE_BOOLEAN,
                                       DialectCapabilityId.NATIVE_JSON),
                SchemaCompatibilityMode.SAFE_INCREMENTAL);

        List<String> expected = List.of(
                "ADD_COLUMN:charlie:SAFE_INCREMENTAL",
                "ADD_COLUMN:delta:SAFE_INCREMENTAL",
                "DROP_COLUMN:alpha:COMPATIBLE_EXTRA",
                "DROP_COLUMN:bravo:COMPATIBLE_EXTRA");
        assertEquals(expected, signatures(first));
        assertEquals(expected, signatures(second));
    }

    private static List<String> signatures(SchemaCompatibilityReport report) {
        return report.operations().stream()
                .map(operation -> operation.kind() + ":" + operation.objectName()
                        + ":" + operation.compatibility())
                .toList();
    }

    private static RelationalTableDefinition table(String... columns) {
        RelationalTableDefinition.Builder builder = RelationalTableDefinition.builder(ACCOUNTS);
        for (String column : columns) {
            builder.addColumn(ColumnDefinition.builder(column, "BIGINT")
                                              .nullable("id".equals(column) ? false : true)
                                              .build());
        }
        return builder.primaryKey(PrimaryKeyDefinition.of("pk_accounts", "id")).build();
    }
}
