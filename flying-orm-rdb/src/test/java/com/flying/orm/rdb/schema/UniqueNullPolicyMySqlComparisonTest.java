package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.IndexKeyPart;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.core.metadata.UniqueNullPolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

class UniqueNullPolicyMySqlComparisonTest {

    @Test
    void mysqlRecognizesDesiredDistinctAsItsNativeUniqueIndexSemantics() {
        var report = compare(distinctTable(), indexedTable());

        assertTrue(report.operations().isEmpty());
    }

    @Test
    void mysqlRecognizesObservedDistinctAsItsNativeUniqueIndexSemantics() {
        var report = compare(indexedTable(), distinctTable());

        assertTrue(report.operations().isEmpty());
    }

    private static SchemaCompatibilityReport compare(RelationalTableDefinition desired, RelationalTableDefinition actual) {
        RdbDialect dialect = RdbDialect.mysql();
        return SchemaDiffer.diff(desired, SchemaSnapshot.present(actual), dialect.capabilities(),
                SchemaCompatibilityMode.EXACT, "mysql", dialect.schema());
    }

    private static RelationalTableDefinition distinctTable() {
        return table().addUnique(new UniqueConstraintDefinition("uq_email", List.of("email"), UniqueNullPolicy.DISTINCT))
                .build();
    }

    private static RelationalTableDefinition indexedTable() {
        return table().addIndex(IndexDefinition.builder("uq_email").unique().addKey(IndexKeyPart.asc("email")).build())
                .build();
    }

    private static RelationalTableDefinition.Builder table() {
        return RelationalTableDefinition.builder(RelationIdentity.table("accounts"))
                .addColumn(ColumnDefinition.builder("email", "VARCHAR").length(64).nullable(true).build());
    }
}
