package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.CheckConstraintDefinition;
import com.flying.orm.core.metadata.CheckPredicate;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.internal.plan.SqlExecutionStatements;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

class RelationalSchemaLiteralValueTest {

    @Test
    void preservesEmptyAndWhitespaceDefaultValues() {
        assertDefaultValues(List.of("", " ", "  padded  ", "  O'Reilly  "));
    }

    @Test
    void preservesSqlDelimiterTextInsideColumnDefaults() {
        assertDefaultValues(List.of("hello; world", "x--y", "/*text*/"));
    }

    private static void assertDefaultValues(List<String> values) {
        for (RdbDialect dialect : dialects()) {
            for (String value : values) {
                ColumnDefinition column = ColumnDefinition.builder("value", "VARCHAR")
                        .length(64).defaultValue(ColumnDefault.literal(value)).build();
                SchemaOperation operation = SchemaOperation.of(
                        SchemaOperation.Kind.ADD_COLUMN, RelationIdentity.table("literal_values"),
                        column.name(), null, column, SchemaOperation.Compatibility.REQUIRES_REVIEW);
                var request = RelationalSchemaSqlRenderer.create(dialect.schema()).render(operation).getFirst();

                String literal = (dialect.name().equals("sqlserver") ? "N" : "")
                        + "'" + value.replace("'", "''") + "'";
                assertTrue(request.sql().contains("default " + literal),
                           dialect.name() + ": " + request.sql());
                SqlExecutionStatements.canonical(request, dialect.name());
            }
        }
    }

    @Test
    void preservesEmptyAndWhitespaceCheckValues() {
        for (RdbDialect dialect : dialects()) {
            for (String value : List.of("", " ", "  padded  ", "  O'Reilly  ")) {
                CheckConstraintDefinition check = CheckConstraintDefinition.of(
                        "ck_literal_value", CheckPredicate.compare(
                                "value", CheckPredicate.ComparisonOperator.NOT_EQUAL, value));
                SchemaOperation operation = SchemaOperation.of(
                        SchemaOperation.Kind.ADD_CHECK, RelationIdentity.table("literal_values"),
                        check.name(), null, check, SchemaOperation.Compatibility.REQUIRES_REVIEW);
                var request = RelationalSchemaSqlRenderer.create(dialect.schema()).render(operation).getFirst();

                String literal = (dialect.name().equals("sqlserver") ? "N" : "")
                        + "'" + value.replace("'", "''") + "'";
                assertTrue(request.sql().contains("<> " + literal),
                           dialect.name() + ": " + request.sql());
                SqlExecutionStatements.canonical(request, dialect.name());
            }
        }
    }

    private static List<RdbDialect> dialects() {
        return List.of(RdbDialect.h2(), RdbDialect.mysql(), RdbDialect.postgresql(),
                       RdbDialect.oracle(), RdbDialect.sqlServer());
    }
}
