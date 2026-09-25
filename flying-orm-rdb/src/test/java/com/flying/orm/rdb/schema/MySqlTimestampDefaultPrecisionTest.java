package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MySqlTimestampDefaultPrecisionTest {

    @TestFactory
    Stream<DynamicTest> defaultUsesTheFinalPhysicalColumnPrecision() {
        return Stream.of(
                new Case("TIMESTAMP(6)", null, "datetime(6)", "current_timestamp(6)"),
                new Case("TIMESTAMPTZ", null, "timestamp(6)", "current_timestamp(6)"),
                new Case("TIMESTAMP(3) WITH TIME ZONE", null, "timestamp(3)", "current_timestamp(3)"),
                new Case("TIMESTAMP", 4, "datetime(4)", "current_timestamp(4)"),
                new Case("TIMESTAMP", null, "datetime", "current_timestamp"),
                new Case("TIMESTAMP(0)", null, "datetime(0)", "current_timestamp"))
                .map(test -> DynamicTest.dynamicTest(test.type() + "/" + test.precision(),
                        () -> verifyRendering(test)));
    }

    private static void verifyRendering(Case test) {
        RelationIdentity relation = RelationIdentity.table("events");
        ColumnDefinition before = ColumnDefinition.builder("created_at", test.physicalType()).build();
        ColumnDefinition after = ColumnDefinition.builder("created_at", test.type())
                .temporalPrecision(test.precision()).defaultValue(ColumnDefault.currentTimestamp()).build();
        RelationalTableDefinition table = RelationalTableDefinition.builder(relation).addColumn(after).build();
        RelationalSchemaSqlRenderer renderer = RelationalSchemaSqlRenderer.create(RdbDialect.mysql().schema());
        String fragment = "`created_at` " + test.physicalType() + " default " + test.expression();
        assertAll(
                () -> assertSql(renderer, SchemaOperation.of(SchemaOperation.Kind.CREATE_TABLE,
                        relation, "events", null, table, SchemaOperation.Compatibility.SAFE_INCREMENTAL), fragment),
                () -> assertSql(renderer, SchemaOperation.of(SchemaOperation.Kind.ADD_COLUMN,
                        relation, "created_at", null, after, SchemaOperation.Compatibility.SAFE_INCREMENTAL), fragment),
                () -> assertSql(renderer, SchemaOperation.of(SchemaOperation.Kind.CHANGE_COLUMN,
                        relation, "created_at", before, after,
                        SchemaOperation.Compatibility.REQUIRES_REVIEW), "set default " + test.expression()));
    }

    private static void assertSql(RelationalSchemaSqlRenderer renderer, SchemaOperation operation,
                                  String fragment) {
        List<SqlRequest> requests = renderer.render(operation);
        assertTrue(requests.stream().anyMatch(request ->
                request.sql().toLowerCase(Locale.ROOT).contains(fragment)), requests.toString());
    }

    private record Case(String type, Integer precision, String physicalType, String expression) {
    }
}
