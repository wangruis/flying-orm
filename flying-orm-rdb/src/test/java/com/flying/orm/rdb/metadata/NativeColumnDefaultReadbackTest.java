package com.flying.orm.rdb.metadata;

import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.schema.SchemaSnapshot;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

class NativeColumnDefaultReadbackTest {

    @Test
    void wrappedDefaultsShareDialectAwareExpressionBoundaries() {
        MetadataQueryProfile.SnapshotDialect pg = MetadataQueryProfile.SnapshotDialect.POSTGRESQL;
        assertEquals(ColumnDefault.literal("a('b')"), RelationalMetadataValueParser.columnDefault(
                "((('a(''b'')'::text)))", DatabaseType.of("VARCHAR"), pg));
        assertEquals(ColumnDefault.nativeExpression("concat($tag$')('$tag$, 'x')"),
                RelationalMetadataValueParser.columnDefault(
                        "((concat($tag$')('$tag$, 'x')))", DatabaseType.of("VARCHAR"), pg));
        assertEquals(ColumnDefault.literal("(not SQL);"), RelationalMetadataValueParser.columnDefault(
                "(not SQL);", DatabaseType.of("VARCHAR"), MetadataQueryProfile.SnapshotDialect.MYSQL));
    }

    @Test
    void preservesCompositePostgresqlDefaultsInsteadOfLosingTheExpression() {
        ColumnDefault value = assertDoesNotThrow(() -> RelationalMetadataValueParser.columnDefault(
                "('x'::text || 'y'::text)", DatabaseType.of("VARCHAR"),
                MetadataQueryProfile.SnapshotDialect.POSTGRESQL));
        assertEquals("NATIVE_EXPRESSION", value.kind().name());
        assertEquals("'x'::text || 'y'::text", value.value().orElseThrow());
    }

    @Test
    void preservesNumericFunctionDefaults() {
        ColumnDefault value = assertDoesNotThrow(() -> RelationalMetadataValueParser.columnDefault(
                "ABS(-42)", DatabaseType.of("INTEGER"),
                MetadataQueryProfile.SnapshotDialect.H2));
        assertEquals("NATIVE_EXPRESSION", value.kind().name());
        assertEquals("ABS(-42)", value.value().orElseThrow());
    }

    @Test
    void mysqlDistinguishesExpressionTextFromTheSameLiteralText() {
        MetadataQueryProfile.SnapshotDialect mysql = MetadataQueryProfile.SnapshotDialect.MYSQL;
        assertEquals(ColumnDefault.literal("uuid()"), RelationalMetadataValueParser.columnDefault(
                "uuid()", DatabaseType.of("VARCHAR"), mysql, false));
        assertEquals(ColumnDefault.nativeExpression("uuid()"), RelationalMetadataValueParser.columnDefault(
                "(uuid())", DatabaseType.of("VARCHAR"), mysql, true));
        assertEquals(ColumnDefault.none(), RelationalMetadataValueParser.columnDefault(
                null, DatabaseType.of("VARCHAR"), mysql, true));
    }

    @Test
    void preservesArrayDefaultsAndQuotedDelimiters() {
        for (String sql : java.util.List.of("ARRAY[1, 2]", "ARRAY[');', 'x']", "concat($x$);;$x$, 'a')")) {
            assertEquals(ColumnDefault.nativeExpression(sql), RelationalMetadataValueParser.columnDefault(
                    sql, DatabaseType.of("INTEGER[]"), MetadataQueryProfile.SnapshotDialect.POSTGRESQL));
        }
    }

    @Test
    void mysqlSnapshotConversionRetainsTheDictionaryExpressionClassification() {
        for (boolean expression : List.of(false, true)) {
            SchemaSnapshot snapshot = FormMetadataRowConverter.toCompleteSchemaSnapshot(
                    RelationIdentity.table("defaults_test"),
                    List.of(Map.of("COLUMN_NAME", "id", "DATA_TYPE", "varchar", "CHARACTER_MAXIMUM_LENGTH", 36,
                            "COLUMN_DEFAULT", "uuid()", "NATIVE_DEFAULT_EXPRESSION", expression, "NULLABLE", true)),
                    List.of(Map.of("TABLE_COMMENT", "")), List.of(), List.of(), List.of(), List.of(), List.of(),
                    value -> value, value -> value, MetadataQueryProfile.SnapshotDialect.MYSQL);
            assertEquals(expression ? ColumnDefault.nativeExpression("uuid()") : ColumnDefault.literal("uuid()"),
                    snapshot.completeTable().orElseThrow().columns().getFirst().defaultValue());
        }
    }
}
