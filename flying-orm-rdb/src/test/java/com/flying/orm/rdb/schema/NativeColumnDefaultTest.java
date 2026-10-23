package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationalMetadataFingerprint;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.execution.ConnectionAccessTestSupport;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReaders;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import com.flying.orm.core.sql.render.SqlRequest;
import org.h2.jdbcx.JdbcDataSource;
import com.flying.orm.rdb.internal.template.SchemaDefaultExpressions;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.sql.Connection;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class NativeColumnDefaultTest {

    @Test
    void rendersExpressionsWithoutConfusingThemWithLiterals() {
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(), RdbDialect.h2(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            RelationalSchemaSqlRenderer renderer = RelationalSchemaSqlRenderer.create(dialect.schema());
            assertEquals("(ABS(-42))", renderer.defaultExpression(column(ColumnDefault.nativeExpression("ABS(-42)"))));
            assertTrue(renderer.defaultExpression(column(ColumnDefault.literal("ABS(-42)"))).endsWith("'ABS(-42)'"));
        }
    }

    @Test
    void comparesExpressionTextAndChangesTheMetadataFingerprint() {
        SchemaDialect dialect = RdbDialect.postgresql().schema();
        ColumnDefault first = ColumnDefault.nativeExpression("ABS(-42)");
        ColumnDefault second = ColumnDefault.nativeExpression("ABS(-43)");
        assertFalse(SchemaDefinitionEquality.sameDefault(first, second, dialect));
        assertTrue(SchemaDefinitionEquality.sameDefault(first, ColumnDefault.nativeExpression("((ABS(-42)))"), dialect));
        assertFalse(SchemaDefinitionEquality.sameDefault(first, ColumnDefault.literal("ABS(-42)"), dialect));
        assertNotEquals(RelationalMetadataFingerprint.of(table(first)), RelationalMetadataFingerprint.of(table(second)));
        assertTrue(SchemaDefinitionEquality.sameDefault(first, first, null));
        assertFalse(SchemaDefinitionEquality.sameDefault(first, second, null));
        assertTrue(SchemaDiffer.diff(table(first), SchemaSnapshot.present(table(first)),
                RdbDialect.postgresql().capabilities(), SchemaCompatibilityMode.EXACT).operations().isEmpty());
    }

    @Test
    void executesNativeDefaultAndConvergesWithTheDatabaseSnapshot() throws Exception {
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:default_expression_" + UUID.randomUUID().toString().replace("-", ""));
        source.setUser("sa");
        RdbDialect dialect = RdbDialect.h2();
        RelationalTableDefinition desired = RelationalTableDefinition.builder(RelationIdentity.table("defaults_test"))
                .addColumn(ColumnDefinition.builder("id", "UUID")
                        .defaultValue(ColumnDefault.nativeExpression("RANDOM_UUID()")).build()).build();
        try (Connection keeper = source.getConnection()) {
            SyncSqlExecutor jdbc = SyncSqlExecutor.jdbc(ConnectionAccessTestSupport.jdbc(source), dialect);
            RelationalSchemaSqlRenderer.create(dialect.schema()).render(SchemaOperation.of(
                    SchemaOperation.Kind.CREATE_TABLE, desired.identity(), "defaults_test", null, desired,
                    SchemaOperation.Compatibility.SAFE_INCREMENTAL)).forEach(jdbc::rowsUpdated);
            assertEquals(1L, jdbc.rowsUpdated(SqlRequest.nativeSql("insert into defaults_test default values", List.of())));
            assertNotNull(jdbc.query(SqlRequest.nativeSql("select id from defaults_test", List.of())).getFirst().get("ID"));
            SchemaMigrationPlan plan = JdbcSchemaClient.create(jdbc, dialect).reviewRelational(
                    DatabaseDescriptor.of("H2", "2.4.240", dialect), desired,
                    JdbcFormMetadataReaders.create(jdbc, dialect), SchemaCompatibilityMode.EXACT);
            assertTrue(plan.operations().isEmpty(), () -> plan.operations().toString());
        }
    }

    @Test
    void changeAndReverseChangeUseTheRespectiveExpressions() {
        RelationIdentity relation = RelationIdentity.table("defaults_test");
        ColumnDefinition before = column(ColumnDefault.nativeExpression("ABS(-42)"));
        ColumnDefinition after = column(ColumnDefault.nativeExpression("ABS(-43)"));
        RelationalSchemaSqlRenderer renderer = RelationalSchemaSqlRenderer.create(RdbDialect.postgresql().schema());
        for (boolean reverse : List.of(false, true)) {
            SchemaOperation operation = SchemaOperation.of(SchemaOperation.Kind.CHANGE_COLUMN,
                    relation, "value", reverse ? after : before, reverse ? before : after,
                    SchemaOperation.Compatibility.REQUIRES_REVIEW);
            assertTrue(renderer.render(operation).stream().anyMatch(request -> request.sql().contains(
                    "set default (ABS(-" + (reverse ? "42" : "43") + "))")));
        }
    }

    @Test
    void preservesQuotedTextAndRejectsEscapingTheExpressionBoundary() {
        assertEquals("concat('); --', 'x')", SchemaDefaultExpressions.normalize("((concat('); --', 'x')))", "mysql"));
        assertEquals("(1) + (2)", SchemaDefaultExpressions.normalize("((1) + (2))", "postgresql"));
        assertEquals("((1))()", SchemaDefaultExpressions.normalize("((1))()", "postgresql"));
        assertEquals("ARRAY[1, 2]", SchemaDefaultExpressions.normalize("(ARRAY[1, 2])", "postgresql"));
        assertEquals("[dbo].[make,value]()", SchemaDefaultExpressions.normalize("([dbo].[make,value]())", "sql_server"));
        for (String sql : List.of("1); drop table users; --", "1; select 2", "1, 2", "1 -- comment", "(1", "'x", "()")) {
            assertThrows(IllegalArgumentException.class, () -> SchemaDefaultExpressions.normalize(sql, "postgresql"), sql);
        }
        assertThrows(IllegalArgumentException.class, () -> ColumnDefault.nativeExpression(" "));
    }

    private static ColumnDefinition column(ColumnDefault value) {
        return ColumnDefinition.builder("value", "INTEGER").defaultValue(value).build();
    }

    private static RelationalTableDefinition table(ColumnDefault value) {
        return RelationalTableDefinition.builder(RelationIdentity.table("defaults_test")).addColumn(column(value)).build();
    }
}
