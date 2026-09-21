package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FullAuditSchemaRegressionTest {

    private static final String MODE_MARKER =
            "/*flying-orm:mysql-comment-no-backslash-escapes*/";
    private static final List<String> BACKSLASH_COMMENTS = List.of(
            "C:\\data",
            "C:\\data\\",
            "C:\\O'Reilly\\",
            "C:\\data\\'");

    @Test
    void sqlServerTableCommentResolvesLiteralDotTableAsOneIdentifier() {
        RelationIdentity relation = RelationIdentity.table("audit.logs");
        RelationalTableDefinition table = RelationalTableDefinition.builder(relation)
                .comment("table comment")
                .addColumn(column("payload"))
                .build();

        List<String> sql = render(RdbDialect.sqlServer().schema(), SchemaOperation.of(
                SchemaOperation.Kind.CREATE_TABLE, relation, relation.table(), null, table,
                SchemaOperation.Compatibility.REQUIRES_REVIEW));

        assertTrue(sql.stream().anyMatch(statement -> statement.contains("create table [audit.logs]")),
                sql.toString());
        assertSingleSegmentCommentLookup(sql, "table comment");
    }

    @Test
    void sqlServerColumnCommentResolvesLiteralDotTableAsOneIdentifier() {
        RelationIdentity relation = RelationIdentity.table("audit.logs");
        ColumnDefinition desired = column("payload", "column comment");

        List<String> sql = render(RdbDialect.sqlServer().schema(), SchemaOperation.of(
                SchemaOperation.Kind.ADD_COLUMN, relation, desired.name(), null, desired,
                SchemaOperation.Compatibility.REQUIRES_REVIEW));

        assertTrue(sql.stream().anyMatch(statement -> statement.contains("alter table [audit.logs]")),
                sql.toString());
        assertSingleSegmentCommentLookup(sql, "column comment");
    }

    @Test
    void mysqlAddColumnPreservesBackslashesTrailingBackslashesAndQuotes() {
        for (String comment : BACKSLASH_COMMENTS) {
            ColumnDefinition desired = column("payload", comment);
            List<String> sql = render(RdbDialect.mysql().schema(), SchemaOperation.of(
                    SchemaOperation.Kind.ADD_COLUMN, RelationIdentity.table("orders"),
                    desired.name(), null, desired, SchemaOperation.Compatibility.REQUIRES_REVIEW));

            assertEquals(1, sql.size(), comment);
            assertTrue(sql.get(0).startsWith("alter table `orders` add "), sql.get(0));
            assertCommentLiteral(sql.get(0), comment);
        }
    }

    @Test
    void mysqlModifyColumnPreservesBackslashesTrailingBackslashesAndQuotes() {
        ColumnDefinition actual = column("payload", "previous comment");
        for (String comment : BACKSLASH_COMMENTS) {
            ColumnDefinition desired = column("payload", comment);
            List<String> sql = render(RdbDialect.mysql().schema(), SchemaOperation.of(
                    SchemaOperation.Kind.CHANGE_COLUMN, RelationIdentity.table("orders"),
                    desired.name(), actual, desired, SchemaOperation.Compatibility.REQUIRES_REVIEW));

            assertEquals(1, sql.size(), comment);
            assertTrue(sql.get(0).startsWith("alter table `orders` modify column "), sql.get(0));
            assertCommentLiteral(sql.get(0), comment);
        }
    }

    @Test
    void mysqlAddAndModifyStillRejectOtherSqlBlockComments() {
        SchemaDialect dialect = RdbDialect.mysql().schema();
        String markedDefinition = "`payload` varchar(64) comment " + MODE_MARKER + " 'C:\\data\\'";
        for (String block : List.of(
                "/* arbitrary */",
                "/*!80000 invisible */",
                "/*+ arbitrary_hint */",
                "/*flying-orm:mysql-comment-no-backslash-escapes-extra*/")) {
            for (String definition : List.of(
                    block + " `payload` varchar(64)",
                    "`payload` varchar(64) " + block,
                    markedDefinition + " " + block)) {
                assertThrows(IllegalArgumentException.class,
                        () -> dialect.addColumnSql("orders", definition), definition);
                assertThrows(IllegalArgumentException.class,
                        () -> dialect.alterColumnTypeSql(
                                "orders", "payload", "varchar(64)", definition), definition);
            }
        }
    }

    private static ColumnDefinition column(String name) {
        return ColumnDefinition.builder(name, "VARCHAR").length(64).build();
    }

    private static ColumnDefinition column(String name, String comment) {
        return ColumnDefinition.builder(name, "VARCHAR").length(64).comment(comment).build();
    }

    private static List<String> render(SchemaDialect dialect, SchemaOperation operation) {
        return RelationalSchemaSqlRenderer.create(dialect).render(operation).stream()
                .map(SqlRequest::sql)
                .toList();
    }

    private static void assertSingleSegmentCommentLookup(List<String> sql, String comment) {
        List<String> commentSql = sql.stream().filter(statement -> statement.contains(comment)).toList();
        assertEquals(1, commentSql.size(), sql.toString());
        String statement = commentSql.get(0);
        assertTrue(statement.toLowerCase(Locale.ROOT).contains(
                "object_schema_name(object_id(quotename(n''audit.logs'')))"), statement);
        assertTrue(statement.contains("N''" + comment + "''"), statement);
    }

    private static void assertCommentLiteral(String sql, String comment) {
        String expected = " comment " + MODE_MARKER + " '" + comment.replace("'", "''") + "'";
        assertTrue(sql.contains(expected), sql);
    }
}
