package com.flying.orm.rdb.metadata;

import com.flying.orm.core.metadata.CheckConstraintDefinition;
import com.flying.orm.core.metadata.CheckPredicate;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.IndexKeyPart;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.ReferentialAction;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.schema.SchemaSnapshot;
import com.flying.orm.rdb.schema.SchemaSnapshotFingerprint;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlServerCompleteSchemaMetadataTest {

    @Test
    void declaresEveryCompleteSnapshotQueryAndRepresentabilityBoundary() {
        InformationSchemaFormMetadataReader.Queries queries = SqlServerReactiveFormMetadataReader.queries();

        assertTrue(InformationSchemaFormMetadataReader.coverage(queries).isComplete());
        assertEquals(InformationSchemaFormMetadataReader.SnapshotDialect.SQL_SERVER,
                     queries.snapshotDialect());
        assertProjection(queries.tableQuery().create("app", "orders"),
                         "TABLE_COMMENT", "TABLE_REPRESENTABLE");
        assertProjection(queries.columnQuery().create("app", "orders"),
                         "COLUMN_DEFAULT", "GENERATION_START", "GENERATION_INCREMENT",
                         "GENERATION_CACHE", "COLUMN_COLLATION", "COLUMN_REPRESENTABLE",
                         "UNSUPPORTED_COLUMN_REASON");
        assertProjection(queries.primaryKeyQuery().create("app", "orders"),
                         "CONSTRAINT_NAME", "CONSTRAINT_REPRESENTABLE");
        assertTrue(queries.primaryKeyQuery().create("app", "orders").sql()
                          .contains("i.is_primary_key = 1"));
        assertProjection(queries.uniqueConstraintQuery().create("app", "orders"),
                         "CONSTRAINT_NAME", "CONSTRAINT_REPRESENTABLE");
        assertTrue(queries.uniqueConstraintQuery().create("app", "orders").sql()
                          .contains("i.is_unique_constraint = 1"));
        assertProjection(queries.indexQuery().create("app", "orders"),
                         "INDEX_DIRECTION", "INDEX_REPRESENTABLE", "UNSUPPORTED_INDEX_REASON");
        assertProjection(queries.foreignKeyQuery().create("app", "orders"),
                         "ON_DELETE", "ON_UPDATE", "CONSTRAINT_REPRESENTABLE");
        assertProjection(queries.checkConstraintQuery().create("app", "orders"),
                         "CHECK_EXPRESSION", "CHECK_REPRESENTABLE");
        String checkSql = queries.checkConstraintQuery().create("app", "orders").sql();
        assertTrue(checkSql.contains("cc.definition as CHECK_EXPRESSION"));
        assertFalse(checkSql.contains("charindex(nchar(39), cc.definition)"));
        assertFalse(checkSql.contains("replace(replace(cc.definition"));
    }

    @Test
    void assemblesCapturedSqlServerRowsIntoAnExactCompleteSnapshot() {
        CapturedSqlServerExecutor executor = new CapturedSqlServerExecutor(false);
        SchemaSnapshot snapshot = SqlServerReactiveFormMetadataReader.create(executor)
                .readSnapshot("app", "orders")
                .block();

        assertEquals(SchemaSnapshotFingerprint.of(SchemaSnapshot.present(expected())),
                     SchemaSnapshotFingerprint.of(snapshot));
        assertEquals(7, executor.sql.size());
    }

    @Test
    void rejectsAnIndexWhosePhysicalShapeCannotBeRepresented() {
        CapturedSqlServerExecutor executor = new CapturedSqlServerExecutor(true);

        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> SqlServerReactiveFormMetadataReader.create(executor)
                        .readSnapshot("app", "orders")
                        .block());

        assertTrue(error.getMessage().contains("filtered index"));
    }

    private static RelationalTableDefinition expected() {
        RelationIdentity orders = RelationIdentity.of(null, "app", "orders");
        return RelationalTableDefinition.builder(orders)
                .comment("订单")
                .addColumn(ColumnDefinition.builder("id", "BIGINT")
                                           .nullable(false)
                                           .generation(ValueGeneration.identity(7, 3, 100))
                                           .build())
                .addColumn(ColumnDefinition.builder("customer_id", "BIGINT")
                                           .nullable(false)
                                           .build())
                .addColumn(ColumnDefinition.builder("status", "VARCHAR")
                                           .length(16)
                                           .nullable(false)
                                           .defaultValue(ColumnDefault.literal("NEW"))
                                           .collation("Latin1_General_100_CI_AS")
                                           .build())
                .addColumn(ColumnDefinition.builder("amount", "DECIMAL")
                                           .precision(12).scale(2).nullable(false).build())
                .primaryKey(PrimaryKeyDefinition.of("pk_orders", "id"))
                .addUnique(UniqueConstraintDefinition.of("uk_orders_status", "status"))
                .addIndex(IndexDefinition.builder("ix_orders_status")
                                         .addKey(IndexKeyPart.desc("status")).build())
                .addForeignKey(ForeignKeyDefinition.builder("fk_orders_customer")
                                                    .addColumn("customer_id")
                                                    .reference(RelationIdentity.of(null, "crm", "customers"))
                                                    .addReferenceColumn("id")
                                                    .onDelete(ReferentialAction.CASCADE)
                                                    .onUpdate(ReferentialAction.SET_NULL)
                                                    .build())
                .addCheck(CheckConstraintDefinition.of(
                        "ck_orders_amount",
                        CheckPredicate.compare("amount",
                                               CheckPredicate.ComparisonOperator.GREATER_THAN_OR_EQUAL,
                                               BigDecimal.ZERO)))
                .build();
    }

    private static void assertProjection(SqlRequest request, String... aliases) {
        assertEquals(List.of("orders", "app"), request.parameters());
        for (String alias : aliases) {
            assertTrue(request.sql().contains(alias), alias + " missing from " + request.sql());
        }
    }

    private static final class CapturedSqlServerExecutor implements ReactiveSqlExecutor {

        private final boolean unsupportedIndex;
        private final List<String> sql = new ArrayList<>();

        private CapturedSqlServerExecutor(boolean unsupportedIndex) {
            this.unsupportedIndex = unsupportedIndex;
        }

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            String normalized = request.sql().toUpperCase(Locale.ROOT);
            sql.add(normalized);
            if (normalized.contains("FROM INFORMATION_SCHEMA.COLUMNS C")) {
                return rows(
                        row("COLUMN_NAME", "id", "DATA_TYPE", "BIGINT", "NULLABLE", "NO",
                            "IS_IDENTITY", true, "GENERATION_START", 7, "GENERATION_INCREMENT", 3,
                            "GENERATION_CACHE", 100, "COLUMN_REPRESENTABLE", true),
                        row("COLUMN_NAME", "customer_id", "DATA_TYPE", "BIGINT", "NULLABLE", "NO",
                            "IS_IDENTITY", false, "COLUMN_REPRESENTABLE", true),
                        row("COLUMN_NAME", "status", "DATA_TYPE", "VARCHAR",
                            "CHARACTER_MAXIMUM_LENGTH", 16, "NULLABLE", "NO", "IS_IDENTITY", false,
                            "COLUMN_DEFAULT", "('NEW')", "GENERATION_EXPRESSION", "('NEW')",
                            "COLUMN_COLLATION", "Latin1_General_100_CI_AS",
                            "COLUMN_REPRESENTABLE", true),
                        row("COLUMN_NAME", "amount", "DATA_TYPE", "DECIMAL", "NUMERIC_PRECISION", 12,
                            "NUMERIC_SCALE", 2, "NULLABLE", "NO", "IS_IDENTITY", false,
                            "COLUMN_REPRESENTABLE", true));
            }
            if (normalized.contains("FROM SYS.TABLES T")) {
                return rows(row("TABLE_COMMENT", "订单", "TABLE_REPRESENTABLE", true));
            }
            if (normalized.contains("FROM SYS.CHECK_CONSTRAINTS CC")) {
                return rows(row("CONSTRAINT_NAME", "ck_orders_amount",
                                "CHECK_EXPRESSION", "([amount] >= (0))",
                                "CHECK_REPRESENTABLE", true));
            }
            if (normalized.contains("KC.TYPE = 'PK'")) {
                return rows(row("CONSTRAINT_NAME", "pk_orders", "COLUMN_NAME", "id",
                                "CONSTRAINT_REPRESENTABLE", true));
            }
            if (normalized.contains("KC.TYPE = 'UQ'")) {
                return rows(row("CONSTRAINT_NAME", "uk_orders_status", "COLUMN_NAME", "status",
                                "CONSTRAINT_REPRESENTABLE", true));
            }
            if (normalized.contains("FROM SYS.INDEXES I")) {
                return rows(row("INDEX_NAME", "ix_orders_status", "COLUMN_NAME", "status",
                                "UNIQUE_INDEX", false, "INDEX_REPRESENTABLE", !unsupportedIndex,
                                "UNSUPPORTED_INDEX_REASON", unsupportedIndex ? "filtered index" : null,
                                "INDEX_DIRECTION", "DESC"));
            }
            if (normalized.contains("FROM SYS.FOREIGN_KEYS FK")) {
                return rows(row("TABLE_SCHEMA", "app", "FOREIGN_KEY_NAME", "fk_orders_customer",
                                "COLUMN_NAME", "customer_id", "REFERENCED_TABLE_SCHEMA", "crm",
                                "REFERENCED_TABLE_NAME", "customers", "REFERENCED_COLUMN_NAME", "id",
                                "ON_DELETE", "CASCADE", "ON_UPDATE", "SET_NULL",
                                "CONSTRAINT_REPRESENTABLE", true));
            }
            throw new AssertionError("unexpected SQL Server metadata query: " + request.sql());
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            return Mono.error(new UnsupportedOperationException());
        }

        private static Flux<DynamicRow> rows(DynamicRow... rows) {
            return Flux.fromArray(rows);
        }

        private static DynamicRow row(Object... values) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (int index = 0; index < values.length; index += 2) {
                row.put((String) values[index], values[index + 1]);
            }
            return DynamicRow.copyOf(row);
        }
    }
}
