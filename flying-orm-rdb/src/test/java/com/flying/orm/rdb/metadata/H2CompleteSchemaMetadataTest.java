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
import static org.junit.jupiter.api.Assertions.assertTrue;

class H2CompleteSchemaMetadataTest {

    @Test
    void declaresAndProjectsEveryCompleteSnapshotFact() {
        InformationSchemaFormMetadataReader.Queries queries = H2ReactiveFormMetadataReader.queries();

        assertTrue(InformationSchemaFormMetadataReader.coverage(queries).isComplete());
        assertEquals(InformationSchemaFormMetadataReader.SnapshotDialect.H2,
                     queries.snapshotDialect());
        assertTrue(queries.tableQuery().create("app", "orders").sql().contains("TABLE_COMMENT"));
        assertTrue(queries.primaryKeyQuery().create("app", "orders").sql().contains("CONSTRAINT_NAME"));
        assertTrue(queries.uniqueConstraintQuery().create("app", "orders").sql().contains("CONSTRAINT_NAME"));
        assertTrue(queries.checkConstraintQuery().create("app", "orders").sql().contains("CHECK_EXPRESSION"));

        String columns = queries.columnQuery().create("app", "orders").sql();
        assertTrue(columns.contains("COLUMN_DEFAULT"));
        assertTrue(columns.contains("GENERATION_START"));
        assertTrue(columns.contains("GENERATION_INCREMENT"));
        assertTrue(columns.contains("GENERATION_CACHE"));
        assertTrue(columns.contains("COLUMN_REPRESENTABLE"));
        assertTrue(columns.contains("lower(c.COLUMN_NAME) as COLUMN_NAME"));
        assertTrue(columns.contains("c.COLUMN_NAME = upper(c.COLUMN_NAME)"));
        assertTrue(columns.contains("lower(c.TABLE_SCHEMA) as RESOLUTION_SCHEMA"));
        String indexes = queries.indexQuery().create("app", "orders").sql();
        assertTrue(indexes.contains("INDEX_DIRECTION"));
        assertTrue(indexes.contains("lower(i.INDEX_NAME) as INDEX_NAME"));
        assertTrue(indexes.contains("lower(ic.COLUMN_NAME) as COLUMN_NAME"));
        assertTrue(indexes.contains("SETTING_NAME = 'DEFAULT_NULL_ORDERING'"));
        assertTrue(indexes.contains("when 'LOW'"));
        assertTrue(indexes.contains("when 'HIGH'"));
        assertTrue(indexes.contains("when 'FIRST'"));
        assertTrue(indexes.contains("when 'LAST'"));
        String foreignKeys = queries.foreignKeyQuery().create("app", "orders").sql();
        assertTrue(foreignKeys.contains("lower(fk.CONSTRAINT_NAME) as FOREIGN_KEY_NAME"));
        assertTrue(foreignKeys.contains("lower(pk.TABLE_NAME) as REFERENCED_TABLE_NAME"));
        assertTrue(foreignKeys.contains("ON_DELETE"));
        assertTrue(foreignKeys.contains("ON_UPDATE"));
        assertTrue(queries.primaryKeyQuery().create("app", "orders").sql()
                          .contains("lower(tc.CONSTRAINT_NAME) as CONSTRAINT_NAME"));
        assertTrue(queries.checkConstraintQuery().create("app", "orders").sql()
                          .contains("lower(tc.CONSTRAINT_NAME) as CONSTRAINT_NAME"));
    }

    @Test
    void assemblesCapturedH2RowsIntoAnExactCompleteSnapshot() {
        CapturedH2Executor executor = new CapturedH2Executor();
        SchemaSnapshot snapshot = H2ReactiveFormMetadataReader.create(executor)
                .readSnapshot("app", "orders")
                .block();

        assertEquals(SchemaSnapshotFingerprint.of(SchemaSnapshot.present(expected())),
                     SchemaSnapshotFingerprint.of(snapshot));
        assertEquals(7, executor.sql.size());
    }

    private static RelationalTableDefinition expected() {
        RelationIdentity orders = RelationIdentity.of(null, "app", "orders");
        return RelationalTableDefinition.builder(orders)
                .comment("订单")
                .addColumn(ColumnDefinition.builder("id", "BIGINT")
                                           .nullable(false)
                                           .generation(ValueGeneration.identity(5, 2, 100))
                                           .build())
                .addColumn(ColumnDefinition.builder("customer_id", "BIGINT")
                                           .nullable(false)
                                           .build())
                .addColumn(ColumnDefinition.builder("status", "VARCHAR")
                                           .length(16)
                                           .nullable(false)
                                           .defaultValue(ColumnDefault.literal("NEW"))
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
                                                    .build())
                .addCheck(CheckConstraintDefinition.of(
                        "ck_orders_amount",
                        CheckPredicate.compare("amount",
                                               CheckPredicate.ComparisonOperator.GREATER_THAN_OR_EQUAL,
                                               BigDecimal.ZERO)))
                .build();
    }

    private static final class CapturedH2Executor implements ReactiveSqlExecutor {

        private final List<String> sql = new ArrayList<>();

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            String normalized = request.sql().toUpperCase(Locale.ROOT);
            sql.add(normalized);
            if (normalized.contains("FROM INFORMATION_SCHEMA.COLUMNS C")) {
                return rows(
                        row("COLUMN_NAME", "id", "DATA_TYPE", "BIGINT", "NULLABLE", "NO",
                            "IS_IDENTITY", true, "GENERATION_START", 5, "GENERATION_INCREMENT", 2,
                            "GENERATION_CACHE", 100, "COLUMN_REPRESENTABLE", true),
                        row("COLUMN_NAME", "customer_id", "DATA_TYPE", "BIGINT", "NULLABLE", "NO",
                            "IS_IDENTITY", false, "COLUMN_REPRESENTABLE", true),
                        row("COLUMN_NAME", "status", "DATA_TYPE", "VARCHAR",
                            "CHARACTER_MAXIMUM_LENGTH", 16, "NULLABLE", "NO", "IS_IDENTITY", false,
                            "COLUMN_DEFAULT", "'NEW'", "GENERATION_EXPRESSION", "'NEW'",
                            "COLUMN_REPRESENTABLE", true),
                        row("COLUMN_NAME", "amount", "DATA_TYPE", "DECIMAL", "NUMERIC_PRECISION", 12,
                            "NUMERIC_SCALE", 2, "NULLABLE", "NO", "IS_IDENTITY", false,
                            "COLUMN_REPRESENTABLE", true));
            }
            if (normalized.contains("FROM INFORMATION_SCHEMA.TABLES T")) {
                return rows(row("TABLE_COMMENT", "订单", "TABLE_REPRESENTABLE", true));
            }
            if (normalized.contains("FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS")) {
                return rows(row("CONSTRAINT_NAME", "ck_orders_amount",
                                "CHECK_EXPRESSION", "(\"amount\" >= 0)",
                                "CHECK_REPRESENTABLE", true));
            }
            if (normalized.contains("CONSTRAINT_TYPE = 'PRIMARY KEY'")) {
                return rows(row("CONSTRAINT_NAME", "pk_orders", "COLUMN_NAME", "id",
                                "CONSTRAINT_REPRESENTABLE", true));
            }
            if (normalized.contains("CONSTRAINT_TYPE = 'UNIQUE'")) {
                return rows(row("CONSTRAINT_NAME", "uk_orders_status", "COLUMN_NAME", "status",
                                "CONSTRAINT_REPRESENTABLE", true));
            }
            if (normalized.contains("FROM INFORMATION_SCHEMA.INDEXES I")) {
                return rows(row("INDEX_NAME", "ix_orders_status", "COLUMN_NAME", "status",
                                "UNIQUE_INDEX", false, "INDEX_REPRESENTABLE", true,
                                "INDEX_DIRECTION", "DESC"));
            }
            if (normalized.contains("FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC")) {
                return rows(row("TABLE_SCHEMA", "app", "FOREIGN_KEY_NAME", "fk_orders_customer",
                                "COLUMN_NAME", "customer_id", "REFERENCED_TABLE_SCHEMA", "crm",
                                "REFERENCED_TABLE_NAME", "customers", "REFERENCED_COLUMN_NAME", "id",
                                "ON_DELETE", "CASCADE", "ON_UPDATE", "NO_ACTION",
                                "CONSTRAINT_REPRESENTABLE", true));
            }
            throw new AssertionError("unexpected H2 metadata query: " + request.sql());
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
