package com.flying.orm.rdb.metadata;

import com.flying.orm.core.metadata.CheckConstraintDefinition;
import com.flying.orm.core.metadata.CheckPredicate;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.ConnectionAccessTestSupport;
import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;
import com.flying.orm.rdb.schema.RelationalSchemaSqlRenderer;
import com.flying.orm.rdb.schema.SchemaOperation;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.sql.Connection;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class H2TypedLiteralReadbackTest {

    private static final List<LiteralCase> LITERALS = List.of(
            new LiteralCase("DATE", "DATE '2026-09-27'", LocalDate.of(2026, 9, 27)),
            new LiteralCase("TIME", "TIME '12:30:01'", LocalTime.of(12, 30, 1)),
            new LiteralCase("TIME WITH TIME ZONE", "TIME WITH TIME ZONE '12:30:01+08:00'",
                    OffsetTime.parse("12:30:01+08:00")),
            new LiteralCase("TIME WITH TIME ZONE", "TIME WITH TIME ZONE '12:30:01Z'",
                    OffsetTime.parse("12:30:01Z")),
            new LiteralCase("TIME WITH TIME ZONE", "TIME WITH TIME ZONE '12:30:01-05:30:15'",
                    OffsetTime.parse("12:30:01-05:30:15")),
            new LiteralCase("TIMESTAMP", "TIMESTAMP '2026-09-27 12:30:01.123456'",
                    LocalDateTime.parse("2026-09-27T12:30:01.123456")),
            new LiteralCase("TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH TIME ZONE '2026-09-27 12:30:01.123456+08:00'",
                    OffsetDateTime.parse("2026-09-27T12:30:01.123456+08:00")),
            new LiteralCase("UUID", "UUID '01234567-89ab-cdef-0123-456789abcdef'",
                    UUID.fromString("01234567-89ab-cdef-0123-456789abcdef")));

    @TestFactory
    Stream<DynamicTest> readsTypedDefaultsFromTheDatabaseDictionary() {
        return LITERALS.stream().map(literal -> DynamicTest.dynamicTest(literal.type(),
                () -> verifyReadback(literal, false)));
    }

    @TestFactory
    Stream<DynamicTest> readsChecksCreatedByTheOrmAfterDatabaseNormalization() {
        return LITERALS.stream().map(literal -> DynamicTest.dynamicTest(literal.type(),
                () -> verifyReadback(literal, true)));
    }

    private static void verifyReadback(LiteralCase literal, boolean check) throws Exception {
        String database = "typed_literal_" + UUID.randomUUID().toString().replace("-", "");
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:" + database);
        source.setUser("sa");
        RdbDialect dialect = RdbDialect.h2();
        try (Connection keeper = source.getConnection()) {
            SyncSqlExecutor jdbc = SyncSqlExecutor.jdbc(ConnectionAccessTestSupport.jdbc(source), dialect);
            CheckPredicate predicate = CheckPredicate.compare("event_value",
                    CheckPredicate.ComparisonOperator.GREATER_THAN_OR_EQUAL, literal.value());
            if (check) {
                RelationalTableDefinition desired = RelationalTableDefinition.builder(RelationIdentity.table("events"))
                        .addColumn(ColumnDefinition.builder("event_value", literal.type()).build())
                        .addCheck(CheckConstraintDefinition.of("ck_value", predicate)).build();
                RelationalSchemaSqlRenderer.create(dialect.schema()).render(SchemaOperation.of(
                        SchemaOperation.Kind.CREATE_TABLE, desired.identity(), "events", null, desired,
                        SchemaOperation.Compatibility.SAFE_INCREMENTAL)).forEach(jdbc::rowsUpdated);
            } else {
                jdbc.rowsUpdated(new SqlRequest("create table events (event_value " + literal.type()
                        + " default " + literal.expression() + ")", List.of()));
            }
            R2dbcSqlExecutor reactive = R2dbcSqlExecutor.create(ConnectionAccessTestSupport.reactive(
                    ConnectionFactories.get(ConnectionFactoryOptions.builder()
                            .option(ConnectionFactoryOptions.DRIVER, "h2")
                            .option(ConnectionFactoryOptions.PROTOCOL, "mem")
                            .option(ConnectionFactoryOptions.DATABASE, database)
                            .option(ConnectionFactoryOptions.USER, "sa").build())), dialect);
            for (RelationalTableDefinition actual : List.of(
                    JdbcFormMetadataReaders.create(jdbc, dialect).readSnapshot("events")
                            .completeTable().orElseThrow(),
                    ReactiveFormMetadataReaders.create(reactive, dialect).readSnapshot("events")
                            .block(Duration.ofSeconds(10)).completeTable().orElseThrow())) {
                if (check) {
                    assertEquals(predicate, actual.checks().getFirst().predicate());
                } else {
                    assertEquals(ColumnDefault.literal(literal.value()), actual.columns().getFirst().defaultValue());
                }
            }
        }
    }

    @Test
    void defaultsPreserveNativeSqlWithoutWeakeningTypedCheckPredicates() {
        MetadataQueryProfile.SnapshotDialect dialect =
                MetadataQueryProfile.SnapshotDialect.H2;
        for (LiteralCase literal : LITERALS) {
            for (String expression : List.of(literal.expression() + " + 1", literal.expression() + " || 'x'")) {
                assertEquals(ColumnDefault.nativeExpression(expression), RelationalMetadataValueParser.columnDefault(
                        expression, DatabaseType.of(literal.type()), dialect));
                assertThrows(IllegalStateException.class, () -> RelationalMetadataValueParser.checkPredicate(
                        "event_value >= " + expression, Map.of("event_value", DatabaseType.of(literal.type())), dialect));
            }
            assertEquals(ColumnDefault.nativeExpression(literal.expression()), RelationalMetadataValueParser.columnDefault(
                    literal.expression(), DatabaseType.of("VARCHAR"), dialect));
            assertThrows(IllegalStateException.class, () -> RelationalMetadataValueParser.checkPredicate(
                    "event_value >= " + literal.expression(), Map.of("event_value", DatabaseType.of("VARCHAR")), dialect));
        }
    }

    private record LiteralCase(String type, String expression, Object value) { }

    @Test
    void floatingColumnCheckRetainsAnExactDecimalBoundary() throws Exception {
        verifyReadback(new LiteralCase("DOUBLE", "CAST(1.0000000000000001 AS DECFLOAT)",
                new BigDecimal("1.0000000000000001")), true);
    }

    @Test
    void numericCastReadbackPreservesValuesAndRejectsLossyCasts() {
        MetadataQueryProfile.SnapshotDialect dialect =
                MetadataQueryProfile.SnapshotDialect.H2;
        Map<String, DatabaseType> types = Map.of("amount", DatabaseType.of("BIGINT"));
        assertEquals(CheckPredicate.compare("amount", CheckPredicate.ComparisonOperator.GREATER_THAN, 0L),
                RelationalMetadataValueParser.checkPredicate("amount > CAST(0 AS BIGINT)", types, dialect));
        assertEquals(CheckPredicate.compare("amount", CheckPredicate.ComparisonOperator.GREATER_THAN, new BigDecimal("1.5")),
                RelationalMetadataValueParser.checkPredicate("amount > CAST(1.5 AS DECFLOAT)",
                        Map.of("amount", DatabaseType.of("DOUBLE")), dialect));
        for (String cast : List.of("CAST(1.5 AS BIGINT)", "CAST(9223372036854775808 AS BIGINT)",
                "CAST(1.25 AS DECIMAL(3,1))", "CAST(100 AS DECIMAL(2))", "CAST(1.5 AS NUMERIC)")) {
            assertThrows(IllegalStateException.class, () -> RelationalMetadataValueParser.checkPredicate(
                    "amount > " + cast, types, dialect));
        }
    }
}
