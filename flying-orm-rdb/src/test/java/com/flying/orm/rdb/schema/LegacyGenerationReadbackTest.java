package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.ConnectionAccessTestSupport;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReaders;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReaders;
import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.sql.Connection;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LegacyGenerationReadbackTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    @TestFactory
    Stream<DynamicTest> preservesGenerationOptionsAcrossReadbackAndPlanning() {
        return Stream.of(false, true).flatMap(reactive -> Stream.of(
                ValueGeneration.identity(10, 2, 32),
                ValueGeneration.sequence("events_seq", 10, 2, 32))
                .map(generation -> DynamicTest.dynamicTest(
                        (reactive ? "R2DBC " : "JDBC ") + generation.strategy(),
                        () -> verifyReadback(reactive, generation))));
    }

    private static void verifyReadback(boolean reactive, ValueGeneration generation) throws Exception {
        String database = "generation_" + UUID.randomUUID().toString().replace("-", "");
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:" + database);
        source.setUser("sa");
        RdbDialect dialect = RdbDialect.h2();
        DynamicForm desired = DynamicForm.builder("events", "events")
                .addField(DynamicField.primaryKey("id", "BIGINT").withGeneration(generation)).build();
        try (Connection keeper = source.getConnection()) {
            SyncSqlExecutor jdbc = SyncSqlExecutor.jdbc(ConnectionAccessTestSupport.jdbc(source), dialect);
            JdbcSchemaClient schema = JdbcSchemaClient.create(jdbc, dialect);
            schema.createTable(desired);
            DynamicForm form;
            TableMetadata table;
            SchemaMigrationPlan plan;
            if (reactive) {
                ConnectionFactory factory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                        .option(ConnectionFactoryOptions.DRIVER, "h2")
                        .option(ConnectionFactoryOptions.PROTOCOL, "mem")
                        .option(ConnectionFactoryOptions.DATABASE, database)
                        .option(ConnectionFactoryOptions.USER, "sa").build());
                R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(
                        ConnectionAccessTestSupport.reactive(factory), dialect);
                ReactiveFormMetadataReader reader = ReactiveFormMetadataReaders.create(executor, dialect);
                form = reader.readForm("events", "events").block(TIMEOUT);
                table = reader.readTable("events").block(TIMEOUT);
                plan = ReactiveSchemaClient.create(executor, dialect)
                        .planCreateOrAlter(desired, List.of(), reader).block(TIMEOUT);
            } else {
                JdbcFormMetadataReader reader = JdbcFormMetadataReaders.create(jdbc, dialect);
                form = reader.readForm("events", "events");
                table = reader.readTable("events");
                plan = schema.planCreateOrAlter(desired, List.of(), reader);
            }
            assertAll(
                    () -> assertEquals(generation, form.field("id").generation()),
                    () -> assertEquals(generation, table.columns().getFirst().generation()),
                    () -> assertTrue(plan.requests().isEmpty(), plan.requests().toString()),
                    () -> assertTrue(plan.skippedChanges().isEmpty(), plan.skippedChanges().toString()));
        }
    }
}
