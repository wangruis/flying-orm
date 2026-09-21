package com.flying.orm.rdb.schema;

import com.flying.orm.rdb.execution.ConnectionAccessTestSupport;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.IndexKeyPart;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReaders;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReaders;
import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import io.r2dbc.h2.H2ConnectionFactoryProvider;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PROTOCOL;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class H2ForeignKeyIndexReadbackTest {

    private static final RdbDialect DIALECT = RdbDialect.h2();
    private static final DatabaseDescriptor DESCRIPTOR = DatabaseDescriptor.of("H2", "2.4.240", DIALECT);
    private static final RelationIdentity CHILD = RelationIdentity.of(null, "public", "children");
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    @TestFactory
    Stream<DynamicTest> retainsExplicitForeignKeyIndexesButHidesGeneratedBackingIndexes() {
        return Stream.of(false, true).flatMap(reactive -> Stream.of(false, true).map(explicit ->
                DynamicTest.dynamicTest((reactive ? "R2DBC" : "JDBC") + " explicit=" + explicit,
                        () -> verifyRoundTrip(reactive, explicit))));
    }

    private static void verifyRoundTrip(boolean reactive, boolean explicit) throws Exception {
        String database = "fk_index_" + UUID.randomUUID().toString().replace("-", "");
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:" + database);
        source.setUser("sa");
        source.setPassword("");
        // 只由 keeper 保持本次内存库存活；关闭后丢弃测试结构，不操作外部数据库。
        try (var keeper = source.getConnection(); var statement = keeper.createStatement()) {
            statement.execute("create table parents (id bigint primary key)");
            RelationalTableDefinition desired = child(explicit);
            SchemaSnapshot snapshot;
            SchemaExecutionReport report;
            ReviewedSchemaPlan next;
            if (reactive) {
                var factory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                        .option(DRIVER, "h2").option(PROTOCOL, H2ConnectionFactoryProvider.PROTOCOL_MEM)
                        .option(DATABASE, database).option(USER, "sa").option(PASSWORD, "").build());
                var executor = R2dbcSqlExecutor.create(ConnectionAccessTestSupport.reactive(factory), DIALECT);
                var reader = ReactiveFormMetadataReaders.create(executor, DIALECT);
                var client = ReactiveSchemaClient.create(executor, DIALECT);
                var plan = client.reviewRelational(DESCRIPTOR, desired, reader, SchemaCompatibilityMode.EXACT)
                        .block(TIMEOUT);
                assertFalse(plan.requiresManualAction());
                report = client.executeReviewed(plan, reader, SchemaMigrationApproval.approve(plan,
                        "isolated H2 foreign-key index regression")).block(TIMEOUT);
                snapshot = reader.readSnapshot(CHILD).block(TIMEOUT);
                next = client.reviewRelational(DESCRIPTOR, desired, reader, SchemaCompatibilityMode.EXACT)
                        .block(TIMEOUT);
            } else {
                var executor = SyncSqlExecutor.jdbc(ConnectionAccessTestSupport.jdbc(source), DIALECT);
                var reader = JdbcFormMetadataReaders.create(executor, DIALECT);
                var client = JdbcSchemaClient.create(executor, DIALECT);
                var plan = client.reviewRelational(DESCRIPTOR, desired, reader, SchemaCompatibilityMode.EXACT);
                assertFalse(plan.requiresManualAction());
                report = client.executeReviewed(plan, reader, SchemaMigrationApproval.approve(plan,
                        "isolated H2 foreign-key index regression"));
                snapshot = reader.readSnapshot(CHILD);
                next = client.reviewRelational(DESCRIPTOR, desired, reader, SchemaCompatibilityMode.EXACT);
            }
            assertEquals(SchemaExecutionStatus.SUCCESS, report.status(), report.verification().toString());
            assertEquals(explicit ? List.of("fk_parent") : List.of(),
                    snapshot.completeTable().orElseThrow().indexes().stream().map(IndexDefinition::name).toList());
            assertEquals(1, snapshot.completeTable().orElseThrow().foreignKeys().size());
            assertTrue(next.steps().isEmpty(), "a second review must not recreate an existing explicit index");
        }
    }

    private static RelationalTableDefinition child(boolean explicit) {
        var builder = RelationalTableDefinition.builder(CHILD)
                .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
                .addColumn(ColumnDefinition.builder("parent_id", "BIGINT").build())
                .primaryKey(PrimaryKeyDefinition.of("pk_children", "id"))
                .addForeignKey(ForeignKeyDefinition.builder("fk_parent").addColumn("parent_id")
                        .reference(RelationIdentity.of(null, "public", "parents")).addReferenceColumn("id").build());
        if (explicit) {
            builder.addIndex(IndexDefinition.builder("fk_parent").addKey(IndexKeyPart.asc("parent_id")).build());
        }
        return builder.build();
    }
}
