package com.flying.orm.rdb.schema;

import com.flying.orm.rdb.execution.ConnectionAccessTestSupport;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.IndexKeyPart;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
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

class H2UniqueIndexReadbackTest {

    private static final RdbDialect DIALECT = RdbDialect.h2();
    private static final DatabaseDescriptor DESCRIPTOR = DatabaseDescriptor.of("H2", "2.4.240", DIALECT);
    private static final RelationIdentity TABLE = RelationIdentity.of(null, "public", "accounts");
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    @TestFactory
    Stream<DynamicTest> preservesExplicitIndexAcrossUniqueConstraintAdditionAndRemoval() {
        return Stream.of(false, true).flatMap(reactive -> Stream.of(false, true).map(explicit ->
                DynamicTest.dynamicTest((reactive ? "R2DBC" : "JDBC") + " explicit=" + explicit,
                        () -> verifyEvolution(reactive, explicit))));
    }

    private static void verifyEvolution(boolean reactive, boolean explicit) throws Exception {
        String database = "unique_index_" + UUID.randomUUID().toString().replace("-", "");
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:" + database);
        source.setUser("sa");
        source.setPassword("");
        // 每个场景独占内存库；所有结构变更都经过公开的审核、批准、执行及回读入口。
        try (var keeper = source.getConnection()) {
            var jdbcExecutor = SyncSqlExecutor.jdbc(ConnectionAccessTestSupport.jdbc(source), DIALECT);
            var jdbcReader = JdbcFormMetadataReaders.create(jdbcExecutor, DIALECT);
            var jdbc = JdbcSchemaClient.create(jdbcExecutor, DIALECT);
            var factory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                    .option(DRIVER, "h2").option(PROTOCOL, H2ConnectionFactoryProvider.PROTOCOL_MEM)
                    .option(DATABASE, database).option(USER, "sa").option(PASSWORD, "").build());
            var reactiveExecutor = R2dbcSqlExecutor.create(ConnectionAccessTestSupport.reactive(factory), DIALECT);
            var reactiveReader = ReactiveFormMetadataReaders.create(reactiveExecutor, DIALECT);
            var r2dbc = ReactiveSchemaClient.create(reactiveExecutor, DIALECT);
            for (boolean unique : List.of(false, true, false)) {
                RelationalTableDefinition desired = table(explicit, unique);
                var plan = reactive
                        ? r2dbc.reviewRelational(DESCRIPTOR, desired, reactiveReader, SchemaCompatibilityMode.EXACT)
                                .block(TIMEOUT)
                        : jdbc.reviewRelational(DESCRIPTOR, desired, jdbcReader, SchemaCompatibilityMode.EXACT);
                assertFalse(plan.requiresManualAction());
                var approval = SchemaMigrationApproval.approve(plan, "isolated H2 unique index ownership regression");
                var report = reactive
                        ? r2dbc.executeReviewed(plan, reactiveReader, approval).block(TIMEOUT)
                        : jdbc.executeReviewed(plan, jdbcReader, approval);
                assertEquals(SchemaExecutionStatus.SUCCESS, report.status(), report.verification().toString());
                var snapshot = reactive ? reactiveReader.readSnapshot(TABLE).block(TIMEOUT)
                        : jdbcReader.readSnapshot(TABLE);
                var actual = snapshot.completeTable().orElseThrow();
                assertEquals(explicit ? List.of("ix_code") : List.of(),
                        actual.indexes().stream().map(IndexDefinition::name).toList());
                assertEquals(unique ? 1 : 0, actual.uniqueConstraints().size());
                var next = reactive
                        ? r2dbc.reviewRelational(DESCRIPTOR, desired, reactiveReader, SchemaCompatibilityMode.EXACT)
                                .block(TIMEOUT)
                        : jdbc.reviewRelational(DESCRIPTOR, desired, jdbcReader, SchemaCompatibilityMode.EXACT);
                assertTrue(next.steps().isEmpty(), "constraint changes must not hide or recreate an explicit index");
            }
        }
    }

    private static RelationalTableDefinition table(boolean explicit, boolean unique) {
        var builder = RelationalTableDefinition.builder(TABLE)
                .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
                .addColumn(ColumnDefinition.builder("code", "BIGINT").build())
                .primaryKey(PrimaryKeyDefinition.of("pk_accounts", "id"));
        if (explicit) {
            builder.addIndex(IndexDefinition.builder("ix_code").unique().addKey(IndexKeyPart.asc("code")).build());
        }
        if (unique) {
            builder.addUnique(UniqueConstraintDefinition.of("uq_code", "code"));
        }
        return builder.build();
    }
}
