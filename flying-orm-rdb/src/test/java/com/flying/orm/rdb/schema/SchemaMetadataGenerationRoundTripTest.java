package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReaders;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaMetadataGenerationRoundTripTest {

    @Test
    void theDefaultIdentityCacheCanBeReadBackAndVerified() throws Exception {
        assertRoundTrip("identity_cache", ValueGeneration.identity());
    }

    @Test
    void anExplicitlyQualifiedSequenceRetainsItsIdentityDuringVerification() throws Exception {
        assertRoundTrip("qualified_sequence", ValueGeneration.sequence("tenant.events_seq"));
    }

    @Test
    void anAddedColumnReusesTheQualifiedSequenceObservedOnAnExistingColumn() throws Exception {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:qualified_sequence_add_column");
        dataSource.setUser("sa");
        try (Connection keeper = dataSource.getConnection(); Statement setup = keeper.createStatement()) {
            setup.execute("create schema tenant");
            RdbDialect dialect = RdbDialect.h2();
            SyncSqlExecutor executor = SyncSqlExecutor.jdbc(dataSource);
            var metadata = JdbcFormMetadataReaders.create(executor, dialect);
            var client = JdbcSchemaClient.create(executor, dialect);
            var reviewer = RelationalSchemaPlanReviewer.create(dialect);
            DatabaseDescriptor database = DatabaseDescriptor.of("H2", "2.x", dialect);
            RelationIdentity identity = RelationIdentity.of(null, "tenant", "events");
            ValueGeneration generation = ValueGeneration.sequence("tenant.events_seq");
            ColumnDefinition id = ColumnDefinition.builder("id", "BIGINT").nullable(false)
                    .generation(generation).build();
            RelationalTableDefinition initial = RelationalTableDefinition.builder(identity).addColumn(id).build();
            var creation = reviewer.review(database, initial, metadata.readSnapshot("tenant", "events"),
                    metadata.snapshotCoverage(), SchemaCompatibilityMode.EXACT);
            assertEquals(SchemaExecutionStatus.SUCCESS, client.executeReviewed(creation, metadata).status());

            SchemaSnapshot actual = metadata.readSnapshot("tenant", "events");
            assertEquals("events_seq", actual.columns().value().getFirst().generation().sequenceName());
            RelationalTableDefinition desired = RelationalTableDefinition.builder(identity).addColumn(id)
                    .addColumn(ColumnDefinition.builder("second_value", "BIGINT").generation(generation).build())
                    .build();
            var addition = reviewer.review(database, desired, actual,
                    metadata.snapshotCoverage(), SchemaCompatibilityMode.EXACT);

            assertEquals(0, addition.requests().stream()
                    .filter(request -> request.sql().startsWith("create sequence ")).count());
            assertEquals(SchemaExecutionStatus.SUCCESS, client.executeReviewed(addition, metadata,
                    SchemaMigrationApproval.approve(addition, "reuse existing qualified sequence")).status());
            var verified = reviewer.review(database, desired, metadata.readSnapshot("tenant", "events"),
                    metadata.snapshotCoverage(), SchemaCompatibilityMode.EXACT);
            assertTrue(verified.steps().isEmpty(), verified.operations().toString());
        }
    }

    private static void assertRoundTrip(String databaseName, ValueGeneration generation) throws Exception {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:" + databaseName);
        dataSource.setUser("sa");
        try (Connection keeper = dataSource.getConnection(); Statement setup = keeper.createStatement()) {
            setup.execute("create schema tenant");
            RdbDialect dialect = RdbDialect.h2();
            SyncSqlExecutor executor = SyncSqlExecutor.jdbc(dataSource);
            var metadata = JdbcFormMetadataReaders.create(executor, dialect);
            RelationIdentity identity = RelationIdentity.of(null, "tenant", "events");
            RelationalTableDefinition desired = RelationalTableDefinition.builder(identity)
                    .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false)
                            .generation(generation).build())
                    .build();
            var reviewer = RelationalSchemaPlanReviewer.create(dialect);
            DatabaseDescriptor database = DatabaseDescriptor.of("H2", "2.x", dialect);
            var plan = reviewer.review(database, desired, metadata.readSnapshot("tenant", "events"),
                    metadata.snapshotCoverage(), SchemaCompatibilityMode.EXACT);

            var report = JdbcSchemaClient.create(executor, dialect).executeReviewed(plan, metadata);

            assertEquals(SchemaExecutionStatus.SUCCESS, report.status());
            var verified = reviewer.review(database, desired, metadata.readSnapshot("tenant", "events"),
                    metadata.snapshotCoverage(), SchemaCompatibilityMode.EXACT);
            assertTrue(verified.steps().isEmpty(), verified.operations().toString());
        }
    }
}
