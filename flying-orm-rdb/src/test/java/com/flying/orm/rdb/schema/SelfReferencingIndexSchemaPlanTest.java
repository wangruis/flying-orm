package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.IndexKeyPart;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalSchemaDefinition;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReaders;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReaders;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SelfReferencingIndexSchemaPlanTest {

    private static final RelationIdentity TABLE = RelationIdentity.of(null, "audit", "tree_codes");
    private static final IndexDefinition INDEX = IndexDefinition.builder("ix_code").unique()
            .addKey(IndexKeyPart.asc("code")).build();

    @Test
    void bothPublicClientsCreateTheUniqueIndexBeforeItsSelfReferencingForeignKey() {
        RelationalTableDefinition table = table(TABLE).addIndex(INDEX).build();
        RelationalSchemaDefinition.of(List.of(table));
        for (List<String> sql : plans(table)) {
            assertEquals(3, sql.size());
            assertFalse(sql.getFirst().contains("foreign key"));
            assertTrue(sql.get(1).startsWith("create unique index \"ix_code\""));
            assertEquals("alter table \"audit\".\"tree_codes\" add constraint \"fk_parent\" "
                    + "foreign key (\"parent\") references \"audit\".\"tree_codes\" (\"code\")", sql.get(2));
        }
    }

    @Test
    void inlinePrimaryAndUniqueKeysDoNotNeedAnExtraAlterEvenWithARedundantIndex() {
        for (RelationalTableDefinition table : List.of(
                table(TABLE).primaryKey(PrimaryKeyDefinition.of("pk_code", "code")).addIndex(INDEX).build(),
                table(TABLE).addUnique(UniqueConstraintDefinition.of("uk_code", "code")).addIndex(INDEX).build())) {
            for (List<String> sql : plans(table)) {
                assertEquals(2, sql.size());
                assertTrue(sql.getFirst().contains("foreign key"));
                assertTrue(sql.get(1).startsWith("create unique index"));
            }
        }
    }

    @Test
    void sameTableNameInAnotherSchemaIsNotASelfReference() {
        RelationalTableDefinition table = table(RelationIdentity.of(null, "other", "tree_codes"))
                .addIndex(INDEX).build();
        for (List<String> sql : plans(table)) {
            assertEquals(2, sql.size());
            assertTrue(sql.getFirst().contains("references \"other\".\"tree_codes\""));
        }
    }

    @Test
    void compositeUniqueIndexPrecedesTheSelfReference() {
        RelationalTableDefinition table = RelationalTableDefinition.builder(TABLE)
                .addColumn(ColumnDefinition.builder("tenant", "INTEGER").nullable(false).build())
                .addColumn(ColumnDefinition.builder("code", "INTEGER").nullable(false).build())
                .addColumn(ColumnDefinition.builder("parent", "INTEGER").build())
                .addIndex(IndexDefinition.builder("ix_tenant_code").unique()
                        .addKey(IndexKeyPart.asc("tenant")).addKey(IndexKeyPart.asc("code")).build())
                .addForeignKey(ForeignKeyDefinition.builder("fk_parent")
                        .addColumn("tenant").addColumn("parent").reference(TABLE)
                        .addReferenceColumn("tenant").addReferenceColumn("code").build()).build();
        RelationalSchemaDefinition.of(List.of(table));
        for (List<String> sql : plans(table)) {
            assertEquals(3, sql.size());
            assertFalse(sql.getFirst().contains("foreign key"));
            assertTrue(sql.get(1).startsWith("create unique index"));
            assertTrue(sql.get(2).contains("foreign key (\"tenant\", \"parent\")"));
        }
    }

    @Test
    void explicitMySqlSupportIndexStillPrecedesTheSameNamedForeignKey() {
        RelationalTableDefinition table = table(RelationIdentity.table("parent_codes"))
                .addIndex(IndexDefinition.builder("fk_parent").addKey(IndexKeyPart.asc("parent")).build())
                .build();
        List<SqlRequest> requests = RelationalSchemaSqlRenderer.create(RdbDialect.mysql().schema()).render(
                SchemaOperation.of(SchemaOperation.Kind.CREATE_TABLE, TABLE, TABLE.table(),
                        null, table, SchemaOperation.Compatibility.REQUIRES_REVIEW));
        assertEquals(3, requests.size());
        assertFalse(requests.getFirst().sql().contains("foreign key"));
        assertTrue(requests.get(1).sql().startsWith("create index"));
        assertTrue(requests.get(2).sql().contains("foreign key"));
    }

    private static RelationalTableDefinition.Builder table(RelationIdentity reference) {
        return RelationalTableDefinition.builder(TABLE)
                .addColumn(ColumnDefinition.builder("code", "VARCHAR(32)").nullable(false).build())
                .addColumn(ColumnDefinition.builder("parent", "VARCHAR(32)").build())
                .addForeignKey(ForeignKeyDefinition.builder("fk_parent").addColumn("parent")
                        .reference(reference).addReferenceColumn("code").build());
    }

    private static List<List<String>> plans(RelationalTableDefinition table) {
        RdbDialect dialect = RdbDialect.postgresql();
        DatabaseDescriptor database = DatabaseDescriptor.of("PostgreSQL", "16", dialect);
        SyncSqlExecutor sync = new SyncSqlExecutor() {
            @Override
            public List<DynamicRow> query(SqlRequest request) {
                return List.of();
            }

            @Override
            public long rowsUpdated(SqlRequest request) {
                throw new AssertionError("plan review must not execute DDL");
            }

            @Override
            public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
                throw new AssertionError("plan review must not execute DDL");
            }
        };
        ReactiveSqlExecutor reactive = new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.empty();
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.error(new AssertionError("plan review must not execute DDL"));
            }
        };
        ReviewedSchemaPlan jdbc = JdbcSchemaClient.create(sync, dialect).reviewRelational(database, table,
                JdbcFormMetadataReaders.create(sync, dialect), SchemaCompatibilityMode.EXACT);
        ReviewedSchemaPlan r2dbc = ReactiveSchemaClient.create(reactive, dialect).reviewRelational(database, table,
                ReactiveFormMetadataReaders.create(reactive, dialect), SchemaCompatibilityMode.EXACT)
                .block(Duration.ofSeconds(5));
        return List.of(sql(jdbc), sql(r2dbc));
    }

    private static List<String> sql(ReviewedSchemaPlan plan) {
        return plan.steps().stream().map(step -> step.request().orElseThrow().sql()).toList();
    }
}
