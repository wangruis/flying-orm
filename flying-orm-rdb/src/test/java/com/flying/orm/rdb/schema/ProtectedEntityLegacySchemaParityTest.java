package com.flying.orm.rdb.schema;

import com.flying.orm.core.annotation.EncryptedField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableIndex;
import com.flying.orm.core.annotation.TableIndexColumn;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.annotation.TableUnique;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.mapping.EntitySchemaDescriptor;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReaders;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReaders;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProtectedEntityLegacySchemaParityTest {

    @Test
    void legacyEntitySynchronizationUsesTheDescriptorsNamedPhysicalIndexes() {
        RecordingExecutor executor = new RecordingExecutor();
        RdbDialect dialect = RdbDialect.h2();
        JdbcSchemaClient client = JdbcSchemaClient.create(executor, dialect);
        var reader = JdbcFormMetadataReaders.create(executor, dialect);

        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            EntitySchemaDescriptor<?> descriptor = models.schemaDescriptor(ProtectedAccount.class);
            String exactColumn = descriptor.table().uniqueConstraints().getFirst().columns().getFirst();
            EntitySchemaSynchronizer synchronizer = new EntitySchemaSynchronizer(
                    models, null, null, client, reader);

            synchronizer.synchronize(EntitySchemaSyncMode.SAFE_UPDATE,
                    ProtectedAccount.class);

            assertPhysicalNames(executor.updates, exactColumn);
        }
    }

    @Test
    void reactiveLegacyEntitySynchronizationUsesTheSameDescriptorFacts() {
        RecordingReactiveExecutor executor = new RecordingReactiveExecutor();
        RdbDialect dialect = RdbDialect.h2();
        ReactiveSchemaClient client = ReactiveSchemaClient.create(executor, dialect);
        var reader = ReactiveFormMetadataReaders.create(executor, dialect);

        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            EntitySchemaDescriptor<?> descriptor = models.schemaDescriptor(ProtectedAccount.class);
            String exactColumn = descriptor.table().uniqueConstraints().getFirst().columns().getFirst();
            EntitySchemaSynchronizer synchronizer = new EntitySchemaSynchronizer(
                    models, client, reader, null, null);

            synchronizer.synchronizeReactive(EntitySchemaSyncMode.SAFE_UPDATE,
                    ProtectedAccount.class).block();

            assertPhysicalNames(executor.updates, exactColumn);
        }
    }

    private static void assertPhysicalNames(List<SqlRequest> requests, String exactColumn) {
        String ddl = String.join(";", requests.stream().map(SqlRequest::sql).toList());
        assertTrue(ddl.contains("uq_protected_account_mobile"));
        assertTrue(ddl.contains("idx_protected_account_mobile"));
        assertTrue(ddl.contains(exactColumn));
        assertFalse(ddl.contains("uk_pr_"));
    }

    @TableName("protected_account_legacy")
    @TableUnique(id = "mobile-unique", name = "uq_protected_account_mobile", properties = "mobile")
    @TableIndex(id = "mobile-index", name = "idx_protected_account_mobile", unique = true,
            columns = @TableIndexColumn(property = "mobile"))
    private static final class ProtectedAccount {
        @TableId
        private Long id;
        @EncryptedField
        private String mobile;
    }

    private static final class RecordingExecutor implements SyncSqlExecutor {
        private final List<SqlRequest> updates = new ArrayList<>();

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            return List.of();
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            updates.add(request);
            return 0;
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(
                SqlRequest request, SqlExecutionOptions options) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class RecordingReactiveExecutor implements ReactiveSqlExecutor {
        private final List<SqlRequest> updates = new ArrayList<>();

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            return Flux.empty();
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            updates.add(request);
            return Mono.just(0L);
        }
    }
}
