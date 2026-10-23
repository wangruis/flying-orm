package com.flying.orm.rdb.schema;

import com.flying.orm.core.annotation.EncryptedField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.annotation.TableUnique;
import com.flying.orm.core.metadata.UniqueNullPolicy;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReaders;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReaders;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EntityUniqueNullPolicyTest {

    @Test
    void jdbcModesPlanDistinctForOrdinaryAndEncryptedFields() {
        for (EntitySchemaSyncMode mode : modes()) {
            for (Class<?> type : List.of(PlainDistinct.class, EncryptedDistinct.class)) {
                RecordingExecutor executor = new RecordingExecutor();
                RdbDialect dialect = RdbDialect.sqlServer();
                try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
                    EntitySchemaSynchronizer synchronizer = new EntitySchemaSynchronizer(models, null, null,
                            JdbcSchemaClient.create(executor, dialect), JdbcFormMetadataReaders.create(executor, dialect));
                    com.flying.orm.rdb.dialect.DatabaseDescriptor database =
                            com.flying.orm.rdb.dialect.DatabaseDescriptor.of("SQL Server", "16", dialect);
                    if (mode == EntitySchemaSyncMode.VALIDATE) {
                        EntitySchemaSyncException failure = assertThrows(EntitySchemaSyncException.class,
                                () -> synchronizer.synchronize(database, mode, Ordinary.class, type));
                        assertTrue(failure.report().hasDifferences());
                    } else {
                        assertTrue(synchronizer.synchronize(database, mode, Ordinary.class, type).successful());
                    }
                    assertTrue(executor.statements.get() > 0);
                }
            }
        }
    }

    @Test
    void reactiveModesUseTheSameCompletePlanOnSubscription() {
        for (EntitySchemaSyncMode mode : modes()) {
            for (Class<?> type : List.of(PlainDistinct.class, EncryptedDistinct.class)) {
                RecordingReactiveExecutor executor = new RecordingReactiveExecutor();
                RdbDialect dialect = RdbDialect.sqlServer();
                try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
                    EntitySchemaSynchronizer synchronizer = new EntitySchemaSynchronizer(models,
                            ReactiveSchemaClient.create(executor, dialect), ReactiveFormMetadataReaders.create(executor, dialect),
                            null, null);
                    var result = synchronizer.synchronizeReactive(
                            com.flying.orm.rdb.dialect.DatabaseDescriptor.of("SQL Server", "16", dialect),
                            mode, Ordinary.class, type);
                    assertEquals(0, executor.statements.get());
                    if (mode == EntitySchemaSyncMode.VALIDATE) {
                        EntitySchemaSyncException failure = assertThrows(EntitySchemaSyncException.class, result::block);
                        assertTrue(failure.report().hasDifferences());
                    } else {
                        assertTrue(result.block().successful());
                    }
                    assertTrue(executor.statements.get() > 0);
                }
            }
        }
    }

    private static List<EntitySchemaSyncMode> modes() {
        return List.of(EntitySchemaSyncMode.SAFE_UPDATE, EntitySchemaSyncMode.VALIDATE, EntitySchemaSyncMode.FULL_UPDATE);
    }

    @TableName("a_ordinary")
    private static final class Ordinary {
        @TableId
        private Long id;
    }

    @TableName("z_distinct_plain")
    @TableUnique(id = "uq_email", properties = "email", nullPolicy = UniqueNullPolicy.DISTINCT)
    private static final class PlainDistinct {
        @TableId
        private Long id;
        private String email;
    }

    @TableName("z_distinct_encrypted")
    @TableUnique(id = "uq_email", properties = "email", nullPolicy = UniqueNullPolicy.DISTINCT)
    private static final class EncryptedDistinct {
        @TableId
        private Long id;
        @EncryptedField
        private String email;
    }

    private static final class RecordingExecutor implements com.flying.orm.rdb.support.ListQueryFixture {
        private final AtomicInteger statements = new AtomicInteger();

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            statements.incrementAndGet();
            return List.of();
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            statements.incrementAndGet();
            return 0;
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class RecordingReactiveExecutor implements ReactiveSqlExecutor {
        private final AtomicInteger statements = new AtomicInteger();

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            statements.incrementAndGet();
            return Flux.empty();
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            statements.incrementAndGet();
            return Mono.just(0L);
        }
    }
}
