package com.flying.orm.compat;

import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.FieldProtectionRegistry;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.jdbc.JdbcConnectionAccess;
import com.flying.orm.rdb.jdbc.JdbcSqlExecutor;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.reactive.R2dbcConnectionAccess;
import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;
import com.flying.orm.rdb.repository.ReactiveFormRepository;
import com.flying.orm.rdb.repository.SyncFormRepository;
import com.flying.orm.rdb.schema.JdbcSchemaClient;
import com.flying.orm.rdb.schema.ReactiveSchemaClient;
import org.reactivestreams.Publisher;

import java.util.Map;

/**
 * Compiles representative upper-service usage against the current public API.
 */
public final class UpperServiceConsumer {

    private UpperServiceConsumer() {
    }

    public static void compilePublicSurface(DynamicForm form,
                                            ConditionGroup where,
                                            Publisher<CompatibilityEntity> entities,
                                            Publisher<Map<String, Object>> formRows,
                                            SyncFormRepository<CompatibilityEntity> syncRepository,
                                            ReactiveFormRepository<CompatibilityEntity> reactiveRepository,
                                            JdbcSchemaClient jdbcSchema,
                                            ReactiveSchemaClient reactiveSchema,
                                            JdbcSqlExecutor jdbcExecutor,
                                            R2dbcSqlExecutor r2dbcExecutor,
                                            BatchWriteRequest batchRequest,
                                            ProtectedFieldKeyRing keyRing,
                                            SqlRequest request) {
        DataScope scope = DataScope.all().withFields(FieldScope.readWrite("id", "name"));
        QuerySpec.of(form, where).withScope(scope).showSensitive();

        BatchWriteOptions options = BatchWriteOptions.of(100);
        BatchSpec.insert(form, formRows).withScope(scope).withOptions(options);
        syncRepository.createQuery();
        syncRepository.insertBatch(entities, options);
        reactiveRepository.createQuery();
        reactiveRepository.insertBatch(entities, options);

        jdbcSchema.createTable(form);
        reactiveSchema.createTable(form);
        jdbcExecutor.query(request);
        r2dbcExecutor.query(request);
        r2dbcExecutor.writeBatch(batchRequest);

        FieldProtectionRegistry.empty().isEmpty();
        keyRing.currentVersion();
    }

    public static BatchExecutionEvidence compileExecutionSurface(JdbcConnectionAccess jdbc,
                                                                 R2dbcConnectionAccess reactive,
                                                                 RdbDialect dialect,
                                                                 BatchWriteRequest batch) {
        FlyingOrmClients.builder(jdbc).dialect(dialect);
        FlyingOrmClients.builder(reactive).dialect(dialect);
        FlyingOrmClients.builder(jdbc, reactive).dialect(dialect);
        return com.flying.orm.rdb.jdbc.JdbcBatchWriter.create(jdbc, dialect).writeBatch(batch);
    }

    public static final class CompatibilityEntity {
        private Long id;
        private String name;

        @TableField(exist = false)
        private String displayName;
    }
}
