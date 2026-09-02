package com.flying.orm.rdb.bootstrap;

import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.cache.OrmCachePolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.StructuredConditionResolver;
import com.flying.orm.rdb.id.IdGenerator;
import com.flying.orm.rdb.mapping.EntityFieldFiller;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import com.flying.orm.rdb.template.SqlTemplateParameterProvider;
import com.flying.orm.rdb.template.SqlTemplateRegistry;
import com.flying.orm.rdb.template.SyncSqlTemplateParameterProvider;
import com.flying.orm.rdb.transaction.JdbcTransactionParticipant;

import java.util.Objects;

/** 启动校验完成后的装配快照；只在启动期创建，不进入 SQL 热路径。 */
record FlyingOrmAssemblyRequest(ReactiveSqlExecutor reactiveExecutor,
                                SyncSqlExecutor syncExecutor,
                                SyncBatchExecutor syncBatchExecutor,
                                JdbcTransactionParticipant jdbcTransactionParticipant,
                                SqlRenderer renderer,
                                RdbDialect dialect,
                                StructuredConditionResolver resolver,
                                OrmCachePolicy cachePolicy,
                                SqlExecutionOptions executionOptions,
                                IdGenerator idGenerator,
                                EntityFieldFiller fieldFiller,
                                SqlTemplateRegistry sqlTemplates,
                                SqlTemplateParameterProvider reactiveTemplateParameters,
                                SyncSqlTemplateParameterProvider syncTemplateParameters,
                                ProtectedFieldRuntime protectedFields) {

    FlyingOrmAssemblyRequest {
        if ((syncExecutor == null) != (syncBatchExecutor == null)) {
            throw new IllegalArgumentException("JDBC SQL and batch executors must be configured together");
        }
        if (reactiveExecutor == null && syncExecutor == null) {
            throw new IllegalArgumentException("at least one SQL executor must be configured");
        }
        jdbcTransactionParticipant = Objects.requireNonNull(
                jdbcTransactionParticipant, "jdbc transaction participant must not be null");
        renderer = Objects.requireNonNull(renderer, "sql renderer must not be null");
        dialect = Objects.requireNonNull(dialect, "rdb dialect must not be null");
        resolver = Objects.requireNonNull(resolver, "structured condition resolver must not be null");
        cachePolicy = Objects.requireNonNull(cachePolicy, "orm cache policy must not be null");
        executionOptions = Objects.requireNonNull(executionOptions, "sql execution options must not be null");
        idGenerator = Objects.requireNonNull(idGenerator, "id generator must not be null");
        fieldFiller = Objects.requireNonNull(fieldFiller, "entity field filler must not be null");
        sqlTemplates = Objects.requireNonNull(sqlTemplates, "SQL template registry must not be null");
        reactiveTemplateParameters = Objects.requireNonNull(
                reactiveTemplateParameters, "reactive template parameter provider must not be null");
        syncTemplateParameters = Objects.requireNonNull(
                syncTemplateParameters, "sync template parameter provider must not be null");
        protectedFields = Objects.requireNonNull(protectedFields, "protected field runtime must not be null");
    }
}
