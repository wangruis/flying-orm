package com.flying.orm.rdb.bootstrap;

import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.internal.cache.SchemaCacheInvalidationCoordinator;
import com.flying.orm.rdb.internal.plan.StructuralPlanCaches;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.mapping.EntitySchemaDescriptor;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReaders;
import com.flying.orm.rdb.metadata.MetadataCacheInvalidator;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReaders;
import com.flying.orm.rdb.operator.DatabaseOperator;
import com.flying.orm.rdb.operator.SyncDatabaseOperator;
import com.flying.orm.rdb.schema.JdbcSchemaClient;
import com.flying.orm.rdb.schema.ReactiveSchemaClient;


/** 把已经校验的双内核配置装成共享同一方言、安全规则和缓存的客户端对象图。 */
final class FlyingOrmClientAssembler {

    private FlyingOrmClientAssembler() {
    }

    static FlyingOrmClients assemble(FlyingOrmAssemblyRequest request) {
        StructuralPlanCaches planCaches = StructuralPlanCaches.create(request.cachePolicy());
        EntityModelRegistry entityModels = EntityModelRegistry.create(
                request.cachePolicy().entityMappings(), request.idGenerator(), request.fieldFiller(),
                request.entitySchemas());
        FormDataSqlRenderer formRenderer = FormDataSqlRenderer.create(request.renderer(), request.dialect(),
                planCaches, request.cachePolicy().sqlPlans(), request.protectedFields(),
                request.entitySchemas().values().stream().map(EntitySchemaDescriptor::metadata).toList());
        ReactiveFormMetadataReader reactiveMetadata = reactiveMetadata(request);
        JdbcFormMetadataReader jdbcMetadata = jdbcMetadata(request);
        SchemaCacheInvalidationCoordinator invalidator = SchemaCacheInvalidationCoordinator.of(
                reactiveMetadata == null ? MetadataCacheInvalidator.none() : reactiveMetadata,
                jdbcMetadata == null ? MetadataCacheInvalidator.none() : jdbcMetadata,
                planCaches,
                formRenderer.resultPlanInvalidator());
        DatabaseOperator reactive = reactive(request, formRenderer, entityModels, reactiveMetadata, invalidator);
        SyncDatabaseOperator jdbc = jdbc(request, formRenderer, entityModels, jdbcMetadata, invalidator);
        return new FlyingOrmClients(reactive, jdbc, planCaches,
                new FlyingOrmSharedResources(entityModels, invalidator, request.protectedFields()));
    }

    private static DatabaseOperator reactive(FlyingOrmAssemblyRequest request,
                                             FormDataSqlRenderer formRenderer,
                                             EntityModelRegistry entityModels,
                                             ReactiveFormMetadataReader metadata,
                                             SchemaCacheInvalidationCoordinator invalidator) {
        if (request.reactiveExecutor() == null) return null;
        ReactiveFormClient forms = ReactiveFormClient.create(request.reactiveExecutor(), formRenderer,
                request.resolver(), entityModels, request.executionOptions());
        if (request.batchWriteOptions() != null) forms = forms.withDefaultBatchWriteOptions(request.batchWriteOptions());
        ReactiveSchemaClient schema = ReactiveSchemaClient.create(request.reactiveExecutor(), request.dialect())
                .withMetadataInvalidator(invalidator);
        if (request.migrationOptions() != null) schema = schema.withDefaultMigrationExecutionOptions(request.migrationOptions());
        if (request.migrationObserver() != null) schema = schema.withMigrationObserver(request.migrationObserver());
        return DatabaseOperator.create(
                schema, forms, request.reactiveExecutor(), request.renderer(), metadata, request.dialect())
                .withSqlTemplates(request.sqlTemplates(), request.reactiveTemplateParameters());
    }

    private static SyncDatabaseOperator jdbc(FlyingOrmAssemblyRequest request,
                                             FormDataSqlRenderer formRenderer,
                                             EntityModelRegistry entityModels,
                                             JdbcFormMetadataReader rawMetadata,
                                             SchemaCacheInvalidationCoordinator invalidator) {
        if (request.syncExecutor() == null) return null;
        SyncFormClient forms = SyncFormClient.create(request.syncExecutor(), request.syncBatchExecutor(), formRenderer,
                request.resolver(), entityModels, request.executionOptions());
        if (request.batchWriteOptions() != null) forms = forms.withDefaultBatchWriteOptions(request.batchWriteOptions());
        JdbcSchemaClient schema = JdbcSchemaClient.create(
                request.syncExecutor(), request.dialect())
                .withMetadataInvalidator(invalidator);
        if (request.migrationOptions() != null) schema = schema.withDefaultMigrationExecutionOptions(request.migrationOptions());
        if (request.migrationObserver() != null) schema = schema.withMigrationObserver(request.migrationObserver());
        return SyncDatabaseOperator.create(new SyncDatabaseOperator.NativeComponents(
                forms, request.syncExecutor(), request.renderer(), DataScope.none(), request.dialect(), schema,
                rawMetadata, invalidator, request.sqlTemplates(), request.syncTemplateParameters()));
    }

    private static ReactiveFormMetadataReader reactiveMetadata(FlyingOrmAssemblyRequest request) {
        if (request.reactiveExecutor() == null) return null;
        if (request.reactiveMetadata() != null) return request.reactiveMetadata();
        ReactiveFormMetadataReader raw = ReactiveFormMetadataReaders.create(
                request.reactiveExecutor(), request.dialect());
        return !request.cachePolicy().metadata().enabled() ? raw : ReactiveFormMetadataReaders.cached(
                raw, request.cachePolicy().metadata(), MetadataCacheInvalidator.none());
    }

    private static JdbcFormMetadataReader jdbcMetadata(FlyingOrmAssemblyRequest request) {
        if (request.syncExecutor() == null) return null;
        if (request.jdbcMetadata() != null) return request.jdbcMetadata();
        return request.cachePolicy().metadata().enabled()
                ? JdbcFormMetadataReaders.cached(request.syncExecutor(), request.dialect(),
                        request.cachePolicy().metadata(), MetadataCacheInvalidator.none())
                : JdbcFormMetadataReaders.create(request.syncExecutor(), request.dialect());
    }

}
