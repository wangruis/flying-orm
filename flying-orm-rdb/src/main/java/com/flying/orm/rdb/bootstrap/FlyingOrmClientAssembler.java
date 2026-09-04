package com.flying.orm.rdb.bootstrap;

import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.internal.cache.SchemaCacheInvalidationCoordinator;
import com.flying.orm.rdb.internal.plan.StructuralPlanCaches;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.mapping.EntitySchemaDescriptor;
import com.flying.orm.rdb.mapping.EntityTypeMappingRegistry;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReaders;
import com.flying.orm.rdb.metadata.MetadataCacheInvalidator;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReaders;
import com.flying.orm.rdb.metadata.SyncFormMetadataReader;
import com.flying.orm.rdb.operator.DatabaseOperator;
import com.flying.orm.rdb.operator.SyncDatabaseOperator;
import com.flying.orm.rdb.schema.JdbcSchemaClient;
import com.flying.orm.rdb.schema.ReactiveSchemaClient;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;

/** 把已经校验的双内核配置装成共享同一方言、安全规则和缓存的客户端对象图。 */
final class FlyingOrmClientAssembler {

    private FlyingOrmClientAssembler() {
    }

    static FlyingOrmClients assemble(FlyingOrmAssemblyRequest request) {
        StructuralPlanCaches planCaches = StructuralPlanCaches.create(request.cachePolicy());
        EntityModelRegistry entityModels = EntityModelRegistry.create(
                request.cachePolicy().entityMappings(), request.idGenerator(), request.fieldFiller(),
                request.entitySchemas());
        FormDataSqlRenderer formRenderer = FormDataSqlRenderer.create(request.renderer(), request.dialect());
        Map<DynamicField, EntityTypeMappingRegistry.Mapping> entityFieldCodecs = entityFieldCodecs(
                request.entitySchemas());
        if (!entityFieldCodecs.isEmpty()) {
            formRenderer = formRenderer.withEntityFieldCodecs(entityFieldCodecs);
        }
        formRenderer = formRenderer.withPlanCaches(planCaches)
                                                              .withResultPlanCachePolicy(
                                                                      request.cachePolicy().sqlPlans())
                                                              .withProtectedFields(request.protectedFields());
        FlyingOrmReactiveRuntime reactive = reactive(request, formRenderer, entityModels, planCaches);
        MetadataCacheInvalidator reactiveInvalidator = reactive != null
                && reactive.metadata() instanceof MetadataCacheInvalidator cacheInvalidator
                ? cacheInvalidator : MetadataCacheInvalidator.none();
        FlyingOrmJdbcRuntime jdbc = jdbc(request, formRenderer, entityModels, reactiveInvalidator, planCaches);
        SchemaCacheInvalidationCoordinator invalidator = coordinate(
                reactiveInvalidator,
                jdbc == null ? MetadataCacheInvalidator.none() : jdbc.jdbcMetadata(),
                planCaches,
                formRenderer.resultPlanInvalidator());
        if (reactive != null) {
            ReactiveSchemaClient schema = reactive.schema().withMetadataInvalidator(invalidator);
            DatabaseOperator operator = DatabaseOperator.create(
                    schema,
                    reactive.forms(),
                    reactive.executor(),
                    request.renderer(),
                    reactive.metadata(),
                    request.dialect())
                    .withSqlTemplates(request.sqlTemplates(), request.reactiveTemplateParameters());
            reactive = new FlyingOrmReactiveRuntime(
                    reactive.executor(), reactive.forms(), schema, reactive.metadata(), operator);
        }
        FlyingOrmCacheGraph cacheGraph = new FlyingOrmCacheGraph(entityModels, invalidator);
        return new FlyingOrmClients(reactive, jdbc, request.renderer(), request.dialect(), planCaches, cacheGraph,
                                    request.protectedFields());
    }

    private static Map<DynamicField, EntityTypeMappingRegistry.Mapping> entityFieldCodecs(
            Map<Class<?>, EntitySchemaDescriptor<?>> schemas) {
        if (schemas.isEmpty()) {
            return Map.of();
        }
        Map<DynamicField, EntityTypeMappingRegistry.Mapping> codecs = new IdentityHashMap<>();
        schemas.values().forEach(schema -> codecs.putAll(schema.customFieldCodecs()));
        return codecs.isEmpty() ? Map.of() : Collections.unmodifiableMap(codecs);
    }

    private static FlyingOrmReactiveRuntime reactive(FlyingOrmAssemblyRequest request,
                                                      FormDataSqlRenderer formRenderer,
                                                      EntityModelRegistry entityModels,
                                                      StructuralPlanCaches planCaches) {
        if (request.reactiveExecutor() == null) return null;
        ReactiveFormClient forms = ReactiveFormClient.create(request.reactiveExecutor(), formRenderer)
                                                     .withStructuredConditionResolver(request.resolver())
                                                     .withEntityModelRegistry(entityModels)
                                                     .withDefaultExecutionOptions(request.executionOptions());
        ReactiveSchemaClient schema = ReactiveSchemaClient.create(request.reactiveExecutor(), request.dialect());
        ReactiveFormMetadataReader raw = ReactiveFormMetadataReaders.create(
                request.reactiveExecutor(), request.dialect());
        ReactiveFormMetadataReader metadata = !request.cachePolicy().metadata().enabled()
                ? raw : ReactiveFormMetadataReaders.cached(
                        raw, request.cachePolicy().metadata(), MetadataCacheInvalidator.none());
        DatabaseOperator operator = DatabaseOperator.create(
                schema, forms, request.reactiveExecutor(), request.renderer(), metadata, request.dialect())
                .withSqlTemplates(request.sqlTemplates(), request.reactiveTemplateParameters());
        return new FlyingOrmReactiveRuntime(request.reactiveExecutor(), forms, schema, metadata, operator);
    }

    private static FlyingOrmJdbcRuntime jdbc(FlyingOrmAssemblyRequest request,
                                              FormDataSqlRenderer formRenderer,
                                              EntityModelRegistry entityModels,
                                              MetadataCacheInvalidator reactiveInvalidator,
                                              StructuralPlanCaches planCaches) {
        if (request.syncExecutor() == null) return null;
        SyncFormClient forms = SyncFormClient.create(
                request.syncExecutor(), request.syncBatchExecutor(), formRenderer)
                .withStructuredConditionResolver(request.resolver())
                .withEntityModelRegistry(entityModels)
                .withDefaultExecutionOptions(request.executionOptions());
        JdbcFormMetadataReader rawMetadata = request.cachePolicy().metadata().enabled()
                ? JdbcFormMetadataReaders.cached(request.syncExecutor(), request.dialect(),
                                                 request.cachePolicy().metadata(),
                                                 MetadataCacheInvalidator.none())
                : JdbcFormMetadataReaders.create(request.syncExecutor(), request.dialect());
        SyncFormMetadataReader metadata = SyncFormMetadataReader.create(rawMetadata);
        SchemaCacheInvalidationCoordinator invalidator = coordinate(
                reactiveInvalidator, rawMetadata, planCaches, formRenderer.resultPlanInvalidator());
        JdbcSchemaClient schema = JdbcSchemaClient.create(
                request.syncExecutor(), request.dialect(), request.jdbcTransactionParticipant())
                .withMetadataInvalidator(invalidator);
        SyncDatabaseOperator operator = SyncDatabaseOperator.create(new SyncDatabaseOperator.NativeComponents(
                forms, request.syncExecutor(), request.renderer(), DataScope.none(), request.dialect(), schema,
                rawMetadata, metadata, invalidator, request.sqlTemplates(), request.syncTemplateParameters()));
        return new FlyingOrmJdbcRuntime(request.syncExecutor(), forms, schema, rawMetadata, metadata, operator);
    }

    /** 双内核可以各自持有元数据缓存，但一次 DDL 必须把两边和下游计划同时清干净。 */
    static MetadataCacheInvalidator combine(MetadataCacheInvalidator first,
                                            MetadataCacheInvalidator second) {
        return coordinate(first, second, MetadataCacheInvalidator.none());
    }

    private static SchemaCacheInvalidationCoordinator coordinate(MetadataCacheInvalidator... invalidators) {
        Objects.requireNonNull(invalidators, "metadata invalidators must not be null");
        for (MetadataCacheInvalidator invalidator : invalidators) {
            Objects.requireNonNull(invalidator, "metadata invalidator must not be null");
        }
        return SchemaCacheInvalidationCoordinator.of(invalidators);
    }
}
