package com.flying.orm.rdb.bootstrap;

import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.internal.ReflectionFailureSupport;
import com.flying.orm.rdb.internal.plan.StructuralPlanCaches;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
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

import java.util.Objects;

/** 把已经校验的双内核配置装成共享同一方言、安全规则和缓存的客户端对象图。 */
final class FlyingOrmClientAssembler {

    private FlyingOrmClientAssembler() {
    }

    static FlyingOrmClients assemble(FlyingOrmAssemblyRequest request) {
        StructuralPlanCaches planCaches = StructuralPlanCaches.create(request.cachePolicy());
        EntityModelRegistry entityModels = EntityModelRegistry.create(
                request.cachePolicy().entityMappings(), request.idGenerator(), request.fieldFiller());
        FormDataSqlRenderer formRenderer = FormDataSqlRenderer.create(request.renderer(), request.dialect())
                                                              .withPlanCaches(planCaches)
                                                              .withProtectedFields(request.protectedFields());
        FlyingOrmReactiveRuntime reactive = reactive(request, formRenderer, entityModels, planCaches);
        MetadataCacheInvalidator reactiveInvalidator = reactive != null
                && reactive.metadata() instanceof MetadataCacheInvalidator cacheInvalidator
                ? cacheInvalidator : MetadataCacheInvalidator.none();
        FlyingOrmJdbcRuntime jdbc = jdbc(request, formRenderer, entityModels, reactiveInvalidator, planCaches);
        MetadataCacheInvalidator invalidator = combine(
                reactiveInvalidator,
                jdbc == null ? MetadataCacheInvalidator.none() : jdbc.jdbcMetadata());
        if (reactive != null) {
            ReactiveSchemaClient schema = reactive.schema().withMetadataInvalidator(invalidator::invalidate);
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
        FlyingOrmCacheGraph cacheGraph = new FlyingOrmCacheGraph(entityModels, invalidator, planCaches);
        return new FlyingOrmClients(reactive, jdbc, request.renderer(), request.dialect(), planCaches, cacheGraph,
                                    request.protectedFields());
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
                ? raw : ReactiveFormMetadataReaders.cached(raw, request.cachePolicy().metadata(), planCaches);
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
                                                 request.cachePolicy().metadata(), planCaches)
                : JdbcFormMetadataReaders.create(request.syncExecutor(), request.dialect());
        SyncFormMetadataReader metadata = SyncFormMetadataReader.create(rawMetadata);
        MetadataCacheInvalidator invalidator = combine(reactiveInvalidator, rawMetadata);
        JdbcSchemaClient schema = JdbcSchemaClient.create(
                request.syncExecutor(), request.dialect(), request.jdbcTransactionParticipant())
                .withMetadataInvalidator(invalidator::invalidate);
        SyncDatabaseOperator operator = SyncDatabaseOperator.create(new SyncDatabaseOperator.NativeComponents(
                forms, request.syncExecutor(), request.renderer(), DataScope.none(), request.dialect(), schema,
                rawMetadata, metadata, invalidator, request.sqlTemplates(), request.syncTemplateParameters()));
        return new FlyingOrmJdbcRuntime(request.syncExecutor(), forms, schema, rawMetadata, metadata, operator);
    }

    /** 双内核可以各自持有元数据缓存，但一次 DDL 必须把两边和下游计划同时清干净。 */
    static MetadataCacheInvalidator combine(MetadataCacheInvalidator first,
                                            MetadataCacheInvalidator second) {
        MetadataCacheInvalidator safeFirst = Objects.requireNonNull(first, "first metadata invalidator must not be null");
        MetadataCacheInvalidator safeSecond = Objects.requireNonNull(second, "second metadata invalidator must not be null");
        if (safeFirst == safeSecond) {
            return safeFirst;
        }
        return new MetadataCacheInvalidator() {
            @Override
            public void invalidate(String table) {
                invalidateBoth(() -> safeFirst.invalidate(table), () -> safeSecond.invalidate(table));
            }

            @Override
            public void invalidate(String schema, String table) {
                invalidateBoth(() -> safeFirst.invalidate(schema, table),
                               () -> safeSecond.invalidate(schema, table));
            }

            @Override
            public void invalidateAll() {
                invalidateBoth(safeFirst::invalidateAll, safeSecond::invalidateAll);
            }
        };
    }

    /** 两个缓存都尽力失效后，再把首先观察到的 JVM 致命错误原样抛出。 */
    private static void invalidateBoth(Runnable first, Runnable second) {
        VirtualMachineError firstFatal = invalidateSafely(first);
        VirtualMachineError secondFatal = invalidateSafely(second);
        if (firstFatal != null) {
            throw firstFatal;
        }
        if (secondFatal != null) {
            throw secondFatal;
        }
    }

    /**
     * DDL 已成功后，单个可选缓存协作者故障不能阻止其余缓存清理。
     *
     * @return 需要在全部缓存失效完成后传播的 JVM 致命错误，没有则为 {@code null}
     */
    private static VirtualMachineError invalidateSafely(Runnable action) {
        try {
            action.run();
        } catch (VirtualMachineError fatal) {
            return fatal;
        } catch (RuntimeException failure) {
            try {
                ReflectionFailureSupport.rethrowVirtualMachineError(failure);
            } catch (VirtualMachineError fatal) {
                return fatal;
            }
            // 缓存失效是 DDL 成功后的尽力而为动作；其余协作者仍必须继续执行。
        }
        return null;
    }
}
