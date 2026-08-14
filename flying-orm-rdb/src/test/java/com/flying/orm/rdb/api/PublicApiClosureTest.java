package com.flying.orm.rdb.api;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchMemoryBudget;
import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import com.flying.orm.rdb.bootstrap.FlyingOrmClientBuilder;
import com.flying.orm.rdb.cache.BoundedCacheRegion;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReaders;
import com.flying.orm.rdb.operator.DatabaseOperator;
import com.flying.orm.rdb.operator.SqlTemplateOperator;
import com.flying.orm.rdb.operator.SyncDatabaseOperator;
import com.flying.orm.rdb.observation.ResourceCleanupObservation;
import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRowFactory;
import com.flying.orm.rdb.repository.ReactiveFormRepository;
import com.flying.orm.rdb.repository.SyncFormRepository;
import com.flying.orm.rdb.schema.SchemaMigrationReviewer;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import com.flying.orm.rdb.template.SqlTemplate;
import com.flying.orm.rdb.template.SqlTemplateEngine;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 守住 V1 已经收好的公开入口，防止以后为图方便又加回第二套批量契约或一长串创建重载。
 *
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
class PublicApiClosureTest {

    /** 客户端统一组装只从 Builder 开始，配置项继续增加时也不会膨胀成新的 create 重载。 */
    @Test
    void keepsBuilderAsTheOnlyStaticClientFactory() {
        Set<String> factoryNames = Arrays.stream(FlyingOrmClients.class.getDeclaredMethods())
                                         .filter(method -> Modifier.isPublic(method.getModifiers()))
                                         .filter(method -> Modifier.isStatic(method.getModifiers()))
                                         .map(Method::getName)
                                         .collect(Collectors.toSet());
        long builderOverloads = Arrays.stream(FlyingOrmClients.class.getDeclaredMethods())
                                      .filter(method -> method.getName().equals("builder"))
                                      .filter(method -> Modifier.isPublic(method.getModifiers()))
                                      .count();

        assertEquals(Set.of("builder"), factoryNames);
        assertEquals(3L, builderOverloads);
        assertDoesNotThrow(() -> FlyingOrmClients.class.getMethod(
                "builder", io.r2dbc.spi.ConnectionFactory.class));
        assertDoesNotThrow(() -> FlyingOrmClients.class.getMethod(
                "builder", javax.sql.DataSource.class));
        assertDoesNotThrow(() -> FlyingOrmClients.class.getMethod(
                "builder", javax.sql.DataSource.class, io.r2dbc.spi.ConnectionFactory.class));
        assertThrows(NoSuchMethodException.class, () -> FlyingOrmClients.class.getMethod(
                "builder", ReactiveSqlExecutor.class, RdbDialect.class));
        assertEquals(0L, Arrays.stream(FlyingOrmClients.class.getDeclaredMethods())
                              .filter(method -> method.getName().equals("builder"))
                              .flatMap(method -> Arrays.stream(method.getParameterTypes()))
                              .filter(SqlRenderer.class::equals)
                              .count());
        assertDoesNotThrow(() -> FlyingOrmClients.class.getMethod("repository", Class.class));
        assertDoesNotThrow(() -> FlyingOrmClients.class.getMethod("syncRepository", Class.class));

        Set<String> builderOptions = publicInstanceMethodNames(FlyingOrmClientBuilder.class);
        assertTrue(builderOptions.contains("renderer"));
        assertFalse(builderOptions.contains("valueCodecs"));

        long operatorFactories = Arrays.stream(DatabaseOperator.class.getDeclaredMethods())
                                       .filter(method -> method.getName().equals("create"))
                                       .filter(method -> Modifier.isPublic(method.getModifiers()))
                                       .count();
        assertEquals(2L, operatorFactories);
    }

    /** 批量只认 BatchWriteRequest，执行入口和返回模型不会再分叉。 */
    @Test
    void keepsOneBatchRequestContract() {
        Set<Class<?>> updateRequestTypes = Arrays.stream(ReactiveSqlExecutor.class.getMethods())
                                                 .filter(method -> method.getName().equals("rowsUpdated"))
                                                 .map(method -> method.getParameterTypes()[0])
                                                 .collect(Collectors.toSet());

        assertEquals(Set.of(SqlRequest.class), updateRequestTypes);
        assertDoesNotThrow(() -> ReactiveSqlExecutor.class.getMethod("writeBatch", BatchWriteRequest.class));
    }

    /** 同步执行契约不再公开任何 R2DBC 阻塞桥工厂。 */
    @Test
    void removesTheReactiveSyncBridge() {
        assertEquals(0L, publicStaticMethodsNamed(SyncSqlExecutor.class, "bridge"));
    }

    /** Repository 配置由 FormClient 统一持有，只留下默认映射和自定义映射两种真正不同的创建方式。 */
    @Test
    void keepsOneRepositoryFactoryPerExecutionStyle() {
        assertEquals(1L, publicStaticMethodsNamed(ReactiveFormRepository.class, "create"));
        assertEquals(1L, publicStaticMethodsNamed(SyncFormRepository.class, "create"));
    }

    /** 迁移审核器必须复用已经选好方言的结构渲染器，不能再单独走第二条方言装配路径。 */
    @Test
    void keepsOneSchemaReviewerFactory() {
        assertEquals(1L, publicStaticMethodsNamed(SchemaMigrationReviewer.class, "create"));
    }

    /** FormClient 只接收已经组好的数据渲染器，完整方言不会在不同快捷入口里重复拼装。 */
    @Test
    void keepsOneFormClientAndRendererFactory() {
        assertEquals(1L, publicStaticMethodsNamed(ReactiveFormClient.class, "create"));
        assertEquals(1L, publicStaticMethodsNamed(FormDataSqlRenderer.class, "create"));
        assertEquals(1L, publicStaticMethodsNamed(R2dbcSqlExecutor.class, "create"));
    }

    /** SQL 结构计划是缓存实现，不应让普通使用者误以为需要自己创建和维护。 */
    @Test
    void keepsStructuralPlanImplementationOutOfThePublicApi()
            throws NoSuchMethodException, ClassNotFoundException {
        assertThrows(ClassNotFoundException.class,
                     () -> Class.forName("com.flying.orm.rdb.plan.ConditionPlan"));
        assertThrows(ClassNotFoundException.class,
                     () -> Class.forName("com.flying.orm.rdb.plan.ConditionStructurePlan"));
        assertThrows(ClassNotFoundException.class,
                     () -> Class.forName("com.flying.orm.rdb.plan.SqlPlanSpec"));
        assertThrows(ClassNotFoundException.class,
                     () -> Class.forName("com.flying.orm.rdb.plan.SqlStructurePlan"));
        assertThrows(ClassNotFoundException.class,
                     () -> Class.forName("com.flying.orm.rdb.plan.StructuralPlanCaches"));

        Method cacheInjection = FormDataSqlRenderer.class.getMethod(
                "withPlanCaches",
                Class.forName("com.flying.orm.rdb.internal.plan.StructuralPlanCaches"));
        assertTrue(cacheInjection.isAnnotationPresent(InternalApi.class));
    }

    /** 条件值清洗由编译器统一完成，业务代码只面对条件 DSL 和稳定错误。 */
    @Test
    void keepsConditionNormalizationPipelineInternal() {
        assertThrows(ClassNotFoundException.class,
                     () -> Class.forName("com.flying.orm.core.condition.ConditionValueNormalizer"));
        assertThrows(ClassNotFoundException.class,
                     () -> Class.forName("com.flying.orm.core.condition.ConditionValuePolicy"));
    }

    /** 实体反射计划只能从有界注册表进入，避免无缓存快捷类挤进业务 API。 */
    @Test
    void keepsEntityReflectionImplementationInternal() throws NoSuchMethodException {
        assertThrows(ClassNotFoundException.class,
                     () -> Class.forName("com.flying.orm.rdb.mapping.EntityMetadataResolver"));
        assertThrows(ClassNotFoundException.class,
                     () -> Class.forName("com.flying.orm.rdb.mapping.EntityPropertyResolver"));
        assertThrows(ClassNotFoundException.class,
                     () -> Class.forName("com.flying.orm.rdb.mapping.EntityValues"));

        Method internalValues = com.flying.orm.rdb.mapping.EntityModelRegistry.class.getMethod(
                "entityValues", Class.class);
        assertTrue(internalValues.isAnnotationPresent(InternalApi.class));
    }

    /** 执行保护只公开真实配置值，不为每个数值再复制一套 hasXxx 判断 API。 */
    @Test
    void keepsExecutionOptionsFreeOfDerivedBooleanMethods() {
        Set<String> names = publicInstanceMethodNames(SqlExecutionOptions.class);
        assertFalse(names.contains("hasTimeout"));
        assertFalse(names.contains("hasMaxRows"));
        assertFalse(names.contains("hasMaxLargeObjectBytes"));
        assertFalse(names.contains("hasMaxLargeObjectChars"));
        assertFalse(names.contains("hasConnectionAcquireTimeout"));
        assertFalse(names.contains("hasCleanupTimeout"));
    }

    /** TermHandler 的工厂返回接口，两个内置实现不再单独占用业务 API 名额。 */
    @Test
    void hidesBuiltInTermHandlerImplementations() throws ClassNotFoundException {
        assertFalse(Modifier.isPublic(Class.forName(
                "com.flying.orm.core.condition.SimpleTermHandler").getModifiers()));
        assertFalse(Modifier.isPublic(Class.forName(
                "com.flying.orm.core.sql.render.RelationExistsTermHandler").getModifiers()));
    }

    /** 分页方言只公开 SPI 和命名工厂，内置实现名不进入使用方类型系统。 */
    @Test
    void hidesBuiltInPaginationDialectImplementations() {
        assertThrows(ClassNotFoundException.class,
                     () -> Class.forName("com.flying.orm.rdb.dialect.PaginationDialect$LimitOffset"));
        assertThrows(ClassNotFoundException.class,
                     () -> Class.forName("com.flying.orm.rdb.dialect.PaginationDialect$OffsetFetch"));
        assertThrows(ClassNotFoundException.class,
                     () -> Class.forName("com.flying.orm.rdb.dialect.PaginationDialect$SqlServerOffsetFetch"));
    }

    /**
     * FormClient 的业务入口只公开不可变规格和实体 Lambda，不允许重新暴露 DynamicForm 加可选参数的重载矩阵。
     */
    @Test
    void exposesOnlySpecBasedFormClientOperations() throws NoSuchMethodException {
        Set<String> reactiveNames = publicInstanceMethodNames(ReactiveFormClient.class);
        Set<String> syncNames = publicInstanceMethodNames(SyncFormClient.class);

        assertEquals(Set.of("withStructuredConditionResolver", "withDefaultExecutionOptions",
                            "withDefaultBatchWriteOptions", "withDefaultDataScope",
                            "entity", "select", "selectJoin", "page", "pageJoin", "cursorPage", "insert", "update",
                            "delete", "physicalDelete", "writeBatch", "writeBatchChunks"), reactiveNames);
        assertEquals(Set.of("timeout", "entity", "select", "selectJoin", "page", "pageJoin", "cursorPage", "insert",
                            "update", "delete", "physicalDelete", "writeBatch", "writeBatchChunks",
                            "withStructuredConditionResolver", "withDefaultExecutionOptions",
                            "withDefaultBatchWriteOptions", "withDefaultDataScope"), syncNames);

        assertTrue(Arrays.stream(ReactiveFormClient.class.getDeclaredMethods())
                         .filter(method -> Modifier.isPublic(method.getModifiers()))
                         .flatMap(method -> Arrays.stream(method.getParameterTypes()))
                         .noneMatch(type -> type.getName().equals("com.flying.orm.core.form.DynamicForm")));
        assertDoesNotThrow(() -> ReactiveFormClient.class.getMethod("select", QuerySpec.class));
        assertDoesNotThrow(() -> ReactiveFormClient.class.getMethod("insert", WriteSpec.class));
        assertDoesNotThrow(() -> ReactiveFormClient.class.getMethod("writeBatch", BatchSpec.class));
        assertDoesNotThrow(() -> SyncFormClient.class.getMethod("select", QuerySpec.class));
        assertDoesNotThrow(() -> SyncFormClient.class.getMethod("insert", WriteSpec.class));
        assertDoesNotThrow(() -> SyncFormClient.class.getMethod("writeBatch", BatchSpec.class));
    }

    /** Caffeine 包装类是实现细节，默认缓存、指定策略和关联失效器统一从元数据 reader 工厂进入。 */
    @Test
    void hidesMetadataCacheImplementationAndDuplicateFactories() throws ClassNotFoundException {
        Class<?> cacheImplementation = Class.forName(
                "com.flying.orm.rdb.metadata.CachedReactiveFormMetadataReader");

        assertFalse(Modifier.isPublic(cacheImplementation.getModifiers()));
        assertEquals(1L, publicStaticMethodsNamed(ReactiveFormMetadataReaders.class, "create"));
        assertEquals(0L, publicStaticMethodsNamed(ReactiveFormMetadataReaders.class, "createCached"));
        assertEquals(2L, publicStaticMethodsNamed(ReactiveFormMetadataReaders.class, "cached"));
    }

    /** 跨包共享的性能实现即使必须是 public，也必须明确标成内部 API，不能进入版本兼容承诺。 */
    @Test
    void marksCrossPackageImplementationTypesAsInternal() {
        assertInternal(BoundedCacheRegion.class);
        assertInternal(DynamicRowFactory.class);
        assertInternal(BatchMemoryBudget.class);
    }

    /** 自定义方言必须明确给出 upsert，不能因为少传参数而悄悄套用 H2 语法。 */
    @Test
    void requiresExplicitUpsertForCustomDialects() {
        assertEquals(2L, publicStaticMethodsNamed(RdbDialect.class, "of"));
        Arrays.stream(RdbDialect.class.getDeclaredMethods())
              .filter(method -> method.getName().equals("of"))
              .filter(method -> Modifier.isPublic(method.getModifiers()))
              .forEach(method -> assertFalse(method.getParameterCount() < 4));
    }

    @Test
    void separatesUnsafeRawSqlFromRegisteredQueryTemplates() {
        assertEquals(0L, publicInstanceMethodsNamed(DatabaseOperator.class, "sql"));
        assertEquals(0L, publicInstanceMethodsNamed(SyncDatabaseOperator.class, "sql"));
        assertEquals(1L, publicInstanceMethodsNamed(DatabaseOperator.class, "unsafeNativeSql"));
        assertEquals(1L, publicInstanceMethodsNamed(SyncDatabaseOperator.class, "unsafeNativeSql"));
        assertEquals(1L, publicInstanceMethodsNamed(DatabaseOperator.class, "sqlTemplate"));
        assertEquals(1L, publicInstanceMethodsNamed(SyncDatabaseOperator.class, "sqlTemplate"));
        assertEquals(0L, publicInstanceMethodsNamed(SqlTemplateOperator.class, "execute"));
    }

    /**
     * 注册模板只承接服务端预登记的复杂查询。写入和 DDL 已有名字明确的原生 SQL 逃生口，
     * 不再为尚未闭环的模板种类公开额外类型和工厂，避免使用方看到能创建却不能执行的 API。
     */
    @Test
    void keepsRegisteredSqlTemplatesQueryOnly() throws NoSuchMethodException {
        Set<String> factories = Arrays.stream(SqlTemplate.class.getDeclaredMethods())
                                       .filter(method -> Modifier.isPublic(method.getModifiers()))
                                       .filter(method -> Modifier.isStatic(method.getModifiers()))
                                       .map(Method::getName)
                                       .collect(Collectors.toSet());

        assertEquals(Set.of("query"), factories);
        assertEquals(0L, Arrays.stream(SqlTemplate.class.getDeclaredConstructors())
                              .filter(constructor -> Modifier.isPublic(constructor.getModifiers()))
                              .count());
        assertEquals(SqlRequest.class,
                     SqlTemplateEngine.class.getMethod("render", String.class, java.util.Map.class,
                                                       java.util.Map.class).getReturnType());
    }

    /** 清理失败的脱敏异常只是 observation 的内部载体，外部只依赖稳定的 failureKind。 */
    @Test
    void hidesSanitizedCleanupExceptionImplementation() {
        Class<?> sanitized = Arrays.stream(ResourceCleanupObservation.class.getDeclaredClasses())
                                   .filter(type -> type.getSimpleName().equals("SanitizedCleanupException"))
                                   .findFirst()
                                   .orElseThrow();

        assertFalse(Modifier.isPublic(sanitized.getModifiers()));
    }

    /** 异常分类直接挂在结果枚举上，不再要求使用方多认识一个只有静态方法的工具类。 */
    @Test
    void keepsFailureClassificationOnTheResultType() {
        assertThrows(ClassNotFoundException.class,
                     () -> Class.forName("com.flying.orm.rdb.observation.SqlFailureClassifier"));
        assertDoesNotThrow(() -> com.flying.orm.rdb.observation.SqlFailureCategory.class
                .getMethod("classify", Throwable.class));
    }

    private static long publicStaticMethodsNamed(Class<?> type, String name) {
        return Arrays.stream(type.getDeclaredMethods())
                     .filter(method -> method.getName().equals(name))
                     .filter(method -> Modifier.isPublic(method.getModifiers()))
                     .filter(method -> Modifier.isStatic(method.getModifiers()))
                     .count();
    }

    private static long publicInstanceMethodsNamed(Class<?> type, String name) {
        return Arrays.stream(type.getDeclaredMethods())
                     .filter(method -> method.getName().equals(name))
                     .filter(method -> Modifier.isPublic(method.getModifiers()))
                     .filter(method -> !Modifier.isStatic(method.getModifiers()))
                     .count();
    }

    private static Set<String> publicInstanceMethodNames(Class<?> type) {
        return Arrays.stream(type.getDeclaredMethods())
                     .filter(method -> Modifier.isPublic(method.getModifiers()))
                     .filter(method -> !Modifier.isStatic(method.getModifiers()))
                     .filter(method -> Arrays.stream(method.getAnnotations())
                                             .noneMatch(annotation -> annotation.annotationType().getName()
                                                                                .equals("com.flying.orm.rdb.internal.InternalApi")))
                     .map(Method::getName)
                     .collect(Collectors.toSet());
    }

    private static void assertInternal(Class<?> type) {
        assertTrue(Arrays.stream(type.getAnnotations())
                         .anyMatch(annotation -> annotation.annotationType().getName()
                                                           .equals("com.flying.orm.rdb.internal.InternalApi")),
                   () -> type.getName() + " must be marked as an internal implementation type");
    }
}
