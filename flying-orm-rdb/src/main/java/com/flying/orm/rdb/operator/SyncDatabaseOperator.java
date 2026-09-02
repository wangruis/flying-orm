package com.flying.orm.rdb.operator;

import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
import com.flying.orm.rdb.metadata.MetadataCacheInvalidator;
import com.flying.orm.rdb.metadata.SyncFormMetadataReader;
import com.flying.orm.rdb.schema.JdbcSchemaClient;
import com.flying.orm.rdb.schema.SchemaMigrationExecutionOptions;
import com.flying.orm.rdb.schema.SchemaMigrationObserver;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import com.flying.orm.rdb.template.SqlTemplateEngine;
import com.flying.orm.rdb.template.SqlTemplateRegistry;
import com.flying.orm.rdb.template.SyncSqlTemplateParameterProvider;

import java.util.Objects;

/**
 * 同步数据库操作门面。
 *
 * <p>V2 把 DDL、动态 DML、实体 DML、元数据、原生 SQL 和注册模板统一接到 JDBC 同步执行器。
 * 它不会创建 Reactor Publisher，也不会等待 R2DBC。</p>
 *
 * <p>门面只保存可并发共享的不可变组件；每次 {@code dml()}、{@code ddl()} 或 SQL 调用都会创建自己的
 * 轻量命令对象，因此业务代码可以安全复用同一个门面。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v2.0
 */
public final class SyncDatabaseOperator {

    private final SyncFormClient forms;
    private final SyncSqlExecutor executor;
    private final SqlRenderer renderer;
    private final DataScope defaultDataScope;
    private final RdbDialect dialect;
    private final JdbcSchemaClient schema;
    private final JdbcFormMetadataReader jdbcMetadata;
    private final SyncFormMetadataReader metadata;
    private final MetadataCacheInvalidator metadataInvalidator;
    private final SqlTemplateRegistry templates;
    private final SyncSqlTemplateParameterProvider templateParameters;
    private final SqlTemplateEngine templateEngine;

    /**
     * 用启动阶段已经组装好的原生 JDBC 组件创建同步门面。
     *
     * <p>这里不接收 R2DBC 执行器，也不会在内部等待响应式结果。构造器保持私有，避免业务代码分别拼装
     * executor、方言和元数据读取器后得到彼此不一致的运行时配置。</p>
     */
    private SyncDatabaseOperator(NativeComponents components) {
        NativeComponents safe = Objects.requireNonNull(components, "sync operator components must not be null");
        this.forms = safe.forms();
        this.executor = safe.executor();
        this.dialect = safe.dialect();
        this.renderer = safe.renderer().withIdentifierRenderer(this.dialect.schema()::identifier);
        this.defaultDataScope = safe.defaultDataScope();
        this.schema = safe.schema();
        this.jdbcMetadata = safe.jdbcMetadata();
        this.metadata = safe.metadata();
        this.metadataInvalidator = safe.metadataInvalidator();
        this.templates = safe.templates();
        this.templateParameters = safe.templateParameters();
        this.templateEngine = SqlTemplateEngine.create(this.templates,
                                                       this.dialect,
                                                       this.renderer.valueCodecs())
                                               .forJdbc();
    }

    /** 默认范围、批量策略和 schema 配置不改变模板编译上下文，直接复用已装配的只读组件。 */
    private SyncDatabaseOperator(SyncDatabaseOperator source,
                                 SyncFormClient forms,
                                 JdbcSchemaClient schema,
                                 DataScope defaultDataScope) {
        this.forms = forms;
        this.executor = source.executor;
        this.renderer = source.renderer;
        this.defaultDataScope = defaultDataScope;
        this.dialect = source.dialect;
        this.schema = schema;
        this.jdbcMetadata = source.jdbcMetadata;
        this.metadata = source.metadata;
        this.metadataInvalidator = source.metadataInvalidator;
        this.templates = source.templates;
        this.templateParameters = source.templateParameters;
        this.templateEngine = source.templateEngine;
    }

    /** 原生客户端装配使用的单一入口，业务代码通常从 {@code FlyingOrmClients.syncOperator()} 获取门面。 */
    @InternalApi
    public static SyncDatabaseOperator create(NativeComponents components) {
        return new SyncDatabaseOperator(components);
    }

    /** 创建同步 DDL 入口。 */
    public SyncDdlOperator ddl() {
        return SyncDdlOperator.create(schema, jdbcMetadata);
    }

    /** 创建动态查询、更新和删除入口。 */
    public SyncDmlOperator dml() {
        return new SyncDmlOperator(forms, executor, renderer, defaultDataScope);
    }

    /** 创建不需要手写表名和字段名的实体 Lambda DML 入口。 */
    public <T> SyncEntityDmlOperator<T> dml(Class<T> type) {
        return forms.entity(type);
    }

    /** 创建同步元数据读取与缓存失效入口。 */
    public SyncMetadataOperator metadata() {
        return new SyncMetadataOperator(metadata, metadataInvalidator);
    }

    /**
     * 执行后端可信的单条原生 SQL。该入口不会自动添加租户、逻辑删除或权限条件，SQL 正文不能来自前端。
     */
    public SyncNativeSqlOperator unsafeNativeSql(String sql) {
        return new SyncNativeSqlOperator(executor, renderer.valueCodecs(), forms.entityModels(), dialect, sql);
    }

    /** 按启动阶段注册的稳定模板 ID 创建一次同步调用。 */
    public SyncSqlTemplateOperator sqlTemplate(String templateId) {
        return new SyncSqlTemplateOperator(executor,
                                           renderer.valueCodecs(),
                                           forms.entityModels(),
                                           templateEngine,
                                           templateId,
                                           templates.serverParameters(templateId),
                                           templateParameters);
    }

    /** 返回继续收紧默认数据范围的新门面。 */
    @InternalApi
    public SyncDatabaseOperator withDefaultDataScope(DataScope scope) {
        DataScope safeScope = Objects.requireNonNull(scope, "data scope must not be null");
        return copy(forms.withDefaultDataScope(safeScope), schema, defaultDataScope.and(safeScope));
    }

    /** 返回使用新默认批量策略的门面。 */
    @InternalApi
    public SyncDatabaseOperator withDefaultBatchWriteOptions(BatchWriteOptions options) {
        BatchWriteOptions safeOptions = Objects.requireNonNull(options, "batch write options must not be null");
        return copy(forms.withDefaultBatchWriteOptions(safeOptions), schema, defaultDataScope);
    }

    /** 返回使用新 DDL 执行保护的门面。 */
    @InternalApi
    public SyncDatabaseOperator withSchemaExecutionOptions(SchemaMigrationExecutionOptions options) {
        return copy(forms, schema.withDefaultMigrationExecutionOptions(options), defaultDataScope);
    }

    /** 返回使用新 DDL observer 的门面。 */
    @InternalApi
    public SyncDatabaseOperator withSchemaObserver(SchemaMigrationObserver observer) {
        return copy(forms, schema.withMigrationObserver(observer), defaultDataScope);
    }

    private SyncDatabaseOperator copy(SyncFormClient configuredForms,
                                      JdbcSchemaClient configuredSchema,
                                      DataScope configuredScope) {
        return new SyncDatabaseOperator(this, configuredForms, configuredSchema, configuredScope);
    }

    /**
     * 原生同步门面的一次不可变装配快照。把启动参数收成一个对象，避免公开一个容易传错顺序的九参数工厂。
     */
    @InternalApi
    public record NativeComponents(SyncFormClient forms,
                                   SyncSqlExecutor executor,
                                   SqlRenderer renderer,
                                   DataScope defaultDataScope,
                                   RdbDialect dialect,
                                   JdbcSchemaClient schema,
                                   JdbcFormMetadataReader jdbcMetadata,
                                   SyncFormMetadataReader metadata,
                                   MetadataCacheInvalidator metadataInvalidator,
                                   SqlTemplateRegistry templates,
                                   SyncSqlTemplateParameterProvider templateParameters) {

        public NativeComponents {
            forms = Objects.requireNonNull(forms, "sync form client must not be null");
            executor = Objects.requireNonNull(executor, "sync sql executor must not be null");
            renderer = Objects.requireNonNull(renderer, "sql renderer must not be null");
            defaultDataScope = Objects.requireNonNull(defaultDataScope, "default data scope must not be null");
            dialect = Objects.requireNonNull(dialect, "rdb dialect must not be null");
            schema = Objects.requireNonNull(schema, "jdbc schema client must not be null");
            jdbcMetadata = Objects.requireNonNull(jdbcMetadata, "jdbc metadata reader must not be null");
            metadata = Objects.requireNonNull(metadata, "sync metadata reader must not be null");
            metadataInvalidator = Objects.requireNonNull(
                    metadataInvalidator, "metadata cache invalidator must not be null");
            templates = Objects.requireNonNull(templates, "SQL template registry must not be null");
            templateParameters = Objects.requireNonNull(
                    templateParameters, "sync SQL template parameter provider must not be null");
        }
    }
}
