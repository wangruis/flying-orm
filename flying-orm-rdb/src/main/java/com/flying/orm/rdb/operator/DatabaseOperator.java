package com.flying.orm.rdb.operator;

import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReaders;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.schema.ReactiveSchemaClient;
import com.flying.orm.rdb.schema.SchemaMigrationObserver;
import com.flying.orm.rdb.schema.SchemaMigrationExecutionOptions;
import com.flying.orm.rdb.template.SqlTemplate;
import com.flying.orm.rdb.template.SqlTemplateEngine;
import com.flying.orm.rdb.template.SqlTemplateParameterProvider;
import com.flying.orm.rdb.template.SqlTemplateRegistry;

import java.util.Objects;

/**
 * 轻量响应式 Operator 门面，把 DDL、DML、元数据和安全原生 SQL 组合在一个对象上。
 * 它不保存连接或事务，也不建立另一套 SQL 内核；所有操作仍委托动态表单、schema client 和 R2DBC executor。
 *
 * <p>直接使用 {@code create(...)} 时需要明确传入方言。应用通常可以优先通过
 * {@code FlyingOrmClients.builder(...)} 统一装配执行器、方言、执行保护和数据范围，避免各入口拿到不一致的配置。</p>
 *
 * <p>withDefault... 方法返回新门面，不修改原对象；因此装配完成后可以作为单例并发共享。</p>
 *
 * @author wangr
 * @date 2026-07-27
 * @version v1.0
 */
public final class DatabaseOperator {

    private static final SqlTemplateRegistry EMPTY_SQL_TEMPLATES = SqlTemplateRegistry.builder().build();

    private final ReactiveSchemaClient schemaClient;
    private final ReactiveFormClient formClient;
    private final ReactiveSqlExecutor executor;
    private final SqlRenderer renderer;
    private final ReactiveFormMetadataReader metadataReader;
    private final DataScope defaultDataScope;
    /** 原生命名参数要在编译时生成当前驱动的参数标记，因此门面需要保留装配时已经确定的方言。 */
    private final RdbDialect dialect;
    private final SqlTemplateRegistry sqlTemplates;
    private final SqlTemplateParameterProvider sqlTemplateParameters;
    private final SqlTemplateEngine sqlTemplateEngine;

    private DatabaseOperator(ReactiveSchemaClient schemaClient,
                             ReactiveFormClient formClient,
                             ReactiveSqlExecutor executor,
                             SqlRenderer renderer,
                             ReactiveFormMetadataReader metadataReader,
                             DataScope defaultDataScope,
                             RdbDialect dialect,
                             SqlTemplateRegistry sqlTemplates,
                             SqlTemplateParameterProvider sqlTemplateParameters) {
        this.schemaClient = Objects.requireNonNull(schemaClient, "schema client must not be null");
        this.formClient = Objects.requireNonNull(formClient, "form client must not be null");
        this.executor = Objects.requireNonNull(executor, "reactive sql executor must not be null");
        RdbDialect safeDialect = Objects.requireNonNull(dialect, "RDB dialect must not be null");
        this.renderer = Objects.requireNonNull(renderer, "sql renderer must not be null")
                               .withIdentifierRenderer(safeDialect.schema()::identifier);
        this.metadataReader = Objects.requireNonNull(metadataReader, "reactive form metadata reader must not be null");
        this.defaultDataScope = Objects.requireNonNull(defaultDataScope, "default data scope must not be null");
        this.dialect = safeDialect;
        this.sqlTemplates = Objects.requireNonNull(sqlTemplates, "SQL template registry must not be null");
        this.sqlTemplateParameters = Objects.requireNonNull(
                sqlTemplateParameters, "SQL template parameter provider must not be null");
        this.sqlTemplateEngine = SqlTemplateEngine.create(this.sqlTemplates,
                                                          this.dialect,
                                                          this.renderer.valueCodecs());
    }

    /**
     * 用统一 executor、renderer 和方言组装默认门面。元数据不启用缓存，数据范围默认为不限制。
     *
     * @param executor 响应式 SQL 执行器
     * @param renderer 条件与 DML 渲染器
     * @param dialect 当前数据库方言
     * @return 可并发共享的 operator 门面
     */
    public static DatabaseOperator create(ReactiveSqlExecutor executor, SqlRenderer renderer, RdbDialect dialect) {
        ReactiveSqlExecutor safeExecutor = Objects.requireNonNull(executor, "reactive sql executor must not be null");
        SqlRenderer safeRenderer = Objects.requireNonNull(renderer, "sql renderer must not be null");
        RdbDialect safeDialect = Objects.requireNonNull(dialect, "rdb dialect must not be null");
        // 三个入口共享完全相同的 executor、renderer 和 dialect，避免 API 之间参数顺序或方言行为漂移。
        return new DatabaseOperator(ReactiveSchemaClient.create(safeExecutor, safeDialect),
                                    ReactiveFormClient.create(safeExecutor,
                                                              FormDataSqlRenderer.create(safeRenderer, safeDialect)),
                                    safeExecutor,
                                    safeRenderer,
                                    ReactiveFormMetadataReaders.create(safeExecutor, safeDialect),
                                    DataScope.none(),
                                    safeDialect,
                                    EMPTY_SQL_TEMPLATES,
                                    SqlTemplateParameterProvider.none());
    }

    /**
     * 从完整自定义组件和明确方言创建门面。路由数据源或高级扩展可以使用这个入口；
     * 调用方必须保证所有组件采用相同的方言、codec 和参数绑定规则。
     *
     * @param schemaClient schema 执行客户端
     * @param formClient 动态表单客户端
     * @param executor 响应式 SQL 执行器
     * @param renderer SQL 渲染器
     * @param metadataReader 元数据读取器
     * @param dialect 当前数据库方言
     * @return 支持全部 operator 能力的门面
     */
    public static DatabaseOperator create(ReactiveSchemaClient schemaClient,
                                          ReactiveFormClient formClient,
                                          ReactiveSqlExecutor executor,
                                          SqlRenderer renderer,
                                          ReactiveFormMetadataReader metadataReader,
                                          RdbDialect dialect) {
        return new DatabaseOperator(schemaClient,
                                    formClient,
                                    executor,
                                    renderer,
                                    metadataReader,
                                    DataScope.none(),
                                    Objects.requireNonNull(dialect, "RDB dialect must not be null"),
                                    EMPTY_SQL_TEMPLATES,
                                    SqlTemplateParameterProvider.none());
    }

    /**
     * 给 operator 挂默认数据范围。后续 dml().query/update/delete 都会自动带上它。
     * 原对象不变，返回的新门面持有一份不可变范围快照，不依赖线程上下文。
     *
     * @param scope 要追加的数据范围
     * @return 带默认数据范围的新 operator
     */
    public DatabaseOperator withDefaultDataScope(DataScope scope) {
        DataScope safeScope = Objects.requireNonNull(scope, "data scope must not be null");
        DataScope combinedScope = defaultDataScope.and(safeScope);
        // FormClient 和链式 DML 使用同一份不可变范围，避免两个入口的安全边界漂移。
        return new DatabaseOperator(schemaClient,
                                    formClient.withDefaultDataScope(safeScope),
                                    executor,
                                    renderer,
                                    metadataReader,
                                    combinedScope,
                                    dialect,
                                    sqlTemplates,
                                    sqlTemplateParameters);
    }

    /**
     * 给 DML operator 的无 options 批量入口设置默认策略。显式传入的批量选项仍然优先。
     *
     * @param options 默认批量策略
     * @return 带默认批量策略的新 operator
     */
    public DatabaseOperator withDefaultBatchWriteOptions(BatchWriteOptions options) {
        return new DatabaseOperator(schemaClient,
                                    formClient.withDefaultBatchWriteOptions(options),
                                    executor,
                                    renderer,
                                    metadataReader,
                                    defaultDataScope,
                                    dialect,
                                    sqlTemplates,
                                    sqlTemplateParameters);
    }

    /**
     * 给 DDL 审核执行挂迁移级 observer。返回新门面，DML、元数据 reader、scope 和方言保持原样；
     * observer 只接收计划指纹、风险、步骤进度和错误分类，不包含参数值。
     */
    public DatabaseOperator withSchemaMigrationObserver(SchemaMigrationObserver observer) {
        return new DatabaseOperator(schemaClient.withMigrationObserver(observer),
                                    formClient,
                                    executor,
                                    renderer,
                                    metadataReader,
                                    defaultDataScope,
                                    dialect,
                                    sqlTemplates,
                                    sqlTemplateParameters);
    }

    /**
     * 给审核后 DDL 设置统一的 timeout、锁等待上限和可选精确批准。调用方仍可在单次执行时显式覆盖。
     */
    public DatabaseOperator withDefaultSchemaMigrationExecutionOptions(SchemaMigrationExecutionOptions options) {
        return new DatabaseOperator(schemaClient.withDefaultMigrationExecutionOptions(options),
                                    formClient,
                                    executor,
                                    renderer,
                                    metadataReader,
                                    defaultDataScope,
                                    dialect,
                                    sqlTemplates,
                                    sqlTemplateParameters);
    }

    /**
     * 创建动态 DDL 入口。每次调用都返回新的轻量对象，不会复制连接池或缓存。
     *
     * @return 响应式 DDL operator
     */
    public DdlOperator ddl() {
        // 每次返回轻量命令入口，内部共享线程安全的 schema client 和 metadata reader。
        return new DdlOperator(schemaClient, metadataReader);
    }

    /**
     * 创建动态查询、更新和删除入口。
     *
     * @return 响应式 DML operator
     */
    public DmlOperator dml() {
        return new DmlOperator(formClient, executor, renderer, defaultDataScope);
    }

    /**
     * 直接绑定实体类型的 Lambda DML 入口，业务代码无需填写物理表名和字段名。
     *
     * @param type 目标实体类型
     * @param <T> 实体类型
     * @return 可继续调用 query/update/delete 的类型化 DML 入口
     */
    public <T> EntityDmlOperator<T> dml(Class<T> type) {
        return dml().entity(type);
    }

    /**
     * 创建表结构读取和缓存失效入口。
     *
     * @return 响应式元数据 operator
     */
    public MetadataOperator metadata() {
        return new MetadataOperator(metadataReader);
    }

    /**
     * 直接执行后端代码中写下的单条参数化 SQL。方法名故意带 {@code unsafe}，提醒调用方该入口绕过实体安全改写。
     *
     * <p>值使用 {@code :name} 占位并通过 {@link NativeSqlOperator#bind(String, Object)} 绑定。这个入口不会
     * 自动改写 SQL，因此不会自动追加租户范围、数据权限、逻辑删除或乐观锁条件；调用方必须把这些约束明确写进
     * SQL 并绑定来自可信服务端上下文的值。SQL 正文绝不能来自前端请求。</p>
     *
     * @param sql 后端代码或可信配置中的单条 SQL
     * @return 只供本次执行使用的原生 SQL 构建器
     */
    public NativeSqlOperator unsafeNativeSql(String sql) {
        return new NativeSqlOperator(executor,
                                     renderer.valueCodecs(),
                                     formClient.entityModels(),
                                     dialect,
                                     sql);
    }

    /**
     * 返回一套注册了服务端 SQL 模板的新门面。模板和参数提供器都应在应用启动期组好，原门面保持不变。
     *
     * @param templates 只读模板注册表
     * @param parameterProvider 每次订阅时读取租户、用户等可信参数的提供器
     * @return 共享原执行器、方言、codec 和安全配置的新门面
     */
    public DatabaseOperator withSqlTemplates(SqlTemplateRegistry templates,
                                             SqlTemplateParameterProvider parameterProvider) {
        return new DatabaseOperator(schemaClient,
                                    formClient,
                                    executor,
                                    renderer,
                                    metadataReader,
                                    defaultDataScope,
                                    dialect,
                                    templates,
                                    parameterProvider);
    }

    /**
     * 按稳定 ID 创建一次服务端 SQL 模板调用。SQL 正文只能在启动阶段注册，普通调用方只能绑定非安全参数。
     *
     * @param templateId 已注册模板 ID
     * @return 单次响应式模板执行构建器
     */
    public SqlTemplateOperator sqlTemplate(String templateId) {
        SqlTemplate template = sqlTemplates.template(templateId);
        return new SqlTemplateOperator(executor,
                                       renderer.valueCodecs(),
                                       formClient.entityModels(),
                                       sqlTemplateEngine,
                                       template,
                                       sqlTemplates.serverParameters(template.id()),
                                       sqlTemplateParameters);
    }

}
