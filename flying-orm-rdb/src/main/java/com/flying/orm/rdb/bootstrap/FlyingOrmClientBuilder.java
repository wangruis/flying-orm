package com.flying.orm.rdb.bootstrap;

import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.batch.BatchMemoryLimits;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.cache.OrmCachePolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.StructuredConditionResolver;
import com.flying.orm.rdb.id.IdGenerator;
import com.flying.orm.rdb.mapping.EntityFieldFiller;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionLogOptions;
import com.flying.orm.rdb.observation.SqlExecutionLogSelection;
import com.flying.orm.rdb.observation.SqlExecutionLogSink;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.protection.MaskingPolicyRegistry;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedValueNormalizerRegistry;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.schema.SchemaMigrationExecutionOptions;
import com.flying.orm.rdb.schema.SchemaMigrationObserver;
import com.flying.orm.rdb.template.SqlTemplateParameterProvider;
import com.flying.orm.rdb.template.SqlTemplateRegistry;
import com.flying.orm.rdb.template.SyncSqlTemplateParameterProvider;
import com.flying.orm.rdb.transaction.JdbcTransactionParticipant;
import com.flying.orm.rdb.transaction.R2dbcTransactionParticipant;
import io.r2dbc.spi.ConnectionFactory;

import javax.sql.DataSource;
import java.util.Map;
import java.util.Objects;

/**
 * 启动期统一配置门面。
 *
 * <p>这里保留较多链式配置方法，是为了让使用方只接触一个稳定入口；校验、运行时组装和具体执行责任
 * 都在 {@link FlyingOrmClientBuilderSupport} 与两个 runtime 中完成。门面不保存可变执行状态，构建完成后
 * 只共享不可变的 {@link FlyingOrmClients}。</p>
 *
 * @author wangr
 * @version v2.0.0
 */
public final class FlyingOrmClientBuilder {
    private final FlyingOrmClientBuilderSupport s;
    private FlyingOrmClientBuilder(FlyingOrmClientBuilderSupport support) {
        this.s = support;
    }

    static FlyingOrmClientBuilder reactive(ConnectionFactory cf) {
        return new FlyingOrmClientBuilder(new FlyingOrmClientBuilderSupport(null, Objects.requireNonNull(cf)));
    }

    static FlyingOrmClientBuilder jdbc(DataSource ds) {
        return new FlyingOrmClientBuilder(new FlyingOrmClientBuilderSupport(Objects.requireNonNull(ds), null));
    }

    static FlyingOrmClientBuilder dual(DataSource ds, ConnectionFactory cf) {
        return new FlyingOrmClientBuilder(new FlyingOrmClientBuilderSupport(
                Objects.requireNonNull(ds), Objects.requireNonNull(cf)));
    }

    static FlyingOrmClientBuilder reactive(ReactiveSqlExecutor e, RdbDialect d) {
        return new FlyingOrmClientBuilder(new FlyingOrmClientBuilderSupport(e, d));
    }

    public FlyingOrmClientBuilder renderer(SqlRenderer v) {
        s.renderer = Objects.requireNonNull(v);
        return this;
    }

    public FlyingOrmClientBuilder configuredDialect(String v) {
        s.configuredDialect = v;
        return this;
    }

    public FlyingOrmClientBuilder validateReactiveDataSourceDialects(
            Map<String, ? extends ConnectionFactory> v) {
        Objects.requireNonNull(v, "physical reactive data sources must not be null")
               .forEach((name, factory) -> {
                   Objects.requireNonNull(name, "physical reactive data source name must not be null");
                   Objects.requireNonNull(factory, "physical reactive data source must not be null");
               });
        return this;
    }

    public FlyingOrmClientBuilder validateJdbcDataSourceDialects(Map<String, ? extends DataSource> v) {
        Objects.requireNonNull(v, "physical jdbc data sources must not be null")
               .forEach((name, source) -> {
                   Objects.requireNonNull(name, "physical jdbc data source name must not be null");
                   Objects.requireNonNull(source, "physical jdbc data source must not be null");
               });
        return this;
    }

    /** 保留这个简短名称，明确表示校验的是 R2DBC 连接工厂集合。 */
    public FlyingOrmClientBuilder validateDataSourceDialects(
            Map<String, ? extends ConnectionFactory> v) {
        return validateReactiveDataSourceDialects(v);
    }

    public FlyingOrmClientBuilder structuredConditionResolver(StructuredConditionResolver v) {
        s.resolver = Objects.requireNonNull(v);
        return this;
    }

    public FlyingOrmClientBuilder executionOptions(SqlExecutionOptions v) {
        s.executionOptions = Objects.requireNonNull(v);
        return this;
    }

    public FlyingOrmClientBuilder batchWriteOptions(BatchWriteOptions v) {
        s.batchWriteOptions = Objects.requireNonNull(v);
        return this;
    }

    public FlyingOrmClientBuilder batchMemoryLimits(BatchMemoryLimits v) {
        s.batchMemoryLimits = Objects.requireNonNull(v);
        return this;
    }

    public FlyingOrmClientBuilder cachePolicy(OrmCachePolicy v) {
        s.cachePolicy = Objects.requireNonNull(v);
        return this;
    }

    /** 为 ASSIGN_ID 实体主键设置当前客户端共享的线程安全生成器。 */
    public FlyingOrmClientBuilder idGenerator(IdGenerator v) {
        s.idGenerator = Objects.requireNonNull(v);
        return this;
    }

    /** 为 TableField.fill 设置当前客户端共享的线程安全填充器。 */
    public FlyingOrmClientBuilder fieldFiller(EntityFieldFiller v) {
        s.fieldFiller = Objects.requireNonNull(v);
        return this;
    }

    /**
     * 配置字段保护使用的版本化主密钥环。
     *
     * <p>构建成功后密钥环由返回的客户端对象图管理，并在客户端关闭时清零；未声明受保护字段时不会改变 SQL。
     * 上层只需提供当前写入密钥和可读旧密钥，不需要把基础设施 SDK 交给 ORM。配置密钥后当前 Builder 只能成功
     * 构建一次，避免两个独立客户端同时声称拥有同一密钥环。</p>
     *
     * @param keys 版本化主密钥环
     * @return 当前构建器
     */
    public FlyingOrmClientBuilder protectedFields(ProtectedFieldKeyRing keys) {
        s.protectedFieldKeys = Objects.requireNonNull(keys);
        return this;
    }

    /**
     * 配置字段保护使用的值规范化器和脱敏策略；普通使用无需调用，默认提供通用内置规则。
     *
     * <p>两个 registry 都必须是上层在启动期创建的不可变、并发安全对象。只扩展其中一类规则时，另一参数传入
     * 对应的 {@code standard()} registry，避免把两项相关配置继续扩张成多个顶层 Builder 方法。</p>
     *
     * @param normalizers 字段保护值规范化器
     * @param policies    结果脱敏策略
     * @return 当前构建器
     */
    public FlyingOrmClientBuilder protectedFieldPolicies(ProtectedValueNormalizerRegistry normalizers,
                                                         MaskingPolicyRegistry policies) {
        s.protectedValueNormalizers = Objects.requireNonNull(
                normalizers, "protected value normalizer registry must not be null");
        s.maskingPolicies = Objects.requireNonNull(policies, "masking policy registry must not be null");
        return this;
    }

    public FlyingOrmClientBuilder migrationExecutionOptions(SchemaMigrationExecutionOptions v) {
        s.migrationOptions = Objects.requireNonNull(v);
        return this;
    }

    public FlyingOrmClientBuilder migrationObserver(SchemaMigrationObserver v) {
        s.migrationObserver = Objects.requireNonNull(v);
        return this;
    }

    public FlyingOrmClientBuilder sqlTemplates(SqlTemplateRegistry v) {
        s.templates = Objects.requireNonNull(v);
        return this;
    }

    public FlyingOrmClientBuilder sqlTemplateParameterProvider(SqlTemplateParameterProvider v) {
        s.reactiveParameters = Objects.requireNonNull(v);
        return this;
    }

    public FlyingOrmClientBuilder syncSqlTemplateParameterProvider(SyncSqlTemplateParameterProvider v) {
        s.syncParameters = Objects.requireNonNull(v);
        return this;
    }

    public FlyingOrmClientBuilder transactionParticipant(R2dbcTransactionParticipant v) {
        s.reactiveTransaction = Objects.requireNonNull(v);
        return this;
    }

    public FlyingOrmClientBuilder transactionParticipant(JdbcTransactionParticipant v) {
        s.jdbcTransaction = Objects.requireNonNull(v);
        return this;
    }

    public FlyingOrmClientBuilder observers(SqlExecutionObserver sql, BatchExecutionObserver batch) {
        s.sqlObserver = Objects.requireNonNull(sql);
        s.batchObserver = Objects.requireNonNull(batch);
        return this;
    }

    public FlyingOrmClientBuilder sqlExecutionLog(SqlExecutionLogOptions o, SqlExecutionLogSink sink) {
        s.configureLog(o, SqlExecutionLogSelection.defaults(), sink);
        return this;
    }

    public FlyingOrmClientBuilder sqlExecutionLog(SqlExecutionLogOptions o,
                                                  SqlExecutionLogSelection selection,
                                                  SqlExecutionLogSink sink) {
        s.configureLog(o, selection, sink);
        return this;
    }

    public FlyingOrmClients build() {
        return s.build();
    }
}
