package com.flying.orm.rdb.metadata;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.schema.SchemaSnapshotCoverage;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * 按方言挑选动态表单元数据读取器。没有实现的库会明确告诉调用方还没支持。
 * 工厂没有共享状态，可以并发调用；返回的 reader 是否缓存由调用方选择。
 *
 * <p>每个内置 reader 都通过 {@link SchemaSnapshotCoverage} 声明真实覆盖范围。查询集按方言版本选择；
 * 只有确实无法从该版本稳定观察的事实才降低 coverage，并让完整关系 DDL 进入人工验证计划，绝不把
 * 未观察到的事实误判为不存在。</p>
 *
 * @author wangr
 * @date 2026-07-28
 * @version v1.0
 */
public final class ReactiveFormMetadataReaders {

    private ReactiveFormMetadataReaders() {
    }

    /**
     * 根据方言创建对应的元数据 reader，业务不需要认识各数据库的系统表实现。
     *
     * @param executor SQL 执行器
     * @param dialect  已识别的数据库方言
     * @return 对应方言的元数据 reader
     */
    public static ReactiveFormMetadataReader create(ReactiveSqlExecutor executor, RdbDialect dialect) {
        ReactiveSqlExecutor safeExecutor = Objects.requireNonNull(executor, "reactive sql executor must not be null");
        RdbDialect safeDialect = Objects.requireNonNull(dialect, "rdb dialect must not be null");
        MetadataQueryProfile profile = MetadataQueryProfile.resolve(safeDialect);
        return profile == null
                ? unsupported(safeDialect.name())
                : new InformationSchemaFormMetadataReader(safeExecutor, profile);
    }

    /**
     * 给自定义 reader 套默认元数据缓存。
     *
     * @param delegate 实际读取数据库元数据的 reader
     * @return 带默认缓存的 reader
     */
    public static ReactiveFormMetadataCache cached(ReactiveFormMetadataReader delegate) {
        return CachedReactiveFormMetadataReader.create(delegate);
    }

    /**
     * 使用统一缓存区域策略包装元数据读取器。最大权重、单条权重、访问后过期、统计开关和禁用语义
     * 均由同一策略原样执行，不再转换成条目数或写入后过期。
     *
     * @param delegate 实际读取数据库元数据的 reader
     * @param policy 元数据区域的统一缓存策略
     * @param dependentInvalidator DDL 后需要联动失效的计划缓存
     * @return 严格遵守统一策略的元数据缓存
     */
    public static ReactiveFormMetadataCache cached(ReactiveFormMetadataReader delegate,
                                                   CacheRegionPolicy policy,
                                                   MetadataCacheInvalidator dependentInvalidator) {
        return CachedReactiveFormMetadataReader.create(delegate, policy, dependentInvalidator);
    }

    /**
     * 创建一个明确报“不支持”的 reader，供自定义方言在尚未实现元数据读取时使用。
     *
     * @param dialectName 方言名称
     * @return 每次读取都会返回 UnsupportedOperationException 的 reader
     */
    public static ReactiveFormMetadataReader unsupported(String dialectName) {
        return new UnsupportedReactiveFormMetadataReader(dialectName);
    }

    private record UnsupportedReactiveFormMetadataReader(String dialectName) implements ReactiveFormMetadataReader {

        private UnsupportedReactiveFormMetadataReader {
            dialectName = Objects.requireNonNull(dialectName, "dialect name must not be null");
        }

        @Override
        public Mono<DynamicForm> readForm(String formId, String table) {
            return unsupported();
        }

        @Override
        public Mono<DynamicForm> readForm(String formId, String schema, String table) {
            return unsupported();
        }

        private Mono<DynamicForm> unsupported() {
            return Mono.error(new UnsupportedOperationException(
                    "metadata reader is not implemented for the requested dialect"));
        }
    }
}
