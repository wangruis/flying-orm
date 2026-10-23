package com.flying.orm.rdb.metadata;

import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.schema.SchemaSnapshotCoverage;
import com.flying.orm.rdb.sync.SyncSqlExecutor;

import java.util.Objects;
import java.util.function.Function;

/**
 * 创建原生 JDBC 动态表单元数据 reader 的小工厂。
 *
 * <p>调用方提供已经装配好的同步执行器与明确方言。工厂不接受数据源，不获取连接或探测数据库。</p>
 * <p>自定义方言没有内置字典查询时，reader 声明无结构覆盖并在读取时报告不支持；
 * 不阻止调用方用显式模型执行普通 CRUD。</p>
 *
 * @author wangr
 * @version v2.0.0
 */
public final class JdbcFormMetadataReaders {

    private JdbcFormMetadataReaders() {
    }

    /** 使用已经装配好的同步 SQL 执行器创建 reader。 */
    public static JdbcFormMetadataReader create(SyncSqlExecutor executor, RdbDialect dialect) {
        SyncSqlExecutor safeExecutor = Objects.requireNonNull(executor, "sync sql executor must not be null");
        RdbDialect safeDialect = Objects.requireNonNull(dialect, "rdb dialect must not be null");
        return new DefaultJdbcFormMetadataReader(safeExecutor, profile(safeDialect),
                CacheRegionPolicy.disabled(), MetadataCacheInvalidator.none());
    }

    /** 使用统一的有界 Caffeine 策略包装 JDBC 元数据读取，并把 DDL 失效继续传给计划缓存。 */
    public static JdbcFormMetadataReader cached(SyncSqlExecutor executor,
                                                RdbDialect dialect,
                                                CacheRegionPolicy policy,
                                                MetadataCacheInvalidator dependentInvalidator) {
        SyncSqlExecutor safeExecutor = Objects.requireNonNull(executor, "sync sql executor must not be null");
        RdbDialect safeDialect = Objects.requireNonNull(dialect, "rdb dialect must not be null");
        return new DefaultJdbcFormMetadataReader(safeExecutor,
                                          profile(safeDialect),
                                          policy,
                                          dependentInvalidator);
    }

    private static MetadataQueryProfile profile(RdbDialect dialect) {
        MetadataQueryProfile profile = MetadataQueryProfile.resolve(dialect);
        if (profile == null) {
            return new MetadataQueryProfile(new MetadataQueryProfile.Queries(
                    (schema, table) -> {
                        throw new UnsupportedOperationException(
                                "metadata reader is not implemented for the requested dialect");
                    }, null, null, Function.identity()), SchemaSnapshotCoverage.none());
        }
        return profile;
    }
}
