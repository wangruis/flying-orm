package com.flying.orm.rdb.metadata;

import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.sync.SyncSqlExecutor;

import javax.sql.DataSource;
import java.util.Objects;

/**
 * 创建原生 JDBC 动态表单元数据 reader 的小工厂。
 *
 * <p>调用方只需要提供已经确定的方言。方言自动识别属于连接装配层，避免元数据工厂在每次创建 reader
 * 时偷偷借连接；直接传 {@link DataSource} 的重载只是一个方便入口，执行器仍然是原生 JDBC。</p>
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
        return new JdbcFormMetadataReader(safeExecutor, queries(safeDialect));
    }

    /** 直接从 DataSource 创建原生 JDBC reader；连接的关闭和归还由 JdbcSqlExecutor 负责。 */
    public static JdbcFormMetadataReader create(DataSource dataSource, RdbDialect dialect) {
        return create(SyncSqlExecutor.jdbc(Objects.requireNonNull(dataSource, "jdbc data source must not be null")),
                      dialect);
    }

    /** 使用统一的有界 Caffeine 策略包装 JDBC 元数据读取，并把 DDL 失效继续传给计划缓存。 */
    public static JdbcFormMetadataReader cached(SyncSqlExecutor executor,
                                                RdbDialect dialect,
                                                CacheRegionPolicy policy,
                                                MetadataCacheInvalidator dependentInvalidator) {
        SyncSqlExecutor safeExecutor = Objects.requireNonNull(executor, "sync sql executor must not be null");
        RdbDialect safeDialect = Objects.requireNonNull(dialect, "rdb dialect must not be null");
        return new JdbcFormMetadataReader(safeExecutor,
                                          queries(safeDialect),
                                          policy,
                                          dependentInvalidator);
    }

    private static InformationSchemaFormMetadataReader.Queries queries(RdbDialect dialect) {
        return switch (dialect.name()) {
            case "h2" -> H2ReactiveFormMetadataReader.queries();
            case "mysql" -> MySqlReactiveFormMetadataReader.queries();
            case "postgresql" -> PostgreSqlReactiveFormMetadataReader.queries();
            case "oracle" -> OracleReactiveFormMetadataReader.queries();
            case "sqlserver" -> SqlServerReactiveFormMetadataReader.queries();
            default -> throw new UnsupportedOperationException(
                    "metadata reader is not implemented for the requested dialect");
        };
    }
}
