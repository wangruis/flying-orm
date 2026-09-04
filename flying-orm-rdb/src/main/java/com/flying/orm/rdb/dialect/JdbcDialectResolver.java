package com.flying.orm.rdb.dialect;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;

/**
 * 从 JDBC DataSource 识别并校验 flying-orm 方言。
 *
 * <p>显式配置决定使用哪个方言，不会为了重复核对配置而借用连接。动态数据源的物理拓扑由上层治理；
 * 只有未配置方言时，ORM 才从运行时统一入口读取一次 metadata 自动识别。</p>
 *
 * @author wangr
 * @version v2.0.0
 */
public final class JdbcDialectResolver {

    private JdbcDialectResolver() {
    }

    /** 显式方言有值时直接采用；否则从 JDBC metadata 自动识别。 */
    public static RdbDialect resolve(String configuredDialect, DataSource dataSource) {
        if (configuredDialect != null && !configuredDialect.isBlank()) {
            return RdbDialectResolver.tryResolveName(configuredDialect)
                    .orElseThrow(() -> new IllegalArgumentException(
                            "unsupported configured rdb dialect"));
        }
        return resolve(dataSource);
    }

    /** 从数据库产品名和 JDBC URL 识别方言，无法确定时直接失败，不猜默认数据库。 */
    public static RdbDialect resolve(DataSource dataSource) {
        return inspect(dataSource).dialect();
    }

    /**
     * 在自动识别方言使用的同一个 JDBC 连接上读取产品和版本，不额外借连接，也不保存 URL、账号或数据源。
     */
    public static DatabaseDescriptor describe(DataSource dataSource) {
        ResolvedDatabase resolved = inspect(dataSource);
        return DatabaseDescriptor.of(resolved.product(), resolved.version(), resolved.dialect());
    }

    private static ResolvedDatabase inspect(DataSource dataSource) {
        DataSource safeDataSource = Objects.requireNonNull(dataSource, "jdbc data source must not be null");
        try (Connection connection = safeDataSource.getConnection()) {
            DatabaseMetaData metadata = Objects.requireNonNull(
                    connection.getMetaData(), "jdbc database metadata must not be null");
            String productName = metadata.getDatabaseProductName();
            String jdbcUrl = metadata.getURL();
            RdbDialect dialect = RdbDialectResolver.tryResolveName(productName)
                    .or(() -> tryResolveJdbcUrl(jdbcUrl))
                    .orElseThrow(() -> new IllegalArgumentException(
                            "unsupported rdb dialect from jdbc metadata"));
            String version = metadata.getDatabaseProductVersion();
            return new ResolvedDatabase(productName == null || productName.isBlank() ? "unknown" : productName,
                                        version == null || version.isBlank() ? "unknown" : version,
                                        dialect);
        } catch (SQLException error) {
            throw new IllegalStateException("failed to read jdbc metadata for dialect detection", error);
        }
    }

    /** 核对运行时统一 JDBC 入口；物理数据源清单仅为兼容现有配置 API，不由 ORM 枚举或打开。 */
    public static RdbDialect resolveAndValidate(String configuredDialect,
                                                 DataSource routingDataSource,
                                                 Map<String, ? extends DataSource> physicalDataSources) {
        DataSource safeRoutingDataSource = Objects.requireNonNull(
                routingDataSource, "routing jdbc data source must not be null");
        Objects.requireNonNull(
                physicalDataSources, "physical jdbc data sources must not be null");
        return resolve(configuredDialect, safeRoutingDataSource);
    }

    private static java.util.Optional<RdbDialect> tryResolveJdbcUrl(String url) {
        if (url == null || url.isBlank()) {
            return java.util.Optional.empty();
        }
        int prefix = url.indexOf(':');
        int driverEnd = prefix < 0 ? -1 : url.indexOf(':', prefix + 1);
        if (prefix < 0 || driverEnd < 0) {
            return java.util.Optional.empty();
        }
        return RdbDialectResolver.tryResolveName(url.substring(prefix + 1, driverEnd));
    }

    private record ResolvedDatabase(String product, String version, RdbDialect dialect) {
    }

}
