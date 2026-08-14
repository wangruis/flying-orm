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
 * <p>显式配置决定使用哪个方言。Bootstrap 的校验入口仍会短暂借一条连接核对真实数据库，识别结束立即归还；
 * 这样配置写错时会在启动期失败，不会把另一种数据库的 SQL 发给驱动。动态数据源校验会逐个检查物理库，
 * 避免路由到运行时才发现某个库的方言与其余数据库不一致。</p>
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
        DataSource safeDataSource = Objects.requireNonNull(dataSource, "jdbc data source must not be null");
        try (Connection connection = safeDataSource.getConnection()) {
            DatabaseMetaData metadata = Objects.requireNonNull(
                    connection.getMetaData(), "jdbc database metadata must not be null");
            String productName = metadata.getDatabaseProductName();
            String jdbcUrl = metadata.getURL();
            return RdbDialectResolver.tryResolveName(productName)
                    .or(() -> tryResolveJdbcUrl(jdbcUrl))
                    .orElseThrow(() -> new IllegalArgumentException(
                            "unsupported rdb dialect from jdbc metadata"));
        } catch (SQLException error) {
            throw new IllegalStateException("failed to read jdbc metadata for dialect detection", error);
        }
    }

    /**
     * 核对动态数据源背后的所有物理 JDBC 数据源。显式配置仍然要和每个物理库一致，不能掩盖错误路由。
     */
    public static RdbDialect resolveAndValidate(String configuredDialect,
                                                DataSource routingDataSource,
                                                Map<String, ? extends DataSource> physicalDataSources) {
        DataSource safeRoutingDataSource = Objects.requireNonNull(
                routingDataSource, "routing jdbc data source must not be null");
        Map<String, ? extends DataSource> safePhysicalDataSources = Objects.requireNonNull(
                physicalDataSources, "physical jdbc data sources must not be null");
        if (safePhysicalDataSources.isEmpty()) {
            RdbDialect actual = resolve(safeRoutingDataSource);
            return validateConfigured(configuredDialect, actual, "jdbc data source");
        }
        RdbDialect selected = configuredDialect == null || configuredDialect.isBlank()
                ? null : resolve(configuredDialect, safeRoutingDataSource);
        String selectedSource = selected == null ? null : "configured dialect";
        for (Map.Entry<String, ? extends DataSource> entry : safePhysicalDataSources.entrySet()) {
            String name = Objects.requireNonNull(
                    entry.getKey(), "physical jdbc data source name must not be null").trim();
            if (name.isEmpty()) {
                throw new IllegalArgumentException("physical jdbc data source name must not be blank");
            }
            RdbDialect actual = resolve(Objects.requireNonNull(
                    entry.getValue(), "physical jdbc data source must not be null"));
            if (selected == null) {
                selected = actual;
                selectedSource = "physical jdbc data source";
            } else if (!selected.name().equals(actual.name())) {
                throw new IllegalArgumentException(
                        "physical jdbc data source dialect mismatch: expected " + selected.name() + " from "
                                + selectedSource + ", but another source uses " + actual.name());
            }
        }
        return selected;
    }

    private static RdbDialect validateConfigured(String configuredDialect,
                                                  RdbDialect actual,
                                                  String source) {
        if (configuredDialect == null || configuredDialect.isBlank()) {
            return actual;
        }
        RdbDialect selected = RdbDialectResolver.tryResolveName(configuredDialect)
                .orElseThrow(() -> new IllegalArgumentException(
                        "unsupported configured rdb dialect"));
        if (!selected.name().equals(actual.name())) {
            throw new IllegalArgumentException(source + " dialect mismatch: configured "
                                                       + selected.name() + ", but metadata reports " + actual.name());
        }
        return selected;
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

}
