package com.flying.orm.rdb.dialect;

import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 根据连接信息自动认出数据库方言。
 *
 * <p>使用方不需要到处写 {@code RdbDialect.mysql()}。配置集成可以先读取显式方言名，
 * 没有配置时再把 R2DBC URL 或 {@link ConnectionFactory} 交给这里识别。</p>
 *
 * @author wangr
 * @date 2026-07-30
 * @version v1.0
 */
public final class RdbDialectResolver {

    private static final Option<String> PROTOCOL = Option.valueOf("protocol");

    private RdbDialectResolver() {
    }

    /**
     * 配置里明确写了方言时，以配置为准。
     *
     * <p>配置集成可以先用显式方言名兜底，再把连接工厂传进来自动识别。
     * 没有显式配置时，它会继续从 {@link ConnectionFactory} 的 metadata 里判断。</p>
     *
     * @param configuredDialect 配置里的方言名，可以为空
     * @param connectionFactory  R2DBC 连接工厂
     * @return 已识别的方言
     */
    public static RdbDialect resolve(String configuredDialect, ConnectionFactory connectionFactory) {
        if (configuredDialect != null && !configuredDialect.isBlank()) {
            return tryResolveName(configuredDialect)
                    .orElseThrow(() -> unsupported("configured dialect"));
        }
        return resolve(connectionFactory);
    }

    /**
     * 为动态数据源或主从库一次确定并核对方言。
     *
     * <p>{@code routingConnectionFactory} 是运行时真正拿连接的统一入口，{@code physicalDataSources} 只在启动阶段
     * 用来核对每个物理库。显式方言有值时先按配置选择，但仍会逐个检查物理库，防止把 PostgreSQL 配成 MySQL；
     * 没有显式方言时，从第一项物理库推断，并要求其余项完全一致。物理库清单为空时退回单连接工厂解析。</p>
     *
     * <p>整个检查只读取 {@link ConnectionFactoryMetadata}，不会创建连接，也不会让数据库调用进入启动热路径。</p>
     *
     * @param configuredDialect       上层配置的方言名，可以为空
     * @param routingConnectionFactory 运行时使用的路由或普通连接工厂
     * @param physicalDataSources     数据源名称到物理连接工厂的映射，可以为空但不能为 {@code null}
     * @return 最终唯一方言
     */
    public static RdbDialect resolveAndValidate(String configuredDialect,
                                                ConnectionFactory routingConnectionFactory,
                                                Map<String, ? extends ConnectionFactory> physicalDataSources) {
        ConnectionFactory safeRoutingFactory = Objects.requireNonNull(
                routingConnectionFactory, "routing connection factory must not be null");
        Map<String, ? extends ConnectionFactory> safeDataSources = Objects.requireNonNull(
                physicalDataSources, "physical data sources must not be null");
        if (safeDataSources.isEmpty()) {
            RdbDialect actual = resolve(safeRoutingFactory);
            if (configuredDialect == null || configuredDialect.isBlank()) {
                return actual;
            }
            RdbDialect selected = resolve(configuredDialect, safeRoutingFactory);
            if (!selected.name().equals(actual.name())) {
                throw new IllegalArgumentException("r2dbc connection factory dialect mismatch: configured "
                                                           + selected.name() + ", but metadata reports " + actual.name());
            }
            return selected;
        }

        RdbDialect selected = configuredDialect == null || configuredDialect.isBlank()
                ? null : resolve(configuredDialect, safeRoutingFactory);
        String selectedSource = selected == null ? null : "configured dialect";
        for (Map.Entry<String, ? extends ConnectionFactory> entry : safeDataSources.entrySet()) {
            String name = Objects.requireNonNull(entry.getKey(), "physical data source name must not be null").trim();
            if (name.isEmpty()) {
                throw new IllegalArgumentException("physical data source name must not be blank");
            }
            ConnectionFactory factory = Objects.requireNonNull(
                    entry.getValue(), "physical data source must not be null");
            RdbDialect actual;
            try {
                actual = resolve(factory);
            } catch (IllegalArgumentException exception) {
                throw new IllegalArgumentException(
                        "cannot resolve dialect for physical data source", exception);
            }
            if (selected == null) {
                selected = actual;
                selectedSource = "physical data source";
            } else if (!selected.name().equals(actual.name())) {
                throw new IllegalArgumentException(
                        "physical data source dialect mismatch: expected " + selected.name() + " from "
                                + selectedSource + ", but another source uses " + actual.name());
            }
        }
        return selected;
    }

    /**
     * 从 R2DBC 连接工厂 metadata 里识别方言。
     *
     * @param connectionFactory R2DBC 连接工厂
     * @return 已识别的方言
     */
    public static RdbDialect resolve(ConnectionFactory connectionFactory) {
        ConnectionFactory safeConnectionFactory = Objects.requireNonNull(connectionFactory,
                                                                         "connection factory must not be null");
        ConnectionFactoryMetadata metadata = Objects.requireNonNull(safeConnectionFactory.getMetadata(),
                                                                    "connection factory metadata must not be null");
        return tryResolveName(metadata.getName())
                .orElseThrow(() -> unsupported("connection factory metadata name"));
    }

    /**
     * 从 R2DBC URL 里识别方言。
     *
     * <p>普通 URL 看 driver，比如 {@code r2dbc:mysql://...}。连接池 URL 通常是
     * {@code r2dbc:pool:mysql://...}，这时 driver 是 pool，真正数据库在 protocol 里。</p>
     *
     * @param url R2DBC URL
     * @return 已识别的方言
     */
    public static RdbDialect resolveUrl(String url) {
        return tryResolveUrl(url).orElseThrow(() -> unsupported("r2dbc url"));
    }

    /**
     * 尝试从 R2DBC URL 识别方言，识别不了时返回空。
     *
     * @param url R2DBC URL
     * @return 识别结果
     */
    public static Optional<RdbDialect> tryResolveUrl(String url) {
        if (url == null || url.isBlank()) {
            return Optional.empty();
        }
        ConnectionFactoryOptions options;
        try {
            options = ConnectionFactoryOptions.parse(url);
        } catch (IllegalArgumentException ignored) {
            // URL 可能包含用户名和密码，解析失败时不能把驱动原始消息或 URL 作为 cause 暴露。
            return Optional.empty();
        }
        Optional<RdbDialect> byDriver = tryResolveName(value(options, ConnectionFactoryOptions.DRIVER));
        if (byDriver.isPresent()) {
            return byDriver;
        }
        return tryResolveName(value(options, PROTOCOL));
    }

    /**
     * 尝试根据驱动名、metadata 名或配置名识别方言。
     *
     * @param name 名字，可能来自配置、URL driver、URL protocol 或连接工厂 metadata
     * @return 识别结果
     */
    public static Optional<RdbDialect> tryResolveName(String name) {
        if (name == null || name.isBlank()) {
            return Optional.empty();
        }
        return switch (normalize(name)) {
            case "h2", "h2database", "h2databaseengine" -> Optional.of(RdbDialect.h2());
            case "mysql", "mariadb", "mysqlconnectionfactoryprovider" -> Optional.of(RdbDialect.mysql());
            case "postgresql", "postgres", "postgresqlconnectionfactoryprovider" -> Optional.of(RdbDialect.postgresql());
            case "oracle", "oracledatabase", "oracleconnectionfactoryprovider" -> Optional.of(RdbDialect.oracle());
            case "sqlserver", "sql-server", "sql_server", "mssql", "microsoftsqlserver" -> Optional.of(RdbDialect.sqlServer());
            default -> Optional.empty();
        };
    }

    private static String value(ConnectionFactoryOptions options, Option<String> option) {
        Object value = options.getValue(option);
        return value == null ? null : value.toString();
    }

    private static String normalize(String name) {
        return name.trim()
                   .toLowerCase(Locale.ROOT)
                   .replace(" ", "")
                   .replace("_", "")
                   .replace("-", "");
    }

    private static IllegalArgumentException unsupported(String source) {
        return new IllegalArgumentException("unsupported rdb dialect from " + source);
    }
}
