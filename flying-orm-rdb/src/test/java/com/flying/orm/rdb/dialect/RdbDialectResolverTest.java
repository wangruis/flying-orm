package com.flying.orm.rdb.dialect;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 方言解析器的目标很简单：上层自动配置能认出数据库，业务代码不用手动选方言。
 *
 * @author wangr
 * @date 2026-07-30
 * @version v1.0
 */
class RdbDialectResolverTest {

    @Test
    void resolvesDialectFromPlainR2dbcUrl() {
        RdbDialect dialect = RdbDialectResolver.resolveUrl("r2dbc:mysql://localhost:3306/test");

        assertEquals("mysql", dialect.name());
    }

    @Test
    void resolvesDialectFromPooledR2dbcUrlProtocol() {
        RdbDialect dialect = RdbDialectResolver.resolveUrl("r2dbc:pool:postgresql://localhost:5432/test");

        assertEquals("postgresql", dialect.name());
    }

    @Test
    void resolvesDialectFromConnectionFactoryMetadata() {
        RdbDialect dialect = RdbDialectResolver.resolve(connectionFactory("PostgreSQL"));

        assertEquals("postgresql", dialect.name());
    }

    /**
     * Oracle 官方 R2DBC 驱动使用数据库产品名，而不是简写的驱动名。自动方言识别必须接受该真实 metadata，
     * 否则即使上层显式配置 Oracle，也会在双执行内核启动校验阶段被错误拒绝。
     */
    @Test
    void resolvesOracleOfficialConnectionFactoryMetadataName() {
        RdbDialect dialect = RdbDialectResolver.resolve(connectionFactory("Oracle Database"));

        assertEquals("oracle", dialect.name());
    }

    @Test
    void explicitConfigurationWinsOverConnectionFactoryMetadata() {
        RdbDialect dialect = RdbDialectResolver.resolve("mysql", connectionFactory("PostgreSQL"));

        assertEquals("mysql", dialect.name());
    }

    @Test
    void startupValidationRejectsConfiguredDialectThatDoesNotMatchSingleFactory() {
        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> RdbDialectResolver.resolveAndValidate(
                        "mysql", connectionFactory("PostgreSQL"), Map.of()));

        assertTrue(failure.getMessage().contains("dialect mismatch"));
    }

    @Test
    void resolvesDialectFromConsistentPhysicalDataSourcesBehindProxy() {
        RdbDialect dialect = RdbDialectResolver.resolveAndValidate(
                null,
                connectionFactory("routing-proxy"),
                Map.of("primary", connectionFactory("MySQL"),
                       "replica", connectionFactory("MariaDB")));

        assertEquals("mysql", dialect.name());
    }

    @Test
    void rejectsConfiguredDialectThatDoesNotMatchPhysicalDataSource() {
        String sensitiveName = "primary-password=must-not-leak";
        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> RdbDialectResolver.resolveAndValidate(
                        "mysql",
                        connectionFactory("routing-proxy"),
                        Map.of(sensitiveName, connectionFactory("PostgreSQL"))));

        assertTrue(failure.getMessage().contains("dialect mismatch"));
        assertFalse(failure.getMessage().contains(sensitiveName));
    }

    @Test
    void unsupportedDialectDoesNotEchoUrlOrConfiguredValue() {
        String sensitiveUrl = "r2dbc:unknown://user:password@localhost/database";
        IllegalArgumentException urlFailure = assertThrows(
                IllegalArgumentException.class,
                () -> RdbDialectResolver.resolveUrl(sensitiveUrl));
        assertFalse(urlFailure.getMessage().contains(sensitiveUrl));

        String sensitiveDialect = "password=must-not-leak";
        IllegalArgumentException configuredFailure = assertThrows(
                IllegalArgumentException.class,
                () -> RdbDialectResolver.resolve(sensitiveDialect, connectionFactory("PostgreSQL")));
        assertFalse(configuredFailure.getMessage().contains(sensitiveDialect));

        String malformedUrl = "r2dbc:not a url password=must-not-leak";
        IllegalArgumentException malformedFailure = assertThrows(
                IllegalArgumentException.class,
                () -> RdbDialectResolver.resolveUrl(malformedUrl));
        assertFalse(malformedFailure.getMessage().contains(malformedUrl));
    }

    @Test
    void rejectsDifferentDialectsInOneDynamicDataSourceGroup() {
        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> RdbDialectResolver.resolveAndValidate(
                        null,
                        connectionFactory("routing-proxy"),
                        Map.of("primary", connectionFactory("MySQL"),
                               "replica", connectionFactory("PostgreSQL"))));

        assertTrue(failure.getMessage().contains("dialect mismatch"));
    }

    @Test
    void unknownNameReturnsEmptyOptional() {
        Optional<RdbDialect> dialect = RdbDialectResolver.tryResolveName("some-proxy-driver");

        assertTrue(dialect.isEmpty());
    }

    private static ConnectionFactory connectionFactory(String metadataName) {
        return new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                return Mono.error(new UnsupportedOperationException("这个测试只看 metadata，不会真的连库"));
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> metadataName;
            }
        };
    }
}
