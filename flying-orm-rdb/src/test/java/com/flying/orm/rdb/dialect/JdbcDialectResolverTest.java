package com.flying.orm.rdb.dialect;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 验证 JDBC 接入既能自动识别，也能在显式配置时避免无意义的启动连接。 */
class JdbcDialectResolverTest {

    @Test
    void detectsH2FromDatabaseMetadata() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:dialect_detection;DB_CLOSE_DELAY=-1");

        assertEquals("h2", JdbcDialectResolver.resolve(dataSource).name());
    }

    @Test
    void explicitDialectDoesNotBorrowAConnection() {
        assertEquals("mysql", JdbcDialectResolver.resolve("mysql", new FailingDataSource()).name());
    }

    @Test
    void startupValidationRejectsConfiguredDialectThatDoesNotMatchSingleDataSource() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:dialect_validation;DB_CLOSE_DELAY=-1");

        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> JdbcDialectResolver.resolveAndValidate("mysql", dataSource, Map.of()));

        assertTrue(failure.getMessage().contains("dialect mismatch"));
    }

    @Test
    void unsupportedConfiguredDialectDoesNotEchoInput() {
        String sensitiveDialect = "password=must-not-leak";

        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> JdbcDialectResolver.resolve(sensitiveDialect, new FailingDataSource()));

        assertFalse(failure.getMessage().contains(sensitiveDialect));
    }

    private static final class FailingDataSource implements DataSource {

        @Override
        public Connection getConnection() throws SQLException {
            throw new SQLException("connection must not be requested");
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            return getConnection();
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            throw new SQLException("not a wrapper");
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) {
            return false;
        }

        @Override
        public java.io.PrintWriter getLogWriter() {
            return null;
        }

        @Override
        public void setLogWriter(java.io.PrintWriter out) {
        }

        @Override
        public void setLoginTimeout(int seconds) {
        }

        @Override
        public int getLoginTimeout() {
            return 0;
        }

        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException();
        }
    }
}
