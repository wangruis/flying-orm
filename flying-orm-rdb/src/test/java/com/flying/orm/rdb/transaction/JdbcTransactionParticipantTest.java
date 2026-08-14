package com.flying.orm.rdb.transaction;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 验证 JDBC 外部事务只传播连接和锁定路由，不接管上层事务生命周期。 */
class JdbcTransactionParticipantTest {

    @Test
    void returnsTheExternallyOwnedConnectionAndRejectsRouteChanges() throws Exception {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:jdbc_transaction_context;DB_CLOSE_DELAY=-1");
        try (Connection connection = dataSource.getConnection()) {
            JdbcTransactionContext context = JdbcTransactionContext.external(connection, "primary");
            JdbcTransactionParticipant participant = () -> Optional.of(context);

            assertSame(connection, participant.currentTransaction("primary").orElseThrow().connection());
            assertThrows(IllegalStateException.class, () -> participant.currentTransaction("replica"));
        }
    }

    @Test
    void noneParticipantDoesNotInventATransaction() {
        assertTrue(JdbcTransactionParticipant.none().currentTransaction().isEmpty());
    }

    @Test
    void executionEntryChecksTheCurrentDynamicRoute() throws Exception {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:jdbc_transaction_route;DB_CLOSE_DELAY=-1");
        try (Connection connection = dataSource.getConnection()) {
            JdbcTransactionContext context = JdbcTransactionContext.external(connection, "primary");
            JdbcTransactionParticipant participant = new JdbcTransactionParticipant() {
                @Override
                public Optional<JdbcTransactionContext> currentTransaction() {
                    return Optional.of(context);
                }

                @Override
                public String currentRoutingIdentity() {
                    return "replica";
                }
            };

            assertThrows(IllegalStateException.class, participant::currentTransactionForExecution);
        }
    }
}
