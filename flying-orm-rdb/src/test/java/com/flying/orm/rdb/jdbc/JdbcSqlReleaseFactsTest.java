package com.flying.orm.rdb.jdbc;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.observation.*;
import org.junit.jupiter.api.Test;
import java.lang.reflect.Proxy;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
class JdbcSqlReleaseFactsTest {
    @Test void releaseFailureKeepsActualUpdateCountsInBothWriteObservations() {
        for (boolean keys : new boolean[]{false,true}) {
            List<SqlExecutionObservation> events = new ArrayList<>();
            PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(
                    PreparedStatement.class.getClassLoader(), new Class<?>[]{PreparedStatement.class},
                    (self,method,args) -> switch(method.getName()) {
                        case "executeLargeUpdate" -> 7L;
                        case "getGeneratedKeys", "setObject", "close" -> null;
                        default -> throw new AssertionError(method.getName());
                    });
            Connection connection = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                    new Class<?>[]{Connection.class},(self,method,args) -> {
                        if (method.getName().equals("prepareStatement")) return statement;
                        throw new AssertionError(method.getName());
                    });
            JdbcConnectionAccess access = new JdbcConnectionAccess() {
                public Connection getConnection(SqlRequest request) { return connection; }
                public void releaseConnection(Connection actual,SqlRequest request) throws SQLException {
                    throw new SQLException("release failed");
                }
            };
            var executor = JdbcSqlExecutor.create(access,RdbDialect.h2()).withObserver(events::add);
            SqlRequest request = new SqlRequest("update sample set value_col=?",List.of(1));
            assertThrows(RuntimeException.class, () -> {
                if (keys) executor.rowsUpdatedReturningKeys(request,SqlExecutionOptions.safeDefaults());
                else executor.rowsUpdated(request);
            });
            assertEquals(1,events.size());
            assertEquals(SqlExecutionStatus.ERROR,events.getFirst().status());
            assertEquals(7,events.getFirst().rows());
        }
    }
}
