package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.*;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import java.lang.reflect.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class JdbcGeneratedBatchCompletionTest {
    @Test void earlierGeneratedRowCompletesAfterStatementCleanupWhenLaterKeyReadFails() throws Exception {
        try (Connection database = database()) {
            var reads = new AtomicInteger();
            var closes = new AtomicInteger();
            var released = new AtomicInteger();
            var assigned = new ArrayList<Long>();
            var completed = new ArrayList<Long>();
            Connection connection = decorate(database, reads, closes, true);
            var writer = JdbcBatchWriter.create(access(connection, released), RdbDialect.h2());
            var failure = assertThrows(BatchExecutionEvidenceException.class, () -> writer.writeBatch(
                    request(3, false, assigned), offset -> {
                        assertEquals(1, closes.get());
                        completed.add(offset);
                    }));
            assertEquals(List.of(0L), assigned);
            assertEquals(List.of(0L), completed);
            assertEquals(List.of(0L,1L), failure.evidence().successfulOffsets().boxed().toList());
            assertEquals(3, failure.evidence().inputCount());
            assertEquals(1, released.get());
            assertFalse(database.isClosed());
        }
    }

    @Test void generatedProtectedRowsDoNotCompleteBeforeFailedTokenFlush() throws Exception {
        try (Connection database = database()) {
            var assigned = new ArrayList<Long>();
            var released = new AtomicInteger();
            var writer = JdbcBatchWriter.create(access(database,released),RdbDialect.h2());
            var failure = assertThrows(BatchExecutionEvidenceException.class, () -> writer.writeProtectedBatch(
                    request(2,true,assigned), offset -> fail("incomplete token work must not publish POST")));
            assertEquals(List.of(0L,1L),assigned);
            assertEquals(2,failure.evidence().successfulCount());
            assertEquals(2,failure.evidence().affectedRows().value());
            assertEquals(1,released.get());
        }
    }

    @Test void statementCloseFailureKeepsAppliedKeysAndPreventsPost() throws Exception {
        try (Connection database = database()) {
            var assigned = new ArrayList<Long>();
            var released = new AtomicInteger();
            var closes = new AtomicInteger();
            Connection connection = decorate(database,new AtomicInteger(),closes,false);
            var writer = JdbcBatchWriter.create(access(connection,released),RdbDialect.h2());
            var failure = assertThrows(BatchExecutionEvidenceException.class, () -> writer.writeBatch(
                    request(2,false,assigned), offset -> fail("statement cleanup failed")));
            assertEquals(List.of(0L,1L),assigned);
            assertEquals(2,failure.evidence().successfulCount());
            assertEquals(1,closes.get());
            assertEquals(1,released.get());
        }
    }

    private static BatchWriteRequest request(int size, boolean protectedRows, List<Long> assigned) {
        String sql = "insert into samples(value_col) values (?)";
        var rows = Flux.range(0,size).map(i -> {
            Object[] values = {"value-"+i};
            if (!protectedRows) return values;
            var work = new ProtectedWriteWork(ProtectedWriteWork.Kind.INSERT,
                    new SqlRequest(sql,List.of(values)),null,List.of("id"),Map.of(),"id = ?",
                    "delete from missing_tokens where id = ? and field_tag = ?",
                    "insert into missing_tokens(id,field_tag,token) values (?,?,?)",
                    List.of(new ProtectedWriteWork.FieldTokens("value_col",List.of(new byte[]{1}))));
            return ProtectedBatchRows.extend(values,work);
        });
        var base = BatchWriteRequests.request(sql,1,List.of(String.class),SqlBindMarkerStyle.CANONICAL,
                rows,BatchWriteOptions.of(size));
        return new BatchWriteRequest(base.statement(),base.parameterTypes(),base.rows(),base.options(),
                base.rowCountPolicy(),BatchGeneratedKeys.required("id",(offset,key) -> {
                    assertNotNull(key.value(0)); assigned.add(offset);
                }));
    }

    private static Connection database() throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:h2:mem:generated_"+UUID.randomUUID());
        try (Statement statement=connection.createStatement()) {
            statement.execute("create table samples(id bigint generated by default as identity primary key,value_col varchar(100))");
        }
        return connection;
    }

    private static JdbcConnectionAccess access(Connection connection, AtomicInteger released) {
        return new JdbcConnectionAccess() {
            public Connection getConnection(SqlRequest request) { return connection; }
            public void releaseConnection(Connection actual,SqlRequest request) {
                assertSame(connection,actual); released.incrementAndGet();
            }
        };
    }

    private static Connection decorate(Connection database, AtomicInteger reads, AtomicInteger closes,
                                       boolean failSecondRead) {
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),new Class<?>[]{Connection.class},
                (self,method,args) -> {
                    if (!method.getName().equals("prepareStatement")) throw new AssertionError(method.getName());
                    PreparedStatement delegate = (PreparedStatement) invoke(database,method,args);
                    return Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(),new Class<?>[]{PreparedStatement.class},
                            (statement,call,parameters) -> {
                                if (call.getName().equals("getGeneratedKeys") && failSecondRead && reads.incrementAndGet()==2)
                                    throw new SQLException("second key read failed");
                                if (call.getName().equals("close")) {
                                    closes.incrementAndGet();
                                    delegate.close();
                                    if (!failSecondRead) throw new SQLException("statement close failed");
                                    return null;
                                }
                                return invoke(delegate,call,parameters);
                            });
                });
    }

    private static Object invoke(Object target,Method method,Object[] arguments) throws Throwable {
        try { return method.invoke(target,arguments); }
        catch (InvocationTargetException failure) { throw failure.getCause(); }
    }
}
