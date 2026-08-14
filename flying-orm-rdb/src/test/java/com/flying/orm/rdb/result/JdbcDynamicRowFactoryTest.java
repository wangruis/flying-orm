package com.flying.orm.rdb.result;

import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlLargeObjectLimitExceededException;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.lang.reflect.Proxy;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLXML;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 验证 JDBC 行读取沿用紧凑布局，并在 JDBC LOB 路径上守住物化上限。 */
class JdbcDynamicRowFactoryTest {

    @Test
    void readsLabelsOnceAndFallsBackToColumnName() throws Exception {
        ResultSet resultSet = resultSet(new String[]{"user_id", "name", "payload"},
                                        new String[]{"id", "", "payload"},
                                        new Object[]{7L, "Ada", new byte[]{1, 2}});

        JdbcDynamicRowFactory factory = JdbcDynamicRowFactory.from(resultSet, SqlExecutionOptions.safeDefaults());
        DynamicRow row = factory.readCurrentRow();

        assertEquals(7L, row.get("id"));
        assertEquals("Ada", row.get("name"));
        assertArrayEquals(new byte[]{1, 2}, row.get("payload", byte[].class));
        assertEquals("id", row.columnName(0));
        assertEquals("name", row.columnName(1));
    }

    @Test
    void materializesJdbcBlobAndClobThenFreesThem() throws Exception {
        AtomicBoolean blobFreed = new AtomicBoolean();
        AtomicBoolean clobFreed = new AtomicBoolean();
        ResultSet resultSet = resultSet(new String[]{"binary_data", "text_data"},
                                        new String[]{"binary_data", "text_data"},
                                        new Object[]{blob(new byte[]{3, 4}, blobFreed), clob("hello", clobFreed)});

        DynamicRow row = JdbcDynamicRowFactory.from(resultSet, SqlExecutionOptions.safeDefaults()).readCurrentRow();

        assertArrayEquals(new byte[]{3, 4}, row.get("binary_data", byte[].class));
        assertEquals("hello", row.get("text_data"));
        assertTrue(blobFreed.get());
        assertTrue(clobFreed.get());
    }

    /** PostgreSQL JDBC 以 java.sql.Array 返回数组列，读取边界必须物化并释放驱动资源。 */
    @Test
    void materializesJdbcArrayAndFreesIt() throws Exception {
        AtomicBoolean freed = new AtomicBoolean();
        ResultSet resultSet = resultSet(new String[]{"tags"}, new String[]{"tags"},
                                        new Object[]{sqlArray(new String[]{"alpha", "beta"}, freed)});

        DynamicRow row = JdbcDynamicRowFactory.from(resultSet, SqlExecutionOptions.safeDefaults()).readCurrentRow();

        assertArrayEquals(new String[]{"alpha", "beta"}, (Object[]) row.get("tags"));
        assertTrue(freed.get());
    }

    /** SQL Server XML 列会以事务期 SQLXML 句柄返回，行读取边界必须转成文本并释放驱动资源。 */
    @Test
    void materializesJdbcSqlXmlAndFreesIt() throws Exception {
        AtomicBoolean freed = new AtomicBoolean();
        ResultSet resultSet = resultSet(new String[]{"document"}, new String[]{"document"},
                                        new Object[]{sqlXml("<root>value</root>", freed)});

        DynamicRow row = JdbcDynamicRowFactory.from(resultSet, SqlExecutionOptions.safeDefaults()).readCurrentRow();

        assertEquals("<root>value</root>", row.get("document"));
        assertTrue(freed.get());
    }

    /** 数组物化与释放同时失败时保留主失败，并把普通清理失败作为无环诊断。 */
    @Test
    void keepsArrayMaterializationFailureWhenReleaseAlsoFails() throws Exception {
        AssertionError primary = new AssertionError("array materialization failed");
        java.sql.SQLException cleanup = new java.sql.SQLException("array release failed");
        ResultSet resultSet = resultSet(new String[]{"tags"}, new String[]{"tags"},
                                        new Object[]{failingSqlArray(primary, cleanup)});

        AssertionError observed = assertThrows(AssertionError.class,
                () -> JdbcDynamicRowFactory.from(resultSet, SqlExecutionOptions.safeDefaults()).readCurrentRow());

        assertSame(primary, observed);
        assertSame(cleanup, observed.getSuppressed()[0]);
    }

    /** 普通物化失败遇到释放 VME 时提升原 fatal，同时保留普通失败且不形成异常环。 */
    @Test
    void promotesArrayReleaseVirtualMachineErrorOverOrdinaryFailure() throws Exception {
        java.sql.SQLException primary = new java.sql.SQLException("array materialization failed");
        OutOfMemoryError fatal = new OutOfMemoryError("array release failed");
        ResultSet resultSet = resultSet(new String[]{"tags"}, new String[]{"tags"},
                                        new Object[]{failingSqlArray(primary, fatal)});

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class,
                () -> JdbcDynamicRowFactory.from(resultSet, SqlExecutionOptions.safeDefaults()).readCurrentRow());

        assertSame(fatal, observed);
        assertSame(primary, observed.getSuppressed()[0]);
        assertEquals(0, primary.getSuppressed().length);
    }

    @Test
    void rejectsOversizedJdbcLobsAndStillFreesResources() throws Exception {
        AtomicBoolean blobFreed = new AtomicBoolean();
        ResultSet blobResultSet = resultSet(new String[]{"payload"}, new String[]{"payload"},
                                             new Object[]{blob(new byte[]{1, 2, 3}, blobFreed)});
        JdbcDynamicRowFactory blobFactory = JdbcDynamicRowFactory.from(
                blobResultSet, SqlExecutionOptions.safeDefaults().withMaxLargeObjectBytes(2));

        SqlLargeObjectLimitExceededException binaryError = assertThrows(
                SqlLargeObjectLimitExceededException.class, blobFactory::readCurrentRow);

        assertEquals(SqlLargeObjectLimitExceededException.Kind.BINARY, binaryError.kind());
        assertTrue(blobFreed.get());

        AtomicBoolean clobFreed = new AtomicBoolean();
        ResultSet clobResultSet = resultSet(new String[]{"content"}, new String[]{"content"},
                                             new Object[]{clob("abc", clobFreed)});
        JdbcDynamicRowFactory clobFactory = JdbcDynamicRowFactory.from(
                clobResultSet, SqlExecutionOptions.safeDefaults().withMaxLargeObjectChars(2));

        SqlLargeObjectLimitExceededException characterError = assertThrows(
                SqlLargeObjectLimitExceededException.class, clobFactory::readCurrentRow);

        assertEquals(SqlLargeObjectLimitExceededException.Kind.CHARACTER, characterError.kind());
        assertTrue(clobFreed.get());

        AtomicBoolean sqlXmlFreed = new AtomicBoolean();
        ResultSet sqlXmlResultSet = resultSet(new String[]{"document"}, new String[]{"document"},
                                               new Object[]{sqlXml("abc", sqlXmlFreed)});
        JdbcDynamicRowFactory sqlXmlFactory = JdbcDynamicRowFactory.from(
                sqlXmlResultSet, SqlExecutionOptions.safeDefaults().withMaxLargeObjectChars(2));

        SqlLargeObjectLimitExceededException sqlXmlError = assertThrows(
                SqlLargeObjectLimitExceededException.class, sqlXmlFactory::readCurrentRow);

        assertEquals(SqlLargeObjectLimitExceededException.Kind.CHARACTER, sqlXmlError.kind());
        assertTrue(sqlXmlFreed.get());
    }

    @Test
    void rejectsOversizedMaterializedBinaryAndCharacterValues() throws Exception {
        ResultSet binary = resultSet(new String[]{"payload"}, new String[]{"payload"},
                                     new Object[]{new byte[]{1, 2, 3}});
        JdbcDynamicRowFactory binaryFactory = JdbcDynamicRowFactory.from(
                binary, SqlExecutionOptions.safeDefaults().withMaxLargeObjectBytes(2));

        assertThrows(SqlLargeObjectLimitExceededException.class, binaryFactory::readCurrentRow);

        ResultSet text = resultSet(new String[]{"content"}, new String[]{"content"}, new Object[]{"abc"});
        JdbcDynamicRowFactory textFactory = JdbcDynamicRowFactory.from(
                text, SqlExecutionOptions.safeDefaults().withMaxLargeObjectChars(2));

        assertThrows(SqlLargeObjectLimitExceededException.class, textFactory::readCurrentRow);
    }

    private static ResultSet resultSet(String[] names, String[] labels, Object[] values) {
        ResultSetMetaData metadata = (ResultSetMetaData) Proxy.newProxyInstance(
                JdbcDynamicRowFactoryTest.class.getClassLoader(), new Class<?>[]{ResultSetMetaData.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getColumnCount" -> names.length;
                    case "getColumnName" -> names[(Integer) arguments[0] - 1];
                    case "getColumnLabel" -> labels[(Integer) arguments[0] - 1];
                    default -> defaultValue(method.getReturnType());
                });
        return (ResultSet) Proxy.newProxyInstance(JdbcDynamicRowFactoryTest.class.getClassLoader(),
                                                   new Class<?>[]{ResultSet.class},
                                                   (proxy, method, arguments) -> switch (method.getName()) {
                                                       case "getMetaData" -> metadata;
                                                       case "getObject" -> values[(Integer) arguments[0] - 1];
                                                       default -> defaultValue(method.getReturnType());
                                                   });
    }

    private static Blob blob(byte[] bytes, AtomicBoolean freed) {
        return (Blob) Proxy.newProxyInstance(JdbcDynamicRowFactoryTest.class.getClassLoader(),
                                              new Class<?>[]{Blob.class},
                                              (proxy, method, arguments) -> switch (method.getName()) {
                                                  case "getBinaryStream" -> new ByteArrayInputStream(bytes);
                                                  case "free" -> {
                                                      freed.set(true);
                                                      yield null;
                                                  }
                                                  default -> defaultValue(method.getReturnType());
                                              });
    }

    private static Clob clob(String text, AtomicBoolean freed) {
        return (Clob) Proxy.newProxyInstance(JdbcDynamicRowFactoryTest.class.getClassLoader(),
                                              new Class<?>[]{Clob.class},
                                              (proxy, method, arguments) -> switch (method.getName()) {
                                                  case "getCharacterStream" -> new StringReader(text);
                                                  case "free" -> {
                                                      freed.set(true);
                                                      yield null;
                                                  }
                                                  default -> defaultValue(method.getReturnType());
                                              });
    }

    private static Array sqlArray(Object values, AtomicBoolean freed) {
        return (Array) Proxy.newProxyInstance(JdbcDynamicRowFactoryTest.class.getClassLoader(),
                                               new Class<?>[]{Array.class},
                                               (proxy, method, arguments) -> switch (method.getName()) {
                                                   case "getArray" -> values;
                                                   case "free" -> {
                                                       freed.set(true);
                                                       yield null;
                                                   }
                                                   default -> defaultValue(method.getReturnType());
                                               });
    }

    private static Array failingSqlArray(Throwable materializationFailure, Throwable releaseFailure) {
        return (Array) Proxy.newProxyInstance(JdbcDynamicRowFactoryTest.class.getClassLoader(),
                                               new Class<?>[]{Array.class},
                                               (proxy, method, arguments) -> switch (method.getName()) {
                                                   case "getArray" -> throw materializationFailure;
                                                   case "free" -> throw releaseFailure;
                                                   default -> defaultValue(method.getReturnType());
                                               });
    }

    private static SQLXML sqlXml(String text, AtomicBoolean freed) {
        return (SQLXML) Proxy.newProxyInstance(JdbcDynamicRowFactoryTest.class.getClassLoader(),
                                                new Class<?>[]{SQLXML.class},
                                                (proxy, method, arguments) -> switch (method.getName()) {
                                                    case "getString" -> text;
                                                    case "getCharacterStream" -> new StringReader(text);
                                                    case "free" -> {
                                                        freed.set(true);
                                                        yield null;
                                                    }
                                                    default -> defaultValue(method.getReturnType());
                                                });
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == char.class) {
            return '\0';
        }
        return 0;
    }
}
