package com.flying.orm.rdb.result;

import com.flying.orm.rdb.codec.ArrayValueCodec;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.mapping.RowMapper;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Types;
import java.time.LocalTime;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JdbcTimePrecisionRegressionTest {

    private static final LocalTime PRECISE = LocalTime.of(12, 34, 56, 123456000);

    @Test
    void scalarTimeReachesEntityWithoutFractionalLoss() throws Exception {
        Counts counts = new Counts();
        ResultSet rows = rows(new int[]{Types.TIME}, new Object[][]{{PRECISE}}, counts, null);
        rows.next();

        DynamicRow row = JdbcDynamicRowFactory.from(rows, SqlExecutionOptions.safeDefaults()).readCurrentRow();

        assertEquals(PRECISE, RowMapper.of(TimeRow.class).map(row).c0());
        assertEquals(1, counts.typedReads.get());
        assertEquals(0, counts.rawReads.get());
        assertEquals(0, counts.closes.get(), "The caller still owns the outer ResultSet");
    }

    @Test
    void nullAndZeroPrecisionTimeRemainValid() throws Exception {
        Counts counts = new Counts();
        ResultSet rows = rows(new int[]{Types.TIME},
                new Object[][]{{null}, {LocalTime.of(1, 2, 3)}}, counts, null);
        JdbcDynamicRowFactory factory = JdbcDynamicRowFactory.from(rows, SqlExecutionOptions.safeDefaults());

        rows.next();
        assertNull(factory.readCurrentRow().get("c0"));
        rows.next();
        assertEquals(LocalTime.of(1, 2, 3), RowMapper.of(TimeRow.class).map(factory.readCurrentRow()).c0());
    }

    @Test
    void ordinaryColumnsKeepTheirOriginalSingleRead() throws Exception {
        Counts counts = new Counts();
        byte[] binary = {1, 2};
        ResultSet rows = rows(new int[]{Types.INTEGER, Types.VARBINARY},
                new Object[][]{{17, binary}}, counts, null);
        rows.next();

        DynamicRow row = JdbcDynamicRowFactory.from(rows, SqlExecutionOptions.safeDefaults()).readCurrentRow();

        assertEquals(17, row.get("c0"));
        assertSame(binary, row.get("c1"));
        assertEquals(2, counts.rawReads.get());
        assertEquals(0, counts.typedReads.get());
    }

    @Test
    void timeArrayKeepsFractionsNullElementsAndReleasesResources() throws Exception {
        Counts counts = new Counts();
        Array value = timeArray(new LocalTime[]{PRECISE, null, LocalTime.MIDNIGHT}, counts, null);

        Object materialized = arrayRow(value);

        assertArrayEquals(new LocalTime[]{PRECISE, null, LocalTime.MIDNIGHT},
                (LocalTime[]) ArrayValueCodec.read(materialized, LocalTime[].class));
        assertEquals(1, counts.closes.get());
        assertEquals(1, counts.frees.get());
        assertEquals(0, counts.arrayReads.get());
    }

    @Test
    void emptyTimeArrayIsNotNullAndStillReleasesResources() throws Exception {
        Counts counts = new Counts();

        assertArrayEquals(new LocalTime[0],
                (LocalTime[]) ArrayValueCodec.read(arrayRow(timeArray(new LocalTime[0], counts, null)),
                        LocalTime[].class));
        assertEquals(1, counts.closes.get());
        assertEquals(1, counts.frees.get());
    }

    @Test
    void timeArrayReadFailureClosesTheElementCursorAndFreesTheArray() {
        Counts counts = new Counts();
        SQLException failure = new SQLException("typed time read failed", "22007");

        assertSame(failure, assertThrows(SQLException.class,
                () -> arrayRow(timeArray(new LocalTime[]{PRECISE}, counts, failure))));
        assertEquals(1, counts.closes.get());
        assertEquals(1, counts.frees.get());
    }

    @Test
    void ordinaryArrayStillUsesItsExistingMaterialization() throws Exception {
        Counts counts = new Counts();
        Integer[] expected = {1, null, 2};
        Array array = proxy(Array.class, (method, args) -> switch (method) {
            case "getBaseType" -> Types.INTEGER;
            case "getArray" -> { counts.arrayReads.incrementAndGet(); yield expected; }
            case "free" -> { counts.frees.incrementAndGet(); yield null; }
            default -> throw new UnsupportedOperationException(method);
        });

        assertSame(expected, arrayRow(array));
        assertEquals(1, counts.arrayReads.get());
        assertEquals(1, counts.frees.get());
    }

    private static Object arrayRow(Array array) throws Exception {
        ResultSet rows = rows(new int[]{Types.ARRAY}, new Object[][]{{array}}, new Counts(), null);
        rows.next();
        return JdbcDynamicRowFactory.from(rows, SqlExecutionOptions.safeDefaults()).readCurrentRow().get("c0");
    }

    private static Array timeArray(LocalTime[] times, Counts counts, SQLException failure) {
        Object[][] elements = new Object[times.length][2];
        Time[] legacy = new Time[times.length];
        for (int i = 0; i < times.length; i++) {
            elements[i][0] = (long) i + 1;
            elements[i][1] = times[i];
            legacy[i] = times[i] == null ? null : Time.valueOf(times[i]);
        }
        return proxy(Array.class, (method, args) -> switch (method) {
            case "getBaseType" -> Types.TIME;
            case "getArray" -> { counts.arrayReads.incrementAndGet(); yield legacy; }
            case "getResultSet" -> rows(new int[]{Types.BIGINT, Types.TIME}, elements, counts, failure);
            case "free" -> { counts.frees.incrementAndGet(); yield null; }
            default -> throw new UnsupportedOperationException(method);
        });
    }

    private static ResultSet rows(int[] types, Object[][] values, Counts counts, SQLException failure) {
        ResultSetMetaData metadata = proxy(ResultSetMetaData.class, (method, args) -> switch (method) {
            case "getColumnCount" -> types.length;
            case "getColumnLabel", "getColumnName" -> "c" + ((int) args[0] - 1);
            case "getColumnType" -> types[(int) args[0] - 1];
            default -> throw new UnsupportedOperationException(method);
        });
        AtomicInteger position = new AtomicInteger(-1);
        return proxy(ResultSet.class, (method, args) -> switch (method) {
            case "getMetaData" -> metadata;
            case "next" -> position.incrementAndGet() < values.length;
            case "close" -> { counts.closes.incrementAndGet(); yield null; }
            case "getObject" -> {
                Object value = values[position.get()][(int) args[0] - 1];
                if (args.length == 2) {
                    counts.typedReads.incrementAndGet();
                    assertSame(LocalTime.class, args[1]);
                    if (failure != null) { throw failure; }
                    yield value;
                }
                counts.rawReads.incrementAndGet();
                yield value instanceof LocalTime time ? Time.valueOf(time) : value;
            }
            default -> throw new UnsupportedOperationException(method);
        });
    }

    private static <T> T proxy(Class<T> type, Call call) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
                (proxy, method, args) -> call.invoke(method.getName(), args)));
    }

    private interface Call {
        Object invoke(String method, Object[] args) throws Throwable;
    }

    private static final class Counts {
        final AtomicInteger typedReads = new AtomicInteger();
        final AtomicInteger rawReads = new AtomicInteger();
        final AtomicInteger closes = new AtomicInteger();
        final AtomicInteger frees = new AtomicInteger();
        final AtomicInteger arrayReads = new AtomicInteger();
    }

    record TimeRow(LocalTime c0) { }
}
