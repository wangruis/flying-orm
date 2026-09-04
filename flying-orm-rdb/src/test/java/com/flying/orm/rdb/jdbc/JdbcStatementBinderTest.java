package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.internal.binding.SqlNullParameter;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/** JDBC 空值绑定必须把调用方声明的 Java 类型转换成驱动可见的 SQL 类型。 */
class JdbcStatementBinderTest {

    @Test
    void bindsDeclaredJavaNullTypesAsJdbcTypes() throws Exception {
        Map<Class<?>, Integer> expectedTypes = Map.ofEntries(
                Map.entry(Boolean.class, Types.BOOLEAN),
                Map.entry(Byte.class, Types.TINYINT),
                Map.entry(Short.class, Types.SMALLINT),
                Map.entry(Integer.class, Types.INTEGER),
                Map.entry(Long.class, Types.BIGINT),
                Map.entry(BigInteger.class, Types.NUMERIC),
                Map.entry(BigDecimal.class, Types.DECIMAL),
                Map.entry(Float.class, Types.REAL),
                Map.entry(Double.class, Types.DOUBLE),
                Map.entry(Character.class, Types.CHAR),
                Map.entry(String.class, Types.VARCHAR),
                Map.entry(byte[].class, Types.VARBINARY),
                Map.entry(ByteBuffer.class, Types.VARBINARY),
                Map.entry(LocalDate.class, Types.DATE),
                Map.entry(LocalTime.class, Types.TIME),
                Map.entry(OffsetTime.class, Types.TIME_WITH_TIMEZONE),
                Map.entry(LocalDateTime.class, Types.TIMESTAMP),
                Map.entry(Instant.class, Types.TIMESTAMP_WITH_TIMEZONE),
                Map.entry(OffsetDateTime.class, Types.TIMESTAMP_WITH_TIMEZONE),
                Map.entry(CustomValue.class, Types.OTHER));

        for (Map.Entry<Class<?>, Integer> entry : expectedTypes.entrySet()) {
            assertEquals(entry.getValue(), bindNullType(entry.getKey()), entry.getKey().getName());
        }
    }

    private static Integer bindNullType(Class<?> javaType) throws Exception {
        AtomicReference<Integer> jdbcType = new AtomicReference<>();
        AtomicBoolean usedUntypedBinding = new AtomicBoolean();
        PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(
                JdbcStatementBinderTest.class.getClassLoader(),
                new Class<?>[]{PreparedStatement.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("setNull")) {
                        jdbcType.set((Integer) arguments[1]);
                    } else if (method.getName().equals("setObject")) {
                        usedUntypedBinding.set(true);
                    }
                    return null;
                });

        JdbcStatementBinder.bind(statement, List.of(new SqlNullParameter(javaType)));

        assertFalse(usedUntypedBinding.get(), "typed null must not fall back to setObject(index, null)");
        return jdbcType.get();
    }

    private static final class CustomValue {
    }
}
