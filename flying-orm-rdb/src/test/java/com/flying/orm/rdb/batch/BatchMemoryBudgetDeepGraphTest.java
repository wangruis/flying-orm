package com.flying.orm.rdb.batch;

import com.flying.orm.rdb.codec.SqlTypedValue;
import com.flying.orm.rdb.result.DynamicRow;
import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.R2dbcType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BatchMemoryBudgetDeepGraphTest {

    @Test
    void estimatesTenThousandNestedArraysThroughEveryRowEntryPoint() {
        Object value = new byte[5];
        for (int depth = 0; depth < 10_000; depth++) {
            value = new Object[]{value};
        }

        assertEquals(320_021L, BatchMemoryBudget.estimateValueBytes(value));
        assertEquals(320_053L, BatchMemoryBudget.estimateRowBytes(new Object[]{value}));
        assertEquals(320_053L, BatchMemoryBudget.estimateRowBytes(new Object[]{value, new byte[100]}, 1));
        assertEquals(320_053L, BatchMemoryBudget.estimateRowBytes(DynamicRow.copyOf(Map.of("value", value))));
    }

    @Test
    void estimatesTenThousandNestedCollections() {
        Object value = new byte[5];
        for (int depth = 0; depth < 10_000; depth++) {
            value = List.of(value);
        }

        assertEquals(320_021L, BatchMemoryBudget.estimateValueBytes(value));
    }

    @Test
    void estimatesTenThousandNestedMapsIncludingNullKeys() {
        Object value = new byte[5];
        for (int depth = 0; depth < 10_000; depth++) {
            Map<Object, Object> map = new LinkedHashMap<>();
            map.put(null, value);
            value = map;
        }

        assertEquals(720_021L, BatchMemoryBudget.estimateValueBytes(value));
    }

    @Test
    void estimatesTenThousandNestedDriverParameters() {
        Object value = new byte[5];
        for (int depth = 0; depth < 10_000; depth++) {
            value = Parameters.in(R2dbcType.VARBINARY, value);
        }

        assertEquals(240_021L, BatchMemoryBudget.estimateValueBytes(value));
    }

    @Test
    void countsCyclesAndAliasesByIdentityWithoutDuplicatingPayloads() {
        byte[] payload = new byte[5];
        List<Object> cycle = new ArrayList<>();
        cycle.add(payload);
        cycle.add(cycle);
        cycle.add(payload);

        assertEquals(133L, BatchMemoryBudget.estimateValueBytes(new Object[]{cycle, cycle}));
    }

    @Test
    void retainsExistingContainerAndScalarWeightContract() {
        Object[] row = {null, "A", new byte[3], ByteBuffer.allocate(5), new int[2],
                new SqlTypedValue(SqlTypedValue.Kind.CLOB, "xy"), new BigDecimal("123.45"),
                BigInteger.valueOf(256), 1000, List.of("z"), Map.of("k", "v"), new Object()};

        assertEquals(600L, BatchMemoryBudget.estimateRowBytes(row));
        assertEquals(600L, BatchMemoryBudget.estimateValueBytes(row));
        assertEquals(187L, BatchMemoryBudget.estimateShallowScalarRowBytes(
                new Object[]{null, "A", new BigDecimal("123.45"), BigInteger.valueOf(256), 1000}));
    }
}
