package com.flying.orm.rdb.reactive;

import io.r2dbc.spi.Parameter;
import io.r2dbc.spi.Type;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证批量回执摘要稳定，后面 UNKNOWN 才能靠它判断是不是同一批数据。
 *
 * @author wangr
 * @date 2026-07-24
 * @version v1.0
 */
class BatchPayloadHasherTest {

    /**
     * 同样的值每次都要算出同一个摘要，不同数字类型不能混在一起。
     */
    @Test
    void hashesSameTypedValuesDeterministicallyAndKeepsTypesDistinct() {
        BatchPayloadHasher hasher = new BatchPayloadHasher();

        String first = hasher.hashRows(List.<Object[]>of(new Object[]{1L, "A", null}));
        String second = hasher.hashRows(List.<Object[]>of(new Object[]{1L, "A", null}));
        String integer = hasher.hashRows(List.<Object[]>of(new Object[]{1, "A", null}));

        assertEquals(first, second);
        assertNotEquals(first, integer);
    }

    /**
     * 常见动态表单值都能进入摘要，包含 R2DBC 的显式类型参数。
     */
    @Test
    void hashesCommonFormValues() {
        BatchPayloadHasher hasher = new BatchPayloadHasher();

        String first = hasher.hashRows(List.<Object[]>of(new Object[]{
                true,
                new BigDecimal("12.30"),
                UUID.fromString("00000000-0000-0000-0000-000000000001"),
                LocalDate.of(2026, 7, 24),
                new byte[]{1, 2, 3},
                ByteBuffer.wrap(new byte[]{4, 5}),
                emptyParameter(String.class)
        }));

        String second = hasher.hashRows(List.<Object[]>of(new Object[]{
                true,
                new BigDecimal("12.30"),
                UUID.fromString("00000000-0000-0000-0000-000000000001"),
                LocalDate.of(2026, 7, 24),
                new byte[]{1, 2, 3},
                ByteBuffer.wrap(new byte[]{4, 5}),
                emptyParameter(String.class)
        }));

        assertEquals(first, second);
    }

    /**
     * 数组和带时区时间已经能作为动态表单值写入，它们也必须能生成稳定且区分类型的恢复摘要。
     */
    @Test
    void hashesArrayCollectionAndOffsetTimeValues() {
        BatchPayloadHasher hasher = new BatchPayloadHasher();
        Object[] values = {new String[]{"alpha", "beta"},
                           new int[]{1, 2},
                           List.of("x", "y"),
                           OffsetTime.parse("13:40:00+08:00")};

        String first = hasher.hashRows(List.<Object[]>of(values));
        String second = hasher.hashRows(List.<Object[]>of(new Object[]{new String[]{"alpha", "beta"},
                                                                         new int[]{1, 2},
                                                                         List.of("x", "y"),
                                                                         OffsetTime.parse("13:40:00+08:00")}));
        String changed = hasher.hashRows(List.<Object[]>of(new Object[]{new String[]{"alpha", "gamma"},
                                                                          new int[]{1, 2},
                                                                          List.of("x", "y"),
                                                                          OffsetTime.parse("13:40:00+08:00")}));

        assertEquals(first, second);
        assertNotEquals(first, changed);
    }

    /** 同一瞬间的不同 offset/zone 仍是不同数据库参数，恢复摘要不能把它们误认成同一载荷。 */
    @Test
    void distinguishesOffsetAndZoneIdentityAtTheSameInstant() {
        BatchPayloadHasher hasher = new BatchPayloadHasher();
        OffsetDateTime offsetDateTime = OffsetDateTime.parse("2026-08-11T13:40:00+08:00");
        OffsetDateTime utcDateTime = offsetDateTime.withOffsetSameInstant(ZoneOffset.UTC);
        ZonedDateTime shanghai = offsetDateTime.atZoneSameInstant(ZoneId.of("Asia/Shanghai"));
        ZonedDateTime utc = shanghai.withZoneSameInstant(ZoneOffset.UTC);

        assertNotEquals(hasher.hashRows(List.<Object[]>of(new Object[]{offsetDateTime})),
                        hasher.hashRows(List.<Object[]>of(new Object[]{utcDateTime})));
        assertNotEquals(hasher.hashRows(List.<Object[]>of(new Object[]{shanghai})),
                        hasher.hashRows(List.<Object[]>of(new Object[]{utc})));
    }

    /** 自引用参数必须作为非法输入尽早失败，不能递归到 StackOverflowError。 */
    @Test
    void rejectsCyclicBatchValues() {
        BatchPayloadHasher hasher = new BatchPayloadHasher();
        List<Object> cyclic = new ArrayList<>();
        cyclic.add(cyclic);

        assertThrows(IllegalArgumentException.class,
                     () -> hasher.hashRows(List.<Object[]>of(new Object[]{cyclic})));
    }

    private static Parameter emptyParameter(Class<?> javaType) {
        Type type = new Type() {
            @Override
            public Class<?> getJavaType() {
                return javaType;
            }

            @Override
            public String getName() {
                return javaType.getName();
            }
        };
        return new Parameter() {
            @Override
            public Type getType() {
                return type;
            }

            @Override
            public Object getValue() {
                return null;
            }
        };
    }
}
