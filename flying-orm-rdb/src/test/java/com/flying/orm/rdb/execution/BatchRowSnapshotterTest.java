package com.flying.orm.rdb.execution;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchMemoryLimitExceededException;
import com.flying.orm.rdb.codec.SqlTypedValue;
import io.r2dbc.spi.Parameter;
import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.Type;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证批量 onNext 所有权边界完整冻结可变参数图和内部受保护写元数据。
 *
 * @author wangr
 * @date 2026-08-13
 * @version v1.0
 */
class BatchRowSnapshotterTest {

    /** byte[]、ByteBuffer、文本外壳和标准 Parameter 都应冻结，并保留重复引用关系。 */
    @Test
    void snapshotsMutablePayloadsAndPreservesAliasesAndNullElements() {
        byte[] bytes = new byte[]{1, 2};
        ByteBuffer root = ByteBuffer.allocate(64).order(ByteOrder.LITTLE_ENDIAN);
        root.position(62).put((byte) 3).put((byte) 4).flip().position(62);
        ByteBuffer view = root.slice().order(ByteOrder.LITTLE_ENDIAN);
        StringBuilder text = new StringBuilder("secret");
        List<Object> list = new ArrayList<>(Arrays.asList(bytes, null, text));
        Map<Object, Object> map = new LinkedHashMap<>();
        map.put("payload", bytes);
        map.put(null, null);
        Parameter parameter = Parameters.in(bytes);
        SqlTypedValue typed = new SqlTypedValue(SqlTypedValue.Kind.CLOB, text);

        Object[] result = BatchRowSnapshotter.snapshot(
                new Object[]{bytes, view, text, list, map, parameter, typed});
        bytes[0] = 9;
        root.put(62, (byte) 9);
        text.setLength(0);
        list.clear();
        map.clear();

        byte[] ownedBytes = assertInstanceOf(byte[].class, result[0]);
        ByteBuffer ownedBuffer = assertInstanceOf(ByteBuffer.class, result[1]);
        assertNotSame(bytes, ownedBytes);
        assertArrayEquals(new byte[]{1, 2}, ownedBytes);
        assertTrue(ownedBuffer.isReadOnly());
        assertEquals(2, ownedBuffer.capacity());
        assertEquals(ByteOrder.LITTLE_ENDIAN, ownedBuffer.order());
        assertEquals(3, ownedBuffer.get(0));
        assertEquals("secret", result[2]);
        List<?> ownedList = assertInstanceOf(List.class, result[3]);
        Map<?, ?> ownedMap = assertInstanceOf(Map.class, result[4]);
        assertSame(ownedBytes, ownedList.get(0));
        assertEquals(null, ownedList.get(1));
        assertSame(result[2], ownedList.get(2));
        assertSame(ownedBytes, ownedMap.get("payload"));
        assertTrue(ownedMap.containsKey(null));
        assertSame(ownedBytes, assertInstanceOf(Parameter.class, result[5]).getValue());
        assertSame(result[2], assertInstanceOf(SqlTypedValue.class, result[6]).value());
    }

    /** JDBC 旧时间类型可变，批量接收后必须与上游对象彻底分离并保留 Timestamp 纳秒。 */
    @Test
    void snapshotsMutableJdbcTemporalValues() {
        java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf("2026-08-16 12:34:56.123456789");
        java.sql.Date sqlDate = java.sql.Date.valueOf("2026-08-16");
        java.sql.Time sqlTime = java.sql.Time.valueOf("12:34:56");
        java.util.Date utilDate = new java.util.Date(1_755_325_696_789L);
        long timestampMillis = timestamp.getTime();
        int timestampNanos = timestamp.getNanos();
        long sqlDateMillis = sqlDate.getTime();
        long sqlTimeMillis = sqlTime.getTime();
        long utilDateMillis = utilDate.getTime();

        Object[] result = BatchRowSnapshotter.snapshot(new Object[]{timestamp, sqlDate, sqlTime, utilDate});
        timestamp.setTime(0L);
        timestamp.setNanos(1);
        sqlDate.setTime(0L);
        sqlTime.setTime(0L);
        utilDate.setTime(0L);

        java.sql.Timestamp ownedTimestamp = assertInstanceOf(java.sql.Timestamp.class, result[0]);
        java.sql.Date ownedDate = assertInstanceOf(java.sql.Date.class, result[1]);
        java.sql.Time ownedTime = assertInstanceOf(java.sql.Time.class, result[2]);
        java.util.Date ownedUtilDate = assertInstanceOf(java.util.Date.class, result[3]);
        assertNotSame(timestamp, ownedTimestamp);
        assertNotSame(sqlDate, ownedDate);
        assertNotSame(sqlTime, ownedTime);
        assertNotSame(utilDate, ownedUtilDate);
        assertEquals(timestampMillis, ownedTimestamp.getTime());
        assertEquals(timestampNanos, ownedTimestamp.getNanos());
        assertEquals(sqlDateMillis, ownedDate.getTime());
        assertEquals(sqlTimeMillis, ownedTime.getTime());
        assertEquals(utilDateMillis, ownedUtilDate.getTime());
    }

    /** ProtectedWriteWork、SqlRequest 和尾槽 Metadata 中的可变值必须一起重建。 */
    @Test
    void rebuildsProtectedWorkRequestsAndReceiptMetadata() {
        byte[] payload = new byte[]{7};
        StringBuilder owner = new StringBuilder("tenant-a");
        SqlRequest write = new SqlRequest("update T set payload = ? where owner = ?", List.of(payload, owner));
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.UPDATE, write,
                new SqlRequest("select id from T where owner = ?", List.of(owner)),
                List.of("id"), Map.of("id", payload), "id = ?",
                "delete from T_token where id = ? and field_tag = ?",
                "insert into T_token(id, field_tag, token_hash) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("payload", List.of(new byte[]{1}))));
        Object[] row = ProtectedBatchRows.extend(
                new Object[]{payload, owner}, work, new Object[]{payload, owner});

        Object[] result = BatchRowSnapshotter.snapshot(row, 2, 8_192L, "buffered bytes");
        payload[0] = 9;
        owner.setLength(0);

        ProtectedWriteWork owned = ProtectedBatchRows.work(result, 2);
        Object[] receipt = ProtectedBatchRows.receiptParameters(result, 2);
        assertNotSame(work, owned);
        assertArrayEquals(new byte[]{7}, assertInstanceOf(byte[].class, result[0]));
        assertEquals("tenant-a", result[1]);
        assertArrayEquals(new byte[]{7}, assertInstanceOf(byte[].class, owned.writeRequest().parameters().get(0)));
        assertEquals("tenant-a", owned.writeRequest().parameters().get(1));
        assertEquals("tenant-a", owned.ownerQuery().parameters().getFirst());
        assertArrayEquals(new byte[]{7}, assertInstanceOf(byte[].class, owned.knownOwner().get("id")));
        assertArrayEquals(new byte[]{7}, assertInstanceOf(byte[].class, receipt[0]));
        assertEquals("tenant-a", receipt[1]);
    }

    /** 循环、超过 64 层以及需要改写 payload 的自定义 Parameter 都失败闭合。 */
    @Test
    void rejectsUnsafeGraphsAndCustomMutableParameters() {
        List<Object> cycle = new ArrayList<>();
        cycle.add(cycle);
        Object deep = "leaf";
        for (int index = 0; index < 66; index++) deep = List.of(deep);
        Object tooDeep = deep;

        assertThrows(IllegalArgumentException.class,
                     () -> BatchRowSnapshotter.snapshot(new Object[]{cycle}));
        assertThrows(IllegalArgumentException.class,
                     () -> BatchRowSnapshotter.snapshot(new Object[]{tooDeep}));
        assertThrows(IllegalArgumentException.class,
                     () -> BatchRowSnapshotter.snapshot(new Object[]{customParameter(new byte[]{1})}));
    }

    /** 自定义 Parameter 即使首次返回不可变值也不能跨所有权边界保留其可变实现。 */
    @Test
    void rejectsCustomParameterBeforeReadingItsStatefulPayload() {
        AtomicInteger reads = new AtomicInteger();
        Parameter parameter = changingParameter(reads);

        assertThrows(IllegalArgumentException.class,
                     () -> BatchRowSnapshotter.snapshot(new Object[]{parameter}));
        assertEquals(0, reads.get());
    }

    /** 预算拒绝发生在非 String 文本物化之前，不能先创建超大 String 再报告超限。 */
    @Test
    void rejectsOversizedPayloadBeforeMaterializingIt() {
        AtomicInteger materializations = new AtomicInteger();
        CharSequence payload = new SizedText(1_024, materializations);

        assertThrows(BatchMemoryLimitExceededException.class,
                     () -> BatchRowSnapshotter.snapshot(
                             new Object[]{payload}, 1, 128L, "buffered bytes"));
        assertEquals(0, materializations.get());
    }

    /** 非 String 文本按 CharSequence 契约逐字符冻结，不能信任可返回任意大对象的自定义 toString。 */
    @Test
    void snapshotsCustomTextWithoutCallingItsUntrustedToString() {
        AtomicInteger materializations = new AtomicInteger();
        CharSequence payload = new SizedText(3, materializations);

        Object[] result = BatchRowSnapshotter.snapshot(new Object[]{payload}, 1, 128L, "buffered bytes");

        assertEquals("xxx", result[0]);
        assertEquals(0, materializations.get());
    }

    /** 文本只能按取得所有权时首次观察到的长度冻结，不能在第二次读取时增长。 */
    @Test
    void freezesTextFromOneLengthObservation() {
        CharSequence payload = new GrowingText(1, 1_024);

        Object[] result = BatchRowSnapshotter.snapshot(
                new Object[]{payload}, 1, 128L, "buffered bytes");

        assertEquals("x", result[0]);
    }

    /** 列表元素只能读取一次，避免第二次读取保留未计入预算的可变值。 */
    @Test
    void freezesListElementsFromOneTraversal() {
        ChangingList payload = new ChangingList("safe", new byte[1_024]);

        Object[] result = BatchRowSnapshotter.snapshot(
                new Object[]{payload}, 1, 128L, "buffered bytes");

        assertEquals("safe", assertInstanceOf(List.class, result[0]).getFirst());
        assertEquals(1, payload.reads());
    }

    /** Map 条目也只能遍历一次，不能在第二遍引入未计费的可变值。 */
    @Test
    void freezesMapEntriesFromOneTraversal() {
        ChangingMap payload = new ChangingMap("safe", new byte[1_024]);

        Object[] result = BatchRowSnapshotter.snapshot(
                new Object[]{payload}, 1, 128L, "buffered bytes");

        assertEquals("safe", assertInstanceOf(Map.class, result[0]).get("payload"));
        assertEquals(1, payload.reads());
    }

    private static Parameter customParameter(Object value) {
        Type type = new Type() {
            @Override public Class<?> getJavaType() { return value.getClass(); }
            @Override public String getName() { return value.getClass().getName(); }
        };
        return new Parameter() {
            @Override public Type getType() { return type; }
            @Override public Object getValue() { return value; }
        };
    }

    private static Parameter changingParameter(AtomicInteger reads) {
        Type type = new Type() {
            @Override public Class<?> getJavaType() { return String.class; }
            @Override public String getName() { return String.class.getName(); }
        };
        return new Parameter() {
            @Override public Type getType() { return type; }
            @Override public Object getValue() { return reads.getAndIncrement() == 0 ? "safe" : "changed"; }
        };
    }

    /** 只在真正物化时计数的定长测试文本。 */
    private record SizedText(int length, AtomicInteger materializations) implements CharSequence {
        @Override public char charAt(int index) { return 'x'; }
        @Override public CharSequence subSequence(int start, int end) { return this; }
        @Override public String toString() {
            materializations.incrementAndGet();
            return "x".repeat(length);
        }
    }

    /** 每次读取长度都切换到下一阶段的测试文本。 */
    private static final class GrowingText implements CharSequence {
        private final int inspectedLength;
        private final int copiedLength;
        private int reads;

        private GrowingText(int inspectedLength, int copiedLength) {
            this.inspectedLength = inspectedLength;
            this.copiedLength = copiedLength;
        }

        @Override public int length() { return reads++ == 0 ? inspectedLength : copiedLength; }
        @Override public char charAt(int index) { return 'x'; }
        @Override public CharSequence subSequence(int start, int end) { return this; }
    }

    /** 预检和复制时返回不同元素的测试列表。 */
    private static final class ChangingList extends AbstractList<Object> {
        private final Object inspectedValue;
        private final Object copiedValue;
        private int reads;

        private ChangingList(Object inspectedValue, Object copiedValue) {
            this.inspectedValue = inspectedValue;
            this.copiedValue = copiedValue;
        }

        @Override public Object get(int index) { return reads++ == 0 ? inspectedValue : copiedValue; }
        @Override public int size() { return 1; }
        private int reads() { return reads; }
    }

    /** 每次遍历返回不同值的测试 Map。 */
    private static final class ChangingMap extends AbstractMap<Object, Object> {
        private final Object inspectedValue;
        private final Object copiedValue;
        private int reads;

        private ChangingMap(Object inspectedValue, Object copiedValue) {
            this.inspectedValue = inspectedValue;
            this.copiedValue = copiedValue;
        }

        @Override
        public Set<Entry<Object, Object>> entrySet() {
            Object value = reads++ == 0 ? inspectedValue : copiedValue;
            return Set.of(new SimpleImmutableEntry<>("payload", value));
        }

        @Override public int size() { return 1; }
        private int reads() { return reads; }
    }
}
