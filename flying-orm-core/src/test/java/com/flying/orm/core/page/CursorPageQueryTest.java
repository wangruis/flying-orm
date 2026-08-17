package com.flying.orm.core.page;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CursorPageQueryTest {

    @Test
    void keepsCompositeCursorInStableSortOrder() {
        CursorPageQuery page = CursorPageQuery.after(
                50,
                List.of(100L, "u-9"),
                CursorSort.desc("createdAt"),
                CursorSort.asc("id"));

        assertEquals(50, page.size());
        assertEquals(List.of(100L, "u-9"), page.cursor());
        assertEquals(List.of("createdAt", "id"), page.sorts().stream().map(CursorSort::field).toList());
    }

    @Test
    void defersCursorCountValidationUntilEntitySortNormalization() {
        CursorPageQuery page = CursorPageQuery.after(
                20, List.of(1L, "u-1"), CursorSort.asc("createdAt"));

        assertEquals(2, page.cursor().size());
        assertEquals(1, page.sorts().size());
    }

    @Test
    void rejectsNullCursorValues() {
        assertThrows(NullPointerException.class,
                     () -> CursorPageQuery.after(20, Arrays.asList(1L, null),
                                                 CursorSort.asc("createdAt"), CursorSort.asc("id")));
    }

    /** 二进制排序游标在请求创建和读取时都不能泄漏可变数组。 */
    @Test
    void snapshotsArrayCursorValuesAtTheQueryBoundary() {
        byte[] binaryKey = {1, 2};
        CursorPageQuery page = CursorPageQuery.after(20, List.of(binaryKey), CursorSort.asc("binaryKey"));

        binaryKey[0] = 9;
        assertArrayEquals(new byte[]{1, 2}, (byte[]) page.cursor().getFirst());

        ((byte[]) page.cursor().getFirst())[1] = 8;
        assertArrayEquals(new byte[]{1, 2}, (byte[]) page.cursor().getFirst());
    }

    /** 游标快照在列表范围内保持共享数组身份，并隔离嵌套数组和自环图。 */
    @Test
    void snapshotsNestedCursorArrayGraphAtBothBoundaries() {
        Object marker = new Object();
        byte[] shared = {1, 2};
        Object[] cycle = new Object[1];
        cycle[0] = cycle;
        Object[] root = {shared, shared, cycle, marker};
        CursorPageQuery page = CursorPageQuery.after(
                20, List.of(root, root, marker), CursorSort.asc("binaryKey"));

        shared[0] = 9;
        cycle[0] = new Object();
        root[3] = new Object();
        List<Object> first = page.cursor();
        Object[] firstRoot = (Object[]) first.getFirst();

        assertSame(first.get(0), first.get(1));
        assertSame(firstRoot[0], firstRoot[1]);
        assertArrayEquals(new byte[]{1, 2}, (byte[]) firstRoot[0]);
        assertSame(firstRoot[2], ((Object[]) firstRoot[2])[0]);
        assertSame(marker, firstRoot[3]);
        assertSame(marker, first.get(2));

        ((byte[]) firstRoot[0])[0] = 8;
        ((Object[]) firstRoot[2])[0] = null;
        firstRoot[3] = null;
        List<Object> second = page.cursor();
        Object[] secondRoot = (Object[]) second.getFirst();

        assertNotSame(firstRoot, secondRoot);
        assertNotSame(firstRoot[0], secondRoot[0]);
        assertSame(second.get(0), second.get(1));
        assertSame(secondRoot[0], secondRoot[1]);
        assertArrayEquals(new byte[]{1, 2}, (byte[]) secondRoot[0]);
        assertSame(secondRoot[2], ((Object[]) secondRoot[2])[0]);
        assertSame(marker, secondRoot[3]);
        assertSame(marker, second.get(2));
    }

    /** ByteBuffer 游标必须冻结当前可读值，每次访问返回独立只读视图。 */
    @Test
    void snapshotsByteBufferCursorValuesAtBothBoundaries() {
        ByteBuffer source = ByteBuffer.wrap(new byte[]{1, 2});
        CursorPageQuery page = CursorPageQuery.after(20, List.of(source), CursorSort.asc("binaryKey"));

        source.put(0, (byte) 9);
        ByteBuffer first = (ByteBuffer) page.cursor().getFirst();
        assertEquals(1, first.get(0));
        assertThrows(ReadOnlyBufferException.class, () -> first.put(0, (byte) 8));

        first.position(1);
        ByteBuffer second = (ByteBuffer) page.cursor().getFirst();
        assertEquals(0, second.position());
        assertEquals(1, second.get(0));
    }
}
