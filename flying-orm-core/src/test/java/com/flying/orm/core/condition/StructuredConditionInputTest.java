package com.flying.orm.core.condition;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证前端结构化条件发布后不再保留调用方可变容器和值。
 *
 * @author wangr
 * @date 2026-08-16
 * @version v2.0
 */
class StructuredConditionInputTest {

    /** Map、List、数组和可变文本在构造及访问两端都必须隔离，同时保留值图共享关系。 */
    @Test
    void snapshotsMutableStructuredValueGraphAtBothBoundaries() {
        StringBuilder text = new StringBuilder("Alice");
        StringBuilder[] typedText = {new StringBuilder("stable")};
        byte[] binary = {1, 2};
        List<Object> values = new ArrayList<>(List.of(text, binary));
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("values", values);
        source.put("alias", values);
        source.put("typedText", typedText);
        StructuredConditionInput input = StructuredConditionInput.term("profile", "json-contains", source);

        text.replace(0, text.length(), "Mallory");
        binary[0] = 9;
        typedText[0].replace(0, typedText[0].length(), "changed");
        values.add("unexpected");
        source.put("extra", true);

        Map<?, ?> first = (Map<?, ?>) input.value();
        List<?> firstValues = (List<?>) first.get("values");
        assertSame(firstValues, first.get("alias"));
        assertEquals("Alice", firstValues.getFirst());
        assertArrayEquals(new byte[]{1, 2}, (byte[]) firstValues.get(1));
        assertEquals("stable", ((StringBuilder[]) first.get("typedText"))[0].toString());
        assertThrows(UnsupportedOperationException.class, first::clear);
        assertThrows(UnsupportedOperationException.class, firstValues::clear);

        ((byte[]) firstValues.get(1))[1] = 8;
        Map<?, ?> second = (Map<?, ?>) input.value();
        List<?> secondValues = (List<?>) second.get("values");
        assertNotSame(first, second);
        assertNotSame(firstValues, secondValues);
        assertArrayEquals(new byte[]{1, 2}, (byte[]) secondValues.get(1));
    }

    /** 结构化扩展仍可携带可信的非容器标量，快照不能臆测并改写其业务类型。 */
    @Test
    void preservesOpaqueStructuredScalarIdentity() {
        Object opaque = new Object();
        StructuredConditionInput input = StructuredConditionInput.term("profile", "custom", opaque);

        assertSame(opaque, input.value());
    }

    /** DTO 边界必须在复制前拒绝永远不可能通过编译器硬上限的无界值图。 */
    @Test
    void rejectsStructuredValueGraphBeyondHardReferenceLimit() {
        List<Object> excessive = new ArrayList<>();
        for (int index = 0; index <= 20_000; index++) {
            excessive.add(null);
        }

        assertThrows(StructuredConditionException.class,
                     () -> StructuredConditionInput.term("profile", "json-contains", excessive));
    }
}
