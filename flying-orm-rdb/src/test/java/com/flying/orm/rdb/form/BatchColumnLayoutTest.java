package com.flying.orm.rdb.form;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 批量字段布局要在计划阶段算好，后续每一行只按下标塞参数。
 *
 * @author wangr
 * @date 2026-07-24
 * @version v1.0
 */
class BatchColumnLayoutTest {

    /**
     * 后续行字段顺序和大小写可以不同，但参数下标必须跟首行布局一致。
     */
    @Test
    void mapsRowValuesByPrecomputedColumnIndexes() {
        BatchColumnLayout layout = BatchColumnLayout.of(form(),
                                                        List.of(form().field("id"),
                                                                form().field("name"),
                                                                form().field("age")),
                                                        "h2",
                                                        false,
                                                        ValueCodecRegistry.standard());

        Object[] parameters = layout.parameters(orderedMap("AGE", 18, "name", "王", "ID", "u1"), 1);

        assertArrayEquals(new Object[]{"u1", "王", 18}, parameters);
    }

    /**
     * 一行里不能用大小写别名重复提交同一个字段。
     */
    @Test
    void rejectsDuplicateNormalizedFieldsInLaterRows() {
        BatchColumnLayout layout = BatchColumnLayout.of(form(),
                                                        List.of(form().field("id"),
                                                                form().field("name")),
                                                        "h2",
                                                        false,
                                                        ValueCodecRegistry.standard());

        assertThrows(IllegalArgumentException.class,
                     () -> layout.parameters(orderedMap("id", "u1", "ID", "u2"), 1));
    }

    /**
     * 验证归一化重复字段的公开异常不回显调用方提供的超长原始键。
     */
    @Test
    void rejectsDuplicateNormalizedFieldsWithoutEchoingRawCallerKey() {
        BatchColumnLayout layout = BatchColumnLayout.of(form(),
                                                        List.of(form().field("id"),
                                                                form().field("name")),
                                                        "h2",
                                                        false,
                                                        ValueCodecRegistry.standard());
        String secretRawFieldName = " ".repeat(512) + "name" + " ".repeat(512);

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> layout.parameters(orderedMap("name", "first", secretRawFieldName, "second"), 1));

        assertEquals("duplicate normalized batch insert field", error.getMessage());
        assertFalse(error.getMessage().contains(secretRawFieldName));
    }

    @Test
    void rejectsUpdateDeltaInEveryLaterBatchRowBeforeBinding() {
        BatchColumnLayout layout = BatchColumnLayout.of(form(),
                                                        List.of(form().field("id"), form().field("age")),
                                                        "h2",
                                                        false,
                                                        ValueCodecRegistry.standard());

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> layout.parameters(orderedMap("id", "u2", "age", UpdateDelta.increment(1)), 2));

        assertEquals("batch write row [2] field [age] does not allow update delta", error.getMessage());
    }

    private static DynamicForm form() {
        return DynamicForm.builder("userForm", "Users")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("name", "VARCHAR"))
                          .addField(DynamicField.of("age", "INTEGER"))
                          .build();
    }

    private static Map<String, Object> orderedMap(Object... pairs) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            values.put((String) pairs[i], pairs[i + 1]);
        }
        return values;
    }
}
