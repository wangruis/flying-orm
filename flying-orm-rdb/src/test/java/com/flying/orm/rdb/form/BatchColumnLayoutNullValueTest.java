package com.flying.orm.rdb.form;

import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BatchColumnLayoutNullValueTest {
    @Test
    void nullAndReorderedFieldsAreBoundToTheirOriginalColumns() {
        BatchColumnLayout layout = layout(ValueCodecRegistry.standard());
        Object[] target = {"previous", "previous", "scope"};
        layout.writeParameters(row("NAME", null, "ID", 2L), 1, target);
        assertArrayEquals(new Object[]{2L, null, "scope"}, target);
        layout.writeParameters(row("ID", null, "NAME", "Ada"), 2, target);
        assertArrayEquals(new Object[]{null, "Ada", "scope"}, target);
    }

    @Test
    void nullStillCountsAsAnAlreadyBoundColumn() {
        BatchColumnLayout layout = layout(ValueCodecRegistry.standard());
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> layout.parameters(row("name", null, "NAME", "duplicate"), 1));
        assertEquals("duplicate normalized batch insert field", failure.getMessage());
    }

    @Test
    void codecReturningNullStillCountsAsAnAlreadyBoundColumn() {
        AtomicInteger writes = new AtomicInteger();
        ValueCodec codec = new ValueCodec() {
            @Override
            public boolean supports(Class<?> type) { return type == String.class; }

            @Override
            public Object write(Object value) { writes.incrementAndGet(); return null; }

            @Override
            public Object read(Object value, Class<?> type) { return value; }
        };
        BatchColumnLayout layout = layout(ValueCodecRegistry.standard().withFirst(codec));
        writes.set(0);
        assertArrayEquals(new Object[]{2L, null}, layout.parameters(row("NAME", "Ada", "ID", 2L), 1));
        assertEquals(1, writes.get());
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> layout.parameters(row("name", "one", "NAME", "two"), 2));
        assertEquals("duplicate normalized batch insert field", failure.getMessage());
        assertEquals(2, writes.get(), "the duplicate must be rejected before its codec runs");
    }

    private static BatchColumnLayout layout(ValueCodecRegistry codecs) {
        DynamicForm form = DynamicForm.builder("items", "items")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("name", "VARCHAR")).build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().valueCodecs(codecs).build(), RdbDialect.postgresql());
        return renderer.batchRenderer.insertPlan(form, row("id", 1L, "name", "first")).columnLayout();
    }

    private static Map<String, Object> row(String first, Object firstValue, String second, Object secondValue) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put(first, firstValue);
        row.put(second, secondValue);
        return row;
    }
}
