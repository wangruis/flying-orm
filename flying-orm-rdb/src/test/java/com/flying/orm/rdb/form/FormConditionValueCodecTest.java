package com.flying.orm.rdb.form;

import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FormConditionValueCodecTest {

    @Test
    void encodesWriteAndConditionValuesExactlyOnce() {
        ValueCodecRegistry codecs = ValueCodecRegistry.standard().withFirst(new PrefixCodec());
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build().withValueCodecs(codecs),
                RdbDialect.postgresql());
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("code", "VARCHAR"))
                                      .build();

        assertEquals("encoded:a", codecs.write("a"));
        assertEquals(List.of("encoded:a"), renderer.insert(form, Map.of("code", "a")).parameters());
        assertEquals(List.of("encoded:a"), renderer.select(
                form, ConditionGroup.and().where("code", "=", "a").build()).parameters());
        assertEquals(List.of("encoded:a", "encoded:b"), renderer.select(
                form, ConditionGroup.and().where("code", "in", List.of("a", "b")).build()).parameters());
        assertEquals(List.of("encoded:a"), renderer.select(
                form, ConditionGroup.and().where("code", "=", "a").build()).parameters());
    }

    @Test
    void doesNotReencodeFieldAwareBooleanConditions() {
        ValueCodecRegistry codecs = ValueCodecRegistry.standard().withFirst(new InvertingBooleanCodec());
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build().withValueCodecs(codecs),
                RdbDialect.postgresql());
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("enabled", "BOOLEAN"))
                                      .build();

        assertEquals(List.of(Boolean.FALSE), renderer.insert(form, Map.of("enabled", true)).parameters());
        assertEquals(List.of(Boolean.FALSE), renderer.select(
                     form, ConditionGroup.and().where("enabled", "=", true).build()).parameters());
    }

    @Test
    void rejectsCustomHandlersUsingAStandardOperatorId() {
        assertThrows(IllegalArgumentException.class,
                     () -> SqlRenderer.builder()
                                      .addTerm(SqlTermHandler.of("=", (term, context) -> SqlFragment.of("")))
                                      .build());
    }

    private static final class PrefixCodec implements ValueCodec {

        @Override
        public boolean supports(Class<?> targetType) {
            return targetType == String.class;
        }

        @Override
        public Object write(Object value) {
            return "encoded:" + value;
        }

        @Override
        public Object read(Object value, Class<?> targetType) {
            return value.toString();
        }
    }

    private static final class InvertingBooleanCodec implements ValueCodec {

        @Override
        public boolean supports(Class<?> targetType) {
            return targetType == Boolean.class || targetType == boolean.class;
        }

        @Override
        public Object write(Object value) {
            return !((Boolean) value);
        }

        @Override
        public Object read(Object value, Class<?> targetType) {
            return value;
        }
    }
}
