package com.flying.orm.rdb.form;

import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class ArrayValueCodecRegistryTest {

    @Test
    void writesEnumArrayElementsByTheirStableNames() {
        Object parameter = renderer(ValueCodecRegistry.standard())
                .insert(form(), Map.of("tags", List.of(State.ACTIVE)))
                .parameters()
                .getFirst();

        assertArrayEquals(new String[]{"ACTIVE"}, (String[]) parameter);
    }

    @Test
    void appliesTheConfiguredValueCodecToArrayElements() {
        ValueCodec codec = new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                return targetType == Code.class;
            }

            @Override
            public Object write(Object value) {
                return "coded:" + ((Code) value).value();
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                return new Code(value.toString());
            }
        };
        ValueCodecRegistry registry = ValueCodecRegistry.standard().withFirst(codec);

        Object parameter = renderer(registry)
                .insert(form(), Map.of("tags", List.of(new Code("a"))))
                .parameters()
                .getFirst();

        assertArrayEquals(new String[]{"coded:a"}, (String[]) parameter);
    }

    private static FormDataSqlRenderer renderer(ValueCodecRegistry registry) {
        SqlRenderer sqlRenderer = SqlRenderer.builder().addDefaultTerms().build().withValueCodecs(registry);
        return FormDataSqlRenderer.create(sqlRenderer, RdbDialect.postgresql());
    }

    private static DynamicForm form() {
        return DynamicForm.builder("events", "events")
                          .addField(DynamicField.of("tags", "TEXT[]"))
                          .build();
    }

    private enum State {
        ACTIVE;

        @Override
        public String toString() {
            return "display-active";
        }
    }

    private record Code(String value) {
        @Override
        public String toString() {
            return "display-" + value;
        }
    }
}
