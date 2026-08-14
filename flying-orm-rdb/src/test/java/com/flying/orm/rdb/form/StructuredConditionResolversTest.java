package com.flying.orm.rdb.form;

import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.condition.StructuredConditionErrorCode;
import com.flying.orm.core.condition.StructuredConditionException;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StructuredConditionResolversTest {

    @Test
    void explicitOperatorPresetUsesApplicationValueCodecs() {
        ValueCodecRegistry codecs = ValueCodecRegistry.standard().withFirst(new EnabledBooleanCodec());
        StructuredConditionResolver resolver = StructuredConditionResolvers.allowOperators(codecs, "=");
        DynamicForm form = DynamicForm.builder("users", "Users")
                                      .addField(DynamicField.of("active", "boolean"))
                                      .build();

        TermCondition term = (TermCondition) resolver.compile(
                form,
                StructuredConditionInput.term("active", "=", "enabled"),
                StructuredConditionPolicy.defaults()).children().getFirst();

        assertEquals(true, term.value());
    }

    @Test
    void rejectsDeepInputBeforeRunningRecursiveCustomizer() {
        AtomicBoolean adapted = new AtomicBoolean();
        StructuredConditionResolver resolver = StructuredConditionResolver.composite(
                new StructuredConditionCustomizer() {
                    @Override
                    public StructuredConditionInput adapt(DynamicForm form, StructuredConditionInput input) {
                        adapted.set(true);
                        return input;
                    }
                });
        StructuredConditionInput input = StructuredConditionInput.and(
                StructuredConditionInput.or(StructuredConditionInput.term("active", "=", true)));

        StructuredConditionException error = assertThrows(
                StructuredConditionException.class,
                () -> resolver.compile(DynamicForm.builder("users", "Users")
                                                   .addField(DynamicField.of("active", "boolean"))
                                                   .build(),
                                       input,
                                       StructuredConditionPolicy.defaults().withMaxDepth(1)));

        assertEquals(StructuredConditionErrorCode.DEPTH_EXCEEDED, error.code());
        assertFalse(adapted.get());
    }

    private static final class EnabledBooleanCodec implements ValueCodec {

        @Override
        public boolean supports(Class<?> targetType) {
            return targetType == Boolean.class;
        }

        @Override
        public Object read(Object value, Class<?> targetType) {
            return "enabled".equalsIgnoreCase(value.toString());
        }
    }
}
