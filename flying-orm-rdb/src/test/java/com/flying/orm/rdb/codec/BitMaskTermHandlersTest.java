package com.flying.orm.rdb.codec;

import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlTermPackage;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BitMaskTermHandlersTest {
    @Test
    void rejectsFractionalOverflowAndSqlTextRatherThanTruncatingOrInlining() {
        SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms()
                .addTermPackage(BitMaskTermHandlers.postgresql()).build();
        DynamicForm form = DynamicForm.builder("flags", "flags")
                .addField(DynamicField.of("flags", "BIGINT")).build();
        for (Object value : List.of(1.5, Double.NaN, "1 or true", java.math.BigInteger.ONE.shiftLeft(64))) {
            assertThrows(IllegalArgumentException.class, () -> renderer.renderWhere(
                    BitMaskTermHandlers.conditions().compile(form, StructuredConditionInput.term("flags", "mask-any", value),
                            StructuredConditionPolicy.defaults())));
        }
    }

    @Test
    void eachDialectBindsSignedMasksAndKeepsAnyAllAndSubsetDistinct() {
        DynamicForm form = DynamicForm.builder("flags", "flags")
                .addField(DynamicField.of("flags", "BIGINT")).build();
        for (SqlTermPackage terms : List.of(BitMaskTermHandlers.postgresql(), BitMaskTermHandlers.mysql(),
                BitMaskTermHandlers.sqlServer(), BitMaskTermHandlers.oracle())) {
            SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().addTermPackage(terms).build();
            String and = switch (terms.name()) {
                case "bit-mask-mysql" -> "cast((flags & ?) as signed)";
                case "bit-mask-oracle" -> "bitand(flags, ?)";
                default -> "(flags & ?)";
            };
            for (long mask : new long[]{0L, 9L, Long.MIN_VALUE}) {
                for (String kind : List.of("any", "all", "contained-by")) {
                    for (boolean not : List.of(false, true)) {
                        String operator = "mask-" + (not ? "not-" : "") + kind;
                        SqlFragment sql = renderer.renderWhere(BitMaskTermHandlers.conditions().compile(form,
                                StructuredConditionInput.term("flags", operator, mask), StructuredConditionPolicy.defaults()));
                        String comparison = kind.equals("any") ? not ? " = " : " <> " : not ? " <> " : " = ";
                        String right = kind.equals("any") ? "0" : kind.equals("all") ? "?" : "flags";
                        assertEquals("(" + and + comparison + right + ")", sql.sql());
                        assertEquals(kind.equals("all") ? List.of(mask, mask) : List.of(mask), sql.parameters());
                    }
                }
            }
        }
    }
}
