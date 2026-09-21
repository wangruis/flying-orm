package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.cache.OrmCachePolicy;
import com.flying.orm.rdb.dialect.DialectFeature;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.internal.plan.StructuralPlanCaches;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class FormConditionValueOwnershipTest {

    @Test
    void reusesUnchangedMutableScalarTerm() {
        DynamicForm form = form();
        ConditionGroup where = ConditionGroup.and().where("payload", "=", new byte[]{1, 2, 3}).build();

        assertSame(where, support().normalizeCondition(form, where));
    }

    @Test
    void reusesUnchangedMutableCollectionTerm() {
        DynamicForm form = form();
        ConditionGroup where = ConditionGroup.and()
                                             .where("payload", "in", List.of(
                                                     new byte[]{1, 2}, new byte[]{3, 4}))
                                             .build();

        assertSame(where, support().normalizeCondition(form, where));
    }

    @Test
    void repeatedNormalizationReadsOwnedInAndBetweenValuesWithoutCopyingThemAgain() {
        AtomicInteger copies = new AtomicInteger();
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("included_at", "DATE"))
                                      .addField(DynamicField.of("created_at", "DATE"))
                                      .build();
        ConditionGroup where = ConditionGroup.and()
                                             .where("included_at", "in", List.of(
                                                     new CountingDate(1L, copies),
                                                     new CountingDate(2L, copies)))
                                             .where("created_at", "between", List.of(
                                                     new CountingDate(3L, copies),
                                                     new CountingDate(4L, copies)))
                                             .build();
        copies.set(0);
        FormSqlRenderSupport support = support();

        assertSame(where, support.normalizeCondition(form, where));
        assertSame(where, support.normalizeCondition(form, where));
        assertEquals(0, copies.get(),
                     "trusted normalization must reuse the construction-time mutable snapshots");
    }

    private static DynamicForm form() {
        return DynamicForm.builder("events", "events")
                          .addField(DynamicField.of("payload", "OTHER"))
                          .build();
    }

    private static FormSqlRenderSupport support() {
        RdbDialect dialect = RdbDialect.postgresql();
        UnaryOperator<String> identifiers = dialect.schema()::identifier;
        SqlRenderer conditions = SqlRenderer.builder()
                                            .addDefaultTerms()
                                            .build()
                                            .withIdentifierRenderer(identifiers);
        return new FormSqlRenderSupport(
                conditions,
                dialect.json(),
                dialect.name(),
                dialect.supports(DialectFeature.NATIVE_BOOLEAN),
                identifiers,
                StructuralPlanCaches.create(OrmCachePolicy.safeDefaults()));
    }

    private static final class CountingDate extends Date {

        private final AtomicInteger copies;

        private CountingDate(long time, AtomicInteger copies) {
            super(time);
            this.copies = copies;
        }

        @Override
        public Object clone() {
            copies.incrementAndGet();
            return new CountingDate(getTime(), copies);
        }
    }
}
