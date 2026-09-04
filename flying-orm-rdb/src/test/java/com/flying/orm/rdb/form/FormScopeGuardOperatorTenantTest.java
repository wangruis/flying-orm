package com.flying.orm.rdb.form;

import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.scope.ScopeErrorCode;
import com.flying.orm.core.scope.TenantScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FormScopeGuardOperatorTenantTest {

    @Test
    void mergesEquivalentTextTenantCarriersWithoutChangingPublishedValues() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        FormScopeGuard guard = new FormScopeGuard(
                renderer,
                StructuredConditionResolver.defaults(),
                DataScope.tenant("tenant_id", "a"));

        TenantScope characters = guard.effectiveScope(
                DataScope.tenant("tenant_id", new char[]{'a'}))
                .tenantScope("tenant_id")
                .orElseThrow();
        TenantScope character = guard.effectiveScope(
                DataScope.tenant("tenant_id", 'a'))
                .tenantScope("tenant_id")
                .orElseThrow();

        assertArrayEquals(
                new char[]{'a'},
                assertInstanceOf(char[].class, characters.value()));
        assertEquals(
                Character.valueOf('a'),
                assertInstanceOf(Character.class, character.value()));
        assertThrows(
                IllegalArgumentException.class,
                () -> guard.effectiveScope(DataScope.tenant("tenant_id", "b"))
                        .tenantScope("tenant_id"));
    }

    @Test
    void defaultCompilerIsAlreadyTheValidatedSingleTraversalBoundary() {
        StructuredConditionResolver resolver = StructuredConditionResolver.defaults();

        assertSame(resolver, StructuredConditionResolvers.validating(resolver));
    }

    @Test
    void resolvesStructuredConditionsThroughOneValidationAndNormalizationPass() {
        AtomicInteger validations = new AtomicInteger();
        AtomicInteger adaptations = new AtomicInteger();
        AtomicInteger normalizations = new AtomicInteger();
        ValueCodecRegistry codecs = ValueCodecRegistry.standard().withFirst(new CountingMarkerCodec(normalizations));
        StructuredConditionResolver resolver = StructuredConditionResolver.composite(
                codecs, new CountingCustomizer(validations, adaptations));
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("marker", "OTHER"))
                                      .build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        FormScopeGuard guard = new FormScopeGuard(renderer, resolver, DataScope.none());

        guard.scopedStructuredRead(form,
                                   StructuredConditionInput.term("marker", "=", new Marker("one")),
                                   StructuredConditionPolicy.defaults(),
                                   DataScope.none());

        assertEquals(1, validations.get());
        assertEquals(1, adaptations.get());
        assertEquals(1, normalizations.get());
    }

    @Test
    void rejectsTenantReassignmentForMetadataLightOperatorForms() {
        DynamicForm operatorForm = DynamicForm.builder("orders", "orders")
                                              .addField(DynamicField.of("tenant_id", "OTHER"))
                                              .addField(DynamicField.of("id", "OTHER"))
                                              .build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        FormScopeGuard guard = new FormScopeGuard(
                renderer, StructuredConditionResolver.defaults(), DataScope.tenant("tenant_id", 1));
        ConditionGroup where = ConditionGroup.and().where("id", "=", 7).build();

        ScopeAccessException failure = assertThrows(
                ScopeAccessException.class,
                () -> guard.writableActiveWhere(
                        operatorForm, Map.of("tenant_id", 2), where, DataScope.none()));

        assertEquals(ScopeErrorCode.TENANT_VALUE_MISMATCH, failure.code());
        assertEquals("tenant_id", failure.field());
    }

    @Test
    void validatesAnExplicitTenantUpdateInOneValuesTraversal() {
        DynamicForm form = DynamicForm.builder("orders", "orders")
                                      .addField(DynamicField.of("tenant_id", "OTHER"))
                                      .addField(DynamicField.of("id", "OTHER"))
                                      .tenant("tenant_id", com.flying.orm.core.form.TenantStrategy.MANUAL)
                                      .build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        FormScopeGuard guard = new FormScopeGuard(
                renderer, StructuredConditionResolver.defaults(), DataScope.none());
        AtomicInteger traversals = new AtomicInteger();
        Map<String, Object> values = countingMap(Map.of("tenant_id", 1), traversals);

        guard.writableActiveWhere(form,
                                  values,
                                  ConditionGroup.and().where("id", "=", 7).build(),
                                  DataScope.tenant("tenant_id", 1));

        assertEquals(1, traversals.get());
    }

    private static <K, V> Map<K, V> countingMap(Map<K, V> delegate, AtomicInteger traversals) {
        return new AbstractMap<>() {
            @Override
            public Set<Entry<K, V>> entrySet() {
                Set<Entry<K, V>> entries = delegate.entrySet();
                return new AbstractSet<>() {
                    @Override
                    public Iterator<Entry<K, V>> iterator() {
                        traversals.incrementAndGet();
                        return entries.iterator();
                    }

                    @Override
                    public int size() {
                        return entries.size();
                    }
                };
            }
        };
    }

    private record Marker(String value) {
    }

    private record CountingMarkerCodec(AtomicInteger normalizations) implements ValueCodec {

        @Override
        public boolean supports(Class<?> targetType) {
            return targetType == Marker.class;
        }

        @Override
        public Object write(Object value) {
            normalizations.incrementAndGet();
            return value;
        }

        @Override
        public Object read(Object value, Class<?> targetType) {
            return value;
        }
    }

    private record CountingCustomizer(AtomicInteger validations,
                                      AtomicInteger adaptations) implements StructuredConditionCustomizer {

        @Override
        public void validate(DynamicForm form,
                             StructuredConditionInput input,
                             StructuredConditionPolicy policy) {
            validations.incrementAndGet();
        }

        @Override
        public StructuredConditionInput adapt(DynamicForm form, StructuredConditionInput input) {
            adaptations.incrementAndGet();
            return input;
        }
    }
}
