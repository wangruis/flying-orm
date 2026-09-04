package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionValueShape;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.condition.TermExtensionDescriptor;
import com.flying.orm.core.condition.TermHandler;
import com.flying.orm.core.condition.TermRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.rdb.aggregate.AggregateExpression;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.dialect.DialectCapabilityId;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TermExtensionFieldUseFailClosedTest {

    @Test
    void governedQueryRejectsLegacyCustomTermBeforeExecution() {
        AtomicInteger executions = new AtomicInteger();
        ReactiveFormClient client = client(
                SqlTermHandler.of("trusted-only", (term, context) -> new SqlFragment(
                        context.identifier(term.field()) + " = ?", List.of(context.parameter(term.value())))),
                RdbDialect.h2(),
                executions);

        assertThrows(IllegalArgumentException.class,
                     () -> client.select(query("trusted-only")).collectList().block());
        assertEquals(0, executions.get());
    }

    @Test
    void governedQueryRejectsMissingDialectCapabilityBeforeExecution() {
        AtomicInteger executions = new AtomicInteger();
        TermExtensionDescriptor descriptor = TermExtensionDescriptor.filter(
                "needs-vector", Set.of(DialectCapabilityId.POSTGRESQL_VECTOR.value()), 1, 1);
        ReactiveFormClient client = client(
                SqlTermHandler.of(descriptor, ConditionValueShape.SCALAR,
                                  (term, context) -> new SqlFragment(
                                          context.identifier(term.field()) + " = ?",
                                          List.of(context.parameter(term.value())))),
                RdbDialect.h2(),
                executions);

        assertThrows(UnsupportedOperationException.class,
                     () -> client.select(query("needs-vector")).collectList().block());
        assertEquals(0, executions.get());
    }

    @Test
    void governedStructuredQueryRejectsMissingDialectCapabilityBeforeExecution() {
        AtomicInteger executions = new AtomicInteger();
        TermExtensionDescriptor descriptor = TermExtensionDescriptor.filter(
                "structured-needs-vector", Set.of(DialectCapabilityId.POSTGRESQL_VECTOR.value()), 1, 1);
        ReactiveFormClient client = client(
                SqlTermHandler.of(descriptor, ConditionValueShape.SCALAR,
                                  (term, context) -> new SqlFragment(
                                          context.identifier(term.field()) + " = ?",
                                          List.of(context.parameter(term.value())))),
                RdbDialect.h2(),
                executions);

        assertThrows(UnsupportedOperationException.class,
                     () -> client.select(structuredQuery(descriptor.id())).collectList().block());
        assertEquals(0, executions.get());
    }

    @Test
    void governedStructuredQueryRegistersCompiledExtensionFieldAsFilter() {
        AtomicInteger executions = new AtomicInteger();
        TermExtensionDescriptor descriptor = TermExtensionDescriptor.filter(
                "structured-filter", Set.of(), 1, 1);
        ReactiveFormClient client = client(
                SqlTermHandler.of(descriptor, ConditionValueShape.SCALAR,
                                  (term, context) -> new SqlFragment(
                                          context.identifier(term.field()) + " = ?",
                                          List.of(context.parameter(term.value())))),
                RdbDialect.h2(),
                executions);

        var snapshot = client.previewFieldUse(structuredQuery(descriptor.id()));

        assertTrue(snapshot.decisions().stream().anyMatch(
                decision -> decision.field().equals("id") && decision.use() == FieldUse.FILTER));
        assertEquals(0, executions.get());
    }

    @Test
    void governedStructuredAggregateRejectsMissingDialectCapabilityBeforeExecution() {
        AtomicInteger executions = new AtomicInteger();
        TermExtensionDescriptor descriptor = TermExtensionDescriptor.filter(
                "structured-aggregate-needs-vector",
                Set.of(DialectCapabilityId.POSTGRESQL_VECTOR.value()), 1, 1);
        ReactiveFormClient client = client(
                SqlTermHandler.of(descriptor, ConditionValueShape.SCALAR,
                                  (term, context) -> new SqlFragment(
                                          context.identifier(term.field()) + " = ?",
                                          List.of(context.parameter(term.value())))),
                RdbDialect.h2(),
                executions);

        assertThrows(UnsupportedOperationException.class,
                     () -> client.aggregate(aggregate(descriptor)).collectList().block());
        assertEquals(0, executions.get());
    }

    @Test
    void governedStructuredAggregateRegistersCompiledExtensionFieldAsFilter() {
        AtomicInteger executions = new AtomicInteger();
        TermExtensionDescriptor descriptor = TermExtensionDescriptor.filter(
                "structured-aggregate-filter", Set.of(), 1, 1);
        ReactiveFormClient client = client(
                SqlTermHandler.of(descriptor, ConditionValueShape.SCALAR,
                                  (term, context) -> new SqlFragment(
                                          context.identifier(term.field()) + " = ?",
                                          List.of(context.parameter(term.value())))),
                RdbDialect.h2(),
                executions);

        var snapshot = client.previewFieldUse(aggregate(descriptor));

        assertTrue(snapshot.decisions().stream().anyMatch(
                decision -> decision.field().equals("id") && decision.use() == FieldUse.FILTER));
        assertEquals(0, executions.get());
    }

    @Test
    void describedTermCannotRenderMoreParametersThanItDeclared() {
        AtomicInteger executions = new AtomicInteger();
        TermExtensionDescriptor descriptor = TermExtensionDescriptor.filter(
                "no-parameters", Set.of(), 0, 1);
        ReactiveFormClient client = client(
                SqlTermHandler.of(descriptor, ConditionValueShape.SCALAR,
                                  (term, context) -> new SqlFragment(
                                          context.identifier(term.field()) + " = ?",
                                          List.of(context.parameter(term.value())))),
                RdbDialect.h2(),
                executions);

        assertThrows(IllegalArgumentException.class,
                     () -> client.select(query("no-parameters")).collectList().block());
        assertEquals(0, executions.get());
    }

    private static ReactiveFormClient client(SqlTermHandler extension,
                                             RdbDialect dialect,
                                             AtomicInteger executions) {
        ReactiveSqlExecutor executor = (ReactiveSqlExecutor) Proxy.newProxyInstance(
                ReactiveSqlExecutor.class.getClassLoader(),
                new Class<?>[]{ReactiveSqlExecutor.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "query" -> {
                        executions.incrementAndGet();
                        yield Flux.<DynamicRow>empty();
                    }
                    case "rowsUpdated" -> Mono.just(0L);
                    default -> throw new UnsupportedOperationException(method.toString());
                });
        SqlRenderer conditions = SqlRenderer.builder()
                                            .addDefaultTerms()
                                            .addTerm(extension)
                                            .build();
        return ReactiveFormClient.create(executor, FormDataSqlRenderer.create(conditions, dialect))
                                 .withFieldUsePolicy(policy());
    }

    private static QuerySpec query(String operator) {
        DynamicForm form = DynamicForm.builder("accounts", "accounts")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .build();
        return QuerySpec.of(form, ConditionGroup.and().where("id", operator, 7L).build())
                        .withProjection(List.of("id"), List.of());
    }

    private static QuerySpec structuredQuery(String operator) {
        DynamicForm form = DynamicForm.builder("accounts", "accounts")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .build();
        return QuerySpec.structured(
                        form, StructuredConditionInput.term("id", operator, 7L))
                .withStructuredPolicy(StructuredConditionPolicy.defaults().allowOperator(operator))
                .withProjection(List.of("id"), List.of());
    }

    private static AggregateSpec aggregate(TermExtensionDescriptor descriptor) {
        DynamicForm form = DynamicForm.builder("accounts", "accounts")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .build();
        TermRegistry terms = TermRegistry.builder()
                                         .add(TermHandler.described(
                                                 descriptor, ConditionValueShape.SCALAR))
                                         .build();
        QuerySpec query = QuerySpec.structured(
                        form, StructuredConditionInput.term("id", descriptor.id(), 7L))
                .withStructuredPolicy(StructuredConditionPolicy.defaults()
                                                                .allowOperator(descriptor.id())
                                                                .withTerms(terms));
        return AggregateSpec.builder(query)
                            .aggregate(AggregateExpression.count("id", "total"))
                            .build();
    }

    private static FieldUsePolicy policy() {
        return FieldUsePolicy.builder()
                             .visibility("id", FieldVisibility.FULL)
                             .allow("id", FieldUse.PROJECT, FieldUse.FILTER, FieldUse.AGGREGATE)
                             .build();
    }
}
