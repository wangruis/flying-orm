package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FieldUseFailBeforeConnectionTest {

    @Test
    void rejectsCallerFilterBeforeTheReactiveExecutorCanAcquireAConnection() {
        AtomicInteger executions = new AtomicInteger();
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
        DynamicForm form = form();
        QuerySpec spec = QuerySpec.of(
                form, ConditionGroup.and().where("secret", "=", "classified").build())
                                  .withProjection(List.of("id"), List.of());
        FieldUsePolicy policy = FieldUsePolicy.builder()
                                              .visibility("id", FieldVisibility.FULL)
                                              .allow("id", FieldUse.PROJECT)
                                              .build();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer())
                                                      .withFieldUsePolicy(policy);

        assertThrows(ScopeAccessException.class, () -> client.select(spec).collectList().block());
        assertEquals(0, executions.get());
    }

    private static DynamicForm form() {
        return DynamicForm.builder("accounts", "accounts")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("secret", "VARCHAR"))
                          .build();
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
    }
}
