package com.flying.orm.rdb.operator;

import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.junit.jupiter.api.Assertions.assertThrows;

class GovernedDmlQueryFilterTest {

    @Test
    void trustedRelationQueryQualifiesBareFieldsAndPreservesQualifiedFields() {
        SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms()
                .addTerm(com.flying.orm.core.sql.render.SqlTermHandler.relationExists(
                        "member-of", "membership", "users", "owner_id", "group_id"))
                .build().withIdentifierRenderer(RdbDialect.postgresql().schema()::identifier);
        for (String field : java.util.List.of("id", "users.id")) {
            DmlQueryCommand command = new DmlQueryCommand(renderer, DataScope.none());
            command.from("users");
            command.where(where -> where.term(field, "member-of", 1L));
            String sql = command.toRequest().sql();
            org.junit.jupiter.api.Assertions.assertTrue(sql.contains(
                    "\"users_relation\".\"owner_id\" = \"users\".\"id\""), sql);
        }
    }

    @Test
    void governedMetadataQueryCannotFilterByAFieldThatThePolicyOnlyHides() {
        DynamicForm form = DynamicForm.builder("accounts", "accounts")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("secret", "VARCHAR"))
                                      .build();
        FieldUsePolicy policy = FieldUsePolicy.builder()
                                              .visibility("id", FieldVisibility.FULL)
                                              .allow("id", FieldUse.PROJECT)
                                              .build();
        SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().build();
        ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
            @Override public Flux<DynamicRow> query(com.flying.orm.core.sql.render.SqlRequest request) {
                return Flux.error(new AssertionError("denied query reached the executor"));
            }

            @Override public Mono<Long> rowsUpdated(com.flying.orm.core.sql.render.SqlRequest request) {
                return Mono.error(new UnsupportedOperationException());
            }
        };
        ReactiveFormClient forms = ReactiveFormClient.create(
                executor, FormDataSqlRenderer.create(renderer, RdbDialect.postgresql()));
        QueryOperator query = new DmlOperator(forms, executor, renderer, DataScope.none())
                .query().from(form, policy, QueryShapeLimits.defaults()).select("id")
                .where(where -> where.is("secret", "classified"));

        assertThrows(ScopeAccessException.class, () -> query.fetchMap().blockLast());
    }
}
