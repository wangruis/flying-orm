package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.lock.ReadLock;
import com.flying.orm.rdb.protection.ProtectedConditions;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ComputedContainsPlanTest {
    static final DynamicForm FORM = DynamicForm.builder("contains_computed", "contains_computed")
            .addField(DynamicField.primaryKey("id", "BIGINT"))
            .addField(DynamicField.of("amount", "INTEGER"))
            .addField(DynamicField.of("payload", "JSON"))
            .addField(DynamicField.of("secret", "VARCHAR"))
            .encrypted("secret", EncryptedFieldDefinition.builder()
                    .searchModes(EncryptedSearchMode.CONTAINS).build())
            .masked("secret", MaskedFieldDefinition.builder("full").build()).build();

    static QuerySpec query() {
        return QuerySpec.of(FORM, ConditionGroup.and().add(ProtectedConditions.contains("secret", "pha"))
                .where("id", ">", 0L).build()).withProjection(List.of("id"), List.of())
                .withExpression("total", ValueExpression.field("amount").add(ValueExpression.value(7)));
    }

    @Test
    void computedSelectFirstPageAndCursorReuseContainsVerification() {
        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                    RdbDialect.oracle(), RdbDialect.sqlServer())) {
                FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                        SqlRenderer.builder().addDefaultTerms().build(), dialect).withProtectedFields(runtime);
                FormOperationPlanner planner = new FormOperationPlanner(renderer,
                        new FormScopeSupport(renderer, StructuredConditionResolver.defaults(),
                                com.flying.orm.core.scope.DataScope.none()),
                        SqlExecutionOptions.unlimited());
                QuerySpec spec = query().withSorts(List.of(PageSort.desc("total")));
                FormReadPlanner.PlannedQuery selected = planner.reads.select(spec);
                FormReadPlanner.PlannedQuery first = planner.reads.first(spec);
                FormReadPlanner.PlannedQuery page = planner.reads.page(spec, PageQuery.of(2, 1));
                FormReadPlanner.PlannedKeysetPage cursor = planner.reads.keysetPage(query(),
                        KeysetPageQuery.after(1, com.flying.orm.core.page.CursorPosition.of(List.of(1L)), KeysetSort.asc("id", com.flying.orm.core.page.NullOrder.LAST)));
                assertTrue(selected.contains());
                assertTrue(first.contains());
                assertTrue(page.contains());
                assertTrue(cursor.contains());
                assertNull(page.countRequest(), "total must follow exact plaintext verification");
                assertEquals(List.of("id", "total"), selected.outputFields());
                assertEquals(selected.request().sql(), first.request().sql(), "first must not truncate unverified candidates");
                for (SqlRequest request : List.of(selected.request(), page.request(), cursor.request())) {
                    assertEquals(7, request.parameters().getFirst());
                    assertTrue(request.sql().contains("fop_business."), request.sql());
                    assertTrue(request.sql().contains("secret"), "hidden verification column must be selected");
                }
                assertEquals(0L, page.request().parameters().getLast());
                assertEquals(1L, cursor.request().parameters().getLast());
                assertThrows(UnsupportedOperationException.class,
                        () -> planner.reads.lockingRead(spec.withLock(ReadLock.update())));
                assertThrows(IllegalArgumentException.class, () -> planner.reads.select(query()
                        .withExpression("exposed", ValueExpression.field("secret"))));
            }
        }
    }
}
