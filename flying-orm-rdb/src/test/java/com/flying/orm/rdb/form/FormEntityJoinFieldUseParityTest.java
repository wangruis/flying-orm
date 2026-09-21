package com.flying.orm.rdb.form;

import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FormEntityJoinFieldUseParityTest {

    @Test
    void formEntityAndJoinEntrypointsUseTheSameCallerFilterDecision() {
        AtomicInteger executions = new AtomicInteger();
        FormDataSqlRenderer renderer = renderer();
        SyncFormClient client = SyncFormClient.create(
                syncExecutor(executions), syncBatchExecutor(), renderer)
                                              .withFieldUsePolicy(FieldUsePolicy.builder()
                                                                              .visibility("id", FieldVisibility.FULL)
                                                                              .allow("id", FieldUse.PROJECT)
                                                                              .build());
        DynamicForm form = form();
        QuerySpec formQuery = QuerySpec.of(
                form, ConditionGroup.and().where("secret", "=", "classified").build())
                                       .withProjection(List.of("id"), List.of());
        JoinQuerySpec.Builder joinBuilder = JoinQuerySpec.builder(form);
        JoinSource root = joinBuilder.root();
        JoinQuerySpec joinQuery = joinBuilder.select(root, "id")
                                              .where(root, ConditionGroup.and()
                                                                         .where("secret", "=", "classified")
                                                                         .build())
                                              .build();

        assertThrows(ScopeAccessException.class, () -> client.select(formQuery));
        assertThrows(ScopeAccessException.class, () -> client.entity(Account.class)
                                                              .query()
                                                              .select(Account::id)
                                                              .where(Account::secret, "classified")
                                                              .executeRows());
        assertThrows(ScopeAccessException.class, () -> client.selectJoin(joinQuery));
        assertEquals(0, executions.get());
    }

    private static DynamicForm form() {
        return DynamicForm.builder("accounts", "accounts")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("secret", "VARCHAR"))
                          .build();
    }

    private static SyncSqlExecutor syncExecutor(AtomicInteger executions) {
        return (SyncSqlExecutor) Proxy.newProxyInstance(
                SyncSqlExecutor.class.getClassLoader(), new Class<?>[]{SyncSqlExecutor.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("query")) {
                        executions.incrementAndGet();
                        return List.<DynamicRow>of();
                    }
                    throw new UnsupportedOperationException(method.toString());
                });
    }

    private static SyncBatchExecutor syncBatchExecutor() {
        return (SyncBatchExecutor) Proxy.newProxyInstance(
                SyncBatchExecutor.class.getClassLoader(), new Class<?>[]{SyncBatchExecutor.class},
                (proxy, method, arguments) -> { throw new UnsupportedOperationException(method.toString()); });
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
    }

    @TableName("accounts")
    private record Account(@TableId Long id, String secret) {
    }
}
