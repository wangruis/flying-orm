package com.flying.orm.rdb.operator;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.protection.ProtectedConditions;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GovernedDmlFormKernelTest {

    @Test
    void governedFormKeepsTheLiteralRelationNameInTheFormKernel() {
        DynamicForm form = DynamicForm.relationalBuilder("orders",
                com.flying.orm.core.metadata.RelationIdentity.table("orders.archive.history"))
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
        var conditions = SqlRenderer.builder().addDefaultTerms().build();
        var renderer = FormDataSqlRenderer.create(conditions, RdbDialect.postgresql());
        var request = new AtomicReference<SqlRequest>();
        var raw = DynamicRow.copyOf(Map.of("id", 1L));
        var sync = syncExecutor(request, raw);
        new SyncDmlOperator(SyncFormClient.create(sync, syncBatchExecutor(), renderer),
                sync, conditions, DataScope.none()).query().from(form, FieldUsePolicy.unrestricted())
                .fetchMap();
        assertTrue(request.get().sql().contains("\"orders.archive.history\""));
        var reactive = reactiveExecutor(request, raw);
        new DmlOperator(ReactiveFormClient.create(reactive, renderer), reactive, conditions, DataScope.none())
                .query().from(form, FieldUsePolicy.unrestricted()).fetchMap().collectList().block();
        assertTrue(request.get().sql().contains("\"orders.archive.history\""));
    }

    @Test
    void syncAndReactiveGovernedQueriesReuseProtectionProjectionAndResultDecoding() {
        DynamicForm form = protectedForm();
        SqlRenderer conditions = SqlRenderer.builder().addDefaultTerms().build();
        AtomicReference<SqlRequest> syncRequest = new AtomicReference<>();
        AtomicReference<SqlRequest> reactiveRequest = new AtomicReference<>();

        try (ProtectedFieldRuntime protection = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            FormDataSqlRenderer forms = FormDataSqlRenderer.create(
                    conditions, RdbDialect.postgresql()).withProtectedFields(protection);
            Map<String, Object> stored = new LinkedHashMap<>(protection
                    .prepareWrite(form, Map.<String, Object>of("secret", "classified"),
                                  DataScope.none(), conditions.valueCodecs())
                    .values());
            // 模拟数据库只返回 SELECT 投影；盲索引只出现在 WHERE，不应进入结果行。
            stored.keySet().removeIf(name -> name.startsWith("__fop_"));
            stored.put("id", 7L);
            DynamicRow raw = DynamicRow.copyOf(stored);
            SyncSqlExecutor syncExecutor = syncExecutor(syncRequest, raw);
            ReactiveSqlExecutor reactiveExecutor = reactiveExecutor(reactiveRequest, raw);
            SyncFormClient syncForms = SyncFormClient.create(
                    syncExecutor, syncBatchExecutor(), forms);
            ReactiveFormClient reactiveForms = ReactiveFormClient.create(reactiveExecutor, forms);

            DynamicRow sync = new SyncDmlOperator(
                    syncForms, syncExecutor, conditions, DataScope.none())
                    .query().from(form, FieldUsePolicy.unrestricted())
                    .where(where -> where.term(
                            "secret", ProtectedConditions.EXACT, "classified"))
                    .fetchMap().getFirst();
            DynamicRow reactive = new DmlOperator(
                    reactiveForms, reactiveExecutor, conditions, DataScope.none())
                    .query().from(form, FieldUsePolicy.unrestricted())
                    .where(where -> where.term(
                            "secret", ProtectedConditions.EXACT, "classified"))
                    .fetchMap().single().block();

            assertKernelRequest(syncRequest.get());
            assertKernelRequest(reactiveRequest.get());
            assertDecoded(sync);
            assertDecoded(reactive);
        }
    }

    private static void assertKernelRequest(SqlRequest request) {
        assertFalse(request.sql().contains("select *"));
        assertTrue(request.sql().contains("__fop_e_"));
        assertFalse(request.sql().contains("\"secret\" = ?"));
        assertFalse(request.sql().substring(0, request.sql().indexOf(" from ")).contains("__fop_"));
    }

    private static void assertDecoded(DynamicRow row) {
        assertEquals(7L, row.get("id"));
        assertEquals("classified", row.get("secret"));
        assertTrue(row.keySet().stream().noneMatch(name -> name.startsWith("__fop_")),
                   row.keySet().toString());
    }

    private static DynamicForm protectedForm() {
        return DynamicForm.builder("people", "people")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR"))
                .encrypted("secret", EncryptedFieldDefinition.builder()
                        .searchModes(EncryptedSearchMode.EXACT)
                        .build())
                .build();
    }

    private static SyncSqlExecutor syncExecutor(
            AtomicReference<SqlRequest> request,
            DynamicRow row) {
        return (SyncSqlExecutor) Proxy.newProxyInstance(
                SyncSqlExecutor.class.getClassLoader(), new Class<?>[]{SyncSqlExecutor.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("queryMapped")) {
                        request.set((SqlRequest) arguments[0]);
                        return List.of(((RowMapper<?>) arguments[2]).map(row));
                    }
                    throw new UnsupportedOperationException(method.toString());
                });
    }

    private static ReactiveSqlExecutor reactiveExecutor(
            AtomicReference<SqlRequest> request,
            DynamicRow row) {
        return new ReactiveSqlExecutor() {
            @Override public Flux<DynamicRow> query(SqlRequest sql) {
                request.set(sql);
                return Flux.just(row);
            }

            @Override public Mono<Long> rowsUpdated(SqlRequest sql) {
                return Mono.error(new UnsupportedOperationException());
            }
        };
    }

    private static SyncBatchExecutor syncBatchExecutor() {
        return (SyncBatchExecutor) Proxy.newProxyInstance(
                SyncBatchExecutor.class.getClassLoader(), new Class<?>[]{SyncBatchExecutor.class},
                (proxy, method, arguments) -> {
                    throw new UnsupportedOperationException(method.toString());
                });
    }
}
