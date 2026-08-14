package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.TenantStrategy;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.join.JoinType;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.protection.ProtectedConditions;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证响应式与同步 JOIN 执行共享 Scope、SQL 请求和动态结果语义。
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
class JoinQueryOperationsTest {

    /** R2DBC 与 JDBC 必须从同一 AST 生成相同 SQL，并分别走自己的原生执行器。 */
    @Test
    void reactiveAndSyncClientsShareJoinScopeAndSqlRequest() {
        DynamicForm users = protectedForm("users", "deleted", "id", "name", "tenant_id");
        DynamicForm orders = protectedForm("orders", "removed", "id", "user_id", "order_no", "tenant_id");
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(users);
        JoinSource root = builder.root();
        JoinSource joined = builder.join(JoinType.LEFT, orders, root, "id", "user_id");
        JoinQuerySpec spec = builder.selectAs(root, "name", "userName")
                                    .selectAs(joined, "order_no", "orderNo")
                                    .scope(root, DataScope.tenant("tenant_id", "tenant-a"))
                                    .scope(joined, DataScope.tenant("tenant_id", "tenant-a"))
                                    .build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2());
        CapturingReactiveExecutor reactiveExecutor = new CapturingReactiveExecutor();
        CapturingSyncExecutor syncExecutor = new CapturingSyncExecutor();
        ReactiveFormClient reactive = ReactiveFormClient.create(reactiveExecutor, renderer);
        SyncFormClient sync = SyncFormClient.create(syncExecutor, new UnsupportedBatchExecutor(), renderer);

        StepVerifier.create(reactive.selectJoin(spec))
                    .assertNext(row -> assertEquals("alice", row.get("userName")))
                    .verifyComplete();
        List<DynamicRow> syncRows = sync.selectJoin(spec);

        assertEquals("alice", syncRows.getFirst().get("userName"));
        assertEquals(reactiveExecutor.request, syncExecutor.request);
        assertEquals(List.of("tenant-a", 0, "tenant-a", 0), reactiveExecutor.request.parameters());
        assertEquals("select t0.name as userName, t1.order_no as orderNo "
                             + "from (select * from users where tenant_id = ? and deleted = ?) t0 "
                             + "left outer join (select * from orders where tenant_id = ? and removed = ?) t1 "
                             + "on t0.id = t1.user_id",
                     reactiveExecutor.request.sql());
    }

    /** JOIN 的受保护 WHERE 使用盲索引，投影在结果边界解密后按字段声明脱敏。 */
    @Test
    void protectsJoinSearchAndMasksEncryptedProjection() {
        DynamicForm customers = encryptedCustomerForm();
        DynamicForm orders = protectedForm("orders", "deleted", "id", "customer_id", "tenant_id");
        DataScope tenant = DataScope.tenant("tenant_id", "tenant-a");
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", new byte[32])) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2())
                                                           .withProtectedFields(
                                                                   ProtectedFieldRuntime.create(keys));
            byte[] ciphertext = (byte[]) renderer.protection().prepareWrite(
                    customers, Map.of("contact", "13800138000", "tenant_id", "tenant-a"), tenant)
                                                .values().get("contact");
            CapturingReactiveExecutor executor = new CapturingReactiveExecutor();
            executor.row = DynamicRow.copyOf(Map.of("contactValue", ciphertext));
            ReactiveFormClient client = ReactiveFormClient.create(executor, renderer);
            JoinQuerySpec.Builder builder = JoinQuerySpec.builder(customers);
            JoinSource root = builder.root();
            JoinSource joined = builder.join(JoinType.LEFT, orders, root, "id", "customer_id");
            JoinQuerySpec spec = builder.selectAs(root, "contact", "contactValue")
                                        .selectAs(joined, "id", "orderId")
                                        .where(root, com.flying.orm.core.condition.ConditionGroup.and()
                                                      .add(ProtectedConditions.exact(
                                                              "contact", "13800138000"))
                                                      .build())
                                        .scope(root, tenant)
                                        .scope(joined, tenant)
                                        .build();

            StepVerifier.create(client.selectJoin(spec))
                        .assertNext(row -> assertEquals("13*******00", row.get("contactValue")))
                        .verifyComplete();

            StepVerifier.create(client.selectJoin(builder.showSensitive().build()))
                        .assertNext(row -> assertEquals("13800138000", row.get("contactValue")))
                        .verifyComplete();

            assertTrue(executor.request.sql().contains("__fop_e_"));
            assertFalse(executor.request.sql().contains("contact = ?"));
        }
    }

    /** 随机密文不能参与 ON 或排序；这些写法没有稳定业务语义，必须在 SQL 前拒绝。 */
    @Test
    void rejectsEncryptedJoinKeysAndOrderingBeforeSql() {
        DynamicForm customers = encryptedCustomerForm();
        DynamicForm archived = encryptedCustomerForm("archived_customers");
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", new byte[32])) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2())
                                                           .withProtectedFields(
                                                                   ProtectedFieldRuntime.create(keys));
            ReactiveFormClient client = ReactiveFormClient.create(new CapturingReactiveExecutor(), renderer);

            JoinQuerySpec.Builder onBuilder = JoinQuerySpec.builder(customers);
            JoinSource onRoot = onBuilder.root();
            JoinSource onJoined = onBuilder.join(JoinType.INNER, archived, onRoot, "contact", "contact");
            onBuilder.select(onRoot, "id")
                     .scope(onRoot, DataScope.tenant("tenant_id", "tenant-a"))
                     .scope(onJoined, DataScope.tenant("tenant_id", "tenant-a"));
            IllegalArgumentException onError = assertThrows(
                    IllegalArgumentException.class, () -> client.selectJoin(onBuilder.build()));
            assertEquals("encrypted field must not be used as a join key", onError.getMessage());

            JoinQuerySpec.Builder orderBuilder = JoinQuerySpec.builder(customers);
            JoinSource orderRoot = orderBuilder.root();
            JoinSource orderJoined = orderBuilder.join(JoinType.LEFT, archived, orderRoot, "id", "id");
            orderBuilder.select(orderJoined, "contact")
                        .scope(orderRoot, DataScope.tenant("tenant_id", "tenant-a"))
                        .scope(orderJoined, DataScope.tenant("tenant_id", "tenant-a"))
                        .orderBy(orderJoined, "contact", com.flying.orm.core.page.PageSort.Direction.ASC);
            IllegalArgumentException orderError = assertThrows(
                    IllegalArgumentException.class, () -> client.selectJoin(orderBuilder.build()));
            assertEquals("encrypted field must not be used for join ordering", orderError.getMessage());
        }
    }

    /** 响应式与 JDBC JOIN 分页必须复用同一 count/data 计划并返回相同页头。 */
    @Test
    void reactiveAndSyncClientsPageTheSameJoinPlan() {
        DynamicForm users = protectedForm("users", "deleted", "id", "name", "tenant_id");
        DynamicForm orders = protectedForm("orders", "removed", "id", "user_id", "tenant_id");
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(users);
        JoinSource root = builder.root();
        JoinSource joined = builder.join(JoinType.LEFT, orders, root, "id", "user_id");
        JoinQuerySpec spec = builder.selectAs(root, "name", "userName")
                                    .selectAs(joined, "id", "orderId")
                                    .scope(root, DataScope.tenant("tenant_id", "tenant-a"))
                                    .scope(joined, DataScope.tenant("tenant_id", "tenant-a"))
                                    .orderBy(root, "id", PageSort.Direction.ASC)
                                    .build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2());
        CapturingReactiveExecutor reactiveExecutor = new CapturingReactiveExecutor();
        CapturingSyncExecutor syncExecutor = new CapturingSyncExecutor();
        ReactiveFormClient reactive = ReactiveFormClient.create(reactiveExecutor, renderer);
        SyncFormClient sync = SyncFormClient.create(syncExecutor, new UnsupportedBatchExecutor(), renderer);

        StepVerifier.create(reactive.pageJoin(spec, PageQuery.of(2, 20)))
                    .assertNext(page -> {
                        assertEquals(2L, page.total());
                        assertEquals("alice", page.rows().getFirst().get("userName"));
                    })
                    .verifyComplete();
        PageResult<DynamicRow> syncPage = sync.pageJoin(spec, PageQuery.of(2, 20));

        assertEquals(2L, syncPage.total());
        assertEquals("alice", syncPage.rows().getFirst().get("userName"));
        assertEquals(reactiveExecutor.requests, syncExecutor.requests);
        assertTrue(reactiveExecutor.requests.getFirst().sql().startsWith("select count(*) as total"));
        assertTrue(reactiveExecutor.requests.getLast().sql().endsWith("limit ? offset ?"));
    }

    private static DynamicForm protectedForm(String table, String deletedField, String... fields) {
        DynamicForm.Builder builder = DynamicForm.builder(table + "-form", table);
        for (String field : fields) {
            builder.addField(DynamicField.of(field, "VARCHAR"));
        }
        return builder.addField(DynamicField.of(deletedField, "INTEGER"))
                      .tenant("tenant_id", TenantStrategy.AUTO)
                      .logicDelete(deletedField)
                      .build();
    }

    private static DynamicForm encryptedCustomerForm() {
        return encryptedCustomerForm("customers");
    }

    private static DynamicForm encryptedCustomerForm(String table) {
        return DynamicForm.builder(table + "-form", table)
                          .addField(DynamicField.of("id", "VARCHAR"))
                          .addField(DynamicField.of("tenant_id", "VARCHAR"))
                          .addField(DynamicField.of("contact", "VARCHAR"))
                          .tenant("tenant_id", TenantStrategy.AUTO)
                          .encrypted("contact", EncryptedFieldDefinition.builder()
                                                                         .searchModes(
                                                                                 EncryptedSearchMode.EXACT,
                                                                                 EncryptedSearchMode.SUFFIX)
                                                                         .normalizer("digits")
                                                                         .suffixLengths(4)
                                                                         .build())
                          .masked("contact", MaskedFieldDefinition.builder("partial")
                                                                   .prefix(2)
                                                                   .suffix(2)
                                                                   .build())
                          .build();
    }

    private static final class CapturingReactiveExecutor implements ReactiveSqlExecutor {
        private SqlRequest request;
        private final java.util.ArrayList<SqlRequest> requests = new java.util.ArrayList<>();
        private DynamicRow row = DynamicRow.copyOf(Map.of("userName", "alice", "orderNo", "A-1"));

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            this.request = request;
            requests.add(request);
            if (request.sql().startsWith("select count(*)")) {
                return Flux.just(DynamicRow.copyOf(Map.of("total", 2L)));
            }
            return Flux.just(row);
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            return Mono.error(new UnsupportedOperationException("not used"));
        }
    }

    private static final class CapturingSyncExecutor implements SyncSqlExecutor {
        private SqlRequest request;
        private final java.util.ArrayList<SqlRequest> requests = new java.util.ArrayList<>();

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            this.request = request;
            requests.add(request);
            if (request.sql().startsWith("select count(*)")) {
                return List.of(DynamicRow.copyOf(Map.of("total", 2L)));
            }
            return List.of(DynamicRow.copyOf(Map.of("userName", "alice", "orderNo", "A-1")));
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            throw new UnsupportedOperationException("not used");
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
            throw new UnsupportedOperationException("not used");
        }
    }

    private static final class UnsupportedBatchExecutor implements SyncBatchExecutor {
        @Override
        public BatchWriteResult writeBatch(BatchWriteRequest request) {
            throw new UnsupportedOperationException("not used");
        }

        @Override
        public List<BatchChunkResult> writeBatchChunks(BatchWriteRequest request) {
            throw new UnsupportedOperationException("not used");
        }
    }
}
