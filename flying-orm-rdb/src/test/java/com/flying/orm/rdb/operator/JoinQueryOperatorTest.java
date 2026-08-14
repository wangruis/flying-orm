package com.flying.orm.rdb.operator;

import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
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

/**
 * 验证 DynamicForm 的响应式与同步轻量 JOIN 链式入口。
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
class JoinQueryOperatorTest {

    /** 实体入口只接受直接 getter，并把两个实体的字段解析到同一 JOIN AST。 */
    @Test
    void executesEntityLambdaJoinWithoutTableOrFieldStrings() {
        FormDataSqlRenderer formRenderer = FormDataSqlRenderer.create(renderer(), RdbDialect.h2());
        CapturingReactiveExecutor executor = new CapturingReactiveExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, formRenderer);
        DmlOperator dml = new DmlOperator(client, executor, renderer(), DataScope.none());

        EntityJoinQueryOperator<UserEntity> query = dml.joinQuery(UserEntity.class)
                                                       .leftJoin(OrderEntity.class,
                                                                 UserEntity::getId,
                                                                 OrderEntity::getUserId)
                                                       .andOn(UserEntity::getTenantId,
                                                              OrderEntity::getTenantId)
                                                       .selectAs(UserEntity.class,
                                                                 UserEntity::getName,
                                                                 "userName")
                                                       .selectAs(OrderEntity.class,
                                                                 OrderEntity::getOrderNo,
                                                                 "orderNo")
                                                       .where(UserEntity.class,
                                                              UserEntity::getName,
                                                              "=",
                                                              "alice")
                                                       .declaredDisplay()
                                                       .masked()
                                                       .showSensitive();

        StepVerifier.create(query.executeRows()).expectNextCount(1).verifyComplete();

        assertEquals("select t0.name as userName, t1.order_no as orderNo from users t0 "
                             + "left outer join orders t1 on t0.user_id = t1.user_id "
                             + "and t0.tenant_id = t1.tenant_id where t0.name = ?",
                     executor.request.sql());
        assertEquals(List.of("alice"), executor.request.parameters());
    }

    /** 已确认的 join/leftJoin/rightJoin API 必须只构建结构，并共享 FormClient 安全执行链。 */
    @Test
    void executesSameDynamicJoinBuilderThroughReactiveAndSyncClients() {
        DynamicForm users = form("users", "id", "tenant_id", "name");
        DynamicForm orders = form("orders", "id", "tenant_id", "user_id", "order_no");
        FormDataSqlRenderer formRenderer = FormDataSqlRenderer.create(renderer(), RdbDialect.h2());
        CapturingReactiveExecutor reactiveExecutor = new CapturingReactiveExecutor();
        CapturingSyncExecutor syncExecutor = new CapturingSyncExecutor();
        DmlOperator reactiveDml = new DmlOperator(
                ReactiveFormClient.create(reactiveExecutor, formRenderer),
                reactiveExecutor,
                renderer(),
                DataScope.none());
        SyncDmlOperator syncDml = new SyncDmlOperator(
                SyncFormClient.create(syncExecutor, new UnsupportedBatchExecutor(), formRenderer),
                syncExecutor,
                renderer(),
                DataScope.none());

        JoinQueryOperator reactive = reactiveDml.joinQuery(users)
                                                .leftJoin(orders, "id", "user_id")
                                                .andOn("tenant_id", "tenant_id")
                                                .selectAs(users, "name", "userName")
                                                .selectAs(orders, "order_no", "orderNo")
                                                .where(users, "name", "=", "alice")
                                                .orderByAsc(orders, "order_no")
                                                .declaredDisplay()
                                                .masked()
                                                .showSensitive();
        SyncJoinQueryOperator sync = syncDml.joinQuery(users)
                                            .leftJoin(orders, "id", "user_id")
                                            .andOn("tenant_id", "tenant_id")
                                            .selectAs(users, "name", "userName")
                                            .selectAs(orders, "order_no", "orderNo")
                                            .where(users, "name", "=", "alice")
                                            .orderByAsc(orders, "order_no")
                                            .declaredDisplay()
                                            .masked()
                                            .showSensitive();

        StepVerifier.create(reactive.executeRows())
                    .assertNext(row -> assertEquals("alice", row.get("userName")))
                    .verifyComplete();
        assertEquals("alice", sync.executeRows().getFirst().get("userName"));

        assertEquals(reactiveExecutor.request, syncExecutor.request);
        assertEquals("select t0.name as userName, t1.order_no as orderNo from users t0 "
                             + "left outer join orders t1 on t0.id = t1.user_id "
                             + "and t0.tenant_id = t1.tenant_id where t0.name = ? order by t1.order_no asc",
                     reactiveExecutor.request.sql());
        assertEquals(List.of("alice"), reactiveExecutor.request.parameters());
    }

    private static SqlRenderer renderer() {
        return SqlRenderer.builder().addDefaultTerms().build();
    }

    private static DynamicForm form(String table, String... fields) {
        DynamicForm.Builder builder = DynamicForm.builder(table + "-form", table);
        for (String field : fields) {
            builder.addField(DynamicField.of(field, "VARCHAR"));
        }
        return builder.build();
    }

    private static final class CapturingReactiveExecutor implements ReactiveSqlExecutor {
        private SqlRequest request;

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            this.request = request;
            return Flux.just(DynamicRow.copyOf(Map.of("userName", "alice", "orderNo", "A-1")));
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            return Mono.error(new UnsupportedOperationException("not used"));
        }
    }

    private static final class CapturingSyncExecutor implements SyncSqlExecutor {
        private SqlRequest request;

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            this.request = request;
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

    @TableName("users")
    private static final class UserEntity {
        @TableField("user_id")
        private String id;
        @TableField("tenant_id")
        private String tenantId;
        private String name;

        String getId() { return id; }
        String getTenantId() { return tenantId; }
        String getName() { return name; }
    }

    @TableName("orders")
    private static final class OrderEntity {
        @TableField("user_id")
        private String userId;
        @TableField("tenant_id")
        private String tenantId;
        @TableField("order_no")
        private String orderNo;

        String getUserId() { return userId; }
        String getTenantId() { return tenantId; }
        String getOrderNo() { return orderNo; }
    }
}
