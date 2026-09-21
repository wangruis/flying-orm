package com.flying.orm.rdb.operator;

import com.flying.orm.core.metadata.IndexMetadata;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReaders;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReaders;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.schema.JdbcSchemaClient;
import com.flying.orm.rdb.schema.ReactiveSchemaClient;
import com.flying.orm.rdb.schema.SchemaMigrationPlan;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 固定同步与响应式 DDL fluent API 共享的结构语义，只允许终端执行模型不同。 */
class DdlBuilderBehaviorParityTest {

    private static final RdbDialect DIALECT = RdbDialect.h2();

    @Test
    void syncAndReactiveBuildersProduceTheSameCompleteCreatePlan() {
        SchemaMigrationPlan reactivePlan = describe(reactive(new EmptyReactiveExecutor())).plan().block();
        SchemaMigrationPlan syncPlan = describe(sync(new EmptySyncExecutor())).plan();

        assertNotNull(reactivePlan);
        assertAll(
                () -> assertFalse(reactivePlan.tableExists()),
                () -> assertFalse(syncPlan.tableExists()),
                () -> assertEquals(syncPlan.target().fields(), reactivePlan.target().fields()),
                () -> assertEquals(indexes(syncPlan), indexes(reactivePlan)),
                () -> assertEquals(syncPlan.targetForeignKeys(), reactivePlan.targetForeignKeys()),
                () -> assertEquals(syncPlan.sqlTexts(), reactivePlan.sqlTexts()));
    }

    @Test
    void interleavedChildrenCommitInCommitOrderAndUncommittedChildrenStayOut() {
        CreateOrAlterTableBuilder reactive = reactive(new EmptyReactiveExecutor());
        ColumnBuilder reactiveFirst = reactive.addColumn().name("first").number(10);
        ColumnBuilder reactiveSecond = reactive.addColumn().name("second").varchar(32);
        reactiveSecond.commit();
        reactive.addColumn().name("not_committed").varchar(8);
        reactiveFirst.commit();

        SyncCreateOrAlterTableBuilder sync = sync(new EmptySyncExecutor());
        SyncColumnBuilder syncFirst = sync.addColumn().name("first").number(10);
        SyncColumnBuilder syncSecond = sync.addColumn().name("second").varchar(32);
        syncSecond.commit();
        sync.addColumn().name("not_committed").varchar(8);
        syncFirst.commit();

        assertEquals(List.of("second", "first"), fieldNames(reactive.plan().block()));
        assertEquals(List.of("second", "first"), fieldNames(sync.plan()));
    }

    @Test
    void repeatedColumnCommitStillFailsWhenTheTableDraftIsMaterialized() {
        CreateOrAlterTableBuilder reactive = reactive(new EmptyReactiveExecutor());
        ColumnBuilder reactiveColumn = reactive.addColumn().name("id").number(10);
        reactiveColumn.commit();
        reactiveColumn.commit();

        SyncCreateOrAlterTableBuilder sync = sync(new EmptySyncExecutor());
        SyncColumnBuilder syncColumn = sync.addColumn().name("id").number(10);
        syncColumn.commit();
        syncColumn.commit();

        assertAll(
                () -> assertMessage("duplicate dynamic field name", reactive::plan),
                () -> assertMessage("duplicate dynamic field name", sync::plan));
    }

    @Test
    void validationMessagesAndFailureTimingRemainSymmetric() {
        CreateOrAlterTableBuilder reactive = reactive(new EmptyReactiveExecutor());
        SyncCreateOrAlterTableBuilder sync = sync(new EmptySyncExecutor());

        assertAll(
                () -> assertMessage("number precision must be positive",
                                    () -> reactive.addColumn().number(0)),
                () -> assertMessage("number precision must be positive",
                                    () -> sync.addColumn().number(0)),
                () -> assertMessage("varchar length must be positive",
                                    () -> reactive.addColumn().varchar(0)),
                () -> assertMessage("varchar length must be positive",
                                    () -> sync.addColumn().varchar(0)),
                () -> assertMessage(NullPointerException.class, "column name must not be null",
                                    () -> reactive.addColumn().number(10).commit()),
                () -> assertMessage(NullPointerException.class, "column name must not be null",
                                    () -> sync.addColumn().number(10).commit()),
                () -> assertMessage(NullPointerException.class, "column data type must not be null",
                                    () -> reactive.addColumn().name("id").commit()),
                () -> assertMessage(NullPointerException.class, "column data type must not be null",
                                    () -> sync.addColumn().name("id").commit()),
                () -> assertMessage("column comment must not be blank",
                                    () -> reactive.addColumn().comment(" ")),
                () -> assertMessage("column comment must not be blank",
                                    () -> sync.addColumn().comment(" ")),
                () -> assertMessage("index name must not be blank", () -> reactive.addIndex(" ")),
                () -> assertMessage("index name must not be blank", () -> sync.addIndex(" ")),
                () -> assertMessage("foreign key name must not be blank", () -> reactive.addForeignKey(" ")),
                () -> assertMessage("foreign key name must not be blank", () -> sync.addForeignKey(" ")));
    }

    @Test
    void reactivePlanDoesNotReadMetadataBeforeSubscription() {
        EmptyReactiveExecutor executor = new EmptyReactiveExecutor();
        CreateOrAlterTableBuilder builder = reactive(executor);
        builder.addColumn().name("id").number(10).commit();

        Mono<SchemaMigrationPlan> plan = builder.plan();

        assertEquals(0, executor.subscriptions.get());
        assertNotNull(plan.block());
        assertEquals(1, executor.subscriptions.get());
    }

    private static CreateOrAlterTableBuilder describe(CreateOrAlterTableBuilder builder) {
        return builder.addColumn().name("id").number(19).primaryKey().comment("编号").commit()
                .addColumn().name("parent_id").number(19).commit()
                .addColumn().name("code").varchar(64).commit()
                .addIndex("ix_orders_code").unique().columns("code", "parent_id").commit()
                .addForeignKey("fk_orders_parent").column("parent_id")
                .referenceTable("parent_orders").referenceColumn("id").commit();
    }

    private static SyncCreateOrAlterTableBuilder describe(SyncCreateOrAlterTableBuilder builder) {
        return builder.addColumn().name("id").number(19).primaryKey().comment("编号").commit()
                .addColumn().name("parent_id").number(19).commit()
                .addColumn().name("code").varchar(64).commit()
                .addIndex("ix_orders_code").unique().columns("code", "parent_id").commit()
                .addForeignKey("fk_orders_parent").column("parent_id")
                .referenceTable("parent_orders").referenceColumn("id").commit();
    }

    private static CreateOrAlterTableBuilder reactive(EmptyReactiveExecutor executor) {
        return new CreateOrAlterTableBuilder(
                ReactiveSchemaClient.create(executor, DIALECT),
                ReactiveFormMetadataReaders.create(executor, DIALECT),
                "orders");
    }

    private static SyncCreateOrAlterTableBuilder sync(EmptySyncExecutor executor) {
        return new SyncCreateOrAlterTableBuilder(
                JdbcSchemaClient.create(executor, DIALECT),
                JdbcFormMetadataReaders.create(executor, DIALECT),
                "orders");
    }

    private static List<String> fieldNames(SchemaMigrationPlan plan) {
        return plan.target().fields().stream().map(field -> field.name()).toList();
    }

    private static List<IndexShape> indexes(SchemaMigrationPlan plan) {
        return plan.targetIndexes().stream().map(IndexShape::new).toList();
    }

    private static void assertMessage(String message, Runnable operation) {
        assertMessage(IllegalArgumentException.class, message, operation);
    }

    private static <T extends Throwable> void assertMessage(Class<T> type, String message, Runnable operation) {
        T failure = assertThrows(type, operation::run);
        assertEquals(message, failure.getMessage());
    }

    private record IndexShape(String name, boolean unique, List<String> columns) {

        private IndexShape(IndexMetadata index) {
            this(index.name(), index.unique(), index.columns());
        }
    }

    private static final class EmptyReactiveExecutor implements ReactiveSqlExecutor {

        private final AtomicInteger subscriptions = new AtomicInteger();

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            return Flux.defer(() -> {
                subscriptions.incrementAndGet();
                return Flux.empty();
            });
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            return Mono.error(new UnsupportedOperationException());
        }
    }

    private static final class EmptySyncExecutor implements SyncSqlExecutor {

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            return List.of();
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
            throw new UnsupportedOperationException();
        }
    }
}
