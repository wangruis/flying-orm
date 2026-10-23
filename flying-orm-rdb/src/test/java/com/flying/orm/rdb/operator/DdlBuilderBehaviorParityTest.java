package com.flying.orm.rdb.operator;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.ValueGeneration;
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
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 固定同步与响应式 DDL fluent API 共享的结构语义，只允许终端执行模型不同。 */
class DdlBuilderBehaviorParityTest {

    private static final RdbDialect DIALECT = RdbDialect.h2();

    @Test
    void oneColumnConfigurationWorksForBothExecutionModels() {
        CreateOrAlterTableBuilder reactive = reactive(new EmptyReactiveExecutor());
        SyncCreateOrAlterTableBuilder sync = sync(new EmptySyncExecutor());
        java.util.function.Consumer<ColumnBuilder<?>> amount =
                column -> column.number(18, 4).nullable(false);

        assertSame(reactive, reactive.addColumn("amount", amount));
        assertSame(sync, sync.addColumn("amount", amount));
        assertSame(reactive, reactive.addIndex("ix_amount").column("amount").commit());
        assertSame(sync, sync.addIndex("ix_amount").column("amount").commit());
        assertEquals(sync.plan().sqlTexts(), reactive.plan().block().sqlTexts());
    }

    @Test
    void mixedDirectionIndexesHaveIdenticalSyncAndReactivePlans() {
        CreateOrAlterTableBuilder reactive = reactive(new EmptyReactiveExecutor());
        SyncCreateOrAlterTableBuilder sync = sync(new EmptySyncExecutor());
        reactive.addColumn("id", column -> column.type("BIGINT"))
                .addColumn("created_at", column -> column.type("TIMESTAMP"))
                .addIndex("ix_order").column("id")
                .column("created_at", com.flying.orm.core.metadata.IndexKeyPart.Direction.DESC).commit();
        sync.addColumn("id", column -> column.type("BIGINT"))
                .addColumn("created_at", column -> column.type("TIMESTAMP"))
                .addIndex("ix_order").column("id")
                .column("created_at", com.flying.orm.core.metadata.IndexKeyPart.Direction.DESC).commit();
        SchemaMigrationPlan expected = sync.plan();
        SchemaMigrationPlan actual = reactive.plan().block();
        assertEquals(expected.sqlTexts(), actual.sqlTexts());
        assertEquals(com.flying.orm.core.metadata.IndexKeyPart.desc("created_at"),
                actual.target().indexes().getFirst().keys().get(1));
        assertTrue(actual.sqlTexts().stream().anyMatch(sql -> sql.contains(" desc")));
    }

    @Test
    void namedColumnCallbacksFreezeAtReturnWithoutReadingMetadata() {
        EmptyReactiveExecutor executor = new EmptyReactiveExecutor();
        CreateOrAlterTableBuilder reactive = reactive(executor);
        SyncCreateOrAlterTableBuilder sync = sync(new EmptySyncExecutor());
        AtomicReference<ColumnBuilder<CreateOrAlterTableBuilder>> asyncColumn = new AtomicReference<>();
        AtomicReference<ColumnBuilder<SyncCreateOrAlterTableBuilder>> syncColumn = new AtomicReference<>();
        AtomicInteger calls = new AtomicInteger();
        reactive.addColumn("id", column -> {
            calls.incrementAndGet();
            asyncColumn.set(column);
            column.type("BIGINT").primaryKey().generation(ValueGeneration.identity());
        }).addColumn("code", column -> column.varchar(40).nullable(false).unique(true));
        sync.addColumn("id", column -> {
            calls.incrementAndGet();
            syncColumn.set(column);
            column.type("BIGINT").primaryKey().generation(ValueGeneration.identity());
        }).addColumn("code", column -> column.varchar(40).nullable(false).unique(true));
        assertEquals(2, calls.get());
        assertEquals(0, executor.subscriptions.get());
        asyncColumn.get().name("changed").type("VARCHAR(8)");
        syncColumn.get().name("changed").type("VARCHAR(8)");
        Mono<SchemaMigrationPlan> pending = reactive.plan();
        SchemaMigrationPlan expected = sync.plan();
        assertEquals(List.of("id", "code"), fieldNames(expected));
        assertEquals(ValueGeneration.identity(), expected.target().column("id").generation());
        assertEquals(expected.target().columns(), pending.block().target().columns());
        assertEquals(expected.sqlTexts(), pending.block().sqlTexts());
        assertEquals(2, calls.get(), "subscribing must not repeat configuration");
    }

    @Test
    void failedColumnConfigurationDoesNotAddAnUncommittedColumn() {
        CreateOrAlterTableBuilder reactive = reactive(new EmptyReactiveExecutor());
        SyncCreateOrAlterTableBuilder sync = sync(new EmptySyncExecutor());
        reactive.addColumn("id", column -> column.number(19).primaryKey());
        sync.addColumn("id", column -> column.number(19).primaryKey());
        IllegalStateException failure = new IllegalStateException("configuration failure");
        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> reactive.addColumn("incomplete", column -> { column.varchar(20); throw failure; })));
        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> sync.addColumn("incomplete", column -> { column.varchar(20); throw failure; })));
        assertThrows(IllegalArgumentException.class, () -> reactive.addColumn("invalid", column -> column.number(0)));
        assertThrows(IllegalArgumentException.class, () -> sync.addColumn("invalid", column -> column.number(0)));
        assertEquals(List.of("id"), fieldNames(reactive.plan().block()));
        assertEquals(List.of("id"), fieldNames(sync.plan()));
    }

    @Test
    void fluentColumnsExposeTheSameFieldSemanticsWithoutSwitchingModels() {
        CreateOrAlterTableBuilder reactive = reactive(new EmptyReactiveExecutor());
        SyncCreateOrAlterTableBuilder sync = sync(new EmptySyncExecutor());
        ValueGeneration generation = ValueGeneration.identity(10, 2, 0);
        reactive.addColumn("id").type("BIGINT").primaryKey().nullable(true).generation(generation).commit()
                .addColumn("code").varchar(40).nullable(false).unique(true).commit()
                .addColumn("amount").varchar(40).number(18, 4).commit()
                .addColumn("created_at").number(18, 4).type("TIMESTAMP(6)").commit()
                .addColumn("note").type("BOOLEAN").varchar(80).unique(true).unique(false)
                .nullable(false).nullable(true).commit();
        sync.addColumn("id").type("BIGINT").primaryKey().nullable(true).generation(generation).commit()
                .addColumn("code").varchar(40).nullable(false).unique(true).commit()
                .addColumn("amount").varchar(40).number(18, 4).commit()
                .addColumn("created_at").number(18, 4).type("TIMESTAMP(6)").commit()
                .addColumn("note").type("BOOLEAN").varchar(80).unique(true).unique(false)
                .nullable(false).nullable(true).commit();
        List<DynamicField> expected = List.of(
                DynamicField.primaryKey("id", "BIGINT").withGeneration(generation),
                DynamicField.of("code", "VARCHAR").withLength(40).withNullable(false).withUnique(true),
                DynamicField.of("amount", "DECIMAL").withPrecision(18, 4),
                DynamicField.of("created_at", "TIMESTAMP(6)"),
                DynamicField.of("note", "VARCHAR").withLength(80));
        assertEquals(expected, reactive.draft().form().fields());
        assertEquals(expected, sync.draft().form().fields());
        SchemaMigrationPlan plan = sync.plan();
        assertEquals(plan.sqlTexts(), reactive.plan().block().sqlTexts());
        assertTrue(plan.target().indexes().stream().anyMatch(index -> index.unique()
                && index.columns().equals(List.of("code"))));
        assertTrue(plan.target().indexes().stream().noneMatch(index -> index.columns().contains("note")));
    }

    @Test
    void fluentDecimalAndTypeDeclarationsReuseFieldValidation() {
        CreateOrAlterTableBuilder reactive = reactive(new EmptyReactiveExecutor());
        SyncCreateOrAlterTableBuilder sync = sync(new EmptySyncExecutor());
        assertThrows(IllegalArgumentException.class, () -> reactive.addColumn("amount").number(2, 3).commit());
        assertThrows(IllegalArgumentException.class, () -> sync.addColumn("amount").number(2, 3).commit());
        assertThrows(IllegalArgumentException.class, () -> reactive.addColumn("amount").number(0, 0).commit());
        assertThrows(IllegalArgumentException.class, () -> sync.addColumn("amount").number(0, 0).commit());
        assertThrows(IllegalArgumentException.class,
                () -> reactive.addColumn("bad").type("INT; DROP TABLE x").commit().plan().block());
        assertThrows(IllegalArgumentException.class,
                () -> sync.addColumn("bad").type("INT; DROP TABLE x").commit().plan());
        assertThrows(NullPointerException.class, () -> reactive.addColumn("id").generation(null));
        assertThrows(NullPointerException.class, () -> sync.addColumn("id").generation(null));
    }

    @Test
    void choosingAnotherColumnTypeDiscardsPreviousTypeDimensions() {
        CreateOrAlterTableBuilder reactive = reactive(new EmptyReactiveExecutor());
        SyncCreateOrAlterTableBuilder sync = sync(new EmptySyncExecutor());
        reactive.addColumn("name").number(32).varchar(80).commit()
                .addColumn("amount").varchar(80).number(19).commit()
                .addColumn("count").number(32).number(9).commit();
        sync.addColumn("name").number(32).varchar(80).commit()
                .addColumn("amount").varchar(80).number(19).commit()
                .addColumn("count").number(32).number(9).commit();
        List<DynamicField> expected = List.of(DynamicField.of("name", "VARCHAR").withLength(80),
                DynamicField.of("amount", "BIGINT"), DynamicField.of("count", "INTEGER"));
        assertEquals(expected, sync.draft().form().fields());
        assertEquals(expected, reactive.draft().form().fields());
        assertEquals(sync.plan().sqlTexts(), reactive.plan().block().sqlTexts());
    }

    @Test
    void completeFieldDefinitionsRetainAllPropertiesThroughBothFluentPaths() {
        List<DynamicField> fields = List.of(
                DynamicField.primaryKey("id", "BIGINT").generatedByIdentity(),
                DynamicField.of("code", "VARCHAR").withLength(80).withNullable(false).withUnique(true),
                DynamicField.of("amount", "DECIMAL").withPrecision(18, 4).withNullable(false),
                DynamicField.of("created_at", "TIMESTAMP").withPrecision(6, null).withComment("创建时间"),
                DynamicField.of("payload", "VARBINARY").withLength(128));
        CreateOrAlterTableBuilder reactive = reactive(new EmptyReactiveExecutor());
        SyncCreateOrAlterTableBuilder sync = sync(new EmptySyncExecutor());
        fields.forEach(reactive::addColumn);
        fields.forEach(sync::addColumn);
        reactive.addColumn("parent_id").number(19).commit()
                .addForeignKey("fk_orders_parent").column("parent_id")
                .referenceTable("parent_orders").referenceColumn("id").commit();
        sync.addColumn("parent_id").number(19).commit()
                .addForeignKey("fk_orders_parent").column("parent_id")
                .referenceTable("parent_orders").referenceColumn("id").commit();

        SchemaMigrationPlan asyncPlan = reactive.plan().block();
        SchemaMigrationPlan syncPlan = sync.plan();
        assertNotNull(asyncPlan);
        assertEquals(fields.stream().map(DynamicField::column).toList(),
                syncPlan.target().columns().subList(0, fields.size()));
        assertEquals(syncPlan.target().columns(), asyncPlan.target().columns());
        assertEquals(syncPlan.sqlTexts(), asyncPlan.sqlTexts());
        assertEquals(1, syncPlan.target().foreignKeys().size());
        assertTrue(syncPlan.target().indexes().stream().anyMatch(index -> index.unique()
                && index.columns().equals(List.of("code"))));
        String sql = String.join("\n", syncPlan.sqlTexts());
        assertTrue(sql.contains("identity"), sql);
        assertTrue(sql.contains("DECIMAL(18,4)"), sql);
        assertTrue(sql.contains("TIMESTAMP(6)"), sql);
    }

    @Test
    void suppliedFieldsKeepExistingNullAndDuplicateValidation() {
        CreateOrAlterTableBuilder reactive = reactive(new EmptyReactiveExecutor());
        SyncCreateOrAlterTableBuilder sync = sync(new EmptySyncExecutor());
        assertThrows(NullPointerException.class, () -> reactive.addColumn((DynamicField) null));
        assertThrows(NullPointerException.class, () -> sync.addColumn((DynamicField) null));
        DynamicField field = DynamicField.of("code", "VARCHAR(16)");
        reactive.addColumn(field).addColumn("code").varchar(16).commit();
        sync.addColumn(field).addColumn("code").varchar(16).commit();
        assertMessage("duplicate dynamic field name", reactive::plan);
        assertMessage("duplicate dynamic field name", sync::plan);
    }

    @Test
    void syncAndReactiveBuildersProduceTheSameCompleteCreatePlan() {
        SchemaMigrationPlan reactivePlan = describe(reactive(new EmptyReactiveExecutor())).plan().block();
        SchemaMigrationPlan syncPlan = describe(sync(new EmptySyncExecutor())).plan();

        assertNotNull(reactivePlan);
        assertAll(
                () -> assertFalse(reactivePlan.tableExists()),
                () -> assertFalse(syncPlan.tableExists()),
                () -> assertEquals(syncPlan.target().columns(), reactivePlan.target().columns()),
                () -> assertEquals(indexes(syncPlan), indexes(reactivePlan)),
                () -> assertEquals(syncPlan.target().foreignKeys(), reactivePlan.target().foreignKeys()),
                () -> assertEquals(syncPlan.sqlTexts(), reactivePlan.sqlTexts()));
    }

    @Test
    void interleavedChildrenCommitInCommitOrderAndUncommittedChildrenStayOut() {
        CreateOrAlterTableBuilder reactive = reactive(new EmptyReactiveExecutor());
        ColumnBuilder<CreateOrAlterTableBuilder> reactiveFirst = reactive.addColumn().name("first").number(10);
        ColumnBuilder<CreateOrAlterTableBuilder> reactiveSecond = reactive.addColumn().name("second").varchar(32);
        reactiveSecond.commit();
        reactive.addColumn().name("not_committed").varchar(8);
        reactiveFirst.commit();

        SyncCreateOrAlterTableBuilder sync = sync(new EmptySyncExecutor());
        ColumnBuilder<SyncCreateOrAlterTableBuilder> syncFirst = sync.addColumn().name("first").number(10);
        ColumnBuilder<SyncCreateOrAlterTableBuilder> syncSecond = sync.addColumn().name("second").varchar(32);
        syncSecond.commit();
        sync.addColumn().name("not_committed").varchar(8);
        syncFirst.commit();

        assertEquals(List.of("second", "first"), fieldNames(reactive.plan().block()));
        assertEquals(List.of("second", "first"), fieldNames(sync.plan()));
    }

    @Test
    void repeatedColumnCommitStillFailsWhenTheTableDraftIsMaterialized() {
        CreateOrAlterTableBuilder reactive = reactive(new EmptyReactiveExecutor());
        ColumnBuilder<CreateOrAlterTableBuilder> reactiveColumn = reactive.addColumn().name("id").number(10);
        reactiveColumn.commit();
        reactiveColumn.commit();

        SyncCreateOrAlterTableBuilder sync = sync(new EmptySyncExecutor());
        ColumnBuilder<SyncCreateOrAlterTableBuilder> syncColumn = sync.addColumn().name("id").number(10);
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
        return plan.target().columns().stream().map(field -> field.name()).toList();
    }

    private static List<IndexShape> indexes(SchemaMigrationPlan plan) {
        return plan.target().indexes().stream().map(IndexShape::new).toList();
    }

    private static void assertMessage(String message, Runnable operation) {
        assertMessage(IllegalArgumentException.class, message, operation);
    }

    private static <T extends Throwable> void assertMessage(Class<T> type, String message, Runnable operation) {
        T failure = assertThrows(type, operation::run);
        assertEquals(message, failure.getMessage());
    }

    private record IndexShape(String name, boolean unique, List<String> columns) {

        private IndexShape(IndexDefinition index) {
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

    private static final class EmptySyncExecutor implements com.flying.orm.rdb.support.ListQueryFixture {

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            return List.of();
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) {
            throw new UnsupportedOperationException();
        }
    }
}
