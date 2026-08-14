package com.flying.orm.rdb.operator;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnMetadata;
import com.flying.orm.core.metadata.ForeignKeyMetadata;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.RelationTermPackage;
import com.flying.orm.core.sql.render.SqlTermPackage;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.scope.ScopeErrorCode;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.metadata.MetadataCacheInvalidator;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.schema.ReactiveSchemaClient;
import com.flying.orm.rdb.schema.SchemaMigrationPlan;
import com.flying.orm.rdb.schema.SkippedSchemaChange;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 覆盖链式 Operator 与动态表单共享 SQL、安全 scope、DDL 和同步桥接行为。 */
class DatabaseOperatorTest {

    @Test
    void buildsParameterizedQueryFromOperatorDml() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        DatabaseOperator operator = DatabaseOperator.create(executor,
                                                            SqlRenderer.builder().addDefaultTerms().build(),
                                                            RdbDialect.mysql());

        SqlRequest request = operator.dml()
                                     .query()
                                     .select("id", "profile.name")
                                     .from("test_table")
                                     .where(dsl -> dsl.is("name", "Alice"))
                                     .toRequest();

        assertEquals("select `id`, `profile`.`name` from `test_table` where `name` = ?", request.sql());
        assertEquals(List.of("Alice"), request.parameters());
    }

    /** Operator 自动沿用 renderer 的 term 形状，集合值不需要调用方再装一份条件注册表。 */
    @Test
    void operatorWhereUsesCustomTermsFromRenderer() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        SqlTermPackage relations = RelationTermPackage.of("organization-relations",
                                                          "org_user",
                                                          "ou",
                                                          "user_id",
                                                          "org_id",
                                                          "user-in-org",
                                                          "user-not-in-org");
        SqlRenderer renderer = SqlRenderer.builder()
                                          .addDefaultTerms()
                                          .addTermPackage(relations)
                                          .build();
        DatabaseOperator operator = DatabaseOperator.create(executor, renderer, RdbDialect.mysql());

        SqlRequest request = operator.dml()
                                     .query()
                                     .select("id")
                                     .from("Users")
                                     .where(where -> where.term("id",
                                                               "user-in-org",
                                                               List.of("org-1", "org-2")))
                                     .toRequest();

        assertEquals("select `id` from `Users` where exists (select 1 from `org_user` `ou` "
                             + "where `ou`.`user_id` = `id` and `ou`.`org_id` in (?, ?))",
                     request.sql());
        assertEquals(List.of("org-1", "org-2"), request.parameters());
    }

    @Test
    void rejectsUnsafeQueryIdentifiers() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        DatabaseOperator operator = DatabaseOperator.create(executor,
                                                            SqlRenderer.builder().addDefaultTerms().build(),
                                                            RdbDialect.mysql());

        QueryOperator query = operator.dml().query();

        assertThrows(IllegalArgumentException.class, () -> query.from("users; drop table users"));
        assertThrows(IllegalArgumentException.class, () -> query.select("count(*)"));
        assertThrows(IllegalArgumentException.class, () -> query.where(dsl -> dsl.is("name or 1=1", "Alice")));
    }

    @Test
    void queryOperatorPassesExecutionOptionsToExecutor() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        DatabaseOperator operator = DatabaseOperator.create(executor,
                                                            SqlRenderer.builder().addDefaultTerms().build(),
                                                            RdbDialect.mysql());
        SqlExecutionOptions options = SqlExecutionOptions.maxRows(5).withTimeout(Duration.ofSeconds(1));

        StepVerifier.create(operator.dml()
                                    .query()
                                    .select("id")
                                    .from("test_table")
                                    .fetchMap(options))
                    .verifyComplete();

        assertEquals(options, executor.options());
    }

    @Test
    void databaseOperatorCanSetDefaultExecutionOptions() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        SqlExecutionOptions options = SqlExecutionOptions.maxRows(7).withTimeout(Duration.ofSeconds(2));
        DatabaseOperator operator = DatabaseOperator.create(executor.withDefaultExecutionOptions(options),
                                                             SqlRenderer.builder().addDefaultTerms().build(),
                                                             RdbDialect.mysql());

        StepVerifier.create(operator.dml()
                                    .query()
                                    .select("id")
                                    .from("test_table")
                                    .fetchMap())
                    .verifyComplete();

        assertEquals(options, executor.options());
    }

    @Test
    void dmlUpdateOperatorSupportsOptimisticLock() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        DatabaseOperator operator = DatabaseOperator.create(executor,
                                                            SqlRenderer.builder().addDefaultTerms().build(),
                                                            RdbDialect.mysql());

        StepVerifier.create(operator.dml()
                                    .update("Users")
                                    .set("name", "Alice 2")
                                    .where(where -> where.is("id", 1L))
                                    .optimisticLock(OptimisticLockOptions.increment("version", 3))
                                    .execute())
                    .expectNext(1L)
                    .verifyComplete();

        assertEquals("update `Users` set `name` = ?, `version` = `version` + 1 where `id` = ? and `version` = ?",
                     executor.requests().getFirst().sql());
        assertEquals(List.of("Alice 2", 1L, 3), executor.requests().getFirst().parameters());
    }

    @Test
    void dmlDeleteOperatorSupportsOptimisticLock() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        DatabaseOperator operator = DatabaseOperator.create(executor,
                                                            SqlRenderer.builder().addDefaultTerms().build(),
                                                            RdbDialect.mysql());

        StepVerifier.create(operator.dml()
                                    .delete("Users")
                                    .where(where -> where.is("id", 1L))
                                    .optimisticLock(OptimisticLockOptions.increment("version", 3))
                                    .execute())
                    .expectNext(1L)
                    .verifyComplete();

        assertEquals("delete from `Users` where `id` = ? and `version` = ?", executor.requests().getFirst().sql());
        assertEquals(List.of(1L, 3), executor.requests().getFirst().parameters());
    }

    @Test
    void dmlOperatorsCanDeclareLogicDelete() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        DatabaseOperator operator = DatabaseOperator.create(executor,
                                                            SqlRenderer.builder().addDefaultTerms().build(),
                                                            RdbDialect.mysql());

        StepVerifier.create(operator.dml()
                                    .query()
                                    .select("id")
                                    .from("Users")
                                    .where(where -> where.is("id", 1L))
                                    .logicDelete("deleted", 0, 1)
                                    .fetchMap())
                    .verifyComplete();
        assertEquals("select `id` from `Users` where `id` = ? and `deleted` = ?",
                     executor.requests().get(0).sql());
        assertEquals(List.of(1L, 0), executor.requests().get(0).parameters());

        StepVerifier.create(operator.dml()
                                    .update("Users")
                                    .set("name", "Alice 2")
                                    .where(where -> where.is("id", 1L))
                                    .logicDelete("deleted", 0, 1)
                                    .execute())
                    .expectNext(1L)
                    .verifyComplete();
        assertEquals("update `Users` set `name` = ? where `id` = ? and `deleted` = ?",
                     executor.requests().get(1).sql());
        assertEquals(List.of("Alice 2", 1L, 0), executor.requests().get(1).parameters());

        StepVerifier.create(operator.dml()
                                    .delete("Users")
                                    .where(where -> where.is("id", 1L))
                                    .logicDelete("deleted", 0, 1)
                                    .execute())
                    .expectNext(1L)
                    .verifyComplete();
        assertEquals("update `Users` set `deleted` = ? where `id` = ? and `deleted` = ?",
                     executor.requests().get(2).sql());
        assertEquals(List.of(1, 1L, 0), executor.requests().get(2).parameters());
    }

    @Test
    void dmlDeleteOperatorCanForcePhysicalDelete() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        DatabaseOperator operator = DatabaseOperator.create(executor,
                                                            SqlRenderer.builder().addDefaultTerms().build(),
                                                            RdbDialect.mysql());

        StepVerifier.create(operator.dml()
                                    .delete("Users")
                                    .where(where -> where.is("id", 1L))
                                    .logicDelete("deleted", 0, 1)
                                    .physical()
                                    .execute())
                    .expectNext(1L)
                    .verifyComplete();

        assertEquals("delete from `Users` where `id` = ?", executor.requests().getFirst().sql());
        assertEquals(List.of(1L), executor.requests().getFirst().parameters());
    }

    @Test
    void dmlOperatorsCanApplyServerDataScope() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        DatabaseOperator operator = DatabaseOperator.create(executor,
                                                            SqlRenderer.builder().addDefaultTerms().build(),
                                                            RdbDialect.mysql());
        DataScope scope = DataScope.tenant("tenant_id", "t1");

        StepVerifier.create(operator.dml()
                                    .query()
                                    .select("id")
                                    .from("Users")
                                    .where(where -> where.is("id", 1L))
                                    .scope(scope)
                                    .fetchMap())
                    .verifyComplete();
        assertEquals("select `id` from `Users` where `id` = ? and `tenant_id` = ?",
                     executor.requests().get(0).sql());
        assertEquals(List.of(1L, "t1"), executor.requests().get(0).parameters());

        StepVerifier.create(operator.dml()
                                    .delete("Users")
                                    .where(where -> where.is("id", 1L))
                                    .scope(scope)
                                    .execute())
                    .expectNext(1L)
                    .verifyComplete();
        assertEquals("delete from `Users` where `id` = ? and `tenant_id` = ?", executor.requests().get(1).sql());
        assertEquals(List.of(1L, "t1"), executor.requests().get(1).parameters());
    }

    /** 连续追加的显式 Scope 必须全部保留，后一次调用不能放宽前一次范围。 */
    @Test
    void repeatedExplicitScopesOnlyNarrowDynamicQueryAndWrite() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        DatabaseOperator operator = DatabaseOperator.create(executor,
                                                            SqlRenderer.builder().addDefaultTerms().build(),
                                                            RdbDialect.mysql());
        DataScope tenant = DataScope.tenant("tenant_id", "t1");
        DataScope organization = DataScope.where(ConditionGroup.and().where("org_id", "=", "o1").build());

        StepVerifier.create(operator.dml()
                                    .query()
                                    .select("id")
                                    .from("Users")
                                    .where(where -> where.is("id", 1L))
                                    .scope(tenant)
                                    .scope(organization)
                                    .fetchMap())
                    .verifyComplete();
        assertEquals("select `id` from `Users` where `id` = ? and `tenant_id` = ? and `org_id` = ?",
                     executor.requests().get(0).sql());
        assertEquals(List.of(1L, "t1", "o1"), executor.requests().get(0).parameters());

        StepVerifier.create(operator.dml()
                                    .update("Users")
                                    .set("name", "Alice 2")
                                    .where(where -> where.is("id", 1L))
                                    .scope(tenant)
                                    .scope(organization)
                                    .execute())
                    .expectNext(1L)
                    .verifyComplete();
        assertEquals("update `Users` set `name` = ? where `id` = ? and `tenant_id` = ? and `org_id` = ?",
                     executor.requests().get(1).sql());
        assertEquals(List.of("Alice 2", 1L, "t1", "o1"), executor.requests().get(1).parameters());
    }

    @Test
    void databaseOperatorCanSetDefaultDataScopeForDml() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        DatabaseOperator operator = DatabaseOperator.create(executor,
                                                            SqlRenderer.builder().addDefaultTerms().build(),
                                                            RdbDialect.mysql())
                                                    .withDefaultDataScope(DataScope.tenant("tenant_id", "t1"));

        StepVerifier.create(operator.dml()
                                    .query()
                                    .select("id")
                                    .from("Users")
                                    .where(where -> where.is("id", 1L))
                                    .fetchMap())
                    .verifyComplete();
        assertEquals("select `id` from `Users` where `id` = ? and `tenant_id` = ?",
                     executor.requests().get(0).sql());
        assertEquals(List.of(1L, "t1"), executor.requests().get(0).parameters());

        StepVerifier.create(operator.dml()
                                    .update("Users")
                                    .set("name", "Alice 2")
                                    .where(where -> where.is("id", 1L))
                                    .execute())
                    .expectNext(1L)
                    .verifyComplete();
        assertEquals("update `Users` set `name` = ? where `id` = ? and `tenant_id` = ?",
                     executor.requests().get(1).sql());
        assertEquals(List.of("Alice 2", 1L, "t1"), executor.requests().get(1).parameters());
    }

    /** Operator 被分层装配多次时，后加的组织范围也不能覆盖先加的租户范围。 */
    @Test
    void repeatedDefaultDataScopeCanOnlyNarrowOperatorAccess() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        DatabaseOperator operator = DatabaseOperator.create(executor,
                                                            SqlRenderer.builder().addDefaultTerms().build(),
                                                            RdbDialect.mysql())
                                                    .withDefaultDataScope(DataScope.tenant("tenant_id", "t1"))
                                                    .withDefaultDataScope(DataScope.where(
                                                            ConditionGroup.and()
                                                                          .where("org_id", "=", "o1")
                                                                          .build()));

        StepVerifier.create(operator.dml()
                                    .query()
                                    .select("id")
                                    .from("Users")
                                    .where(where -> where.is("id", 1L))
                                    .fetchMap())
                    .verifyComplete();

        assertEquals("select `id` from `Users` where `id` = ? and `tenant_id` = ? and `org_id` = ?",
                     executor.requests().get(0).sql());
        assertEquals(List.of(1L, "t1", "o1"), executor.requests().get(0).parameters());
    }

    @Test
    void databaseOperatorDefaultFieldScopeProtectsDmlQueryAndUpdate() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        DatabaseOperator operator = DatabaseOperator.create(executor,
                                                            SqlRenderer.builder().addDefaultTerms().build(),
                                                    RdbDialect.mysql())
                                                    .withDefaultDataScope(DataScope.none()
                                                                                  .withFields(new FieldScope(
                                                                                          new java.util.LinkedHashSet<>(
                                                                                                  List.of("id",
                                                                                                          "name")),
                                                                                          java.util.Set.of("name"))));

        SqlRequest request = operator.dml()
                                     .query()
                                     .from("Users")
                                     .where(where -> where.is("id", 1L))
                                     .toRequest();
        assertEquals("select `id`, `name` from `Users` where `id` = ?", request.sql());
        assertEquals(List.of(1L), request.parameters());

        ScopeAccessException readError = assertThrows(
                ScopeAccessException.class,
                () -> operator.dml().query().select("password").from("Users").toRequest());
        assertEquals(ScopeErrorCode.FIELD_NOT_READABLE, readError.code());
        assertEquals("Users", readError.formId());
        assertEquals("password", readError.field());

        StepVerifier.create(operator.dml()
                                    .update("Users")
                                    .set("name", "Alice 2")
                                    .where(where -> where.is("id", 1L))
                                    .execute())
                    .expectNext(1L)
                    .verifyComplete();

        ScopeAccessException writeError = assertThrows(
                ScopeAccessException.class,
                () -> operator.dml()
                              .update("Users")
                              .set("password", "secret")
                              .where(where -> where.is("id", 1L))
                              .execute()
                              .block());
        assertEquals(ScopeErrorCode.FIELD_NOT_WRITABLE, writeError.code());
        assertEquals("Users", writeError.formId());
    }

    /** 默认范围与显式范围没有可读字段交集时，应在生成 SQL 前返回稳定权限错误。 */
    @Test
    void databaseOperatorRejectsQueryWhenFieldScopesHaveNoIntersection() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        DatabaseOperator operator = DatabaseOperator.create(executor,
                                                            SqlRenderer.builder().addDefaultTerms().build(),
                                                            RdbDialect.mysql())
                                                    .withDefaultDataScope(DataScope.none()
                                                                                  .withFields(FieldScope.readable("id")));

        ScopeAccessException error = assertThrows(
                ScopeAccessException.class,
                () -> operator.dml()
                              .query()
                              .from("Users")
                              .scope(DataScope.none().withFields(FieldScope.readable("name")))
                              .toRequest());

        assertEquals(ScopeErrorCode.NO_READABLE_FIELDS, error.code());
        assertEquals("Users", error.formId());
    }



    /** 同步门面不能占住 Reactor 事件线程，否则同一调度器上的响应式任务可能互相等待。 */

    /** 没有等待窗口的同步调用没有可靠语义，在创建门面时就直接说清楚。 */

    @Test
    void createsTableFromOperatorDdl() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        DatabaseOperator operator = operatorWithMissingMetadata(executor, RdbDialect.mysql());

        StepVerifier.create(operator.ddl()
                                    .createOrAlter("test_table")
                                    .addColumn().name("id").number(32).primaryKey().comment("ID").commit()
                                    .addColumn().name("name").varchar(128).comment("Name").commit()
                                    .commit())
                    .expectNext(1L)
                    .verifyComplete();

        assertEquals(List.of("create table `test_table` (`id` DECIMAL(32,0) primary key comment 'ID', "
                                     + "`name` VARCHAR(128) comment 'Name')"),
                     executor.sqlRequests());
    }



    @Test
    void metadataOperatorCanInvalidateReaderCache() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().build();
        RdbDialect dialect = RdbDialect.h2();
        InvalidatingMetadataReader metadataReader = new InvalidatingMetadataReader();
        DatabaseOperator operator = DatabaseOperator.create(ReactiveSchemaClient.create(executor, dialect),
                                                            ReactiveFormClient.create(
                                                                    executor,
                                                                    FormDataSqlRenderer.create(renderer, dialect)),
                                                            executor,
                                                            renderer,
                                                            metadataReader,
                                                            dialect);

        operator.metadata().invalidate("Users");
        operator.metadata().invalidateAll();

        assertEquals(List.of("Users", "*"), metadataReader.invalidated());
    }

    @Test
    void createOrAlterInvalidatesMetadataCacheAfterSuccessfulCommit() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().build();
        RdbDialect dialect = RdbDialect.h2();
        InvalidatingMetadataReader metadataReader = new InvalidatingMetadataReader();
        DatabaseOperator operator = DatabaseOperator.create(ReactiveSchemaClient.create(executor, dialect),
                                                            ReactiveFormClient.create(
                                                                    executor,
                                                                    FormDataSqlRenderer.create(renderer, dialect)),
                                                            executor,
                                                            renderer,
                                                            metadataReader,
                                                            dialect);

        StepVerifier.create(operator.ddl()
                                    .createOrAlter("Users")
                                    .addColumn().name("id").number(19).primaryKey().commit()
                                    .commit())
                    .expectNext(1L)
                    .verifyComplete();

        assertEquals(List.of("Users"), metadataReader.invalidated());
    }

    @Test
    void reportsDdlPlanAndDetailedResultToCaller() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().build();
        RdbDialect dialect = RdbDialect.h2();
        TableMetadata current = TableMetadata.builder("Users")
                                             .addColumn(ColumnMetadata.primaryKey("id", "BIGINT"))
                                             .addColumn(ColumnMetadata.of("legacy", "VARCHAR"))
                                             .build();
        DatabaseOperator operator = DatabaseOperator.create(ReactiveSchemaClient.create(executor, dialect),
                                                            ReactiveFormClient.create(
                                                                    executor,
                                                                    FormDataSqlRenderer.create(renderer, dialect)),
                                                            executor,
                                                            renderer,
                                                            new FixedTableMetadataReader(current),
                                                            dialect);

        CreateOrAlterTableBuilder builder = operator.ddl()
                                                    .createOrAlter("Users")
                                                    .addColumn().name("id").number(19).primaryKey().commit()
                                                    .addColumn().name("name").varchar(64).commit();

        StepVerifier.create(builder.plan())
                    .assertNext(plan -> {
                        assertEquals(1, plan.executableSqlCount());
                        assertEquals(SkippedSchemaChange.Kind.DROP_COLUMN, plan.skippedChanges().getFirst().kind());
                    })
                    .verifyComplete();

        StepVerifier.create(builder.commitDetailed())
                    .assertNext(result -> {
                        assertEquals(1L, result.rowsUpdated());
                        assertEquals(1, result.executedSqlCount());
                        assertEquals(SkippedSchemaChange.Kind.DROP_COLUMN,
                                     result.plan().skippedChanges().getFirst().kind());
                    })
                    .verifyComplete();
    }

    @Test
    void addsForeignKeyToReactiveCreateOrAlterPlan() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().build();
        RdbDialect dialect = RdbDialect.h2();
        TableMetadata current = TableMetadata.builder("Orders")
                                             .addColumn(ColumnMetadata.primaryKey("id", "BIGINT"))
                                             .addColumn(ColumnMetadata.of("user_id", "BIGINT"))
                                             .build();
        DatabaseOperator operator = DatabaseOperator.create(ReactiveSchemaClient.create(executor, dialect),
                                                            ReactiveFormClient.create(
                                                                    executor,
                                                                    FormDataSqlRenderer.create(renderer, dialect)),
                                                            executor,
                                                            renderer,
                                                            new FixedTableMetadataReader(current),
                                                            dialect);

        StepVerifier.create(operator.ddl()
                                    .createOrAlter("Orders")
                                    .addColumn().name("id").number(19).primaryKey().commit()
                                    .addColumn().name("user_id").number(19).commit()
                                    .addForeignKey("fk_orders_user")
                                    .column("user_id")
                                    .referenceTable("Users")
                                    .referenceColumn("id")
                                    .commit()
                                    .plan())
                    .assertNext(plan -> {
                        ForeignKeyMetadata foreignKey = plan.targetForeignKeys().getFirst();
                        assertEquals("fk_orders_user", foreignKey.name());
                        assertEquals(List.of("user_id"), foreignKey.columns());
                        assertEquals("Users", foreignKey.referenceTable());
                        assertEquals(List.of("id"), foreignKey.referenceColumns());
                        assertEquals(SkippedSchemaChange.Kind.ADD_FOREIGN_KEY,
                                     plan.skippedChanges().getFirst().kind());
                    })
                    .verifyComplete();
    }


    private static DatabaseOperator operatorWithMissingMetadata(RecordingSqlExecutor executor, RdbDialect dialect) {
        SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().build();
        return DatabaseOperator.create(ReactiveSchemaClient.create(executor, dialect),
                                       ReactiveFormClient.create(
                                               executor,
                                               FormDataSqlRenderer.create(renderer, dialect)),
                                       executor,
                                       renderer,
                                       new MissingMetadataReader(),
                                       dialect);
    }

    private record FixedMetadataReader(DynamicForm form) implements ReactiveFormMetadataReader {

        @Override
        public Mono<DynamicForm> readForm(String formId, String table) {
            return Mono.just(form);
        }

        @Override
        public Mono<DynamicForm> readForm(String formId, String schema, String table) {
            return Mono.just(form);
        }
    }

    private record FixedTableMetadataReader(TableMetadata table) implements ReactiveFormMetadataReader {

        @Override
        public Mono<DynamicForm> readForm(String formId, String table) {
            return Mono.error(new UnsupportedOperationException("table metadata test reads TableMetadata directly"));
        }

        @Override
        public Mono<DynamicForm> readForm(String formId, String schema, String table) {
            return Mono.error(new UnsupportedOperationException("table metadata test reads TableMetadata directly"));
        }

        @Override
        public Mono<TableMetadata> readTable(String table) {
            return Mono.just(this.table);
        }

        @Override
        public Mono<TableMetadata> readTable(String schema, String table) {
            return Mono.just(this.table);
        }
    }

    private static final class InvalidatingMetadataReader implements ReactiveFormMetadataReader, MetadataCacheInvalidator {

        private final List<String> invalidated = new ArrayList<>();

        @Override
        public Mono<DynamicForm> readForm(String formId, String table) {
            return Mono.error(new IllegalArgumentException("table metadata not found"));
        }

        @Override
        public Mono<DynamicForm> readForm(String formId, String schema, String table) {
            return Mono.error(new IllegalArgumentException("table metadata not found"));
        }

        @Override
        public void invalidate(String table) {
            invalidated.add(table);
        }

        @Override
        public void invalidateAll() {
            invalidated.add("*");
        }

        private List<String> invalidated() {
            return invalidated;
        }
    }

    private static final class MissingMetadataReader implements ReactiveFormMetadataReader {

        @Override
        public Mono<DynamicForm> readForm(String formId, String table) {
            return Mono.error(new IllegalArgumentException("table metadata not found"));
        }

        @Override
        public Mono<DynamicForm> readForm(String formId, String schema, String table) {
            return Mono.error(new IllegalArgumentException("table metadata not found"));
        }
    }

    private static final class RecordingSqlExecutor implements ReactiveSqlExecutor {

        private final List<SqlRequest> requests = new ArrayList<>();

        private SqlExecutionOptions options;

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            requests.add(request);
            return Flux.empty();
        }

        @Override
        public Flux<DynamicRow> query(SqlRequest request, SqlExecutionOptions options) {
            this.options = options;
            return query(request);
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            requests.add(request);
            return Mono.just(1L);
        }

        private List<String> sqlRequests() {
            return requests.stream().map(SqlRequest::sql).toList();
        }

        private List<SqlRequest> requests() {
            return requests;
        }

        private SqlExecutionOptions options() {
            return options;
        }
    }
}
