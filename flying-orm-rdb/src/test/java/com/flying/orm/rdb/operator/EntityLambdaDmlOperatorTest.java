package com.flying.orm.rdb.operator;

import com.flying.orm.core.annotation.EncryptedField;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.annotation.Version;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.internal.mapping.EntityMetadataResolver;
import com.flying.orm.rdb.internal.mapping.EntityPropertyResolver;
import com.flying.orm.rdb.mapping.FlyingLogicDelete;
import com.flying.orm.rdb.mapping.MappingException;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EntityLambdaDmlOperatorTest {

    /** 实体 Lambda 查询以轻量方法表达保护搜索和本次显示覆盖，不暴露物理盲索引列。 */
    @Test
    void buildsProtectedSearchAndDisplayOverrideWithEntityLambda() {
        RecordingExecutor executor = new RecordingExecutor();
        DatabaseOperator operator = DatabaseOperator.create(
                executor, SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.mysql());

        EntityDmlQueryOperator<ProtectedUser> query = operator.dml(ProtectedUser.class)
                                                                 .query()
                                                                 .exactEncrypted(
                                                                         ProtectedUser::getContact,
                                                                         "13800138000")
                                                                 .suffixEncrypted(
                                                                         ProtectedUser::getContact,
                                                                         "8000")
                                                                 .showSensitive();

        var spec = query.command().entitySpec();
        assertEquals(SensitiveDisplayMode.FULL, spec.sensitiveDisplayMode());
        assertEquals(List.of("protected-exact", "protected-suffix"),
                     spec.where().children().stream()
                         .map(node -> ((com.flying.orm.core.condition.TermCondition) node).operator())
                         .toList());
    }

    @Test
    void resolvesBeanGetterToMappedColumn() {
        var metadata = EntityMetadataResolver.createUncached(DimensionUser.class);
        assertEquals("user_id", EntityPropertyResolver.column(metadata, DimensionUser::getUserId));
        assertEquals("active", EntityPropertyResolver.column(metadata, DimensionUser::isActive));
    }

    @Test
    void rejectsComputedLambdaBeforeSqlGeneration() {
        assertThrows(MappingException.class,
                     () -> EntityPropertyResolver.column(
                             EntityMetadataResolver.createUncached(DimensionUser.class),
                             value -> value.getUserId()));
    }

    @Test
    void buildsUpdateAndDeleteWithoutTableOrFieldStrings() {
        RecordingExecutor executor = new RecordingExecutor();
        DatabaseOperator operator = DatabaseOperator.create(executor,
                                                            SqlRenderer.builder().addDefaultTerms().build(),
                                                            RdbDialect.mysql());

        StepVerifier.create(operator.dml(DimensionUser.class)
                                    .update()
                                    .set(DimensionUser::getName, "new-name")
                                    .where(DimensionUser::getUserId, "u-1")
                                    .and(DimensionUser::getDimensionId, "d-1")
                                    .optimisticLock(3L)
                                    .execute())
                    .expectNext(1L)
                    .verifyComplete();

        assertEquals("update `dimension_user` set `name` = ?, `version` = `version` + 1 "
                             + "where `user_id` = ? and `dimension_id` = ? and `deleted` = ? and `version` = ?",
                     executor.requests.get(0).sql());
        assertEquals(List.of("new-name", "u-1", "d-1", 0, 3L), executor.requests.get(0).parameters());

        StepVerifier.create(operator.dml(DimensionUser.class)
                                    .delete()
                                    .where(DimensionUser::getUserId, "u-1")
                                    .and(DimensionUser::getDimensionId, "d-1")
                                    .execute())
                    .expectNext(1L)
                    .verifyComplete();

        assertEquals("update `dimension_user` set `deleted` = ? where `user_id` = ? "
                             + "and `dimension_id` = ? and `deleted` = ?",
                     executor.requests.get(1).sql());
        assertEquals(List.of(1, "u-1", "d-1", 0), executor.requests.get(1).parameters());
    }

    /** 实体 Lambda 命令的连续 Scope 调用也只能继续收紧，不能替换前一层业务范围。 */
    @Test
    void repeatedExplicitScopesOnlyNarrowEntityQuery() {
        RecordingExecutor executor = new RecordingExecutor();
        DatabaseOperator operator = DatabaseOperator.create(executor,
                                                            SqlRenderer.builder().addDefaultTerms().build(),
                                                            RdbDialect.mysql());
        DataScope user = DataScope.tenant("user_id", "scope-user");
        DataScope dimension = DataScope.where(
                com.flying.orm.core.condition.ConditionGroup.and().where("dimension_id", "=", "scope-dimension")
                                                         .build());

        StepVerifier.create(operator.dml(DimensionUser.class)
                                    .query()
                                    .select(DimensionUser::getUserId)
                                    .scope(user)
                                    .scope(dimension)
                                    .executeRows())
                    .verifyComplete();

        assertEquals("select `user_id` from `dimension_user` where `user_id` = ? and `dimension_id` = ? and `deleted` = ?",
                     executor.requests.getFirst().sql());
        assertEquals(List.of("scope-user", "scope-dimension", 0), executor.requests.getFirst().parameters());
    }

    @Test
    void rendersBoundArithmeticUpdatesAndKeepsOptimisticLock() {
        RecordingExecutor executor = new RecordingExecutor();
        DatabaseOperator operator = DatabaseOperator.create(executor,
                                                            SqlRenderer.builder().addDefaultTerms().build(),
                                                            RdbDialect.mysql());

        StepVerifier.create(operator.dml(DimensionUser.class)
                                    .update()
                                    .increment(DimensionUser::getScore, 5)
                                    .decrement(DimensionUser::getBalance, 1.25)
                                    .where(DimensionUser::getUserId, "u-1")
                                    .optimisticLock(3L)
                                    .execute())
                    .expectNext(1L)
                    .verifyComplete();

        assertEquals("update `dimension_user` set `score` = `score` + ?, `balance` = `balance` + ?, "
                             + "`version` = `version` + 1 where `user_id` = ? and `deleted` = ? and `version` = ?",
                     executor.requests.getFirst().sql());
        assertEquals(List.of(new java.math.BigDecimal("5"), new java.math.BigDecimal("-1.25"), "u-1", 0, 3L),
                     executor.requests.getFirst().parameters());
    }

    @Test
    void rejectsUnsafeAssignmentsAndInvalidAutomaticVersionsBeforeExecution() {
        RecordingExecutor executor = new RecordingExecutor();
        DatabaseOperator operator = DatabaseOperator.create(executor,
                                                            SqlRenderer.builder().addDefaultTerms().build(),
                                                            RdbDialect.mysql());

        assertThrows(MappingException.class,
                     () -> operator.dml(DimensionUser.class).update()
                                   .set(DimensionUser::getVersion, 4L));
        assertThrows(MappingException.class,
                     () -> operator.dml(DimensionUser.class).update()
                                   .optimisticLock(null));
        assertThrows(MappingException.class,
                     () -> operator.dml(StringVersionEntity.class).update()
                                   .optimisticLock("v1"));
        assertEquals(0, executor.requests.size());
    }

    @Test
    void rendersCollectionRangeAndNullLambdaConditionsWithBoundParameters() {
        RecordingExecutor executor = new RecordingExecutor();
        DatabaseOperator operator = DatabaseOperator.create(executor,
                                                            SqlRenderer.builder().addDefaultTerms().build(),
                                                            RdbDialect.mysql());

        StepVerifier.create(operator.dml(DimensionUser.class)
                                    .query()
                                    .in(DimensionUser::getUserId, List.of("u-1", "u-2"))
                                    .between(DimensionUser::getVersion, 1L, 9L)
                                    .isNotNull(DimensionUser::getName)
                                    .orderByAsc(DimensionUser::getUserId)
                                    .execute())
                    .verifyComplete();

        assertEquals("select `user_id`, `dimension_id`, `name`, `active`, `created_by`, `deleted`, `version`, "
                             + "`score`, `balance` "
                             + "from `dimension_user` where `user_id` in (?, ?) and `version` between ? and ? "
                             + "and `name` is not null and `deleted` = ? order by `user_id` asc",
                     executor.requests.getFirst().sql());
        assertEquals(List.of("u-1", "u-2", 1L, 9L, 0), executor.requests.getFirst().parameters());
    }

    @Test
    void rendersSafeLambdaProjectionGroupAndOrderWithoutPartialEntityMapping() {
        RecordingExecutor executor = new RecordingExecutor();
        DatabaseOperator operator = DatabaseOperator.create(executor,
                                                            SqlRenderer.builder().addDefaultTerms().build(),
                                                            RdbDialect.mysql());

        StepVerifier.create(operator.dml(DimensionUser.class)
                                    .query()
                                    .select(DimensionUser::getDimensionId)
                                    .groupBy(DimensionUser::getDimensionId)
                                    .orderByDesc(DimensionUser::getDimensionId)
                                    .where(DimensionUser::isActive, true)
                                    .executeRows())
                    .verifyComplete();

        assertEquals("select `dimension_id` from `dimension_user` where `active` = ? and `deleted` = ? "
                             + "group by `dimension_id` order by `dimension_id` desc",
                     executor.requests.getFirst().sql());
        assertEquals(List.of(true, 0), executor.requests.getFirst().parameters());
        assertThrows(IllegalStateException.class,
                     () -> operator.dml(DimensionUser.class).query()
                                   .select(DimensionUser::getName).execute());
    }

    @Test
    void keepsNestedLambdaOrGroupInsideOuterSafetyConditions() {
        RecordingExecutor executor = new RecordingExecutor();
        DatabaseOperator operator = DatabaseOperator.create(executor,
                                                            SqlRenderer.builder().addDefaultTerms().build(),
                                                            RdbDialect.mysql());

        StepVerifier.create(operator.dml(DimensionUser.class).query()
                                    .where(DimensionUser::isActive, true)
                                    .or(group -> group.where(DimensionUser::getName, "alpha")
                                                      .where(DimensionUser::getName, "beta"))
                                    .execute())
                    .verifyComplete();

        assertEquals("select `user_id`, `dimension_id`, `name`, `active`, `created_by`, `deleted`, `version`, "
                             + "`score`, `balance` "
                             + "from `dimension_user` where `active` = ? and (`name` = ? or `name` = ?) "
                             + "and `deleted` = ?",
                     executor.requests.getFirst().sql());
        assertEquals(List.of(true, "alpha", "beta", 0), executor.requests.getFirst().parameters());
    }




    @TableName("dimension_user")
    @FlyingLogicDelete(field = "deleted")
    private static final class DimensionUser {
        @TableField("user_id")
        private String userId;
        @TableField("dimension_id")
        private String dimensionId;
        private String name;
        private boolean active;
        private String createdBy;
        private int deleted;
        @Version
        private long version;
        private int score;
        private double balance;

        String getUserId() {
            return userId;
        }

        String getDimensionId() {
            return dimensionId;
        }

        String getName() {
            return name;
        }

        boolean isActive() {
            return active;
        }

        String getCreatedBy() {
            return createdBy;
        }

        long getVersion() {
            return version;
        }

        int getScore() {
            return score;
        }

        double getBalance() {
            return balance;
        }
    }

    @TableName("string_version_entity")
    private static final class StringVersionEntity {
        @Version
        private String version;
    }

    @TableName("protected_user")
    private static final class ProtectedUser {
        @EncryptedField(search = {EncryptedSearchMode.EXACT, EncryptedSearchMode.SUFFIX},
                        normalizer = "digits", suffixLengths = 4)
        private String contact;

        String getContact() {
            return contact;
        }
    }

    private static final class RecordingExecutor implements ReactiveSqlExecutor {
        private final List<SqlRequest> requests = new ArrayList<>();

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            requests.add(request);
            return Flux.empty();
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            requests.add(request);
            return Mono.just(1L);
        }
    }
}
