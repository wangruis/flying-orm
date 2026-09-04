package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableLogic;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.aggregate.AggregateExpression;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RepositoryBoundFormLogicDeleteTest {

    @Test
    void syncBoundFormKeepsAnnotationLogicDeleteAcrossRepositoryEntryPoints() {
        RecordingSql sql = new RecordingSql();
        DynamicForm form = form();
        SyncFormRepository<Person> repository = SyncFormRepository.create(
                SyncFormClient.create(sql.sync(), batches(), renderer()), form, Person.class);
        repository.select(where());
        assertActive(sql.last);
        repository.delete(where());
        String ordinaryDelete = sql.last.sql();

        assertAll(
                () -> {
                    repository.createQuery().where(Person::getId, 7L).execute();
                    assertActive(sql.last);
                },
                () -> {
                    repository.aggregate(aggregate(form));
                    assertActive(sql.last);
                },
                () -> {
                    repository.createUpdate().set(Person::getName, "Ada").where(Person::getId, 7L).execute();
                    assertActive(sql.last);
                },
                () -> {
                    repository.createDelete().where(Person::getId, 7L).execute();
                    assertEquals(ordinaryDelete, sql.last.sql());
                    assertEquals(List.of(1, 7L, 0), sql.last.parameters());
                });
    }

    @Test
    void reactiveBoundFormKeepsAnnotationLogicDeleteAcrossRepositoryEntryPoints() {
        RecordingSql sql = new RecordingSql();
        DynamicForm form = form();
        ReactiveFormRepository<Person> repository = ReactiveFormRepository.create(
                ReactiveFormClient.create(sql.reactive(), renderer()), form, Person.class);
        repository.select(where()).collectList().block();
        assertActive(sql.last);
        repository.delete(where()).block();
        String ordinaryDelete = sql.last.sql();

        assertAll(
                () -> {
                    repository.createQuery().where(Person::getId, 7L).execute().collectList().block();
                    assertActive(sql.last);
                },
                () -> {
                    repository.aggregate(aggregate(form)).collectList().block();
                    assertActive(sql.last);
                },
                () -> {
                    repository.createUpdate().set(Person::getName, "Ada").where(Person::getId, 7L).execute().block();
                    assertActive(sql.last);
                },
                () -> {
                    repository.createDelete().where(Person::getId, 7L).execute().block();
                    assertEquals(ordinaryDelete, sql.last.sql());
                    assertEquals(List.of(1, 7L, 0), sql.last.parameters());
                });
    }

    private static void assertActive(SqlRequest request) {
        assertTrue(request.sql().contains("\"deleted\" = ?"), request.sql());
        assertTrue(request.parameters().contains(0), request.parameters().toString());
    }

    private static DynamicForm form() {
        return DynamicForm.builder("bound_people", "bound_people")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("name", "VARCHAR"))
                .addField(DynamicField.of("deleted", "INTEGER")).build();
    }

    private static ConditionGroup where() {
        return ConditionGroup.and().where("id", "=", 7L).build();
    }

    private static AggregateSpec aggregate(DynamicForm form) {
        return AggregateSpec.builder(QuerySpec.of(form, where()))
                .aggregate(AggregateExpression.count("id", "person_count")).build();
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
    }

    private static SyncBatchExecutor batches() {
        return new SyncBatchExecutor() {
            @Override public BatchWriteResult writeBatch(BatchWriteRequest request) {
                throw new UnsupportedOperationException();
            }
            @Override public List<BatchChunkResult> writeBatchChunks(BatchWriteRequest request) {
                throw new UnsupportedOperationException();
            }
        };
    }

    private static final class RecordingSql {
        private SqlRequest last;

        private SyncSqlExecutor sync() {
            return new SyncSqlExecutor() {
                @Override public List<DynamicRow> query(SqlRequest request) {
                    last = request;
                    return List.of();
                }
                @Override public long rowsUpdated(SqlRequest request) {
                    last = request;
                    return 1L;
                }
                @Override public SqlWriteResult rowsUpdatedReturningKeys(
                        SqlRequest request, SqlExecutionOptions options) {
                    throw new UnsupportedOperationException();
                }
            };
        }

        private ReactiveSqlExecutor reactive() {
            return new ReactiveSqlExecutor() {
                @Override public Flux<DynamicRow> query(SqlRequest request) {
                    return Flux.defer(() -> {
                        last = request;
                        return Flux.empty();
                    });
                }
                @Override public Mono<Long> rowsUpdated(SqlRequest request) {
                    return Mono.fromSupplier(() -> {
                        last = request;
                        return 1L;
                    });
                }
            };
        }
    }

    @TableName("people")
    public static final class Person {
        @TableId(type = IdType.INPUT)
        private Long id;
        private String name;
        @TableLogic
        private Integer deleted;

        public Person() { }
        public Long getId() { return id; }
        public void setId(Long id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public Integer getDeleted() { return deleted; }
        public void setDeleted(Integer deleted) { this.deleted = deleted; }
    }
}
