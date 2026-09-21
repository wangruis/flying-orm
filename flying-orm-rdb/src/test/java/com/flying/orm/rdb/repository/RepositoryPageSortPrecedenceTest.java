package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.OrderBy;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RepositoryPageSortPrecedenceTest {

    @Test
    void syncExplicitPageSortOverridesAnnotationDefault() {
        repositoryPage(false, PageQuery.of(1, 10, PageSort.desc("name")), "\"name\" desc");
    }

    @Test
    void reactiveExplicitPageSortOverridesAnnotationDefault() {
        repositoryPage(true, PageQuery.of(1, 10, PageSort.desc("name")), "\"name\" desc");
    }

    @Test
    void syncUnsortedPageKeepsAnnotationDefault() {
        repositoryPage(false, PageQuery.of(1, 10), "\"id\" asc");
    }

    @Test
    void reactiveUnsortedPageKeepsAnnotationDefault() {
        repositoryPage(true, PageQuery.of(1, 10), "\"id\" asc");
    }

    @Test
    void syncExplicitQuerySpecSortKeepsItsPriorityOverPageSort() {
        explicitSpecPage(false);
    }

    @Test
    void reactiveExplicitQuerySpecSortKeepsItsPriorityOverPageSort() {
        explicitSpecPage(true);
    }

    private static void repositoryPage(boolean reactive, PageQuery page, String expectedOrder) {
        RecordingSql sql = new RecordingSql();
        if (reactive) {
            ReactiveFormClient client = ReactiveFormClient.create(sql.reactive(), renderer());
            DynamicForm form = client.entityModels().metadata(Person.class).toDynamicForm();
            var result = ReactiveFormRepository.create(client, form, Person.class)
                    .page(where(), page).block();
            assertEquals(1L, result.total());
        } else {
            SyncFormClient client = SyncFormClient.create(sql.sync(), batches(), renderer());
            DynamicForm form = client.entityModels().metadata(Person.class).toDynamicForm();
            var result = SyncFormRepository.create(client, form, Person.class).page(where(), page);
            assertEquals(1L, result.total());
        }
        assertOrder(sql, expectedOrder);
    }

    private static void explicitSpecPage(boolean reactive) {
        RecordingSql sql = new RecordingSql();
        PageQuery page = PageQuery.of(1, 10, PageSort.desc("name"));
        if (reactive) {
            ReactiveFormClient client = ReactiveFormClient.create(sql.reactive(), renderer());
            DynamicForm form = client.entityModels().metadata(Person.class).toDynamicForm();
            client.page(QuerySpec.of(form, where()).withSorts(List.of(PageSort.asc("id"))), page).block();
        } else {
            SyncFormClient client = SyncFormClient.create(sql.sync(), batches(), renderer());
            DynamicForm form = client.entityModels().metadata(Person.class).toDynamicForm();
            client.page(QuerySpec.of(form, where()).withSorts(List.of(PageSort.asc("id"))), page);
        }
        assertOrder(sql, "\"id\" asc");
    }

    private static void assertOrder(RecordingSql sql, String expectedOrder) {
        assertEquals(2, sql.requests.size());
        SqlRequest count = sql.requests.getFirst();
        SqlRequest data = sql.requests.getLast();
        assertFalse(count.sql().contains("order by"), count.sql());
        assertEquals(List.of(7L), count.parameters());
        assertTrue(data.sql().contains("order by " + expectedOrder + " limit"), data.sql());
        assertEquals(7L, data.parameters().getFirst());
    }

    private static ConditionGroup where() {
        return ConditionGroup.and().where("id", ">=", 7L).build();
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
    }

    private static SyncBatchExecutor batches() {
        return (request, completed) -> {
            throw new UnsupportedOperationException();
        };
    }

    private static final class RecordingSql {
        private final List<SqlRequest> requests = new ArrayList<>();

        private List<DynamicRow> rows(SqlRequest request) {
            requests.add(request);
            return request.sql().startsWith("select count(*)")
                    ? List.of(DynamicRow.copyOf(Map.of("total", 1L))) : List.of();
        }

        private SyncSqlExecutor sync() {
            return new SyncSqlExecutor() {
                @Override public List<DynamicRow> query(SqlRequest request) {
                    return rows(request);
                }
                @Override public long rowsUpdated(SqlRequest request) {
                    throw new UnsupportedOperationException();
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
                    return Flux.defer(() -> Flux.fromIterable(rows(request)));
                }
                @Override public Mono<Long> rowsUpdated(SqlRequest request) {
                    return Mono.error(new UnsupportedOperationException());
                }
            };
        }
    }

    @TableName("people")
    public static final class Person {
        @TableId(type = IdType.INPUT)
        @OrderBy
        private Long id;
        private String name;

        public Person() { }
        public Long getId() { return id; }
        public void setId(Long id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
    }
}
