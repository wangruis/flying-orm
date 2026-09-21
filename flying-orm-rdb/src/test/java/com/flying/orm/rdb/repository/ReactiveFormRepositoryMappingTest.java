package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.lifecycle.ReactiveEntityListener;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReactiveFormRepositoryMappingTest {

    @Test
    void listenerCopyKeepsTheSharedEntityMappingPlan() {
        AtomicReference<SqlRequest> executed = new AtomicReference<>();
        ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.error(new UnsupportedOperationException());
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                executed.set(request);
                return Mono.just(1L);
            }
        };
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer);
        ReactiveFormRepository<Person> repository = ReactiveFormRepository.create(
                client,
                client.entityModels().metadata(Person.class).toDynamicForm(),
                Person.class).withListener(ReactiveEntityListener.none());

        Long affectedRows = repository.insert(new Person(7L, "Ada")).block();

        assertEquals(1L, affectedRows);
        assertEquals(List.of(7L, "Ada"), executed.get().parameters());
    }

    @TableName("people")
    private static final class Person {

        @TableId(type = IdType.INPUT)
        private final Long id;

        private final String name;

        private Person(Long id, String name) {
            this.id = id;
            this.name = name;
        }

        public Long getId() {
            return id;
        }

        public String getName() {
            return name;
        }
    }
}
