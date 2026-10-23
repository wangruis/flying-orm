package com.flying.orm.rdb.operator;

import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
import com.flying.orm.rdb.metadata.MetadataCacheInvalidator;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.assertEquals;

class MetadataOperatorTest {

    @Test
    void synchronousNamesDispatchBothCustomReaderOverloads() {
        JdbcFormMetadataReader reader = new JdbcFormMetadataReader() {
            public List<String> readTableNames() { return List.of("current"); }
            public List<String> readTableNames(String schema) { return List.of(schema); }
            public List<RelationalTableDefinition> readTables() { return List.of(RelationalTableDefinition.builder("current").build()); }
            public List<RelationalTableDefinition> readTables(String schema) { return List.of(RelationalTableDefinition.builder(schema).build()); }
            public DynamicForm readForm(String id, RelationIdentity relation) {
                throw new AssertionError();
            }

            @Override
            public RelationalTableDefinition readTable(RelationIdentity relation) {
                return readForm(relation.table(), relation).toTableDefinition(relation);
            }

        };
        SyncMetadataOperator operator = new SyncMetadataOperator(
                reader, MetadataCacheInvalidator.none());
        assertEquals(List.of("current"), operator.readTableNames());
        assertEquals(List.of("explicit"), operator.readTableNames("explicit"));
        assertEquals("current", operator.readTables().getFirst().name());
        assertEquals("explicit", operator.readTables("explicit").getFirst().name());
    }

    @Test
    void reactiveNamesDeferBothCustomReaderOverloadsOnEverySubscription() {
        AtomicInteger currentCalls = new AtomicInteger();
        AtomicInteger explicitCalls = new AtomicInteger();
        ReactiveFormMetadataReader reader = new ReactiveFormMetadataReader() {
            public Flux<String> readTableNames() {
                currentCalls.incrementAndGet();
                return Flux.just("current");
            }
            public Flux<String> readTableNames(String schema) {
                explicitCalls.incrementAndGet();
                return Flux.just(schema);
            }
            public Flux<RelationalTableDefinition> readTables() {
                currentCalls.incrementAndGet();
                return Flux.just(RelationalTableDefinition.builder("current").build());
            }
            public Flux<RelationalTableDefinition> readTables(String schema) {
                explicitCalls.incrementAndGet();
                return Flux.just(RelationalTableDefinition.builder(schema).build());
            }
            public Mono<DynamicForm> readForm(String id, RelationIdentity relation) {
                throw new AssertionError();
            }

            @Override
            public Mono<RelationalTableDefinition> readTable(RelationIdentity relation) {
                return readForm(relation.table(), relation).map(form -> form.toTableDefinition(relation));
            }

        };
        MetadataOperator operator = new MetadataOperator(
                com.flying.orm.rdb.metadata.ReactiveFormMetadataReaders.cached(reader));
        Flux<String> current = operator.readTableNames();
        Flux<String> explicit = operator.readTableNames("explicit");
        Flux<RelationalTableDefinition> currentTables = operator.readTables();
        Flux<RelationalTableDefinition> explicitTables = operator.readTables("explicit");
        assertEquals(0, currentCalls.get() + explicitCalls.get());
        for (int i = 0; i < 2; i++) {
            assertEquals(List.of("current"), current.collectList().block());
            assertEquals(List.of("explicit"), explicit.collectList().block());
            assertEquals("current", currentTables.blockFirst().name());
            assertEquals("explicit", explicitTables.blockFirst().name());
        }
        assertEquals(4, currentCalls.get());
        assertEquals(4, explicitCalls.get());
    }
}
