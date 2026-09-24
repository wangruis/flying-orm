package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.EnumValue;
import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableLogic;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.annotation.Version;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.aggregate.AggregateExpression;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.id.IdGenerator;
import com.flying.orm.rdb.mapping.EntityFieldFiller;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.mapping.EntitySchemaDescriptor;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RepositoryBoundFormLogicDeleteTest {

    @Test
    void syncOrdinaryEnumLogicDeleteUsesStoredLiterals() {
        assertDoesNotThrow(() -> assertEnumLogicDelete(false, false));
    }

    @Test
    void reactiveOrdinaryEnumLogicDeleteUsesStoredLiterals() {
        assertDoesNotThrow(() -> assertEnumLogicDelete(true, false));
    }

    @Test
    void syncDescriptorEnumLogicDeleteUsesStoredLiterals() {
        assertEnumLogicDelete(false, true);
    }

    @Test
    void reactiveDescriptorEnumLogicDeleteUsesStoredLiterals() {
        assertEnumLogicDelete(true, true);
    }

    private static void assertEnumLogicDelete(boolean reactive, boolean registered) {
        RecordingSql sql = new RecordingSql();
        Map<Class<?>, EntitySchemaDescriptor<?>> schemas = registered
                ? Map.of(EnumPerson.class, EntitySchemaDescriptor.builder(EnumPerson.class).build())
                : Map.of();
        try (EntityModelRegistry models = EntityModelRegistry.create(
                CacheRegionPolicy.entityMappingDefaults(), IdGenerator.none(), EntityFieldFiller.none(), schemas)) {
            DynamicForm form = models.metadata(EnumPerson.class).toDynamicForm();
            if (reactive) {
                ReactiveFormClient client = ReactiveFormClient.create(sql.reactive(), renderer())
                        .withEntityModelRegistry(models);
                ReactiveFormRepository<EnumPerson> repository = ReactiveFormRepository.create(
                        client, form, EnumPerson.class);
                repository.insert(new EnumPerson()).block();
                assertEquals(List.of("a", 7L), sql.last.parameters());
                repository.select(where()).collectList().block();
                assertEquals(List.of(7L, "a"), sql.last.parameters());
                repository.delete(where()).block();
            } else {
                SyncFormClient client = SyncFormClient.create(sql.sync(), batches(), renderer())
                        .withEntityModelRegistry(models);
                SyncFormRepository<EnumPerson> repository = SyncFormRepository.create(client, form, EnumPerson.class);
                repository.insert(new EnumPerson());
                assertEquals(List.of("a", 7L), sql.last.parameters());
                repository.select(where());
                assertEquals(List.of(7L, "a"), sql.last.parameters());
                repository.delete(where());
            }
            assertTrue(sql.last.sql().startsWith("update "), sql.last.sql());
            assertEquals(List.of("d", 7L, "a"), sql.last.parameters());
        }
    }

    @Test
    void syncExplicitFormRuleKeepsItsValuesAndGroupsOrBeforePhysicalDelete() {
        assertExplicitFormRule(false);
    }

    @Test
    void reactiveExplicitFormRuleKeepsItsValuesAndGroupsOrBeforePhysicalDelete() {
        assertExplicitFormRule(true);
    }

    private static void assertExplicitFormRule(boolean reactive) {
        RecordingSql sql = new RecordingSql();
        DynamicForm explicit = DynamicForm.builder("bound_people", "bound_people")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("name", "VARCHAR"))
                .addField(DynamicField.of("deleted", "INTEGER"))
                .logicDelete("deleted", 10, 20).build();
        ConditionGroup where = ConditionGroup.or().where("id", "=", 7L).where("id", "=", 8L).build();
        if (reactive) {
            ReactiveFormRepository<Person> repository = ReactiveFormRepository.create(
                    ReactiveFormClient.create(sql.reactive(), renderer()), explicit, Person.class);
            repository.delete(where).block();
            assertLogicalOrDelete(sql.last);
            repository.physicalDelete(where).block();
        } else {
            SyncFormRepository<Person> repository = SyncFormRepository.create(
                    SyncFormClient.create(sql.sync(), batches(), renderer()), explicit, Person.class);
            repository.delete(where);
            assertLogicalOrDelete(sql.last);
            repository.physicalDelete(where);
        }
        assertTrue(sql.last.sql().startsWith("delete from "), sql.last.sql());
        assertEquals(List.of(7L, 8L), sql.last.parameters());
    }

    @Test
    void syncEntityDeleteKeepsAutomaticVersionLockOnTheBoundLogicalDelete() {
        assertVersionedLogicalDelete(false);
    }

    @Test
    void reactiveEntityDeleteKeepsAutomaticVersionLockOnTheBoundLogicalDelete() {
        assertVersionedLogicalDelete(true);
    }

    private static void assertVersionedLogicalDelete(boolean reactive) {
        RecordingSql sql = new RecordingSql();
        VersionedPerson person = new VersionedPerson();
        if (reactive) {
            ReactiveFormClient client = ReactiveFormClient.create(sql.reactive(), renderer());
            ReactiveFormRepository.create(client, client.entityModels().metadata(VersionedPerson.class)
                    .toDynamicForm(), VersionedPerson.class).delete(person, where()).block();
        } else {
            SyncFormClient client = SyncFormClient.create(sql.sync(), batches(), renderer());
            SyncFormRepository.create(client, client.entityModels().metadata(VersionedPerson.class)
                    .toDynamicForm(), VersionedPerson.class).delete(person, where());
        }
        assertTrue(sql.last.sql().startsWith("update "), sql.last.sql());
        assertTrue(sql.last.sql().contains("\"version\" = \"version\" + 1"), sql.last.sql());
        assertEquals(List.of(1, 7L, 0, 3L), sql.last.parameters());
    }

    private static void assertLogicalOrDelete(SqlRequest request) {
        assertTrue(request.sql().startsWith("update "), request.sql());
        assertTrue(request.sql().contains("(\"id\" = ? or \"id\" = ?) and \"deleted\" = ?"), request.sql());
        assertEquals(List.of(20, 7L, 8L, 10), request.parameters());
    }

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
            @Override public BatchExecutionEvidence writeBatch(BatchWriteRequest request, java.util.function.LongConsumer rowCompleted) {
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

    private enum DeletionState {
        ACTIVE("a"), DELETED("d");

        @EnumValue
        private final String code;

        DeletionState(String code) { this.code = code; }
    }

    @TableName("enum_people")
    public static final class EnumPerson {
        @TableId(type = IdType.INPUT)
        private Long id = 7L;
        @TableLogic(value = "a", delval = "d")
        private DeletionState deleted = DeletionState.ACTIVE;

        public Long getId() { return id; }
        public DeletionState getDeleted() { return deleted; }
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

    @TableName("versioned_people")
    public static final class VersionedPerson {
        @TableId(type = IdType.INPUT)
        private Long id = 7L;
        @Version
        private Long version = 3L;
        @TableLogic
        private Integer deleted;

        public Long getId() { return id; }
        public Long getVersion() { return version; }
        public Integer getDeleted() { return deleted; }
    }
}
