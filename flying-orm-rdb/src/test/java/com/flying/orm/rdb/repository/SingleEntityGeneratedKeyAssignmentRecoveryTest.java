package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.mapping.MappingException;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SingleEntityGeneratedKeyAssignmentRecoveryTest {

    @Test
    void syncInsertRestoresTheOriginalKeyWhenTheSetterFailsAfterMutation() {
        assertRestoredAssignmentFailure(false);
    }

    @Test
    void reactiveInsertRestoresTheOriginalKeyWhenTheSetterFailsAfterMutation() {
        assertRestoredAssignmentFailure(true);
    }

    @Test
    void syncInsertKeepsAssignmentPrimaryWhenRestorationAlsoFails() {
        assertRestorationFailureIsSecondary(false);
    }

    @Test
    void reactiveInsertKeepsAssignmentPrimaryWhenRestorationAlsoFails() {
        assertRestorationFailureIsSecondary(true);
    }

    @Test
    void syncInsertPreservesFatalRestorationFailure() {
        assertFatalRestorationFailure(false);
    }

    @Test
    void reactiveInsertPreservesFatalRestorationFailure() {
        assertFatalRestorationFailure(true);
    }

    private static void assertFatalRestorationFailure(boolean reactive) {
        VirtualMachineError fatal = new VirtualMachineError("fatal restoration") { };
        Entity entity = new Entity(fatal);

        VirtualMachineError failure = assertThrows(VirtualMachineError.class, () -> {
            if (reactive) {
                reactive().insert(entity).block();
            } else {
                sync().insert(entity);
            }
        });

        assertSame(fatal, failure);
        assertNull(entity.id);
        assertEquals(2, entity.writes);
    }

    private static void assertRestoredAssignmentFailure(boolean reactive) {
        Entity entity = new Entity(false);

        GeneratedKeyResolutionException failure = insertFailure(reactive, entity);

        assertEquals(GeneratedKeyResolutionException.Phase.ASSIGN, failure.phase());
        assertEquals(13L, failure.affectedRows());
        assertNull(entity.id);
        assertEquals(2, entity.writes);
        MappingException assignment = assertInstanceOf(MappingException.class, failure.getCause());
        assertSame(entity.assignmentFailure, assignment.getCause().getCause());
        assertEquals(0, assignment.getSuppressed().length);
    }

    private static void assertRestorationFailureIsSecondary(boolean reactive) {
        Entity entity = new Entity(true);

        GeneratedKeyResolutionException failure = insertFailure(reactive, entity);

        assertEquals(GeneratedKeyResolutionException.Phase.ASSIGN, failure.phase());
        assertEquals(13L, failure.affectedRows());
        assertNull(entity.id);
        assertEquals(2, entity.writes);
        MappingException assignment = assertInstanceOf(MappingException.class, failure.getCause());
        assertSame(entity.assignmentFailure, assignment.getCause().getCause());
        assertEquals(1, assignment.getSuppressed().length);
        MappingException restoration = assertInstanceOf(MappingException.class, assignment.getSuppressed()[0]);
        assertSame(entity.restorationFailure, restoration.getCause().getCause());
    }

    private static GeneratedKeyResolutionException insertFailure(boolean reactive, Entity entity) {
        return assertThrows(GeneratedKeyResolutionException.class, () -> {
            if (reactive) {
                reactive().insert(entity).block();
            } else {
                sync().insert(entity);
            }
        });
    }

    private static SyncFormRepository<Entity> sync() {
        SyncSqlExecutor executor = (SyncSqlExecutor) Proxy.newProxyInstance(
                SyncSqlExecutor.class.getClassLoader(), new Class<?>[]{SyncSqlExecutor.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("rowsUpdatedReturningKeys")) {
                        return keys();
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        SyncBatchExecutor batches = (SyncBatchExecutor) Proxy.newProxyInstance(
                SyncBatchExecutor.class.getClassLoader(), new Class<?>[]{SyncBatchExecutor.class},
                (proxy, method, arguments) -> {
                    throw new AssertionError("unexpected batch execution");
                });
        SyncFormClient client = SyncFormClient.create(executor, batches, renderer());
        return SyncFormRepository.create(client,
                client.entityModels().metadata(Entity.class).toDynamicForm(), Entity.class);
    }

    private static ReactiveFormRepository<Entity> reactive() {
        ReactiveSqlExecutor executor = (ReactiveSqlExecutor) Proxy.newProxyInstance(
                ReactiveSqlExecutor.class.getClassLoader(), new Class<?>[]{ReactiveSqlExecutor.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("rowsUpdatedReturningKeys")) {
                        return Mono.just(keys());
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        return ReactiveFormRepository.create(client,
                client.entityModels().metadata(Entity.class).toDynamicForm(), Entity.class);
    }

    private static SqlWriteResult keys() {
        return new SqlWriteResult(13L, List.of(DynamicRow.copyOf(Map.of("id", 7L))));
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2());
    }

    @TableName("generated_key_recovery_entities")
    private static final class Entity {
        @TableId(type = IdType.AUTO)
        private Long id;
        private String payload = "value";
        private final transient IllegalStateException assignmentFailure =
                new IllegalStateException("assignment failed after mutation");
        private final transient Throwable restorationFailure;
        private transient int writes;

        private Entity(boolean failRestoration) {
            this(failRestoration ? new IllegalArgumentException("restoration failed after mutation") : null);
        }

        private Entity(Throwable restorationFailure) {
            this.restorationFailure = restorationFailure;
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
            writes++;
            if (id != null) {
                throw assignmentFailure;
            }
            if (restorationFailure != null && writes > 1) {
                if (restorationFailure instanceof Error error) {
                    throw error;
                }
                throw (RuntimeException) restorationFailure;
            }
        }
    }
}
