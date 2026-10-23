package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.FieldStrategy;
import com.flying.orm.core.annotation.FieldFill;
import com.flying.orm.core.annotation.EncryptedField;
import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.mapping.FlyingTenant;
import com.flying.orm.rdb.mapping.EntityFieldFiller;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.id.IdGenerator;
import com.flying.orm.rdb.mapping.MappingException;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RepositoryUpsertStageLayoutTest {

    @org.junit.jupiter.api.TestFactory
    java.util.stream.Stream<org.junit.jupiter.api.DynamicTest> fillBackFollowsPublicWriteLifecycle() {
        return java.util.stream.Stream.of(false, true).flatMap(reactive ->
                java.util.stream.Stream.of("upsert", "upsertList", "upsertPublisher", "insertList", "insertPublisher")
                        .map(channel -> org.junit.jupiter.api.DynamicTest.dynamicTest(reactive + "/" + channel, () -> {
                            try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults(),
                                    IdGenerator.none(), EntityFieldFiller.byProperty(Map.of("created", () -> "generated")))) {
                                List<FillBackStage> entities = List.of(new FillBackStage(1L), new FillBackStage(2L));
                                BatchWriteRequest request;
                                List<Object[]> rows;
                                com.flying.orm.rdb.batch.BatchWriteOptions options = com.flying.orm.rdb.batch.BatchWriteOptions.of(2);
                                if (reactive) {
                                    CapturingBatchExecutor executor = new CapturingBatchExecutor();
                                    ReactiveFormClient client = client(executor).withEntityModelRegistry(models);
                                    ReactiveFormRepository<FillBackStage> repository = ReactiveFormRepository.create(client,
                                            models.metadata(FillBackStage.class).toDynamicForm(), FillBackStage.class);
                                    Mono<BatchExecutionEvidence> pending = switch (channel) {
                                        case "upsert" -> repository.upsert(entities.getFirst());
                                        case "upsertList" -> repository.upsertBatch(entities);
                                        case "upsertPublisher" -> repository.upsertBatch(Flux.fromIterable(entities), options);
                                        case "insertList" -> repository.insertBatch(entities);
                                        default -> repository.insertBatch(Flux.fromIterable(entities), options);
                                    };
                                    assertNull(entities.getFirst().created);
                                    assertNull(executor.request.get());
                                    pending.block();
                                    request = executor.request.get();
                                    rows = executor.rows.get();
                                } else {
                                    CapturingSyncBatchExecutor executor = new CapturingSyncBatchExecutor();
                                    SyncFormClient client = SyncFormClient.create(new NoopSyncSqlExecutor(), executor, renderer())
                                            .withEntityModelRegistry(models);
                                    SyncFormRepository<FillBackStage> repository = SyncFormRepository.create(client,
                                            models.metadata(FillBackStage.class).toDynamicForm(), FillBackStage.class);
                                    switch (channel) {
                                        case "upsert" -> repository.upsert(entities.getFirst());
                                        case "upsertList" -> repository.upsertBatch(entities);
                                        case "upsertPublisher" -> repository.upsertBatch(Flux.fromIterable(entities), options);
                                        case "insertList" -> repository.insertBatch(entities);
                                        default -> repository.insertBatch(Flux.fromIterable(entities), options);
                                    }
                                    request = executor.request.get();
                                    rows = executor.rows.get();
                                }
                                assertEquals(channel.equals("upsert") ? 1 : 2, rows.size());
                                for (int index = 0; index < rows.size(); index++) {
                                    assertEquals("generated", entities.get(index).created);
                                    assertTrue(Arrays.asList(rows.get(index)).contains("generated"));
                                }
                                if (channel.startsWith("upsert")) {
                                    assertTrue(request.sql().endsWith("do update set \"name\" = excluded.\"name\""), request.sql());
                                }
                            }
                        })));
    }

    @TableName("fill_back_stage")
    static class FillBackStage {
        @TableId Long id;
        @TableField(fill = FieldFill.INSERT_DEFAULT, fillBack = true) String created;
        String name = "name";
        FillBackStage(Long id) { this.id = id; }
    }

    @Test
    void missingGeneratedIdentityUsesOnlyInsertFieldsAndInsertFill() {
        List<com.flying.orm.rdb.mapping.EntityFieldFiller.Operation> operations = new java.util.ArrayList<>();
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled(),
                IdGenerator.none(), (entity, field, operation, current) -> {
                    operations.add(operation);
                    return "filled";
                })) {
            var metadata = models.metadata(GeneratedStage.class);
            var mapper = RepositoryBatchKind.UPSERT.mapper(metadata, models.entityValues(GeneratedStage.class),
                    RepositoryEntityIdSupport.create(metadata, models), false);
            assertEquals(Map.of("created", "filled"), mapper.apply(new GeneratedStage()));
            assertEquals(List.of(com.flying.orm.rdb.mapping.EntityFieldFiller.Operation.INSERT), operations);
        }
    }

    @TableName("generated_stage")
    static final class GeneratedStage {
        @TableId(type = IdType.AUTO) private Long id;
        @TableField(fill = FieldFill.INSERT, updateStrategy = FieldStrategy.NEVER) private String created;
        @TableField(insertStrategy = FieldStrategy.NEVER) private String changed = "update-only";
    }

    @org.junit.jupiter.api.TestFactory
    java.util.stream.Stream<org.junit.jupiter.api.DynamicTest> businessConflictPropertiesUseEntityMappingAndLeaveAutoKeysToDatabase() {
        return java.util.stream.Stream.of(false, true).flatMap(reactive ->
                java.util.stream.Stream.of("single", "list", "publisher").map(channel ->
                        org.junit.jupiter.api.DynamicTest.dynamicTest(reactive + "/" + channel, () -> {
                            List<BusinessKeyEntity> entities = List.of(new BusinessKeyEntity(null, "a@test", "first"),
                                    new BusinessKeyEntity(null, "b@test", "second"));
                            String[] properties = {"emailAddress"};
                            List<com.flying.orm.rdb.lifecycle.EntityLifecyclePhase> events = new java.util.ArrayList<>();
                            BatchWriteRequest request;
                            List<Object[]> rows;
                            if (reactive) {
                                CapturingBatchExecutor executor = new CapturingBatchExecutor();
                                ReactiveFormClient client = client(executor);
                                ReactiveFormRepository<BusinessKeyEntity> repository = ReactiveFormRepository.create(client,
                                        client.entityModels().metadata(BusinessKeyEntity.class).toDynamicForm(), BusinessKeyEntity.class)
                                        .withListener(event -> { events.add(event.phase()); return Mono.empty(); });
                                Mono<BatchExecutionEvidence> pending = switch (channel) {
                                    case "single" -> repository.upsert(entities.getFirst(), properties);
                                    case "list" -> repository.upsertBatch(entities, properties);
                                    default -> repository.upsertBatch(Flux.fromIterable(entities),
                                            com.flying.orm.rdb.batch.BatchWriteOptions.of(2), properties);
                                };
                                assertNull(executor.request.get());
                                assertTrue(events.isEmpty());
                                properties[0] = "name";
                                pending.block();
                                request = executor.request.get();
                                rows = executor.rows.get();
                            } else {
                                CapturingSyncBatchExecutor executor = new CapturingSyncBatchExecutor();
                                SyncFormClient client = SyncFormClient.create(new NoopSyncSqlExecutor(), executor, renderer());
                                SyncFormRepository<BusinessKeyEntity> repository = SyncFormRepository.create(client,
                                        client.entityModels().metadata(BusinessKeyEntity.class).toDynamicForm(), BusinessKeyEntity.class)
                                        .withListener(event -> { events.add(event.phase()); return Mono.empty(); });
                                switch (channel) {
                                    case "single" -> repository.upsert(entities.getFirst(), properties);
                                    case "list" -> repository.upsertBatch(entities, properties);
                                    default -> repository.upsertBatch(Flux.fromIterable(entities),
                                            com.flying.orm.rdb.batch.BatchWriteOptions.of(2), properties);
                                }
                                request = executor.request.get();
                                rows = executor.rows.get();
                            }
                            assertEquals("insert into \"business_keys\" (\"mail_address\", \"name\") values (?, ?) "
                                    + "on conflict (\"mail_address\") do update set \"name\" = excluded.\"name\"", request.sql());
                            assertEquals(channel.equals("single") ? 1 : 2, rows.size());
                            assertEquals(List.of("a@test", "first"), Arrays.asList(rows.getFirst()));
                            assertEquals(rows.size(), java.util.Collections.frequency(events,
                                    com.flying.orm.rdb.lifecycle.EntityLifecyclePhase.PRE_PERSIST));
                            assertEquals(rows.size(), java.util.Collections.frequency(events,
                                    com.flying.orm.rdb.lifecycle.EntityLifecyclePhase.POST_PERSIST));
                            assertNull(entities.getFirst().id());
                            assertNull(entities.getLast().id());
                        })));
    }

    @TableName("business_keys")
    static final class BusinessKeyEntity {
        @TableId(type = IdType.AUTO)
        private Long id;
        @TableField("mail_address")
        private String emailAddress;
        private String name;

        BusinessKeyEntity(Long id, String emailAddress, String name) {
            this.id = id;
            this.emailAddress = emailAddress;
            this.name = name;
        }

        Long id() { return id; }
    }

    @org.junit.jupiter.api.TestFactory
    java.util.stream.Stream<org.junit.jupiter.api.DynamicTest> businessKeysDoNotInventMissingOrAssignedDatabaseIdentities() {
        return java.util.stream.Stream.of(false, true).flatMap(reactive ->
                java.util.stream.Stream.of("unknown", "generatedKey", "assigned").map(scenario ->
                        org.junit.jupiter.api.DynamicTest.dynamicTest(reactive + "/" + scenario, () -> {
                            String[] properties = switch (scenario) {
                                case "unknown" -> new String[]{"unknown"};
                                case "generatedKey" -> new String[]{"id"};
                                default -> new String[]{"emailAddress"};
                            };
                            BusinessKeyEntity entity = new BusinessKeyEntity(scenario.equals("assigned") ? 7L : null,
                                    "a@test", "name");
                            if (reactive) {
                                CapturingBatchExecutor executor = new CapturingBatchExecutor();
                                ReactiveFormClient client = client(executor);
                                ReactiveFormRepository<BusinessKeyEntity> repository = ReactiveFormRepository.create(client,
                                        client.entityModels().metadata(BusinessKeyEntity.class).toDynamicForm(), BusinessKeyEntity.class);
                                assertThrows(RuntimeException.class, () -> repository.upsert(entity, properties).block());
                                assertNull(executor.request.get());
                            } else {
                                CapturingSyncBatchExecutor executor = new CapturingSyncBatchExecutor();
                                SyncFormClient client = SyncFormClient.create(new NoopSyncSqlExecutor(), executor, renderer());
                                SyncFormRepository<BusinessKeyEntity> repository = SyncFormRepository.create(client,
                                        client.entityModels().metadata(BusinessKeyEntity.class).toDynamicForm(), BusinessKeyEntity.class);
                                assertThrows(RuntimeException.class, () -> repository.upsert(entity, properties));
                                assertNull(executor.request.get());
                            }
                            assertEquals(scenario.equals("assigned") ? 7L : null, entity.id());
                        })));
    }

    @org.junit.jupiter.api.TestFactory
    java.util.stream.Stream<org.junit.jupiter.api.DynamicTest> businessKeysWorkWithoutPrimaryKeysAndWithoutUpdatePermissionOnTheKey() {
        return java.util.stream.Stream.of(false, true).map(reactive ->
                org.junit.jupiter.api.DynamicTest.dynamicTest("reactive=" + reactive, () -> {
                    com.flying.orm.core.scope.FieldUsePolicy policy = com.flying.orm.core.scope.FieldUsePolicy.builder()
                            .allow("mail_address", com.flying.orm.core.scope.FieldUse.INSERT)
                            .allow("name", com.flying.orm.core.scope.FieldUse.INSERT,
                                    com.flying.orm.core.scope.FieldUse.UPDATE).build();
                    BatchWriteRequest request;
                    if (reactive) {
                        CapturingBatchExecutor executor = new CapturingBatchExecutor();
                        ReactiveFormClient client = client(executor).withFieldUsePolicy(policy);
                        ReactiveFormRepository.create(client, client.entityModels().metadata(BusinessOnly.class).toDynamicForm(),
                                BusinessOnly.class).upsert(new BusinessOnly("a@test", "name"), "emailAddress").block();
                        request = executor.request.get();
                    } else {
                        CapturingSyncBatchExecutor executor = new CapturingSyncBatchExecutor();
                        SyncFormClient client = SyncFormClient.create(new NoopSyncSqlExecutor(), executor, renderer())
                                .withFieldUsePolicy(policy);
                        SyncFormRepository.create(client, client.entityModels().metadata(BusinessOnly.class).toDynamicForm(),
                                BusinessOnly.class).upsert(new BusinessOnly("a@test", "name"), "emailAddress");
                        request = executor.request.get();
                    }
                    assertTrue(request.sql().endsWith("on conflict (\"mail_address\") do update set \"name\" = excluded.\"name\""));
                }));
    }

    @TableName("business_only")
    record BusinessOnly(@TableField("mail_address") String emailAddress, String name) { }

    @org.junit.jupiter.api.TestFactory
    java.util.stream.Stream<org.junit.jupiter.api.DynamicTest> generatedNonKeyColumnsStayDatabaseOwnedDuringUpsert() {
        return java.util.stream.Stream.of(false, true).map(reactive ->
                org.junit.jupiter.api.DynamicTest.dynamicTest("reactive=" + reactive, () -> {
                    com.flying.orm.rdb.mapping.EntitySchemaDescriptor<GeneratedColumn> schema =
                            com.flying.orm.rdb.mapping.EntitySchemaDescriptor.builder(GeneratedColumn.class).build();
                    EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults(),
                            IdGenerator.none(), com.flying.orm.rdb.mapping.EntityFieldFiller.none(),
                            Map.of(GeneratedColumn.class, schema));
                    List<GeneratedColumn> entities = List.of(new GeneratedColumn(1L, null, "first"),
                            new GeneratedColumn(2L, 99L, "second"));
                    BatchWriteRequest request;
                    List<Object[]> rows;
                    if (reactive) {
                        CapturingBatchExecutor executor = new CapturingBatchExecutor();
                        ReactiveFormRepository.create(client(executor).withEntityModelRegistry(models),
                                schema.form(), GeneratedColumn.class).upsertBatch(entities).block();
                        request = executor.request.get();
                        rows = executor.rows.get();
                    } else {
                        CapturingSyncBatchExecutor executor = new CapturingSyncBatchExecutor();
                        SyncFormRepository.create(SyncFormClient.create(new NoopSyncSqlExecutor(), executor, renderer())
                                .withEntityModelRegistry(models), schema.form(), GeneratedColumn.class).upsertBatch(entities);
                        request = executor.request.get();
                        rows = executor.rows.get();
                    }
                    assertFalse(request.sql().contains("serial"), request.sql());
                    assertEquals(List.of(1L, "first"), Arrays.asList(rows.getFirst()));
                    assertEquals(List.of(2L, "second"), Arrays.asList(rows.getLast()));
                    assertEquals(99L, entities.getLast().serial());
                }));
    }

    @TableName("generated_columns")
    record GeneratedColumn(@TableId Long id,
                           @com.flying.orm.core.annotation.TableColumn(
                                   generation = com.flying.orm.core.annotation.TableColumn.Generation.IDENTITY)
                           @TableField(updateStrategy = FieldStrategy.NEVER) Long serial, String name) {
    }

    @org.junit.jupiter.api.TestFactory
    java.util.stream.Stream<org.junit.jupiter.api.DynamicTest> nullOnlyDefaultsKeepMixedBatchesUsable() {
        return java.util.stream.Stream.of(false, true).map(reactive ->
                org.junit.jupiter.api.DynamicTest.dynamicTest("reactive=" + reactive, () -> {
                    java.util.concurrent.atomic.AtomicInteger fills = new java.util.concurrent.atomic.AtomicInteger();
                    EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults(),
                            IdGenerator.none(), (entity, field, operation, current) -> {
                                fills.incrementAndGet();
                                return "generated";
                            });
                    List<DefaultStage> entities = List.of(new DefaultStage(1L, null, "first"),
                            new DefaultStage(2L, "provided", "second"));
                    BatchWriteRequest request;
                    List<Object[]> rows;
                    if (reactive) {
                        CapturingBatchExecutor executor = new CapturingBatchExecutor();
                        ReactiveFormClient client = client(executor).withEntityModelRegistry(models);
                        ReactiveFormRepository<DefaultStage> repository = ReactiveFormRepository.create(client,
                                models.metadata(DefaultStage.class).toDynamicForm(), DefaultStage.class);
                        Mono<BatchExecutionEvidence> pending = repository.upsertBatch(entities);
                        assertEquals(0, fills.get());
                        assertNull(executor.request.get());
                        pending.block();
                        request = executor.request.get();
                        rows = executor.rows.get();
                    } else {
                        CapturingSyncBatchExecutor executor = new CapturingSyncBatchExecutor();
                        SyncFormClient client = SyncFormClient.create(new NoopSyncSqlExecutor(), executor, renderer())
                                .withEntityModelRegistry(models);
                        SyncFormRepository.create(client, models.metadata(DefaultStage.class).toDynamicForm(),
                                DefaultStage.class).upsertBatch(entities);
                        request = executor.request.get();
                        rows = executor.rows.get();
                    }
                    assertEquals(1, fills.get());
                    assertEquals(2, rows.size());
                    assertTrue(Arrays.asList(rows.getFirst()).contains("generated"));
                    assertTrue(Arrays.asList(rows.getLast()).contains("provided"));
                    assertTrue(request.sql().endsWith("do update set \"name\" = excluded.\"name\""), request.sql());
                    assertNull(entities.getFirst().created());
                }));
    }

    @TableName("default_stage")
    record DefaultStage(@TableId Long id,
                        @TableField(fill = FieldFill.INSERT_IF_NULL, updateStrategy = FieldStrategy.NEVER)
                        String created, String name) {
    }

    @org.junit.jupiter.api.TestFactory
    java.util.stream.Stream<org.junit.jupiter.api.DynamicTest> insertDefaultsExcludeOnlyGeneratedConflictValues() {
        return java.util.stream.Stream.of(false, true).flatMap(reactive ->
                java.util.stream.Stream.of(false, true).map(generated ->
                        org.junit.jupiter.api.DynamicTest.dynamicTest(reactive + "/generated=" + generated, () -> {
                            java.util.concurrent.atomic.AtomicInteger fills = new java.util.concurrent.atomic.AtomicInteger();
                            EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults(),
                                    IdGenerator.none(), (entity, field, operation, current) -> {
                                        fills.incrementAndGet();
                                        return "generated";
                                    });
                            InsertDefaultStage entity = new InsertDefaultStage(1L, generated ? null : "provided", "name");
                            BatchWriteRequest request;
                            List<Object[]> rows;
                            if (reactive) {
                                CapturingBatchExecutor executor = new CapturingBatchExecutor();
                                ReactiveFormClient client = client(executor).withEntityModelRegistry(models);
                                Mono<BatchExecutionEvidence> pending = ReactiveFormRepository.create(client,
                                        models.metadata(InsertDefaultStage.class).toDynamicForm(), InsertDefaultStage.class)
                                        .upsert(entity);
                                assertEquals(0, fills.get());
                                assertNull(executor.request.get());
                                pending.block();
                                request = executor.request.get();
                                rows = executor.rows.get();
                            } else {
                                CapturingSyncBatchExecutor executor = new CapturingSyncBatchExecutor();
                                SyncFormClient client = SyncFormClient.create(new NoopSyncSqlExecutor(), executor, renderer())
                                        .withEntityModelRegistry(models);
                                SyncFormRepository.create(client, models.metadata(InsertDefaultStage.class).toDynamicForm(),
                                        InsertDefaultStage.class).upsert(entity);
                                request = executor.request.get();
                                rows = executor.rows.get();
                            }
                            assertEquals(generated ? 1 : 0, fills.get());
                            assertEquals(List.of(1L, generated ? "generated" : "provided", "name"),
                                    Arrays.asList(rows.getFirst()));
                            assertEquals(1, rows.size());
                            assertEquals("insert into \"insert_default_stage\" (\"id\", \"created\", \"name\") values (?, ?, ?) "
                                    + "on conflict (\"id\") do update set "
                                    + (generated ? "" : "\"created\" = excluded.\"created\", ")
                                    + "\"name\" = excluded.\"name\"", request.sql());
                            assertEquals(generated ? null : "provided", entity.created());
                        })));
    }

    @TableName("insert_default_stage")
    record InsertDefaultStage(@TableId Long id,
                              @TableField(fill = FieldFill.INSERT_DEFAULT) String created, String name) {
    }

    @Test
    void ignoredUpdatesOnlyExcludeTheSelectedSetColumn() {
        com.flying.orm.core.form.DynamicForm form = com.flying.orm.core.form.DynamicForm.builder("t", "t")
                .addField(com.flying.orm.core.form.DynamicField.primaryKey("id", "BIGINT"))
                .addField(com.flying.orm.core.form.DynamicField.of("created", "VARCHAR"))
                .addField(com.flying.orm.core.form.DynamicField.of("name", "VARCHAR")).build();
        Map<String, Object> row = new java.util.LinkedHashMap<>();
        row.put("id", 1L);
        row.put("created", "original");
        row.put("name", "new");
        com.flying.orm.rdb.form.spec.BatchSpec spec = com.flying.orm.rdb.form.spec.BatchSpec
                .upsert(form, Flux.just(row, Map.of("NAME", "next", "CREATED", "second", "ID", 2L)))
                .ignoreUpdates("created");
        CapturingBatchExecutor reactive = new CapturingBatchExecutor();
        client(reactive).writeBatch(spec).block();
        CapturingSyncBatchExecutor sync = new CapturingSyncBatchExecutor();
        SyncFormClient.create(new NoopSyncSqlExecutor(), sync, renderer()).writeBatch(spec);
        String expected = "insert into \"t\" (\"id\", \"created\", \"name\") values (?, ?, ?) "
                + "on conflict (\"id\") do update set \"name\" = excluded.\"name\"";
        assertEquals(expected, reactive.request.get().sql());
        assertEquals(expected, sync.request.get().sql());
        for (List<Object[]> rows : List.of(reactive.rows.get(), sync.rows.get())) {
            assertEquals(List.of(1L, "original", "new"), Arrays.asList(rows.getFirst()));
            assertEquals(List.of(2L, "second", "next"), Arrays.asList(rows.getLast()));
        }
        org.junit.jupiter.api.Assertions.assertSame(spec, spec.ignoreUpdates("CREATED"));
        assertThrows(UnsupportedOperationException.class, () -> spec.ignoredUpdates().clear());
        assertThrows(IllegalArgumentException.class, () -> spec.ignoreUpdates("unknown"));
        assertThrows(IllegalStateException.class, () -> com.flying.orm.rdb.form.spec.BatchSpec
                .insert(form, Flux.just(row)).ignoreUpdates("name"));
        assertEquals(spec.ignoredUpdates(), spec.ignoreConflicts().ignoredUpdates());
    }

    @org.junit.jupiter.api.TestFactory
    java.util.stream.Stream<org.junit.jupiter.api.DynamicTest> perCallIgnoredUpdatesKeepInsertValuesAndPermissions() {
        return java.util.stream.Stream.of(false, true).flatMap(reactive ->
                java.util.stream.Stream.of(false, true).map(staged ->
                        org.junit.jupiter.api.DynamicTest.dynamicTest(reactive + "/staged=" + staged, () -> {
                            com.flying.orm.rdb.internal.mapping.EntityValues<StageEntity> mapping =
                                    com.flying.orm.rdb.internal.mapping.EntityValues.createUncached(StageEntity.class);
                            com.flying.orm.core.form.DynamicForm form = client(new CapturingBatchExecutor())
                                    .entityModels().metadata(StageEntity.class).toDynamicForm();
                            List<Map<String, Object>> values = List.of(
                                    staged ? mapping.repositoryUpsertValues(new StageEntity(7L, "first", "unused"))
                                            : new java.util.LinkedHashMap<>(Map.of("id", 7L, "insert_only", "first", "update_only", "unused")),
                                    staged ? mapping.repositoryUpsertValues(new StageEntity(8L, null, "unused"))
                                            : new java.util.LinkedHashMap<>(Map.of("id", 8L, "insert_only", "last", "update_only", "unused")));
                            com.flying.orm.rdb.form.spec.BatchSpec base = com.flying.orm.rdb.form.spec.BatchSpec
                                    .upsert(form, Flux.fromIterable(values));
                            com.flying.orm.rdb.form.spec.BatchSpec spec = base.ignoreUpdates("UPDATE_ONLY")
                                    .ignoreUpdates("insert_only").withScope(DataScope.none())
                                    .withOptions(com.flying.orm.rdb.batch.BatchWriteOptions.of(2));
                            assertTrue(base.ignoredUpdates().isEmpty());
                            assertEquals(java.util.Set.of("update_only", "insert_only"), spec.ignoredUpdates());
                            com.flying.orm.core.scope.FieldUsePolicy policy = com.flying.orm.core.scope.FieldUsePolicy.builder()
                                    .allow("id", com.flying.orm.core.scope.FieldUse.INSERT)
                                    .allow("insert_only", com.flying.orm.core.scope.FieldUse.INSERT)
                                    .allow("update_only", com.flying.orm.core.scope.FieldUse.INSERT).build();
                            BatchWriteRequest request;
                            List<Object[]> rows;
                            if (reactive) {
                                CapturingBatchExecutor executor = new CapturingBatchExecutor();
                                Mono<BatchExecutionEvidence> pending = client(executor).withFieldUsePolicy(policy).writeBatch(spec);
                                assertNull(executor.request.get());
                                pending.block();
                                request = executor.request.get();
                                rows = executor.rows.get();
                            } else {
                                CapturingSyncBatchExecutor executor = new CapturingSyncBatchExecutor();
                                SyncFormClient.create(new NoopSyncSqlExecutor(), executor, renderer())
                                        .withFieldUsePolicy(policy).writeBatch(spec);
                                request = executor.request.get();
                                rows = executor.rows.get();
                            }
                            assertTrue(request.sql().endsWith("on conflict (\"id\") do nothing"), request.sql());
                            assertEquals(2, rows.size());
                            assertEquals(staged ? 2 : 3, rows.getFirst().length);
                            assertTrue(Arrays.asList(rows.getFirst()).contains("first"));
                            assertTrue(Arrays.asList(rows.getLast()).contains(8L));
                            if (staged) {
                                assertFalse(request.sql().contains("update_only"));
                                assertTrue(Arrays.asList(rows.getLast()).contains(null));
                            }
                            assertEquals("unused", values.getFirst().get("update_only"));
                        })));
    }

    @org.junit.jupiter.api.TestFactory
    java.util.stream.Stream<org.junit.jupiter.api.DynamicTest> insertIfAbsentUsesOnlyInsertMapping() {
        return java.util.stream.Stream.of(false, true).flatMap(reactive ->
                java.util.stream.Stream.of("single", "list", "publisher", "options").map(channel ->
                        org.junit.jupiter.api.DynamicTest.dynamicTest(reactive + "/" + channel, () -> {
                            List<StageEntity> entities = List.of(new StageEntity(7L, "first", "unused"),
                                    new StageEntity(8L, "last", "unused"));
                            AtomicReference<BatchWriteRequest> request;
                            AtomicReference<List<Object[]>> rows;
                            if (reactive) {
                                CapturingBatchExecutor executor = new CapturingBatchExecutor();
                                ReactiveFormClient client = client(executor);
                                ReactiveFormRepository<StageEntity> repository = ReactiveFormRepository.create(client,
                                        client.entityModels().metadata(StageEntity.class).toDynamicForm(), StageEntity.class);
                                Mono<BatchExecutionEvidence> pending = switch (channel) {
                                    case "single" -> repository.insertIfAbsent(entities.getFirst());
                                    case "list" -> repository.insertBatchIfAbsent(entities);
                                    case "publisher" -> repository.insertBatchIfAbsent(Flux.fromIterable(entities));
                                    default -> repository.insertBatchIfAbsent(Flux.fromIterable(entities),
                                            com.flying.orm.rdb.batch.BatchWriteOptions.of(2));
                                };
                                assertNull(executor.request.get(), "reactive execution stays cold");
                                pending.block();
                                request = executor.request;
                                rows = executor.rows;
                            } else {
                                CapturingSyncBatchExecutor executor = new CapturingSyncBatchExecutor();
                                SyncFormClient client = SyncFormClient.create(new NoopSyncSqlExecutor(), executor, renderer());
                                SyncFormRepository<StageEntity> repository = SyncFormRepository.create(client,
                                        client.entityModels().metadata(StageEntity.class).toDynamicForm(), StageEntity.class);
                                if (channel.equals("single")) repository.insertIfAbsent(entities.getFirst());
                                else if (channel.equals("list")) repository.insertBatchIfAbsent(entities);
                                else repository.insertBatchIfAbsent(Flux.fromIterable(entities),
                                        com.flying.orm.rdb.batch.BatchWriteOptions.of(2));
                                request = executor.request;
                                rows = executor.rows;
                            }
                            assertTrue(request.get().sql().endsWith("on conflict (\"id\") do nothing"));
                            assertFalse(request.get().sql().contains("update_only"));
                            assertEquals(channel.equals("single") ? 1 : 2, rows.get().size());
                            assertEquals(List.of(7L, "first"), Arrays.asList(rows.get().getFirst()));
                            if (!channel.equals("single")) assertEquals(List.of(8L, "last"), Arrays.asList(rows.get().getLast()));
                        })));
    }

    @Test
    void insertIfAbsentDoesNotApplyConditionalUpdateLayoutRestrictions() {
        CapturingBatchExecutor reactiveExecutor = new CapturingBatchExecutor();
        ReactiveFormClient reactiveClient = client(reactiveExecutor);
        ReactiveFormRepository<ConditionalUpdateEntity> reactive = ReactiveFormRepository.create(reactiveClient,
                reactiveClient.entityModels().metadata(ConditionalUpdateEntity.class).toDynamicForm(), ConditionalUpdateEntity.class);
        CapturingSyncBatchExecutor syncExecutor = new CapturingSyncBatchExecutor();
        SyncFormClient syncClient = SyncFormClient.create(new NoopSyncSqlExecutor(), syncExecutor, renderer());
        SyncFormRepository<ConditionalUpdateEntity> sync = SyncFormRepository.create(syncClient,
                syncClient.entityModels().metadata(ConditionalUpdateEntity.class).toDynamicForm(), ConditionalUpdateEntity.class);
        List<ConditionalUpdateEntity> values = List.of(new ConditionalUpdateEntity(7L, "present"),
                new ConditionalUpdateEntity(8L, null));
        reactive.insertBatchIfAbsent(values).block();
        sync.insertBatchIfAbsent(values);
        String sql = "insert into \"conditional_entities\" (\"conditional_update\", \"id\") values (?, ?)"
                + " on conflict (\"id\") do nothing";
        assertEquals(sql, reactiveExecutor.request.get().sql());
        assertEquals(sql, syncExecutor.request.get().sql());
        assertEquals(Arrays.asList(null, 8L), Arrays.asList(reactiveExecutor.rows.get().getLast()));
        assertEquals(Arrays.asList(null, 8L), Arrays.asList(syncExecutor.rows.get().getLast()));
    }

    @Test
    void insertIfAbsentKeepsPersistLifecycleAndRejectsGeneratedIdentityBeforeInput() {
        CapturingBatchExecutor asyncExecutor = new CapturingBatchExecutor();
        ReactiveFormClient asyncClient = client(asyncExecutor);
        CapturingSyncBatchExecutor syncExecutor = new CapturingSyncBatchExecutor();
        SyncFormClient syncClient = SyncFormClient.create(new NoopSyncSqlExecutor(), syncExecutor, renderer());
        List<com.flying.orm.rdb.lifecycle.EntityLifecyclePhase> events = new java.util.ArrayList<>();
        ReactiveFormRepository<StageEntity> async = ReactiveFormRepository.create(asyncClient,
                asyncClient.entityModels().metadata(StageEntity.class).toDynamicForm(), StageEntity.class)
                .withListener(event -> { events.add(event.phase()); return Mono.empty(); });
        SyncFormRepository<StageEntity> sync = SyncFormRepository.create(syncClient,
                syncClient.entityModels().metadata(StageEntity.class).toDynamicForm(), StageEntity.class)
                .withListener(event -> { events.add(event.phase()); return Mono.empty(); });
        StageEntity entity = new StageEntity(7L, "insert", "update");
        Mono<BatchExecutionEvidence> pending = async.insertIfAbsent(entity);
        assertTrue(events.isEmpty());
        pending.block();
        assertEquals(List.of(com.flying.orm.rdb.lifecycle.EntityLifecyclePhase.PRE_PERSIST,
                com.flying.orm.rdb.lifecycle.EntityLifecyclePhase.POST_PERSIST), events);
        events.clear();
        sync.insertIfAbsent(entity);
        assertEquals(List.of(com.flying.orm.rdb.lifecycle.EntityLifecyclePhase.PRE_PERSIST,
                com.flying.orm.rdb.lifecycle.EntityLifecyclePhase.POST_PERSIST), events);

        java.util.concurrent.atomic.AtomicInteger subscribed = new java.util.concurrent.atomic.AtomicInteger();
        Flux<IncompleteCompositeEntity> rows = Flux.defer(() -> {
            subscribed.incrementAndGet();
            return Flux.just(new IncompleteCompositeEntity("name", 7L, null));
        });
        ReactiveFormRepository<IncompleteCompositeEntity> asyncGenerated = ReactiveFormRepository.create(asyncClient,
                asyncClient.entityModels().metadata(IncompleteCompositeEntity.class).toDynamicForm(), IncompleteCompositeEntity.class);
        SyncFormRepository<IncompleteCompositeEntity> syncGenerated = SyncFormRepository.create(syncClient,
                syncClient.entityModels().metadata(IncompleteCompositeEntity.class).toDynamicForm(), IncompleteCompositeEntity.class);
        assertThrows(MappingException.class, () -> asyncGenerated.insertBatchIfAbsent(rows).block());
        assertThrows(MappingException.class, () -> syncGenerated.insertBatchIfAbsent(rows,
                com.flying.orm.rdb.batch.BatchWriteOptions.of(2)));
        assertEquals(0, subscribed.get());
    }

    @Test
    void encryptedReactiveUpsertKeepsStageLayoutsAcrossTheCpuBoundary() {
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.builder().current("v1", new byte[32]).build();
             ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(keys)) {
            for (boolean publisher : List.of(false, true)) {
                CapturingBatchExecutor executor = new CapturingBatchExecutor();
                ReactiveFormClient client = ReactiveFormClient.create(executor, renderer().withProtectedFields(runtime));
                ReactiveFormRepository<EncryptedStage> repository = ReactiveFormRepository.create(client,
                        client.entityModels().metadata(EncryptedStage.class).toDynamicForm(), EncryptedStage.class);
                List<EncryptedStage> entities = List.of(
                        new EncryptedStage(7L, "insert value", "update value", "secret"),
                        new EncryptedStage(8L, "insert value", "update value", "secret"));
                if (publisher) repository.upsertBatch(Flux.fromIterable(entities)).block();
                else repository.upsertBatch(entities).block();
                assertStageLayout(executor.request.get(), executor.rows.get());
                assertEquals(2, executor.rows.get().size());
            }
        }
    }

    @TableName("encrypted_stages")
    private record EncryptedStage(@TableId(type = IdType.INPUT) Long id,
            @TableField(value = "insert_only", insertStrategy = FieldStrategy.ALWAYS,
                    updateStrategy = FieldStrategy.NEVER) String insertOnly,
            @TableField(value = "update_only", insertStrategy = FieldStrategy.NEVER,
                    updateStrategy = FieldStrategy.ALWAYS) String updateOnly,
            @EncryptedField String secret) { }

    @org.junit.jupiter.api.TestFactory
    java.util.stream.Stream<org.junit.jupiter.api.DynamicTest> encryptedConditionalStagesKeepSeparateStatements() {
        return java.util.stream.Stream.of(false, true).flatMap(reactive ->
                java.util.stream.Stream.of(false, true).flatMap(firstNull ->
                        java.util.stream.Stream.of(false, true).map(mismatch ->
                                org.junit.jupiter.api.DynamicTest.dynamicTest(reactive + "/" + firstNull + "/" + mismatch, () -> {
                                    try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.builder().current("v1", new byte[32]).build();
                                         ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(keys)) {
                                        String first = firstNull ? null : "first";
                                        String second = mismatch ? firstNull ? "second" : null : first;
                                        List<ConditionalEncrypted> entities = List.of(
                                                new ConditionalEncrypted(7L, first, "secret"),
                                                new ConditionalEncrypted(8L, second, "secret"));
                                        Runnable execute;
                                        AtomicReference<List<Object[]>> rows;
                                        if (reactive) {
                                            CapturingBatchExecutor executor = new CapturingBatchExecutor();
                                            ReactiveFormClient client = ReactiveFormClient.create(executor, renderer().withProtectedFields(runtime));
                                            ReactiveFormRepository<ConditionalEncrypted> repository = ReactiveFormRepository.create(client,
                                                    client.entityModels().metadata(ConditionalEncrypted.class).toDynamicForm(), ConditionalEncrypted.class);
                                            execute = () -> repository.upsertBatch(entities).block();
                                            rows = executor.rows;
                                        } else {
                                            CapturingSyncBatchExecutor executor = new CapturingSyncBatchExecutor();
                                            SyncFormClient client = SyncFormClient.create(new NoopSyncSqlExecutor(), executor, renderer().withProtectedFields(runtime));
                                            SyncFormRepository<ConditionalEncrypted> repository = SyncFormRepository.create(client,
                                                    client.entityModels().metadata(ConditionalEncrypted.class).toDynamicForm(), ConditionalEncrypted.class);
                                            execute = () -> repository.upsertBatch(entities);
                                            rows = executor.rows;
                                        }
                                        execute.run();
                                        assertEquals(2, rows.get().size());
                                        assertFalse(Arrays.asList(rows.get().getFirst()).contains("secret"));
                                        Object[] last = rows.get().getLast();
                                        assertEquals(mismatch, last[last.length - 1]
                                                instanceof com.flying.orm.rdb.execution.ProtectedBatchRows.Layout);
                                    }
                                }))));
    }

    @TableName("conditional_encrypted")
    private record ConditionalEncrypted(@TableId(type = IdType.INPUT) Long id,
            @TableField(updateStrategy = FieldStrategy.NOT_NULL) String note,
            @EncryptedField String secret) {}

    @Test
    void keepsInsertAndConflictUpdateStrategiesInSeparateStages() {
        CapturingBatchExecutor executor = new CapturingBatchExecutor();
        ReactiveFormClient client = client(executor);
        ReactiveFormRepository<StageEntity> repository = ReactiveFormRepository.create(
                client,
                client.entityModels().metadata(StageEntity.class).toDynamicForm(),
                StageEntity.class);

        repository.upsertBatch(List.of(new StageEntity(7L, "insert value", "update value"))).block();

        assertStageLayout(executor.request.get(), executor.rows.get());
    }

    @Test
    void keepsInsertAndConflictUpdateStrategiesOnSyncRepositoryPath() {
        CapturingSyncBatchExecutor executor = new CapturingSyncBatchExecutor();
        SyncFormClient client = SyncFormClient.create(new NoopSyncSqlExecutor(), executor, renderer());
        SyncFormRepository<StageEntity> repository = SyncFormRepository.create(
                client,
                client.entityModels().metadata(StageEntity.class).toDynamicForm(),
                StageEntity.class);

        repository.upsertBatch(List.of(new StageEntity(7L, "insert value", "update value")));

        assertStageLayout(executor.request.get(), executor.rows.get());
    }

    private static void assertStageLayout(BatchWriteRequest request, List<Object[]> rows) {
        assertNotNull(request);
        String sql = request.sql().toLowerCase(Locale.ROOT);
        int conflict = sql.indexOf(" on conflict ");
        assertTrue(conflict > 0, sql);
        String insert = sql.substring(0, conflict);
        String update = sql.substring(conflict);
        assertFalse(insert.contains("\"update_only\""), sql);
        assertTrue(update.contains("\"update_only\""), sql);
        assertTrue(insert.contains("\"insert_only\""), sql);
        assertFalse(update.contains("\"insert_only\""), sql);

        assertNotNull(rows);
        List<Object> parameters = Arrays.asList(rows.getFirst());
        assertTrue(parameters.contains("update value"), parameters.toString());
        assertTrue(parameters.contains("insert value"), parameters.toString());
        assertTrue(parameters.contains(7L), parameters.toString());
    }

    @Test
    void rejectsAnIncompleteNonLeadingCompositeIdentityBeforeSqlExecution() {
        CapturingBatchExecutor executor = new CapturingBatchExecutor();
        ReactiveFormClient client = client(executor);
        ReactiveFormRepository<IncompleteCompositeEntity> repository = ReactiveFormRepository.create(
                client,
                client.entityModels().metadata(IncompleteCompositeEntity.class).toDynamicForm(),
                IncompleteCompositeEntity.class);

        MappingException error = assertThrows(
                MappingException.class,
                () -> repository.upsertBatch(List.of(new IncompleteCompositeEntity("Ada", null, null))).block());

        assertTrue(error.getMessage().contains("INPUT primary key must be assigned"));
        assertNull(executor.request.get());
    }

    @Test
    void allowsConditionalConflictUpdateWhenActualLayoutIsStable() {
        CapturingBatchExecutor executor = new CapturingBatchExecutor();
        ReactiveFormClient client = client(executor);
        ReactiveFormRepository<ConditionalUpdateEntity> repository = ReactiveFormRepository.create(
                client,
                client.entityModels().metadata(ConditionalUpdateEntity.class).toDynamicForm(),
                ConditionalUpdateEntity.class);

        repository.upsertBatch(List.of(new ConditionalUpdateEntity(7L, "first"),
                new ConditionalUpdateEntity(8L, "second"))).block();
        assertEquals(2, executor.rows.get().size());
        assertEquals(List.of("first", 7L), Arrays.asList(executor.rows.get().getFirst()));
        assertEquals(List.of("second", 8L), Arrays.asList(executor.rows.get().getLast()));
    }

    @org.junit.jupiter.api.TestFactory
    java.util.stream.Stream<org.junit.jupiter.api.DynamicTest> conditionalStrategiesUseActualBatchColumns() {
        return java.util.stream.Stream.of(false, true).flatMap(reactive ->
                java.util.stream.Stream.of(false, true).flatMap(publisher ->
                        java.util.stream.Stream.of("insert", "upsert", "ignore").flatMap(operation ->
                                java.util.stream.Stream.of("absent", "empty", "present").map(shape ->
                                        org.junit.jupiter.api.DynamicTest.dynamicTest(
                                                reactive + "/" + publisher + "/" + operation + "/" + shape, () -> {
                                            String value = shape.equals("absent") ? null : shape.equals("empty") ? "" : "value";
                                            List<ConditionalWriteEntity> entities = List.of(
                                                    new ConditionalWriteEntity(7L, value, value),
                                                    new ConditionalWriteEntity(8L, value, value));
                                            AtomicReference<List<Object[]>> rows;
                                            com.flying.orm.rdb.batch.BatchWriteOptions options =
                                                    com.flying.orm.rdb.batch.BatchWriteOptions.of(2);
                                            if (reactive) {
                                                CapturingBatchExecutor executor = new CapturingBatchExecutor();
                                                ReactiveFormClient client = client(executor);
                                                ReactiveFormRepository<ConditionalWriteEntity> repository = ReactiveFormRepository.create(
                                                        client, client.entityModels().metadata(ConditionalWriteEntity.class).toDynamicForm(),
                                                        ConditionalWriteEntity.class);
                                                Mono<BatchExecutionEvidence> pending = switch (operation) {
                                                    case "insert" -> publisher ? repository.insertBatch(Flux.fromIterable(entities), options)
                                                            : repository.insertBatch(entities);
                                                    case "upsert" -> publisher ? repository.upsertBatch(Flux.fromIterable(entities), options)
                                                            : repository.upsertBatch(entities);
                                                    default -> publisher ? repository.insertBatchIfAbsent(Flux.fromIterable(entities), options)
                                                            : repository.insertBatchIfAbsent(entities);
                                                };
                                                assertNull(executor.request.get());
                                                assertEquals(2L, pending.block().inputCount());
                                                rows = executor.rows;
                                            } else {
                                                CapturingSyncBatchExecutor executor = new CapturingSyncBatchExecutor();
                                                SyncFormClient client = SyncFormClient.create(new NoopSyncSqlExecutor(), executor, renderer());
                                                SyncFormRepository<ConditionalWriteEntity> repository = SyncFormRepository.create(
                                                        client, client.entityModels().metadata(ConditionalWriteEntity.class).toDynamicForm(),
                                                        ConditionalWriteEntity.class);
                                                BatchExecutionEvidence result = switch (operation) {
                                                    case "insert" -> publisher ? repository.insertBatch(Flux.fromIterable(entities), options)
                                                            : repository.insertBatch(entities);
                                                    case "upsert" -> publisher ? repository.upsertBatch(Flux.fromIterable(entities), options)
                                                            : repository.upsertBatch(entities);
                                                    default -> publisher ? repository.insertBatchIfAbsent(Flux.fromIterable(entities), options)
                                                            : repository.insertBatchIfAbsent(entities);
                                                };
                                                assertEquals(2L, result.inputCount());
                                                rows = executor.rows;
                                            }
                                            int columns = shape.equals("absent") ? 1 : shape.equals("empty") ? 2 : 3;
                                            assertEquals(2, rows.get().size());
                                            for (int index = 0; index < 2; index++) {
                                                assertEquals(columns, rows.get().get(index).length);
                                                assertEquals(7L + index, rows.get().get(index)[0]);
                                                for (int column = 1; column < columns; column++) {
                                                    assertEquals(value, rows.get().get(index)[column]);
                                                }
                                            }
                                        })))));
    }

    @org.junit.jupiter.api.TestFactory
    java.util.stream.Stream<org.junit.jupiter.api.DynamicTest> differentUpdateStagesUseTheirOwnStatementsDespiteTheSameUnion() {
        return java.util.stream.Stream.of(false, true).flatMap(reactive ->
                java.util.stream.Stream.of(false, true).map(firstNull ->
                        org.junit.jupiter.api.DynamicTest.dynamicTest(reactive + "/" + firstNull, () -> {
                            List<ConditionalUpdateEntity> entities = List.of(
                                    new ConditionalUpdateEntity(7L, firstNull ? null : "first"),
                                    new ConditionalUpdateEntity(8L, firstNull ? "second" : null));
                            List<Object[]> rows;
                            if (reactive) {
                                CapturingBatchExecutor executor = new CapturingBatchExecutor();
                                ReactiveFormClient client = client(executor);
                                ReactiveFormRepository<ConditionalUpdateEntity> repository = ReactiveFormRepository.create(client,
                                        client.entityModels().metadata(ConditionalUpdateEntity.class).toDynamicForm(),
                                        ConditionalUpdateEntity.class);
                                repository.upsertBatch(entities).block();
                                rows = executor.rows.get();
                            } else {
                                CapturingSyncBatchExecutor executor = new CapturingSyncBatchExecutor();
                                SyncFormClient client = SyncFormClient.create(new NoopSyncSqlExecutor(), executor, renderer());
                                SyncFormRepository<ConditionalUpdateEntity> repository = SyncFormRepository.create(client,
                                        client.entityModels().metadata(ConditionalUpdateEntity.class).toDynamicForm(),
                                        ConditionalUpdateEntity.class);
                                repository.upsertBatch(entities);
                                rows = executor.rows.get();
                            }
                            Object[] second = rows.getLast();
                            com.flying.orm.rdb.execution.ProtectedBatchRows.Layout layout =
                                    org.junit.jupiter.api.Assertions.assertInstanceOf(
                                            com.flying.orm.rdb.execution.ProtectedBatchRows.Layout.class,
                                            second[second.length - 1]);
                            assertEquals(firstNull, layout.statement().sql().contains("do update set"));
                        })));
    }

    @TableName("conditional_writes")
    private record ConditionalWriteEntity(@TableId(type = IdType.INPUT) Long id,
            @TableField(insertStrategy = FieldStrategy.NOT_NULL, updateStrategy = FieldStrategy.NOT_NULL) String note,
            @TableField(insertStrategy = FieldStrategy.NOT_EMPTY, updateStrategy = FieldStrategy.NOT_EMPTY) String tag) {}

    @Test
    void resubscriptionPlansFromTheCurrentPrePersistResult() {
        CapturingBatchExecutor executor = new CapturingBatchExecutor();
        ReactiveFormClient client = client(executor);
        AtomicReference<String> filled = new AtomicReference<>();
        java.util.concurrent.atomic.AtomicInteger posts = new java.util.concurrent.atomic.AtomicInteger();
        ReactiveFormRepository<MutableConditional> repository = ReactiveFormRepository.create(client,
                client.entityModels().metadata(MutableConditional.class).toDynamicForm(), MutableConditional.class)
                .withListener(event -> {
                    if (event.phase() == com.flying.orm.rdb.lifecycle.EntityLifecyclePhase.PRE_PERSIST) {
                        event.entity().value = filled.get();
                    } else if (event.phase() == com.flying.orm.rdb.lifecycle.EntityLifecyclePhase.POST_PERSIST) {
                        posts.incrementAndGet();
                    }
                    return Mono.empty();
                });
        Mono<BatchExecutionEvidence> pending = repository.upsertBatch(List.of(
                new MutableConditional(7L), new MutableConditional(8L)));
        assertNull(executor.request.get());
        pending.block();
        assertEquals(1, executor.request.get().parameterCount());
        assertEquals(List.of(7L), Arrays.asList(executor.rows.get().getFirst()));
        filled.set("prepared");
        pending.block();
        assertEquals(2, executor.request.get().parameterCount());
        assertEquals(List.of(7L, "prepared"), Arrays.asList(executor.rows.get().getFirst()));
        assertEquals(List.of(8L, "prepared"), Arrays.asList(executor.rows.get().getLast()));
        assertEquals(4, posts.get());
    }

    @TableName("mutable_conditional")
    private static final class MutableConditional {
        @TableId(type = IdType.INPUT)
        private final Long id;
        @TableField(insertStrategy = FieldStrategy.NOT_NULL, updateStrategy = FieldStrategy.NOT_NULL)
        private String value;
        private MutableConditional(Long id) { this.id = id; }
        public Long getId() { return id; }
        public String getValue() { return value; }
    }

    @Test
    void executesNonPrimaryTenantUpsertWithTheScopedConflictTarget() {
        CapturingBatchExecutor executor = new CapturingBatchExecutor();
        ReactiveFormClient client = client(executor)
                .withDefaultDataScope(DataScope.tenant("tenant_id", "tenant-a"));
        ReactiveFormRepository<UnsafeTenantEntity> repository = ReactiveFormRepository.create(
                client,
                client.entityModels().metadata(UnsafeTenantEntity.class).toDynamicForm(),
                UnsafeTenantEntity.class);

        BatchExecutionEvidence result = repository.upsertBatch(
                List.of(new UnsafeTenantEntity(7L, "tenant-a", "value"))).block();

        assertEquals(com.flying.orm.rdb.batch.BatchExecutionState.SUCCESS, result.state());
        BatchWriteRequest request = executor.request.get();
        assertNotNull(request);
        assertEquals(List.of(7L, "tenant-a", "value", "tenant-a"), Arrays.asList(executor.rows.get().getFirst()));
        assertEquals(4, request.parameterCount());
        String sql = request.sql().toLowerCase(Locale.ROOT);
        int updateStart = sql.indexOf(" do update set ");
        int scopeStart = sql.indexOf(" where ", updateStart);
        assertTrue(updateStart > 0, sql);
        assertTrue(scopeStart > updateStart, sql);
        assertFalse(sql.substring(updateStart, scopeStart).contains("tenant_id"), sql);
        assertTrue(sql.substring(scopeStart).contains("\"tenant_id\" = ?"), sql);
        assertTrue(sql.substring(scopeStart).contains("case"), sql);
        assertFalse(sql.contains("tenant-a"), sql);
    }

    @Test
    void executesScopedTenantUpsertAndKeepsTheCompositeConflictIdentity() {
        CapturingBatchExecutor executor = new CapturingBatchExecutor();
        ReactiveFormClient client = client(executor)
                .withDefaultDataScope(DataScope.tenant("tenant_id", "tenant-a"));
        ReactiveFormRepository<SafeTenantEntity> repository = ReactiveFormRepository.create(
                client,
                client.entityModels().metadata(SafeTenantEntity.class).toDynamicForm(),
                SafeTenantEntity.class);

        BatchExecutionEvidence result = repository.upsertBatch(
                List.of(new SafeTenantEntity("tenant-a", 7L, "value"))).block();

        assertEquals(com.flying.orm.rdb.batch.BatchExecutionState.SUCCESS, result.state());
        BatchWriteRequest request = executor.request.get();
        assertNotNull(request);
        assertEquals(List.of(7L, "tenant-a", "value", "tenant-a"), Arrays.asList(executor.rows.get().getFirst()));
        assertEquals(4, request.parameterCount());
        String sql = request.sql().toLowerCase(Locale.ROOT);
        int conflictStart = sql.indexOf(" on conflict (");
        int updateStart = sql.indexOf(" do update set ");
        int scopeStart = sql.indexOf(" where ", updateStart);
        assertTrue(conflictStart > 0, sql);
        assertTrue(updateStart > conflictStart, sql);
        assertTrue(scopeStart > updateStart, sql);
        assertTrue(sql.substring(0, conflictStart).contains("(\"id\", \"tenant_id\", \"value\")"), sql);
        assertTrue(sql.substring(0, conflictStart).contains("values (?, ?, ?)"), sql);
        String conflict = sql.substring(conflictStart, updateStart);
        String update = sql.substring(updateStart, scopeStart);
        assertTrue(conflict.contains("\"tenant_id\""), sql);
        assertTrue(conflict.contains("\"id\""), sql);
        assertFalse(update.contains("tenant_id"), sql);
        assertTrue(sql.substring(scopeStart).contains("\"tenant_id\" = ?"), sql);
        assertTrue(sql.substring(scopeStart).contains("case"), sql);
        assertFalse(sql.contains("tenant-a"), sql);
    }

    @Test
    void rejectsNonPrimaryTenantIdentityWhenNoTargetScopeIsAvailable() {
        ReactiveFormClient client = client(new CapturingBatchExecutor());

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> renderer().upsertBatch(
                        client.entityModels().metadata(UnsafeTenantEntity.class).toDynamicForm(),
                        List.of(Map.of("id", 7L, "tenant_id", "tenant-a", "value", "value"))));

        assertTrue(failure.getMessage().contains("tenant field must be part of the primary key"));
    }

    private static ReactiveFormClient client(ReactiveSqlExecutor executor) {
        return ReactiveFormClient.create(executor, renderer());
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
    }

    @TableName("stage_entities")
    private static final class StageEntity {

        @TableId(type = IdType.INPUT)
        private final Long id;

        @TableField(value = "insert_only",
                    insertStrategy = FieldStrategy.ALWAYS,
                    updateStrategy = FieldStrategy.NEVER)
        private final String insertOnly;

        @TableField(value = "update_only",
                    insertStrategy = FieldStrategy.NEVER,
                    updateStrategy = FieldStrategy.ALWAYS)
        private final String updateOnly;

        private StageEntity(Long id, String insertOnly, String updateOnly) {
            this.id = id;
            this.insertOnly = insertOnly;
            this.updateOnly = updateOnly;
        }

        public Long getId() {
            return id;
        }

        public String getInsertOnly() {
            return insertOnly;
        }

        public String getUpdateOnly() {
            return updateOnly;
        }
    }

    @TableName("composite_entities")
    private static final class IncompleteCompositeEntity {

        private final String name;

        @TableId(value = "input_key", type = IdType.INPUT)
        private final Long inputKey;

        @TableId(value = "generated_key", type = IdType.AUTO)
        private Long generatedKey;

        private IncompleteCompositeEntity(String name, Long inputKey, Long generatedKey) {
            this.name = name;
            this.inputKey = inputKey;
            this.generatedKey = generatedKey;
        }

        public Long getGeneratedKey() {
            return generatedKey;
        }

        public void setGeneratedKey(Long generatedKey) {
            this.generatedKey = generatedKey;
        }

        public Long getInputKey() {
            return inputKey;
        }

        public String getName() {
            return name;
        }
    }

    @TableName("conditional_entities")
    private static final class ConditionalUpdateEntity {

        @TableId(type = IdType.INPUT)
        private final Long id;

        @TableField(value = "conditional_update",
                    insertStrategy = FieldStrategy.ALWAYS,
                    updateStrategy = FieldStrategy.NOT_NULL)
        private final String conditionalUpdate;

        private ConditionalUpdateEntity(Long id, String conditionalUpdate) {
            this.id = id;
            this.conditionalUpdate = conditionalUpdate;
        }

        public Long getId() {
            return id;
        }

        public String getConditionalUpdate() {
            return conditionalUpdate;
        }
    }

    @TableName("unsafe_tenant_entities")
    private static final class UnsafeTenantEntity {

        @TableId(type = IdType.INPUT)
        private final Long id;

        @FlyingTenant
        @TableField("tenant_id")
        private final String tenantId;

        private final String value;

        private UnsafeTenantEntity(Long id, String tenantId, String value) {
            this.id = id;
            this.tenantId = tenantId;
            this.value = value;
        }

        public Long getId() {
            return id;
        }

        public String getTenantId() {
            return tenantId;
        }

        public String getValue() {
            return value;
        }
    }

    @TableName("safe_tenant_entities")
    private static final class SafeTenantEntity {

        @FlyingTenant
        @TableId(value = "tenant_id", type = IdType.INPUT)
        private final String tenantId;

        @TableId(type = IdType.INPUT)
        private final Long id;

        private final String value;

        private SafeTenantEntity(String tenantId, Long id, String value) {
            this.tenantId = tenantId;
            this.id = id;
            this.value = value;
        }

        public String getTenantId() {
            return tenantId;
        }

        public Long getId() {
            return id;
        }

        public String getValue() {
            return value;
        }
    }

    private static final class CapturingBatchExecutor implements ReactiveSqlExecutor {

        @Override
        public boolean supportsBatchRowLayouts() { return true; }

        private final AtomicReference<BatchWriteRequest> request = new AtomicReference<>();
        private final AtomicReference<List<Object[]>> rows = new AtomicReference<>();

        @Override
        public Flux<DynamicRow> query(com.flying.orm.core.sql.render.SqlRequest request) {
            return Flux.error(new UnsupportedOperationException());
        }

        @Override
        public Mono<Long> rowsUpdated(com.flying.orm.core.sql.render.SqlRequest request) {
            return Mono.error(new UnsupportedOperationException());
        }

        @Override
        public Mono<BatchExecutionEvidence> writeBatch(BatchWriteRequest request, java.util.function.LongFunction<? extends org.reactivestreams.Publisher<Void>> rowCompleted) {
            this.request.set(request);
            return Flux.from(request.rows()).collectList().flatMap(parameterRows -> {
                rows.set(parameterRows);
                Mono<Void> completion = rowCompleted == null ? Mono.empty()
                        : Flux.range(0, parameterRows.size()).concatMap(index -> rowCompleted.apply(index)).then();
                return completion.thenReturn(com.flying.orm.rdb.repository.BatchEvidenceFixtures.successful(parameterRows.size()));
            });
        }
    }

    private static final class CapturingSyncBatchExecutor implements SyncBatchExecutor {

        @Override
        public boolean supportsBatchRowLayouts() { return true; }

        private final AtomicReference<BatchWriteRequest> request = new AtomicReference<>();
        private final AtomicReference<List<Object[]>> rows = new AtomicReference<>();

        @Override
        public BatchExecutionEvidence writeBatch(BatchWriteRequest request, java.util.function.LongConsumer rowCompleted) {
            this.request.set(request);
            List<Object[]> parameterRows = Flux.from(request.rows()).collectList().block();
            rows.set(parameterRows);
            if (rowCompleted != null) {
                for (int index = 0; index < parameterRows.size(); index++) rowCompleted.accept(index);
            }
            return committed(request, parameterRows);
        }

    }

    private static final class NoopSyncSqlExecutor implements com.flying.orm.rdb.support.ListQueryFixture {

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            throw new UnsupportedOperationException();
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

    private static BatchExecutionEvidence committed(BatchWriteRequest request, List<Object[]> rows) {
        return com.flying.orm.rdb.repository.BatchEvidenceFixtures.successful(rows.size());
    }
}
