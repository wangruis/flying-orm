package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalSchemaDefinition;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.mapping.EntitySchemaDescriptor;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 把一组实体声明编排成启动期表结构校验或迁移。
 *
 * <p>它不负责猜测怎样扫描 classpath。Spring、Micronaut、自研容器或纯 Java 只要把扫描到的实体类型集合交进来，
 * 后面的元数据编译、差异计算、方言渲染、执行和缓存失效都复用 flying-orm 自己的同一条链路。</p>
 *
 * <p>同步 JDBC 与响应式 R2DBC 是两个明确入口。响应式入口保持冷发布器语义，调用方订阅后才读取元数据和执行 DDL，
 * 不会在构建客户端时隐藏 {@code block()}。</p>
 *
 * @author wangr
 * @version v2.0.0
 */
public final class EntitySchemaSynchronizer {

    private final EntityModelRegistry models;
    private final ReactiveSchemaClient reactive;
    private final ReactiveFormMetadataReader reactiveMetadata;
    private final JdbcSchemaClient jdbc;
    private final JdbcFormMetadataReader jdbcMetadata;

    public EntitySchemaSynchronizer(EntityModelRegistry models,
                                    ReactiveSchemaClient reactive,
                                    ReactiveFormMetadataReader reactiveMetadata,
                                    JdbcSchemaClient jdbc,
                                    JdbcFormMetadataReader jdbcMetadata) {
        this.models = Objects.requireNonNull(models, "entity model registry must not be null");
        this.reactive = reactive;
        this.reactiveMetadata = reactiveMetadata;
        this.jdbc = jdbc;
        this.jdbcMetadata = jdbcMetadata;
        if ((reactive == null) != (reactiveMetadata == null)) {
            throw new IllegalArgumentException("reactive schema client and metadata reader must be configured together");
        }
        if ((jdbc == null) != (jdbcMetadata == null)) {
            throw new IllegalArgumentException("JDBC schema client and metadata reader must be configured together");
        }
    }

    /**
     * 把调用方显式给出的实体集合编译成单数据库两阶段计划。
     *
     * <p>该入口是纯冷规划：先严格编译全部实体，再按规范关系身份稳定排序；不会扫描 classpath、
     * 读取数据库或触发 DDL。</p>
     */
    public MultiTableSchemaPlanner.Plan plan(
            DatabaseDescriptor database,
            Collection<Class<?>> entityTypes,
            MultiTableSchemaPlanner.ForeignKeyCycleSupport cycleSupport) {
        List<EntitySchemaDescriptor<?>> targets = EntitySchemaSyncSupport.targets(models, entityTypes);
        RelationalSchemaDefinition desired = relationalSchema(targets);
        return new MultiTableSchemaPlanner(database, cycleSupport).plan(desired);
    }

    public EntitySchemaSyncReport synchronize(DatabaseDescriptor database,
                                               EntitySchemaSyncMode mode, Class<?>... entityTypes) {
        return synchronize(database, mode, List.of(entityTypes));
    }

    public Mono<EntitySchemaSyncReport> synchronizeReactive(DatabaseDescriptor database,
                                                             EntitySchemaSyncMode mode, Class<?>... entityTypes) {
        return synchronizeReactive(database, mode, List.of(entityTypes));
    }

    /** JDBC 实体同步：先规划所有表的完整结构差异与依赖，再按顺序执行 DDL。 */
    public EntitySchemaSyncReport synchronize(
            DatabaseDescriptor database,
            EntitySchemaSyncMode mode,
            Collection<Class<?>> entityTypes) {
        EntitySchemaSyncMode safeMode = Objects.requireNonNull(
                mode, "entity schema sync mode must not be null");
        if (safeMode == EntitySchemaSyncMode.OFF) {
            return EntitySchemaSyncReport.off();
        }
        requireJdbc();
        DatabaseDescriptor safeDatabase = Objects.requireNonNull(
                database, "database descriptor must not be null");
        RelationalBatch batch = relationalBatch(
                safeDatabase, EntitySchemaSyncSupport.targets(models, entityTypes));
        RelationalSchemaPlanReviewer reviewer = jdbc.relationalReviewer();
        List<SchemaSnapshot> snapshots = batch.targets().stream()
                .map(target -> JdbcSchemaClient.readSnapshot(jdbcMetadata, target.identity()))
                .toList();
        List<SchemaMigrationPlan> plans = reviewRelationalBatch(
                safeDatabase, safeMode, batch, snapshots, jdbcMetadata.snapshotCoverage(), reviewer);
        rejectManualClosure(safeMode, batch, plans);
        validateRelationalPlans(safeMode, plans);
        if (safeMode == EntitySchemaSyncMode.VALIDATE) {
            return new EntitySchemaSyncReport(safeMode, plans, List.of());
        }
        List<SchemaMigrationResult> results = plans.stream()
                .map(this::executeRelationalJdbc)
                .toList();
        return new EntitySchemaSyncReport(safeMode, plans, results);
    }

    /** 响应式完整关系同步入口；订阅后规划和执行，与 JDBC 共用结构规则。 */
    public Mono<EntitySchemaSyncReport> synchronizeReactive(
            DatabaseDescriptor database,
            EntitySchemaSyncMode mode,
            Collection<Class<?>> entityTypes) {
        return Mono.defer(() -> {
            EntitySchemaSyncMode safeMode = Objects.requireNonNull(
                    mode, "entity schema sync mode must not be null");
            if (safeMode == EntitySchemaSyncMode.OFF) {
                return Mono.just(EntitySchemaSyncReport.off());
            }
            requireReactive();
            DatabaseDescriptor safeDatabase = Objects.requireNonNull(
                    database, "database descriptor must not be null");
            RelationalBatch batch = relationalBatch(
                    safeDatabase, EntitySchemaSyncSupport.targets(models, entityTypes));
            RelationalSchemaPlanReviewer reviewer = reactive.relationalReviewer();
            return Flux.fromIterable(batch.targets())
                    .concatMap(target -> ReactiveSchemaClient.readSnapshot(
                            reactiveMetadata, target.identity()))
                    .collectList()
                    .map(snapshots -> reviewRelationalBatch(safeDatabase, safeMode, batch,
                            snapshots, reactiveMetadata.snapshotCoverage(), reviewer))
                    .flatMap(plans -> executeRelationalReactive(
                            safeMode, batch, plans));
        });
    }

    private Mono<EntitySchemaSyncReport> executeRelationalReactive(
            EntitySchemaSyncMode mode,
            RelationalBatch batch,
            List<SchemaMigrationPlan> plans) {
        rejectManualClosure(mode, batch, plans);
        validateRelationalPlans(mode, plans);
        if (mode == EntitySchemaSyncMode.VALIDATE) {
            return Mono.just(new EntitySchemaSyncReport(mode, plans, List.of()));
        }
        return Flux.fromIterable(plans)
                .concatMap(this::executeRelationalReactive)
                .collectList()
                .map(results -> new EntitySchemaSyncReport(mode, plans, results));
    }

    private static RelationalBatch relationalBatch(
            DatabaseDescriptor database,
            List<EntitySchemaDescriptor<?>> targets) {
        RelationalSchemaDefinition desired = relationalSchema(targets);
        MultiTableSchemaPlanner.Plan batch = new MultiTableSchemaPlanner(
                database, MultiTableSchemaPlanner.ForeignKeyCycleSupport.MANUAL_REQUIRED)
                .plan(desired);
        Map<RelationIdentity, RelationalTableDefinition> byIdentity =
                new HashMap<>(desired.tables().size());
        desired.tables().forEach(table -> byIdentity.put(table.identity(), table));
        List<RelationalTableDefinition> ordered = batch.firstPhase().stream()
                .map(operation -> byIdentity.get(operation.relation()))
                .toList();
        Set<ForeignKeyKey> manualForeignKeys = new HashSet<>();
        Set<RelationIdentity> manualRelations = new HashSet<>();
        batch.secondPhase().stream()
                .filter(operation -> operation.kind() == SchemaOperation.Kind.VERIFY_MANUALLY)
                .forEach(operation -> {
                    manualForeignKeys.add(new ForeignKeyKey(
                            operation.relation(), operation.objectName()));
                    manualRelations.add(operation.relation());
                });
        return new RelationalBatch(
                ordered, Set.copyOf(manualForeignKeys), Set.copyOf(manualRelations));
    }

    private static void rejectManualClosure(EntitySchemaSyncMode mode,
                                             RelationalBatch batch,
                                             List<SchemaMigrationPlan> plans) {
        if (mode == EntitySchemaSyncMode.VALIDATE || batch.manualForeignKeys().isEmpty()) {
            return;
        }
        boolean required = plans.stream()
                .flatMap(plan -> plan.operations().stream())
                .anyMatch(operation -> requiresManualClosure(batch, operation));
        if (required) {
            throw new EntitySchemaSyncException(
                    "entity relational schema synchronization contains foreign-key dependencies "
                            + "that require manual two-phase closure",
                    new EntitySchemaSyncReport(mode, plans, List.of()));
        }
    }

    private static boolean requiresManualClosure(RelationalBatch batch, SchemaOperation operation) {
        if (operation.kind() == SchemaOperation.Kind.CREATE_TABLE) {
            return batch.manualRelations().contains(operation.relation());
        }
        if (operation.kind() != SchemaOperation.Kind.ADD_FOREIGN_KEY
                && operation.kind() != SchemaOperation.Kind.CHANGE_FOREIGN_KEY) {
            return false;
        }
        return batch.manualForeignKeys().contains(new ForeignKeyKey(
                operation.relation(), operation.objectName()));
    }

    private static RelationalSchemaDefinition relationalSchema(List<EntitySchemaDescriptor<?>> targets) {
        List<RelationalTableDefinition> tables = targets.stream()
                .flatMap(target -> target.schema().tables().stream())
                .toList();
        rejectManagedReferencesToProtectedColumns(targets, tables);
        return RelationalSchemaDefinition.of(tables);
    }

    private static void rejectManagedReferencesToProtectedColumns(
            List<EntitySchemaDescriptor<?>> targets,
            List<RelationalTableDefinition> tables) {
        Map<RelationIdentity, DynamicForm> managedForms = new HashMap<>(targets.size());
        targets.forEach(target -> managedForms.put(
                target.table().identity(), target.form()));
        for (RelationalTableDefinition table : tables) {
            table.foreignKeys().forEach(foreignKey -> {
                DynamicForm target = managedForms.get(foreignKey.reference());
                if (target != null && foreignKey.referenceColumns().stream()
                        .anyMatch(column -> target.protections().encrypted(column).isPresent())) {
                    throw new IllegalArgumentException(
                            "foreign key must not reference an encrypted managed target field");
                }
            });
        }
    }

    private record RelationalBatch(List<RelationalTableDefinition> targets,
                                   Set<ForeignKeyKey> manualForeignKeys,
                                   Set<RelationIdentity> manualRelations) { }

    private record ForeignKeyKey(RelationIdentity relation, String name) { }

    private static List<SchemaMigrationPlan> reviewRelationalBatch(
            DatabaseDescriptor database, EntitySchemaSyncMode mode, RelationalBatch batch,
            List<SchemaSnapshot> snapshots, SchemaSnapshotCoverage coverage,
            RelationalSchemaPlanReviewer reviewer) {
        // 先收齐全部实际快照，后面的已有表也可能持有前面新表复用的序列。
        var sequences = reviewer.observedSequences(snapshots);
        List<SchemaMigrationPlan> plans = new ArrayList<>(batch.targets().size());
        for (int index = 0; index < batch.targets().size(); index++) {
            plans.add(reviewer.review(database, batch.targets().get(index),
                    snapshots.get(index), coverage, compatibilityMode(mode), sequences));
        }
        List<SchemaMigrationPlan> reviewed = List.copyOf(plans);
        EntitySchemaSyncSupport.rejectReferencedCandidateKeyChanges(
                mode, snapshots, reviewed, reviewer.schemaDialect());
        return reviewed;
    }

    private SchemaMigrationResult executeRelationalJdbc(
            SchemaMigrationPlan plan) {
        return jdbc.execute(plan, jdbcMetadata);
    }

    private Mono<SchemaMigrationResult> executeRelationalReactive(
            SchemaMigrationPlan plan) {
        return reactive.execute(plan, reactiveMetadata);
    }

    private static void validateRelationalPlans(
            EntitySchemaSyncMode mode,
            List<SchemaMigrationPlan> plans) {
        EntitySchemaSyncReport report = new EntitySchemaSyncReport(
                mode, plans, List.of());
        if (mode == EntitySchemaSyncMode.VALIDATE && report.hasDifferences()) {
            throw new EntitySchemaSyncException(
                    "entity relational schema validation found database differences", report);
        }
        if (mode == EntitySchemaSyncMode.SAFE_UPDATE
                && plans.stream().flatMap(plan -> plan.operations().stream())
                        .anyMatch(operation -> operation.compatibility()
                                != SchemaOperation.Compatibility.SAFE_INCREMENTAL)) {
            throw new EntitySchemaSyncException(
                    "safe entity relational schema synchronization contains non-incremental operations", report);
        }
    }

    private static SchemaCompatibilityMode compatibilityMode(EntitySchemaSyncMode mode) {
        return switch (mode) {
            case VALIDATE, FULL_UPDATE -> SchemaCompatibilityMode.EXACT;
            case SAFE_UPDATE -> SchemaCompatibilityMode.SAFE_INCREMENTAL;
            case OFF -> throw new IllegalArgumentException("OFF mode does not create a relational schema plan");
        };
    }

    private void requireJdbc() {
        if (jdbc == null) {
            throw new IllegalStateException("JDBC runtime is not configured; use synchronizeReactive instead");
        }
    }

    private void requireReactive() {
        if (reactive == null) {
            throw new IllegalStateException("R2DBC runtime is not configured; use synchronize instead");
        }
    }
}
