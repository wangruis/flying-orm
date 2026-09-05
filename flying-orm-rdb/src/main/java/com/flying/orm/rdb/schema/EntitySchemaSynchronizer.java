package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalSchemaDefinition;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.IndexMetadata;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
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
 * 后面的元数据编译、差异计算、风险审核、方言渲染、执行和缓存失效都复用 flying-orm 自己的同一条链路。</p>
 *
 * <p>同步 JDBC 与响应式 R2DBC 是两个明确入口。响应式入口保持冷发布器语义，调用方订阅后才读取元数据和执行 DDL，
 * 不会在构建客户端时隐藏 {@code block()}。</p>
 *
 * @author wangr
 * @version v2.0.0
 */
public final class EntitySchemaSynchronizer {

    private static final SchemaMigrationOptions FULL_OPTIONS = SchemaMigrationOptions.safe()
            .allowDropColumn()
            .allowColumnChange()
            .allowPrimaryKeyChange()
            .allowDropIndex()
            .allowRebuildIndex();

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
        List<EntitySchemaTarget> targets = EntitySchemaSyncSupport.targets(models, entityTypes);
        RelationalSchemaDefinition desired = relationalSchema(targets);
        return new MultiTableSchemaPlanner(database, cycleSupport).plan(desired);
    }

    /** 使用实体规范关系模型完成同步 JDBC 审阅、执行和执行后验证。 */
    public EntityRelationalSchemaSyncReport synchronizeRelational(
            DatabaseDescriptor database,
            EntitySchemaSyncMode mode,
            Collection<Class<?>> entityTypes) {
        return synchronizeRelational(database, mode, Map.of(), entityTypes);
    }

    /**
     * JDBC 完整关系同步入口。所有表先审阅并核对人工步骤与精确批准，确认整批可执行后才发送第一条 DDL。
     */
    public EntityRelationalSchemaSyncReport synchronizeRelational(
            DatabaseDescriptor database,
            EntitySchemaSyncMode mode,
            Map<String, SchemaMigrationApproval> approvals,
            Collection<Class<?>> entityTypes) {
        EntitySchemaSyncMode safeMode = Objects.requireNonNull(
                mode, "entity schema sync mode must not be null");
        if (safeMode == EntitySchemaSyncMode.OFF) {
            return EntityRelationalSchemaSyncReport.off();
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
        List<ReviewedSchemaPlan> plans = reviewRelationalBatch(
                safeDatabase, safeMode, batch, snapshots, jdbcMetadata.snapshotCoverage(), reviewer);
        rejectManualClosure(safeMode, batch, plans);
        Map<String, SchemaMigrationApproval> safeApprovals = authorizeRelational(
                safeMode, plans, approvals);
        if (safeMode == EntitySchemaSyncMode.VALIDATE) {
            return new EntityRelationalSchemaSyncReport(safeMode, plans, List.of());
        }
        List<SchemaExecutionReport> results = plans.stream()
                .map(plan -> executeRelationalJdbc(plan, safeApprovals))
                .toList();
        return new EntityRelationalSchemaSyncReport(safeMode, plans, results);
    }

    /** 响应式完整关系同步入口；订阅前不编译实体、不读字典、不执行 DDL。 */
    public Mono<EntityRelationalSchemaSyncReport> synchronizeRelationalReactive(
            DatabaseDescriptor database,
            EntitySchemaSyncMode mode,
            Collection<Class<?>> entityTypes) {
        return synchronizeRelationalReactive(database, mode, Map.of(), entityTypes);
    }

    /** 响应式完整关系同步入口，批准规则与同步 JDBC 入口一致。 */
    public Mono<EntityRelationalSchemaSyncReport> synchronizeRelationalReactive(
            DatabaseDescriptor database,
            EntitySchemaSyncMode mode,
            Map<String, SchemaMigrationApproval> approvals,
            Collection<Class<?>> entityTypes) {
        return Mono.defer(() -> {
            EntitySchemaSyncMode safeMode = Objects.requireNonNull(
                    mode, "entity schema sync mode must not be null");
            if (safeMode == EntitySchemaSyncMode.OFF) {
                return Mono.just(EntityRelationalSchemaSyncReport.off());
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
                            safeMode, batch, plans, approvals));
        });
    }

    public EntitySchemaSyncReport synchronize(EntitySchemaSyncMode mode, Class<?>... entityTypes) {
        return synchronize(mode, Map.of(), List.of(entityTypes));
    }

    public EntitySchemaSyncReport synchronize(EntitySchemaSyncMode mode,
                                               Map<String, SchemaMigrationApproval> approvals,
                                               Collection<Class<?>> entityTypes) {
        EntitySchemaSyncMode safeMode = Objects.requireNonNull(mode, "entity schema sync mode must not be null");
        if (safeMode == EntitySchemaSyncMode.OFF) {
            return EntitySchemaSyncReport.off();
        }
        requireJdbc();
        List<EntitySchemaTarget> targets = EntitySchemaSyncSupport.targets(models, entityTypes);
        if (safeMode != EntitySchemaSyncMode.FULL_UPDATE) {
            List<SchemaMigrationPlan> plans = targets.stream().map(this::planJdbc).toList();
            if (safeMode == EntitySchemaSyncMode.VALIDATE) {
                return validate(plans);
            }
            rejectSkipped(safeMode, plans, List.of());
            List<SchemaMigrationResult> results = targets.stream().map(this::executeSafeJdbc).toList();
            return new EntitySchemaSyncReport(safeMode, plans, List.of(), results);
        }
        List<ReviewedSchemaMigrationPlan> reviews = targets.stream().map(this::reviewJdbc).toList();
        List<SchemaMigrationPlan> reviewedPlans = reviews.stream()
                                                         .map(ReviewedSchemaMigrationPlan::migration)
                                                         .toList();
        rejectSkipped(safeMode, reviewedPlans, reviews);
        Map<String, SchemaMigrationApproval> safeApprovals = EntitySchemaSyncSupport.normalizedApprovals(approvals);
        EntitySchemaSyncSupport.verifyApprovals(reviews, safeApprovals);
        List<SchemaMigrationResult> results = reviews.stream()
                .map(review -> executeReviewedJdbc(review, safeApprovals))
                .toList();
        return new EntitySchemaSyncReport(safeMode, reviewedPlans, reviews, results);
    }

    public Mono<EntitySchemaSyncReport> synchronizeReactive(EntitySchemaSyncMode mode, Class<?>... entityTypes) {
        return synchronizeReactive(mode, Map.of(), List.of(entityTypes));
    }

    public Mono<EntitySchemaSyncReport> synchronizeReactive(EntitySchemaSyncMode mode,
                                                             Map<String, SchemaMigrationApproval> approvals,
                                                             Collection<Class<?>> entityTypes) {
        return Mono.defer(() -> {
            EntitySchemaSyncMode safeMode = Objects.requireNonNull(mode,
                                                                    "entity schema sync mode must not be null");
            if (safeMode == EntitySchemaSyncMode.OFF) {
                return Mono.just(EntitySchemaSyncReport.off());
            }
            requireReactive();
            List<EntitySchemaTarget> targets = EntitySchemaSyncSupport.targets(models, entityTypes);
            if (safeMode == EntitySchemaSyncMode.FULL_UPDATE) {
                return Flux.fromIterable(targets)
                           .concatMap(this::reviewReactive)
                           .collectList()
                           .flatMap(reviews -> executeReviewedReactive(approvals, reviews));
            }
            return Flux.fromIterable(targets)
                       .concatMap(this::planReactive)
                       .collectList()
                       .flatMap(plans -> executeReactive(safeMode, targets, plans));
        });
    }

    private Mono<EntitySchemaSyncReport> executeReactive(EntitySchemaSyncMode mode,
                                                          List<EntitySchemaTarget> targets,
                                                          List<SchemaMigrationPlan> plans) {
        if (mode == EntitySchemaSyncMode.VALIDATE) {
            return Mono.fromCallable(() -> validate(plans));
        }
        rejectSkipped(mode, plans, List.of());
        return Flux.fromIterable(targets)
                   .concatMap(this::executeSafeReactive)
                   .collectList()
                   .map(results -> new EntitySchemaSyncReport(mode, plans, List.of(), results));
    }

    private Mono<EntityRelationalSchemaSyncReport> executeRelationalReactive(
            EntitySchemaSyncMode mode,
            RelationalBatch batch,
            List<ReviewedSchemaPlan> plans,
            Map<String, SchemaMigrationApproval> approvals) {
        rejectManualClosure(mode, batch, plans);
        Map<String, SchemaMigrationApproval> safeApprovals = authorizeRelational(
                mode, plans, approvals);
        if (mode == EntitySchemaSyncMode.VALIDATE) {
            return Mono.just(new EntityRelationalSchemaSyncReport(mode, plans, List.of()));
        }
        return Flux.fromIterable(plans)
                .concatMap(plan -> executeRelationalReactive(plan, safeApprovals))
                .collectList()
                .map(results -> new EntityRelationalSchemaSyncReport(mode, plans, results));
    }

    private static RelationalBatch relationalBatch(
            DatabaseDescriptor database,
            List<EntitySchemaTarget> targets) {
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
                                             List<ReviewedSchemaPlan> plans) {
        if (mode == EntitySchemaSyncMode.VALIDATE || batch.manualForeignKeys().isEmpty()) {
            return;
        }
        boolean required = plans.stream()
                .flatMap(plan -> plan.operations().stream())
                .anyMatch(operation -> requiresManualClosure(batch, operation));
        if (required) {
            throw new EntityRelationalSchemaSyncException(
                    "entity relational schema synchronization contains foreign-key dependencies "
                            + "that require manual two-phase closure",
                    new EntityRelationalSchemaSyncReport(mode, plans, List.of()));
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

    private static RelationalSchemaDefinition relationalSchema(List<EntitySchemaTarget> targets) {
        List<RelationalTableDefinition> tables = targets.stream()
                .flatMap(target -> target.descriptor().schema().tables().stream())
                .toList();
        rejectManagedReferencesToProtectedColumns(targets, tables);
        return RelationalSchemaDefinition.of(tables);
    }

    private static void rejectManagedReferencesToProtectedColumns(
            List<EntitySchemaTarget> targets,
            List<RelationalTableDefinition> tables) {
        Map<RelationIdentity, DynamicForm> managedForms = new HashMap<>(targets.size());
        targets.forEach(target -> managedForms.put(
                target.descriptor().table().identity(), target.descriptor().form()));
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

    private static List<ReviewedSchemaPlan> reviewRelationalBatch(
            DatabaseDescriptor database, EntitySchemaSyncMode mode, RelationalBatch batch,
            List<SchemaSnapshot> snapshots, SchemaSnapshotCoverage coverage,
            RelationalSchemaPlanReviewer reviewer) {
        // 先收齐全部实际快照，后面的已有表也可能持有前面新表复用的序列。
        Map<String, String> sequences = reviewer.observedSequences(snapshots);
        List<ReviewedSchemaPlan> plans = new ArrayList<>(batch.targets().size());
        for (int index = 0; index < batch.targets().size(); index++) {
            plans.add(reviewer.review(database, batch.targets().get(index),
                    snapshots.get(index), coverage, compatibilityMode(mode), sequences));
        }
        return List.copyOf(plans);
    }

    private SchemaExecutionReport executeRelationalJdbc(
            ReviewedSchemaPlan plan,
            Map<String, SchemaMigrationApproval> approvals) {
        SchemaMigrationApproval approval = approvalFor(plan, approvals);
        return approval == null
                ? jdbc.executeReviewed(plan, jdbcMetadata)
                : jdbc.executeReviewed(plan, jdbcMetadata, approval);
    }

    private Mono<SchemaExecutionReport> executeRelationalReactive(
            ReviewedSchemaPlan plan,
            Map<String, SchemaMigrationApproval> approvals) {
        SchemaMigrationApproval approval = approvalFor(plan, approvals);
        return approval == null
                ? reactive.executeReviewed(plan, reactiveMetadata)
                : reactive.executeReviewed(plan, reactiveMetadata, approval);
    }

    private static Map<String, SchemaMigrationApproval> authorizeRelational(
            EntitySchemaSyncMode mode,
            List<ReviewedSchemaPlan> plans,
            Map<String, SchemaMigrationApproval> approvals) {
        EntityRelationalSchemaSyncReport report = new EntityRelationalSchemaSyncReport(
                mode, plans, List.of());
        if (mode == EntitySchemaSyncMode.VALIDATE && report.hasDifferences()) {
            throw new EntityRelationalSchemaSyncException(
                    "entity relational schema validation found database differences", report);
        }
        if (report.requiresManualAction()) {
            throw new EntityRelationalSchemaSyncException(
                    "entity relational schema synchronization requires manual SQL", report);
        }
        if (mode == EntitySchemaSyncMode.SAFE_UPDATE
                && plans.stream().anyMatch(plan -> plan.risk() != SchemaMigrationRiskLevel.LOW)) {
            throw new EntityRelationalSchemaSyncException(
                    "safe entity relational schema synchronization contains reviewed-risk operations", report);
        }
        Map<String, SchemaMigrationApproval> normalized =
                EntitySchemaSyncSupport.normalizedApprovals(approvals);
        if (mode == EntitySchemaSyncMode.FULL_UPDATE) {
            for (ReviewedSchemaPlan plan : plans) {
                if (plan.risk() == SchemaMigrationRiskLevel.LOW) {
                    continue;
                }
                SchemaMigrationApproval approval = approvalFor(plan, normalized);
                if (approval == null || !plan.fingerprint().equals(approval.planFingerprint())) {
                    throw new EntityRelationalSchemaSyncException(
                            "entity relational schema plan requires an exact approval", report);
                }
            }
        }
        return normalized;
    }

    private static SchemaMigrationApproval approvalFor(
            ReviewedSchemaPlan plan,
            Map<String, SchemaMigrationApproval> approvals) {
        return approvals.get(qualifiedName(plan.desiredTable().orElseThrow().identity()));
    }

    private static String qualifiedName(com.flying.orm.core.metadata.RelationIdentity identity) {
        StringBuilder name = new StringBuilder();
        identity.catalog().ifPresent(catalog -> name.append(catalog).append('.'));
        identity.schema().ifPresent(schema -> name.append(schema).append('.'));
        return name.append(identity.table()).toString();
    }

    private static SchemaCompatibilityMode compatibilityMode(EntitySchemaSyncMode mode) {
        return switch (mode) {
            case VALIDATE, FULL_UPDATE -> SchemaCompatibilityMode.EXACT;
            case SAFE_UPDATE -> SchemaCompatibilityMode.SAFE_INCREMENTAL;
            case OFF -> throw new IllegalArgumentException("OFF mode does not create a relational schema plan");
        };
    }

    private Mono<EntitySchemaSyncReport> executeReviewedReactive(
            Map<String, SchemaMigrationApproval> approvals,
            List<ReviewedSchemaMigrationPlan> reviews) {
        List<SchemaMigrationPlan> reviewedPlans = reviews.stream()
                                                         .map(ReviewedSchemaMigrationPlan::migration)
                                                         .toList();
        rejectSkipped(EntitySchemaSyncMode.FULL_UPDATE, reviewedPlans, reviews);
        Map<String, SchemaMigrationApproval> safeApprovals = EntitySchemaSyncSupport.normalizedApprovals(approvals);
        EntitySchemaSyncSupport.verifyApprovals(reviews, safeApprovals);
        return Flux.fromIterable(reviews)
                   .concatMap(review -> executeReviewedReactive(review, safeApprovals))
                   .collectList()
                   .map(results -> new EntitySchemaSyncReport(
                           EntitySchemaSyncMode.FULL_UPDATE, reviewedPlans, reviews, results));
    }

    private SchemaMigrationPlan planJdbc(EntitySchemaTarget target) {
        return jdbc.planCreateOrAlter(target.descriptor().form(),
                                      legacyIndexes(target), jdbcMetadata,
                                      SchemaMigrationOptions.safe());
    }

    private Mono<SchemaMigrationPlan> planReactive(EntitySchemaTarget target) {
        return reactive.planCreateOrAlter(target.descriptor().form(),
                                           legacyIndexes(target), reactiveMetadata,
                                           SchemaMigrationOptions.safe());
    }

    private SchemaMigrationResult executeSafeJdbc(EntitySchemaTarget target) {
        return jdbc.createOrAlterDetailed(target.descriptor().form(),
                                           legacyIndexes(target), jdbcMetadata);
    }

    private Mono<SchemaMigrationResult> executeSafeReactive(EntitySchemaTarget target) {
        return reactive.createOrAlterDetailed(target.descriptor().form(),
                                               legacyIndexes(target), reactiveMetadata);
    }

    private ReviewedSchemaMigrationPlan reviewJdbc(EntitySchemaTarget target) {
        return jdbc.reviewCreateOrAlter(target.descriptor().form(),
                                        legacyIndexes(target), List.of(), jdbcMetadata,
                                        FULL_OPTIONS, SchemaMigrationReviewPolicy.preferOnline());
    }

    private Mono<ReviewedSchemaMigrationPlan> reviewReactive(EntitySchemaTarget target) {
        return reactive.reviewCreateOrAlter(target.descriptor().form(),
                                             legacyIndexes(target),
                                             List.of(), reactiveMetadata,
                                             FULL_OPTIONS, SchemaMigrationReviewPolicy.preferOnline());
    }

    private static List<IndexMetadata> legacyIndexes(EntitySchemaTarget target) {
        var descriptor = target.descriptor();
        if (descriptor.form().protections().encryptedFields().isEmpty()) {
            return descriptor.metadata().targetIndexes();
        }
        RelationalTableDefinition table = descriptor.table();
        List<IndexMetadata> indexes = new ArrayList<>(
                table.uniqueConstraints().size() + table.indexes().size());
        table.uniqueConstraints().forEach(unique -> {
            IndexMetadata.Builder index = IndexMetadata.builder(unique.name()).unique();
            unique.columns().forEach(index::addColumn);
            indexes.add(index.build());
        });
        table.indexes().forEach(source -> {
            IndexMetadata.Builder index = IndexMetadata.builder(source.name());
            if (source.unique()) {
                index.unique();
            }
            source.keys().forEach(key -> index.addColumn(key.column()));
            indexes.add(index.build());
        });
        return List.copyOf(indexes);
    }

    private static EntitySchemaSyncReport validate(List<SchemaMigrationPlan> plans) {
        return EntitySchemaSyncSupport.validate(plans);
    }

    private static void rejectSkipped(EntitySchemaSyncMode mode,
                                      List<SchemaMigrationPlan> plans,
                                      List<ReviewedSchemaMigrationPlan> reviews) {
        EntitySchemaSyncSupport.rejectSkipped(mode, plans, reviews);
    }

    private SchemaMigrationResult executeReviewedJdbc(
            ReviewedSchemaMigrationPlan review,
            Map<String, SchemaMigrationApproval> approvals) {
        SchemaMigrationApproval approval = EntitySchemaSyncSupport.approvalFor(review, approvals);
        return approval == null ? jdbc.executeReviewed(review) : jdbc.executeReviewed(review, approval);
    }

    private Mono<SchemaMigrationResult> executeReviewedReactive(
            ReviewedSchemaMigrationPlan review,
            Map<String, SchemaMigrationApproval> approvals) {
        SchemaMigrationApproval approval = EntitySchemaSyncSupport.approvalFor(review, approvals);
        return approval == null
                ? reactive.executeReviewed(review, reactiveMetadata)
                : reactive.executeReviewed(review, reactiveMetadata, approval);
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
