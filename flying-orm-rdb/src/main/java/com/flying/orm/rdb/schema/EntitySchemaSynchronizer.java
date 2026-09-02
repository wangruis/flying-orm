package com.flying.orm.rdb.schema;

import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
        return jdbc.planCreateOrAlter(target.metadata().toDynamicForm(),
                                      target.metadata().targetIndexes(), jdbcMetadata,
                                      SchemaMigrationOptions.safe());
    }

    private Mono<SchemaMigrationPlan> planReactive(EntitySchemaTarget target) {
        return reactive.planCreateOrAlter(target.metadata().toDynamicForm(),
                                           target.metadata().targetIndexes(), reactiveMetadata,
                                           SchemaMigrationOptions.safe());
    }

    private SchemaMigrationResult executeSafeJdbc(EntitySchemaTarget target) {
        return jdbc.createOrAlterDetailed(target.metadata().toDynamicForm(),
                                           target.metadata().targetIndexes(), jdbcMetadata);
    }

    private Mono<SchemaMigrationResult> executeSafeReactive(EntitySchemaTarget target) {
        return reactive.createOrAlterDetailed(target.metadata().toDynamicForm(),
                                               target.metadata().targetIndexes(), reactiveMetadata);
    }

    private ReviewedSchemaMigrationPlan reviewJdbc(EntitySchemaTarget target) {
        return jdbc.reviewCreateOrAlter(target.metadata().toDynamicForm(),
                                        target.metadata().targetIndexes(), List.of(), jdbcMetadata,
                                        FULL_OPTIONS, SchemaMigrationReviewPolicy.preferOnline());
    }

    private Mono<ReviewedSchemaMigrationPlan> reviewReactive(EntitySchemaTarget target) {
        return reactive.reviewCreateOrAlter(target.metadata().toDynamicForm(),
                                             target.metadata().targetIndexes(), List.of(), reactiveMetadata,
                                             FULL_OPTIONS, SchemaMigrationReviewPolicy.preferOnline());
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
