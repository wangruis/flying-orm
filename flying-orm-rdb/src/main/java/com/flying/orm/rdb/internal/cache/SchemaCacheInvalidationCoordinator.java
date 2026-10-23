package com.flying.orm.rdb.internal.cache;

import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.metadata.MetadataCacheInvalidator;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Schema DDL 后唯一负责元数据与结构计划缓存失效的内部协调点。
 *
 * @author wangr
 * @version v3.1
 */
public final class SchemaCacheInvalidationCoordinator
        implements MetadataCacheInvalidator, Consumer<RelationIdentity> {

    private static final String FAILURE_MESSAGE =
            "schema DDL executed but cache invalidation failed";
    private static final SchemaCacheInvalidationCoordinator EMPTY =
            new SchemaCacheInvalidationCoordinator(List.of());

    private final List<Target> targets;

    private SchemaCacheInvalidationCoordinator(List<Target> targets) {
        this.targets = targets;
    }

    public static SchemaCacheInvalidationCoordinator of(MetadataCacheInvalidator... invalidators) {
        Objects.requireNonNull(invalidators, "schema cache invalidators must not be null");
        if (invalidators.length == 0) return EMPTY;
        List<Target> targets = new ArrayList<>(invalidators.length);
        IdentityHashMap<Object, Boolean> identities = new IdentityHashMap<>();
        for (MetadataCacheInvalidator invalidator : invalidators) {
            MetadataCacheInvalidator safe = Objects.requireNonNull(
                    invalidator, "schema cache invalidator must not be null");
            if (safe == MetadataCacheInvalidator.none()) continue;
            if (safe instanceof SchemaCacheInvalidationCoordinator coordinator) {
                for (Target target : coordinator.targets) {
                    if (identities.put(target.identity(), Boolean.TRUE) == null) targets.add(target);
                }
            } else if (identities.put(safe, Boolean.TRUE) == null) {
                targets.add(new Target(safe, safe));
            }
        }
        return targets.isEmpty() ? EMPTY : new SchemaCacheInvalidationCoordinator(targets);
    }

    public static SchemaCacheInvalidationCoordinator from(Consumer<RelationIdentity> invalidator) {
        Consumer<RelationIdentity> safe = Objects.requireNonNull(
                invalidator, "schema metadata invalidator must not be null");
        if (safe instanceof SchemaCacheInvalidationCoordinator coordinator) {
            return coordinator;
        }
        return new SchemaCacheInvalidationCoordinator(List.of(new Target(safe, consumerTarget(safe))));
    }

    /** 为自动迁移加入参与规划的 reader；已属于当前对象图时按身份跳过。 */
    public SchemaCacheInvalidationCoordinator with(Object identity, Consumer<RelationIdentity> invalidator) {
        Object safeIdentity = Objects.requireNonNull(identity, "schema cache identity must not be null");
        Consumer<RelationIdentity> safeInvalidator = Objects.requireNonNull(
                invalidator, "schema cache invalidator must not be null");
        return withTarget(safeIdentity, consumerTarget(safeInvalidator));
    }

    /** 加入能够保留 schema/table 分段身份的失效器。 */
    public SchemaCacheInvalidationCoordinator with(Object identity,
                                                   MetadataCacheInvalidator invalidator) {
        return withTarget(
                Objects.requireNonNull(identity, "schema cache identity must not be null"),
                Objects.requireNonNull(invalidator, "schema cache invalidator must not be null"));
    }

    private SchemaCacheInvalidationCoordinator withTarget(
            Object safeIdentity, MetadataCacheInvalidator safeInvalidator) {
        for (Target target : targets) {
            if (target.identity() == safeIdentity) {
                return this;
            }
        }
        List<Target> combined = new ArrayList<>(targets.size() + 1);
        combined.addAll(targets);
        combined.add(new Target(safeIdentity, safeInvalidator));
        return new SchemaCacheInvalidationCoordinator(combined);
    }

    /** 按规范关系身份失效；每个目标接收同一份分段身份。 */
    @Override
    public void invalidate(RelationIdentity relation) {
        RelationIdentity target = Objects.requireNonNull(
                relation, "schema relation identity must not be null");
        notifyAll(registered -> registered.invalidator().invalidate(target), targets);
    }

    @Override
    public void accept(RelationIdentity table) {
        invalidate(table);
    }

    @Override
    public void invalidateAll() {
        notifyAll(target -> target.invalidator().invalidateAll(), targets);
    }

    /** 所有目标表都必须收到通知；失败只在尝试完成后统一抛出。 */
    public static <T> void invalidateTables(Consumer<T> invalidator, List<T> tables, Throwable primaryFailure) {
        Consumer<T> safeInvalidator = Objects.requireNonNull(
                invalidator, "schema metadata invalidator must not be null");
        List<T> safeTables = Objects.requireNonNull(tables, "schema metadata tables must not be null");
        for (T table : safeTables) {
            Objects.requireNonNull(table, "schema metadata table must not be null");
        }
        try {
            notifyAll(safeInvalidator, safeTables);
        } catch (RuntimeException invalidationFailure) {
            if (primaryFailure == null) {
                throw invalidationFailure;
            }
            suppress(primaryFailure, invalidationFailure);
        }
    }

    private static <T> void notifyAll(Consumer<T> invalidator, List<T> values) {
        if (values.isEmpty()) return;
        List<Throwable> failures = new ArrayList<>();
        for (T value : values) {
            try {
                invalidator.accept(value);
            } catch (RuntimeException | Error failure) {
                record(failures, failure);
            }
        }
        throwFailures(failures);
    }

    private static MetadataCacheInvalidator consumerTarget(Consumer<RelationIdentity> consumer) {
        return new MetadataCacheInvalidator() {
            @Override
            public void invalidateAll() {
                // 现有 Consumer<RelationIdentity> 公共契约没有全量失效能力。
            }

            @Override
            public void invalidate(RelationIdentity relation) {
                consumer.accept(relation);
            }
        };
    }

    private static void record(List<Throwable> failures, Throwable failure) {
        if (failure instanceof RdbException rdb
                && rdb.kind() == RdbErrorKind.UNKNOWN
                && FAILURE_MESSAGE.equals(rdb.getMessage())) {
            Throwable cause = rdb.getCause();
            failures.add(cause == null ? rdb : cause);
            for (Throwable suppressed : rdb.getSuppressed()) {
                failures.add(suppressed);
            }
            return;
        }
        failures.add(failure);
    }

    private static void throwFailures(List<Throwable> failures) {
        if (failures.isEmpty()) {
            return;
        }
        for (Throwable failure : failures) {
            if (failure instanceof VirtualMachineError fatal) {
                suppressOthers(fatal, failures);
                throw fatal;
            }
        }
        for (Throwable failure : failures) {
            if (failure instanceof Error error) {
                suppressOthers(error, failures);
                throw error;
            }
        }
        Throwable first = failures.getFirst();
        RdbException aggregated = new RdbException(
                RdbErrorKind.UNKNOWN, FAILURE_MESSAGE, null, null, first);
        failures.stream().skip(1).forEach(failure -> suppress(aggregated, failure));
        throw aggregated;
    }

    private static void suppressOthers(Throwable primary, List<Throwable> failures) {
        for (Throwable failure : failures) {
            suppress(primary, failure);
        }
    }

    private static void suppress(Throwable primary, Throwable secondary) {
        if (secondary != null && secondary != primary) {
            primary.addSuppressed(secondary);
        }
    }

    private record Target(Object identity, MetadataCacheInvalidator invalidator) {
    }
}
