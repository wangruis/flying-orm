package com.flying.orm.rdb.internal.cache;

import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.internal.InternalApi;
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
@InternalApi
public final class SchemaCacheInvalidationCoordinator
        implements MetadataCacheInvalidator, Consumer<String> {

    private static final String FAILURE_MESSAGE =
            "schema DDL executed but cache invalidation failed";

    private final List<Target> targets;

    private SchemaCacheInvalidationCoordinator(List<Target> targets) {
        this.targets = List.copyOf(targets);
    }

    public static SchemaCacheInvalidationCoordinator of(MetadataCacheInvalidator... invalidators) {
        Objects.requireNonNull(invalidators, "schema cache invalidators must not be null");
        List<Target> targets = new ArrayList<>(invalidators.length);
        IdentityHashMap<Object, Boolean> identities = new IdentityHashMap<>();
        for (MetadataCacheInvalidator invalidator : invalidators) {
            MetadataCacheInvalidator safe = Objects.requireNonNull(
                    invalidator, "schema cache invalidator must not be null");
            if (identities.put(safe, Boolean.TRUE) == null) {
                targets.add(new Target(safe, safe));
            }
        }
        return new SchemaCacheInvalidationCoordinator(targets);
    }

    public static SchemaCacheInvalidationCoordinator from(Consumer<String> invalidator) {
        Consumer<String> safe = Objects.requireNonNull(
                invalidator, "schema metadata invalidator must not be null");
        if (safe instanceof SchemaCacheInvalidationCoordinator coordinator) {
            return coordinator;
        }
        return new SchemaCacheInvalidationCoordinator(List.of(new Target(safe, consumerTarget(safe))));
    }

    /** 为自动迁移加入参与规划的 reader；已属于当前对象图时按身份跳过。 */
    public SchemaCacheInvalidationCoordinator with(Object identity, Consumer<String> invalidator) {
        Object safeIdentity = Objects.requireNonNull(identity, "schema cache identity must not be null");
        Consumer<String> safeInvalidator = Objects.requireNonNull(
                invalidator, "schema cache invalidator must not be null");
        for (Target target : targets) {
            if (target.identity() == safeIdentity) {
                return this;
            }
        }
        List<Target> combined = new ArrayList<>(targets.size() + 1);
        combined.addAll(targets);
        combined.add(new Target(safeIdentity, consumerTarget(safeInvalidator)));
        return new SchemaCacheInvalidationCoordinator(combined);
    }

    @Override
    public void accept(String table) {
        invalidate(table);
    }

    @Override
    public void invalidate(String table) {
        List<Throwable> failures = new ArrayList<>();
        for (Target target : targets) {
            try {
                target.invalidator().invalidate(table);
            } catch (RuntimeException | Error failure) {
                record(failures, failure);
            }
        }
        throwFailures(failures);
    }

    @Override
    public void invalidate(String schema, String table) {
        List<Throwable> failures = new ArrayList<>();
        for (Target target : targets) {
            try {
                target.invalidator().invalidate(schema, table);
            } catch (RuntimeException | Error failure) {
                record(failures, failure);
            }
        }
        throwFailures(failures);
    }

    @Override
    public void invalidateAll() {
        List<Throwable> failures = new ArrayList<>();
        for (Target target : targets) {
            try {
                target.invalidator().invalidateAll();
            } catch (RuntimeException | Error failure) {
                record(failures, failure);
            }
        }
        throwFailures(failures);
    }

    /** 所有目标表都必须收到通知；失败只在尝试完成后统一抛出。 */
    public static void invalidateTables(Consumer<String> invalidator, List<String> tables) {
        Consumer<String> safeInvalidator = Objects.requireNonNull(
                invalidator, "schema metadata invalidator must not be null");
        List<String> safeTables = List.copyOf(Objects.requireNonNull(
                tables, "schema metadata tables must not be null"));
        List<Throwable> failures = new ArrayList<>();
        for (String table : safeTables) {
            try {
                safeInvalidator.accept(table);
            } catch (RuntimeException | Error failure) {
                record(failures, failure);
            }
        }
        throwFailures(failures);
    }

    private static MetadataCacheInvalidator consumerTarget(Consumer<String> consumer) {
        return new MetadataCacheInvalidator() {
            @Override
            public void invalidate(String table) {
                consumer.accept(table);
            }

            @Override
            public void invalidateAll() {
                // 现有 Consumer<String> 公共契约没有全量失效能力。
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
