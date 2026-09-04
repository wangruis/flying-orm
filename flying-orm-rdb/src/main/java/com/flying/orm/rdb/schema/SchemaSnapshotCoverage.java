package com.flying.orm.rdb.schema;

import com.flying.orm.core.internal.hash.StableDigest;
import com.flying.orm.core.internal.hash.StableEncoder;

import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * 元数据 reader 能稳定回读的结构事实集合。
 *
 * <p>coverage 描述 reader 的能力，不描述某一次读取结果。它是不可变、可指纹化的执行前置条件：
 * 只有 {@link #complete()} 才足以自动验证完整关系 DDL；部分 coverage 仍可用于读取和展示已观测子集，
 * 但不能把未观测事实当成“不存在”。</p>
 *
 * @author wangr
 * @version v3.2
 */
public final class SchemaSnapshotCoverage {

    private static final StableDigest.Domain FINGERPRINT_DOMAIN =
            StableDigest.domain("schema-snapshot-coverage/v1");

    public enum Fact {
        TABLE_EXISTENCE,
        TABLE_COMMENT,
        COLUMNS,
        PRIMARY_KEY,
        UNIQUE_CONSTRAINTS,
        INDEXES,
        FOREIGN_KEYS,
        CHECK_CONSTRAINTS,
        PRIMARY_KEY_NAME,
        COLUMN_DEFAULT,
        COLUMN_GENERATION,
        COLUMN_CHARSET,
        COLUMN_COLLATION,
        INDEX_DIRECTION,
        FOREIGN_KEY_ACTION,
        FOREIGN_KEY_REFERENCE_SCOPE
    }

    private static final SchemaSnapshotCoverage NONE = new SchemaSnapshotCoverage(Set.of());
    private static final SchemaSnapshotCoverage COMPLETE =
            new SchemaSnapshotCoverage(EnumSet.allOf(Fact.class));

    private final Set<Fact> observed;
    private final String fingerprint;

    private SchemaSnapshotCoverage(Set<Fact> observed) {
        EnumSet<Fact> copy = observed.isEmpty()
                ? EnumSet.noneOf(Fact.class) : EnumSet.copyOf(observed);
        this.observed = Set.copyOf(copy);
        StableEncoder encoder = StableDigest.sha256(FINGERPRINT_DOMAIN);
        for (Fact fact : Fact.values()) {
            encoder.bool(fact.name(), copy.contains(fact));
        }
        fingerprint = encoder.finishHex();
    }

    public static SchemaSnapshotCoverage none() {
        return NONE;
    }

    public static SchemaSnapshotCoverage complete() {
        return COMPLETE;
    }

    public static SchemaSnapshotCoverage of(Set<Fact> observed) {
        Set<Fact> safeObserved = Objects.requireNonNull(
                observed, "observed schema facts must not be null");
        if (safeObserved.isEmpty()) {
            return NONE;
        }
        if (safeObserved.size() == Fact.values().length) {
            return COMPLETE;
        }
        return new SchemaSnapshotCoverage(safeObserved);
    }

    public boolean observes(Fact fact) {
        return observed.contains(Objects.requireNonNull(fact, "schema fact must not be null"));
    }

    public Set<Fact> observedFacts() {
        return observed;
    }

    public List<Fact> missingFacts() {
        return java.util.Arrays.stream(Fact.values()).filter(fact -> !observed.contains(fact)).toList();
    }

    public boolean isComplete() {
        return observed.size() == Fact.values().length;
    }

    public String fingerprint() {
        return fingerprint;
    }

    @Override
    public boolean equals(Object other) {
        return this == other
                || other instanceof SchemaSnapshotCoverage coverage
                && observed.equals(coverage.observed);
    }

    @Override
    public int hashCode() {
        return observed.hashCode();
    }

    @Override
    public String toString() {
        return "SchemaSnapshotCoverage[observed=" + observed + ']';
    }
}
