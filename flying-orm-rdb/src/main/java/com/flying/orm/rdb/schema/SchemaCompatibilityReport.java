package com.flying.orm.rdb.schema;

import java.util.List;
import java.util.Objects;

/**
 * 一次纯结构 diff 的不可变兼容报告。
 *
 * <p>报告始终保留全部 operation；status 只是按指定 mode 对这些事实做出的汇总结论。调用方因而
 * 可以展示不兼容原因或把 operation 交给风险分类器，又不会把 INCOMPATIBLE 误当作执行开关。</p>
 *
 * @author wangr
 * @version v3.2
 */
public final class SchemaCompatibilityReport {

    private final SchemaCompatibilityMode mode;
    private final SchemaCompatibilityStatus status;
    private final List<SchemaOperation> operations;

    private SchemaCompatibilityReport(SchemaCompatibilityMode mode, List<SchemaOperation> operations) {
        this.mode = Objects.requireNonNull(mode, "schema compatibility mode must not be null");
        this.operations = List.copyOf(Objects.requireNonNull(
                operations, "schema operations must not be null"));
        if (this.operations.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("schema operations must not contain null");
        }
        this.status = accepted() ? SchemaCompatibilityStatus.COMPATIBLE
                                 : SchemaCompatibilityStatus.INCOMPATIBLE;
    }

    /** 根据 mode 汇总一份已确定顺序的 operation 快照。 */
    public static SchemaCompatibilityReport of(SchemaCompatibilityMode mode,
                                               List<SchemaOperation> operations) {
        return new SchemaCompatibilityReport(mode, operations);
    }

    public SchemaCompatibilityMode mode() {
        return mode;
    }

    public SchemaCompatibilityStatus status() {
        return status;
    }

    public List<SchemaOperation> operations() {
        return operations;
    }

    /** @return desired 与 actual 没有任何受管结构差异 */
    public boolean exact() {
        return operations.isEmpty();
    }

    public boolean compatible() {
        return status == SchemaCompatibilityStatus.COMPATIBLE;
    }

    /** JavaBean 风格别名，便于上层策略和模板直接消费。 */
    public boolean isCompatible() {
        return compatible();
    }

    private boolean accepted() {
        if (operations.isEmpty()) {
            return true;
        }
        return mode != SchemaCompatibilityMode.EXACT
                && operations.stream().allMatch(operation -> mode.accepts(operation.compatibility()));
    }
}
