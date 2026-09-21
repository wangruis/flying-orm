package com.flying.orm.core.scope;

import com.flying.orm.core.field.FieldIdentity;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * 字段级读写范围，用于在行级 {@link DataScope} 之外继续限制可见列和可写列。
 *
 * <p>公开构造入口仍把空集合解释成“不限制”，方便只配置读取或写入的一侧。内部合并时另外保存限制状态，
 * 因此两个白名单的交集即使为空，也会准确表示“一个字段都不允许”，不会反过来变成全部放行。</p>
 *
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public final class FieldScope {

    private static final Set<String> EMPTY_FIELDS = Set.of();

    private final Set<String> readableFields;

    private final Set<String> writableFields;

    private final boolean unrestrictedRead;

    private final boolean unrestrictedWrite;

    /** 创建字段范围；空集合沿用公开 API 的既有语义，表示对应方向不限制。 */
    public FieldScope(Set<String> readableFields, Set<String> writableFields) {
        this(normalize(readableFields, "readable fields"),
             normalize(writableFields, "writable fields"),
             Objects.requireNonNull(readableFields, "readable fields must not be null").isEmpty(),
             Objects.requireNonNull(writableFields, "writable fields must not be null").isEmpty());
    }

    /** 只设置可读字段，写入保持不限制。 */
    public FieldScope(Set<String> readableFields) {
        this(readableFields, Set.of());
    }

    private FieldScope(Set<String> readableFields,
                       Set<String> writableFields,
                       boolean unrestrictedRead,
                       boolean unrestrictedWrite) {
        this.readableFields = readableFields;
        this.writableFields = writableFields;
        this.unrestrictedRead = unrestrictedRead;
        this.unrestrictedWrite = unrestrictedWrite;
    }

    /** @return 读写都不限制的字段范围 */
    public static FieldScope unrestricted() {
        return new FieldScope(EMPTY_FIELDS, EMPTY_FIELDS, true, true);
    }

    /** 创建只限制读取字段的范围。字段名按小写比较。 */
    public static FieldScope readable(String first, String... rest) {
        return new FieldScope(fields(first, rest), EMPTY_FIELDS, false, true);
    }

    /** 创建只限制写入字段的范围。字段名按小写比较。 */
    public static FieldScope writable(String first, String... rest) {
        return new FieldScope(EMPTY_FIELDS, fields(first, rest), true, false);
    }

    /** 创建读写字段白名单相同的范围。 */
    public static FieldScope readWrite(String first, String... rest) {
        Set<String> fields = fields(first, rest);
        return new FieldScope(fields, fields, false, false);
    }

    /** @return 规范化后的只读字段白名单；空集合可能表示不限制，也可能表示交集后无权限 */
    public Set<String> readableFields() {
        return readableFields;
    }

    /** @return 规范化后的只写字段白名单；空集合可能表示不限制，也可能表示交集后无权限 */
    public Set<String> writableFields() {
        return writableFields;
    }

    /** @return true 表示读取时不裁剪字段 */
    public boolean unrestrictedRead() {
        return unrestrictedRead;
    }

    /** @return true 表示写入时不按 FieldScope 拒绝字段 */
    public boolean unrestrictedWrite() {
        return unrestrictedWrite;
    }

    /** 判断字段是否允许出现在查询结果中，比较时忽略大小写和首尾空白。 */
    public boolean canRead(String field) {
        return unrestrictedRead || readableFields.contains(FieldIdentity.of(field).key());
    }

    /** 判断字段是否允许出现在 insert/update 数据中，比较时忽略大小写和首尾空白。 */
    public boolean canWrite(String field) {
        return unrestrictedWrite || writableFields.contains(FieldIdentity.of(field).key());
    }

    /**
     * 两个权限范围只能越合越窄。这里保留“受限但交集为空”的状态，避免空集合被公开构造语义误判为不限制。
     */
    static FieldScope intersect(FieldScope left, FieldScope right) {
        FieldScope safeLeft = Objects.requireNonNull(left, "left field scope must not be null");
        FieldScope safeRight = Objects.requireNonNull(right, "right field scope must not be null");
        Set<String> readable = intersectFields(safeLeft.readableFields,
                                               safeLeft.unrestrictedRead,
                                               safeRight.readableFields,
                                               safeRight.unrestrictedRead);
        Set<String> writable = intersectFields(safeLeft.writableFields,
                                               safeLeft.unrestrictedWrite,
                                               safeRight.writableFields,
                                               safeRight.unrestrictedWrite);
        return new FieldScope(readable,
                              writable,
                              safeLeft.unrestrictedRead && safeRight.unrestrictedRead,
                              safeLeft.unrestrictedWrite && safeRight.unrestrictedWrite);
    }

    private static Set<String> intersectFields(Set<String> left,
                                               boolean leftUnrestricted,
                                               Set<String> right,
                                               boolean rightUnrestricted) {
        if (leftUnrestricted) {
            return right;
        }
        if (rightUnrestricted) {
            return left;
        }
        Set<String> values = new LinkedHashSet<>(left);
        values.retainAll(right);
        return Collections.unmodifiableSet(values);
    }

    private static Set<String> normalize(Set<String> fields, String fieldName) {
        Objects.requireNonNull(fields, fieldName + " must not be null");
        Set<String> values = new LinkedHashSet<>();
        for (String field : fields) {
            values.add(FieldIdentity.of(field).key());
        }
        return Collections.unmodifiableSet(values);
    }

    private static Set<String> fields(String first, String... rest) {
        Set<String> fields = new LinkedHashSet<>();
        fields.add(FieldIdentity.of(first).key());
        if (rest != null) {
            for (String field : rest) {
                fields.add(FieldIdentity.of(field).key());
            }
        }
        return Collections.unmodifiableSet(fields);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof FieldScope that)) {
            return false;
        }
        return unrestrictedRead == that.unrestrictedRead
                && unrestrictedWrite == that.unrestrictedWrite
                && readableFields.equals(that.readableFields)
                && writableFields.equals(that.writableFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(readableFields, writableFields, unrestrictedRead, unrestrictedWrite);
    }

    @Override
    public String toString() {
        return "FieldScope[readableFields=" + readableFields
                + ", writableFields=" + writableFields
                + ", unrestrictedRead=" + unrestrictedRead
                + ", unrestrictedWrite=" + unrestrictedWrite + ']';
    }
}
