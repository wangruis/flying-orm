package com.flying.orm.rdb.schema;

import java.util.Objects;

/**
 * 调用方希望用哪条边界判断数据库结构。
 *
 * <p>这三个值只描述“哪些差异可以被接受”，不表示绕过审核或强制执行。不能证明安全的结构
 * 仍由 {@link SchemaCompatibilityStatus#INCOMPATIBLE} 返回，调用方不能通过选择另一个模式把它
 * 变成安全操作。</p>
 *
 * @author wangr
 * @version v3.2
 */
public enum SchemaCompatibilityMode {

    /** 所有已受管结构必须完全一致，不接受任何差异。 */
    EXACT,

    /** 允许不妨碍当前读写的受控多余对象，例如可空的额外列和非唯一额外索引。 */
    ROLLING_COMPATIBLE,

    /** 在 rolling 兼容的基础上，允许由当前事实能够证明安全的增量操作。 */
    SAFE_INCREMENTAL;

    /** 判断一种已分类差异是否落在当前模式的边界内。 */
    public boolean accepts(SchemaOperation.Compatibility compatibility) {
        SchemaOperation.Compatibility impact = Objects.requireNonNull(
                compatibility, "schema operation compatibility must not be null");
        return switch (this) {
            case EXACT -> false;
            case ROLLING_COMPATIBLE -> impact == SchemaOperation.Compatibility.COMPATIBLE_EXTRA;
            case SAFE_INCREMENTAL -> impact != SchemaOperation.Compatibility.REQUIRES_REVIEW;
        };
    }
}
