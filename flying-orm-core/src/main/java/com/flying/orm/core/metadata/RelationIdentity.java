package com.flying.orm.core.metadata;

import com.flying.orm.core.internal.Names;

import java.util.Objects;
import java.util.Optional;

/**
 * 数据库关系的分段身份。
 *
 * <p>catalog、schema 和 table 分别保存，绝不根据点号猜测层级。这样 quoted identifier 中的点号
 * 仍然只是名称的一部分，最终由数据库方言逐段渲染。</p>
 *
 * @author wangr
 * @version v3.2
 */
public final class RelationIdentity {

    private final String catalog;

    private final String schema;

    private final String table;

    private final Optional<String> catalogView;

    private final Optional<String> schemaView;

    private RelationIdentity(String catalog, String schema, String table) {
        this.catalog = optionalSegment(catalog, "relation catalog");
        this.schema = optionalSegment(schema, "relation schema");
        this.table = Names.requireText(table, "relation table");
        this.catalogView = Optional.ofNullable(this.catalog);
        this.schemaView = Optional.ofNullable(this.schema);
    }

    /** 创建显式分段的关系身份；catalog 和 schema 可以不提供。 */
    public static RelationIdentity of(String catalog, String schema, String table) {
        return new RelationIdentity(catalog, schema, table);
    }

    /** 创建只有表名的关系身份；传入名称即使含点号也不会被拆分。 */
    public static RelationIdentity table(String table) {
        return new RelationIdentity(null, null, table);
    }

    public Optional<String> catalog() {
        return catalogView;
    }

    public Optional<String> schema() {
        return schemaView;
    }

    public String table() {
        return table;
    }

    /**
     * 保留 catalog 和 schema，只替换关系名。
     *
     * <p>派生索引表等物理关系使用这个入口，避免把已确认的命名空间降级成一段普通字符串。</p>
     */
    public RelationIdentity withTable(String newTable) {
        return new RelationIdentity(catalog, schema, newTable);
    }

    @Override
    public boolean equals(Object candidate) {
        return this == candidate || candidate instanceof RelationIdentity other
                && Objects.equals(catalog, other.catalog)
                && Objects.equals(schema, other.schema)
                && table.equals(other.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalog, schema, table);
    }

    @Override
    public String toString() {
        return "RelationIdentity[catalog=" + catalog + ", schema=" + schema + ", table=" + table + ']';
    }

    private static String optionalSegment(String value, String name) {
        return value == null ? null : Names.requireText(value, name);
    }
}
