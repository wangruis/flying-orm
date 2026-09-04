package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalSchemaDefinition;
import com.flying.orm.core.metadata.RelationalTableDefinition;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;

/**
 * 显式关系定义构成的不可变依赖图。
 *
 * <p>边从本地表指向其外键引用表，因此依赖顺序会先发布被引用表。只有完整
 * {@link RelationIdentity} 精确存在于传入 Schema 中时才建立边；集合外关系由后续规划层决定，
 * 图本身既不扫描 classpath，也不猜测或加载额外表。</p>
 *
 * @author wangr
 * @date 2026-09-03
 * @version v3.2
 */
public final class SchemaDependencyGraph {

    private static final Comparator<RelationIdentity> IDENTITY_ORDER =
            SchemaDependencyGraph::compareIdentity;

    private final List<RelationalTableDefinition> tables;
    private final List<RelationIdentity> identities;
    private final Map<RelationIdentity, RelationalTableDefinition> tablesByIdentity;
    private final Map<RelationIdentity, List<RelationIdentity>> dependenciesByIdentity;

    private SchemaDependencyGraph(RelationalSchemaDefinition schema) {
        List<RelationalTableDefinition> orderedTables = new ArrayList<>(schema.tables());
        orderedTables.sort(Comparator.comparing(RelationalTableDefinition::identity, IDENTITY_ORDER));

        Map<RelationIdentity, RelationalTableDefinition> tableIndex = new LinkedHashMap<>();
        for (RelationalTableDefinition table : orderedTables) {
            if (tableIndex.putIfAbsent(table.identity(), table) != null) {
                throw new IllegalArgumentException("duplicate relation identity in dependency graph");
            }
        }

        Map<RelationIdentity, List<RelationIdentity>> dependencyIndex = new LinkedHashMap<>();
        for (RelationalTableDefinition table : orderedTables) {
            TreeSet<RelationIdentity> dependencies = new TreeSet<>(IDENTITY_ORDER);
            for (ForeignKeyDefinition foreignKey : table.foreignKeys()) {
                RelationIdentity referencedTable = foreignKey.referencedTable();
                if (tableIndex.containsKey(referencedTable)) {
                    dependencies.add(referencedTable);
                }
            }
            dependencyIndex.put(table.identity(), List.copyOf(dependencies));
        }

        tables = List.copyOf(orderedTables);
        identities = tables.stream().map(RelationalTableDefinition::identity).toList();
        tablesByIdentity = Map.copyOf(tableIndex);
        dependenciesByIdentity = Map.copyOf(dependencyIndex);
    }

    /** 从调用方明确给出的 Schema 快照构建图。 */
    public static SchemaDependencyGraph of(RelationalSchemaDefinition schema) {
        return new SchemaDependencyGraph(Objects.requireNonNull(
                schema, "relational schema definition must not be null"));
    }

    /** @return 按完整关系身份稳定排序的显式表快照 */
    public List<RelationalTableDefinition> tables() {
        return tables;
    }

    /**
     * 返回指定显式表直接依赖的表；结果已按完整关系身份稳定排序。
     */
    public List<RelationalTableDefinition> dependenciesOf(RelationalTableDefinition table) {
        Objects.requireNonNull(table, "relational table definition must not be null");
        List<RelationIdentity> dependencyIdentities = dependenciesByIdentity.get(table.identity());
        if (dependencyIdentities == null) {
            throw new IllegalArgumentException("table does not belong to dependency graph");
        }
        return dependencyIdentities.stream().map(tablesByIdentity::get).toList();
    }

    /** @return 依赖优先、环内身份稳定的表顺序 */
    public List<RelationalTableDefinition> dependencyOrder() {
        return stronglyConnectedComponents().dependencyOrder();
    }

    /** @return 依赖优先排列的强连通分量快照 */
    public SchemaStronglyConnectedComponents stronglyConnectedComponents() {
        return SchemaStronglyConnectedComponents.of(this);
    }

    List<RelationIdentity> identities() {
        return identities;
    }

    List<RelationIdentity> dependencyIdentities(RelationIdentity identity) {
        return dependenciesByIdentity.get(identity);
    }

    RelationalTableDefinition table(RelationIdentity identity) {
        return tablesByIdentity.get(identity);
    }

    static Comparator<RelationIdentity> identityOrder() {
        return IDENTITY_ORDER;
    }

    private static int compareIdentity(RelationIdentity left, RelationIdentity right) {
        int compared = compareNullable(left.catalog().orElse(null), right.catalog().orElse(null));
        if (compared != 0) {
            return compared;
        }
        compared = compareNullable(left.schema().orElse(null), right.schema().orElse(null));
        return compared != 0 ? compared : left.table().compareTo(right.table());
    }

    private static int compareNullable(String left, String right) {
        if (left == null) {
            return right == null ? 0 : -1;
        }
        return right == null ? 1 : left.compareTo(right);
    }
}
