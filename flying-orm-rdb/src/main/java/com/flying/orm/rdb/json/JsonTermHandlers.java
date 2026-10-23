package com.flying.orm.rdb.json;

import com.flying.orm.core.condition.TermExtensionDescriptor;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.core.sql.render.SqlTermPackage;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.dialect.OracleVersion;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.dialect.SqlServerVersion;
import tools.jackson.databind.JsonNode;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Map;
import java.util.Iterator;
import java.util.ArrayDeque;
import java.util.Collections;

import static com.flying.orm.rdb.json.JsonConditionValue.requireKind;

/**
 * JSON 条件的方言扩展入口。值作为参数绑定；路径按方言绑定或从已校验的路径段生成安全字面量。
 *
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
public final class JsonTermHandlers {

    private static final java.util.Set<String> CAPABILITIES = java.util.Set.of("json-functions");

    private JsonTermHandlers() {
    }

    public static SqlTermPackage mysql() {
        return terms(true);
    }

    public static SqlTermPackage postgresql() {
        return terms(false);
    }

    /** SQL Server 2022 基线，与 RdbDialect.sqlServer() 一致。 */
    public static SqlTermPackage sqlServer() {
        return sqlServer(SqlServerVersion.V2022);
    }

    /**
     * 2016+ 提供标量比较、IN、LIKE、路径空值、存在性及对象标量叶子包含，要求兼容级别至少为 130。
     * 2022 使用原生存在性函数；对象包含不支持数组或空对象叶子。
     * OPENJSON 以类型化列提取数值，以 nvarchar(max) 提取文本，不使用 JSON_VALUE 的固定长度返回类型。
     * 输入列应保存有效 JSON；LIKE 保留数据库排序规则和标量文本语义，不承担存储内容校验。
     * IN 及 2016/2019 存在性使用 OPENJSON 成员行，末级成员名受其原生 nvarchar(4000) key 类型约束。
     */
    public static SqlTermPackage sqlServer(SqlServerVersion version) {
        return sqlServer(version, DatabaseType.of("DECIMAL"));
    }

    /**
     * 指定 JSON 数值投影的数据库类型；精度、舍入及溢出遵循所选 SQL Server 类型，不转成浮点数兜底。
     * 文本及布尔值使用标量文本比较，保留数据库排序规则；数值字符串遵循数据库类型转换规则。
     */
    public static SqlTermPackage sqlServer(SqlServerVersion version, DatabaseType numericType) {
        if (Objects.requireNonNull(version, "SQL Server version must not be null") == SqlServerVersion.V2012) {
            throw new UnsupportedOperationException("SQL Server JSON terms require 2016 or later");
        }
        DatabaseType type = Objects.requireNonNull(numericType, "JSON numeric type must not be null");
        if (type.isArray() || !type.logicalType().numeric()) {
            throw new IllegalArgumentException("JSON numeric projection requires a scalar numeric database type");
        }
        String numberSql = RdbDialect.sqlServer(version).schema().dataType(type.canonical());
        List<SqlTermHandler> handlers = new ArrayList<>(List.of(
                sqlServerPath(JsonStructuredConditions.JSON_PATH_EQUALS, "=", numberSql),
                sqlServerPath(JsonStructuredConditions.JSON_PATH_NOT_EQUALS, "<>", numberSql),
                sqlServerPath(JsonStructuredConditions.JSON_PATH_GREATER_THAN, ">", numberSql),
                sqlServerPath(JsonStructuredConditions.JSON_PATH_GREATER_OR_EQUALS, ">=", numberSql),
                sqlServerPath(JsonStructuredConditions.JSON_PATH_LESS_THAN, "<", numberSql),
                sqlServerPath(JsonStructuredConditions.JSON_PATH_LESS_OR_EQUALS, "<=", numberSql),
                sqlServerPath(JsonStructuredConditions.JSON_PATH_LIKE, "like", numberSql),
                sqlServerPath(JsonStructuredConditions.JSON_PATH_NOT_LIKE, "not like", numberSql),
                sqlServerPath(JsonStructuredConditions.JSON_PATH_IN, "in", numberSql),
                sqlServerPath(JsonStructuredConditions.JSON_PATH_NOT_IN, "not in", numberSql),
                sqlServerPath(JsonStructuredConditions.JSON_PATH_IS_NULL, null, numberSql),
                sqlServerPath(JsonStructuredConditions.JSON_PATH_NOT_NULL, null, numberSql)));
        for (String id : List.of(JsonStructuredConditions.JSON_EXISTS, JsonStructuredConditions.JSON_NOT_EXISTS)) {
            handlers.add(sqlServerExists(id, version));
        }
        for (String id : List.of(JsonStructuredConditions.JSON_CONTAINS, JsonStructuredConditions.JSON_NOT_CONTAINS)) {
            handlers.add(objectContains(id, sqlServerPath(JsonStructuredConditions.JSON_PATH_IN, "in", numberSql)));
        }
        return SqlTermPackage.of("json-sqlserver", handlers);
    }

    private static SqlTermHandler sqlServerExists(String id, SqlServerVersion version) {
        return handler(id, 1, (term, context) -> {
            JsonConditionValue value = requireKind(term.value(), JsonConditionValue.Kind.EXISTS);
            String field = context.identifier(term.field());
            String expected = id.equals(JsonStructuredConditions.JSON_EXISTS) ? "1" : "0";
            if (version == SqlServerVersion.V2022) {
                return SqlFragment.of("json_path_exists(" + field + ", ?) = " + expected, jsonPath(value, true));
            }
            if (value.path().isEmpty()) {
                return SqlFragment.of("case when " + field + " is not null then 1 end = " + expected);
            }
            List<Object> parameters = new ArrayList<>(1);
            String present = "exists (select 1 from " + sqlServerMembers(field, value, parameters) + ")";
            return new SqlFragment("case when " + field + " is null then null when " + present
                    + " then 1 else 0 end = " + expected, parameters);
        });
    }

    private static String sqlServerMembers(String field, JsonConditionValue value, List<Object> parameters) {
        String rows = "openjson(N'[' + cast(" + field + " as nvarchar(max)) + N']')";
        if (value.path().isEmpty()) return rows + " j";
        String leaf = value.path().getLast();
        boolean index = leaf.charAt(0) == '[';
        String key = index ? new BigInteger(leaf.substring(1, leaf.length() - 1)).toString()
                : leaf.charAt(0) == '"' ? (String) JsonValueCodec.read(leaf) : leaf;
        parameters.add(key);
        String parent = jsonPath(JsonConditionValue.exists(value.path().subList(0, value.path().size() - 1)), true);
        // 成员行保留 JSON null；容器类型、二进制键和字节长度共同区分数组下标及精确属性名。
        return rows + " with ([parent] nvarchar(max) N'" + parent.replace("'", "''") + "' as json) p "
                + "cross apply openjson(p.[parent]) j where left(p.[parent], 1) = N'" + (index ? "[" : "{")
                + "' and cast(j.[key] as varbinary(max)) = cast(cast(? as nvarchar(max)) as varbinary(max))"
                + " and datalength(j.[key]) = " + 2L * key.length();
    }

    private static SqlTermHandler sqlServerPath(String id, String operator, String numberSql) {
        JsonConditionValue.Kind kind = switch (id) {
            case JsonStructuredConditions.JSON_PATH_EQUALS -> JsonConditionValue.Kind.PATH_EQUALS;
            case JsonStructuredConditions.JSON_PATH_LIKE, JsonStructuredConditions.JSON_PATH_NOT_LIKE -> JsonConditionValue.Kind.PATH_PATTERN;
            case JsonStructuredConditions.JSON_PATH_IS_NULL, JsonStructuredConditions.JSON_PATH_NOT_NULL -> JsonConditionValue.Kind.PATH_NULL;
            case JsonStructuredConditions.JSON_PATH_IN, JsonStructuredConditions.JSON_PATH_NOT_IN -> JsonConditionValue.Kind.PATH_MEMBERSHIP;
            default -> JsonConditionValue.Kind.PATH_COMPARE;
        };
        int binds = kind == JsonConditionValue.Kind.PATH_MEMBERSHIP ? Integer.MAX_VALUE : operator == null ? 1 : 2;
        return handler(id, binds, (term, context) -> {
            JsonConditionValue value = requireKind(term.value(), kind);
            if (kind == JsonConditionValue.Kind.PATH_MEMBERSHIP) {
                boolean negated = id.equals(JsonStructuredConditions.JSON_PATH_NOT_IN);
                List<?> members = (List<?>) value.value();
                if (members.isEmpty()) return SqlFragment.of(negated ? "1 = 1" : "1 = 0");
                List<Object> parameters = new ArrayList<>();
                String rows = sqlServerMembers(context.identifier(term.field()), value, parameters);
                StringBuilder matches = new StringBuilder();
                for (Object member : members) {
                    if (!matches.isEmpty()) matches.append(" or ");
                    if (member == null) {
                        matches.append("j.[type] = 0");
                    } else {
                        int type = member instanceof Number ? 2 : member instanceof Boolean ? 3 : 1;
                        String scalar = type == 2 ? "cast(case when j.[type] = 2 then j.[value] end as " + numberSql + ")"
                                : "j.[value]";
                        matches.append("(j.[type] = ").append(type).append(" and ").append(scalar).append(" = ?)");
                        parameters.add(member instanceof Boolean ? member.toString()
                                : member instanceof BigInteger number ? new BigDecimal(number) : member);
                    }
                }
                // MAX 无输入行时为 NULL：缺失路径不匹配 NOT IN；重复成员中任一匹配也会排除该文档。
                return new SqlFragment("exists (select 1 from " + rows + (negated
                        ? " having max(case when (" + matches + ") then 1 else 0 end) = 0"
                        : (value.path().isEmpty() ? " where (" : " and (") + matches + ")") + ")", parameters);
            }
            String path = "N'" + jsonPath(value, true).replace("'", "''") + "'";
            String type = value.value() instanceof Number ? numberSql : "nvarchar(max)";
            // 外层数组固定一行，根值为对象、数组、标量或 JSON null 时均使用同一个路径基准。
            String projection = "exists (select 1 from openjson(N'[' + cast("
                    + context.identifier(term.field()) + " as nvarchar(max)) + N']') with ([value] " + type + " " + path;
            if (operator != null) {
                Object parameter = value.value() instanceof Boolean ? value.value().toString()
                        : value.value() instanceof BigInteger number ? new BigDecimal(number) : value.value();
                return SqlFragment.of(projection + ") where [value] " + operator + " ?)", parameter);
            }
            String present = projection + ", [object] nvarchar(max) " + path + " as json) "
                    + "where [value] is not null or [object] is not null)";
            return SqlFragment.of(negate(present, id.equals(JsonStructuredConditions.JSON_PATH_IS_NULL)));
        });
    }

    /**
     * Oracle 19c 基线的 JSON 条件包；默认版本与 RdbDialect.oracle() 一致。
     */
    public static SqlTermPackage oracle() {
        return oracle(OracleVersion.V19C);
    }

    /**
     * Oracle 12c 提供存在性判断；19c+ 另提供类型化比较、IN、LIKE、路径空值及对象标量叶子包含。
     * 对象包含不支持数组或空对象叶子，不等同于通用 JSON 结构包含。
     * 路径按数据库要求生成安全字面量；普通字符串和数字绑定到 PASSING 变量。
     * 保留 Oracle 原生路径放宽和 FALSE ON ERROR 语义，不把数字字符串转成数字。
     * 路径空值包含 SQL NULL、缺失路径及 JSON null；LIKE 采用原生 JSON 模式语义，不设置文本长度上限。
     */
    public static SqlTermPackage oracle(OracleVersion version) {
        List<SqlTermHandler> handlers = new ArrayList<>(List.of(
                oracleExists(JsonStructuredConditions.JSON_EXISTS, false),
                oracleExists(JsonStructuredConditions.JSON_NOT_EXISTS, true)));
        if (Objects.requireNonNull(version, "oracle version must not be null") != OracleVersion.V12C) {
            handlers.addAll(List.of(
                    oracleComparison(JsonStructuredConditions.JSON_PATH_EQUALS, "=="),
                    oracleComparison(JsonStructuredConditions.JSON_PATH_NOT_EQUALS, "!="),
                    oracleComparison(JsonStructuredConditions.JSON_PATH_GREATER_THAN, ">"),
                    oracleComparison(JsonStructuredConditions.JSON_PATH_GREATER_OR_EQUALS, ">="),
                    oracleComparison(JsonStructuredConditions.JSON_PATH_LESS_THAN, "<"),
                    oracleComparison(JsonStructuredConditions.JSON_PATH_LESS_OR_EQUALS, "<="),
                    oracleMembership(JsonStructuredConditions.JSON_PATH_IN, false),
                    oracleMembership(JsonStructuredConditions.JSON_PATH_NOT_IN, true),
                    oracleComparison(JsonStructuredConditions.JSON_PATH_LIKE, "like"),
                    oracleComparison(JsonStructuredConditions.JSON_PATH_NOT_LIKE, "like"),
                    oraclePathNull(JsonStructuredConditions.JSON_PATH_IS_NULL, false),
                    oraclePathNull(JsonStructuredConditions.JSON_PATH_NOT_NULL, true)));
            for (String id : List.of(JsonStructuredConditions.JSON_CONTAINS, JsonStructuredConditions.JSON_NOT_CONTAINS)) {
                handlers.add(objectContains(id, oracleMembership(JsonStructuredConditions.JSON_PATH_IN, false)));
            }
        }
        return SqlTermPackage.of("json-oracle", handlers);
    }

    private static SqlTermPackage terms(boolean mysql) {
        return SqlTermPackage.of(mysql ? "json-mysql" : "json-postgresql",
                pathComparison(JsonStructuredConditions.JSON_PATH_EQUALS, "=", mysql),
                pathComparison(JsonStructuredConditions.JSON_PATH_NOT_EQUALS, "<>", mysql),
                pathComparison(JsonStructuredConditions.JSON_PATH_GREATER_THAN, ">", mysql),
                pathComparison(JsonStructuredConditions.JSON_PATH_GREATER_OR_EQUALS, ">=", mysql),
                pathComparison(JsonStructuredConditions.JSON_PATH_LESS_THAN, "<", mysql),
                pathComparison(JsonStructuredConditions.JSON_PATH_LESS_OR_EQUALS, "<=", mysql),
                pathLike(JsonStructuredConditions.JSON_PATH_LIKE, mysql, false),
                pathLike(JsonStructuredConditions.JSON_PATH_NOT_LIKE, mysql, true),
                pathNull(JsonStructuredConditions.JSON_PATH_IS_NULL, mysql, false),
                pathNull(JsonStructuredConditions.JSON_PATH_NOT_NULL, mysql, true),
                pathMembership(JsonStructuredConditions.JSON_PATH_IN, mysql, false),
                pathMembership(JsonStructuredConditions.JSON_PATH_NOT_IN, mysql, true),
                containment(JsonStructuredConditions.JSON_CONTAINS, mysql, false, false),
                containment(JsonStructuredConditions.JSON_NOT_CONTAINS, mysql, false, true),
                containment(JsonStructuredConditions.JSON_CONTAINED_BY, mysql, true, false),
                containment(JsonStructuredConditions.JSON_NOT_CONTAINED_BY, mysql, true, true),
                exists(JsonStructuredConditions.JSON_EXISTS, mysql, false),
                exists(JsonStructuredConditions.JSON_NOT_EXISTS, mysql, true),
                arrayContains(JsonStructuredConditions.JSON_ARRAY_CONTAINS, mysql, false),
                arrayContains(JsonStructuredConditions.JSON_ARRAY_NOT_CONTAINS, mysql, true));
    }

    private static SqlTermHandler handler(String id,
                                          int maxParameters,
                                          com.flying.orm.core.sql.render.SqlTermRenderer renderer) {
        return SqlTermHandler.of(
                TermExtensionDescriptor.filter(id, CAPABILITIES, maxParameters, 1),
                com.flying.orm.core.condition.ConditionValueShape.SCALAR,
                renderer);
    }

    private static SqlTermHandler pathComparison(String id, String operator, boolean mysql) {
        JsonConditionValue.Kind kind = id.equals(JsonStructuredConditions.JSON_PATH_EQUALS)
                ? JsonConditionValue.Kind.PATH_EQUALS : JsonConditionValue.Kind.PATH_COMPARE;
        return handler(id, 2, (term, context) -> {
            JsonConditionValue value = requireKind(term.value(), kind);
            String field = context.identifier(term.field());
            String sql = mysql
                    ? "json_extract(" + field + ", ?) " + operator + " cast(? as json)"
                    : "cast(" + field + " #> cast(? as text[]) as jsonb) " + operator + " cast(? as jsonb)";
            return SqlFragment.of(sql, mysql ? jsonPath(value, false) : postgresqlPath(value),
                                  JsonValueCodec.writeLiteral(value.value()));
        });
    }

    private static SqlTermHandler pathLike(String id, boolean mysql, boolean negated) {
        return handler(id, 2, (term, context) -> {
            JsonConditionValue value = requireKind(term.value(), JsonConditionValue.Kind.PATH_PATTERN);
            String field = context.identifier(term.field());
            String text = mysql
                    ? "json_unquote(nullif(json_extract(" + field + ", ?), cast('null' as json)))"
                    : field + " #>> cast(? as text[])";
            return SqlFragment.of(text + (negated ? " not like ?" : " like ?"),
                    mysql ? jsonPath(value, false) : postgresqlPath(value), value.value());
        });
    }

    private static SqlTermHandler pathNull(String id, boolean mysql, boolean negated) {
        return handler(id, 1, (term, context) -> {
            JsonConditionValue value = requireKind(term.value(), JsonConditionValue.Kind.PATH_NULL);
            String field = context.identifier(term.field());
            String sql = mysql ? "coalesce(json_type(json_extract(" + field + ", ?)), 'NULL') "
                    + (negated ? "<>" : "=") + " 'NULL'"
                    : field + " #>> cast(? as text[]) is " + (negated ? "not null" : "null");
            Object path = mysql ? jsonPath(value, false) : postgresqlPath(value);
            return SqlFragment.of(sql, path);
        });
    }

    private static SqlTermHandler pathMembership(String id, boolean mysql, boolean negated) {
        return handler(id, Integer.MAX_VALUE, (term, context) -> {
            JsonConditionValue value = requireKind(term.value(), JsonConditionValue.Kind.PATH_MEMBERSHIP);
            List<?> members = (List<?>) value.value();
            if (members.isEmpty()) {
                return SqlFragment.of(negated ? "1 = 1" : "1 = 0");
            }
            String field = context.identifier(term.field());
            int binds = mysql ? Math.multiplyExact(members.size(), 2) : Math.addExact(members.size(), 1);
            List<Object> parameters = new ArrayList<>(binds);
            StringBuilder sql = new StringBuilder();
            if (mysql) {
                sql.append(negated ? "not (" : "(");
            } else {
                sql.append("cast(").append(field).append(" #> cast(? as text[]) as jsonb) ")
                        .append(negated ? "not in (" : "in (");
                parameters.add(postgresqlPath(value));
            }
            String path = mysql ? jsonPath(value, false) : null;
            for (int index = 0; index < members.size(); index++) {
                if (index > 0) {
                    sql.append(mysql ? " or " : ", ");
                }
                if (mysql) {
                    sql.append("json_extract(").append(field).append(", ?) = cast(? as json)");
                    parameters.add(path);
                } else {
                    sql.append("cast(? as jsonb)");
                }
                Object member = members.get(index);
                parameters.add(member == null ? "null" : JsonValueCodec.writeLiteral(member));
            }
            return new SqlFragment(sql.append(')').toString(), parameters);
        });
    }

    private static SqlTermHandler containment(String id, boolean mysql, boolean containedBy, boolean negated) {
        return handler(id, containedBy ? 1 : Integer.MAX_VALUE, (term, context) -> {
            JsonConditionValue value = requireKind(term.value(), JsonConditionValue.Kind.CONTAINS);
            String field = context.identifier(term.field());
            String sql;
            if (mysql) {
                sql = containedBy ? "json_contains(cast(? as json), " + field + ")"
                        : "json_contains(" + field + ", cast(? as json))";
            } else {
                sql = "cast(" + field + " as jsonb) " + (containedBy ? "<@" : "@>") + " cast(? as jsonb)";
            }
            return SqlFragment.of(negate(sql, negated), value.value());
        });
    }

    private static SqlTermHandler objectContains(String id, SqlTermHandler membership) {
        return handler(id, Integer.MAX_VALUE, (term, context) -> {
            JsonConditionValue value = requireKind(term.value(), JsonConditionValue.Kind.CONTAINS);
            JsonNode root = JsonValueCodec.conditionTree((String) value.value());
            if (!root.isObject() || root.isEmpty()) {
                throw new IllegalArgumentException("json object contains requires a nonempty object of scalar leaves");
            }
            ArrayDeque<Iterator<Map.Entry<String, JsonNode>>> objects = new ArrayDeque<>();
            objects.push(root.properties().iterator());
            List<String> path = new ArrayList<>();
            List<Object> parameters = new ArrayList<>();
            StringBuilder sql = new StringBuilder("(");
            while (!objects.isEmpty()) {
                if (!objects.peek().hasNext()) {
                    objects.pop();
                    if (!path.isEmpty()) path.removeLast();
                    continue;
                }
                Map.Entry<String, JsonNode> entry = objects.peek().next();
                JsonNode node = entry.getValue();
                path.add(JsonValueCodec.writeLiteral(entry.getKey()));
                if (node.isObject() && !node.isEmpty()) {
                    objects.push(node.properties().iterator());
                    continue;
                }
                if (!node.isValueNode()) {
                    throw new IllegalArgumentException("json object contains does not support array or empty object leaves");
                }
                Object scalar = node.isNull() ? null : node.isNumber() ? node.numberValue()
                        : node.isBoolean() ? node.booleanValue() : node.asString();
                SqlFragment part = membership.render(TermCondition.of(term.identity(), membership.id(),
                        JsonConditionValue.pathIn(path, Collections.singletonList(scalar))), context);
                if (sql.length() > 1) sql.append(" and ");
                sql.append('(').append(part.sql()).append(')');
                parameters.addAll(part.parameters());
                path.removeLast();
            }
            sql.append(')');
            return new SqlFragment(id.equals(JsonStructuredConditions.JSON_NOT_CONTAINS)
                    ? "(" + context.identifier(term.field()) + " is not null and not (" + sql + "))"
                    : sql.toString(), parameters);
        });
    }

    private static SqlTermHandler exists(String id, boolean mysql, boolean negated) {
        return handler(id, 1, (term, context) -> {
            JsonConditionValue value = requireKind(term.value(), JsonConditionValue.Kind.EXISTS);
            String field = context.identifier(term.field());
            String sql = mysql ? negate("json_contains_path(" + field + ", 'one', ?)", negated)
                    : field + " #> cast(? as text[]) is " + (negated ? "null" : "not null");
            Object path = mysql ? jsonPath(value, false) : postgresqlPath(value);
            return SqlFragment.of(sql, path);
        });
    }

    private static SqlTermHandler arrayContains(String id, boolean mysql, boolean negated) {
        return handler(id, 2, (term, context) -> {
            JsonConditionValue value = requireKind(term.value(), JsonConditionValue.Kind.ARRAY_CONTAINS);
            String field = context.identifier(term.field());
            String sql = mysql ? "json_contains(json_extract(" + field + ", ?), cast(? as json))"
                    : "cast(" + field + " #> cast(? as text[]) as jsonb) @> cast(? as jsonb)";
            return SqlFragment.of(negate(sql, negated), mysql ? jsonPath(value, false) : postgresqlPath(value),
                    mysql ? JsonValueCodec.writeLiteral(value.value()) : JsonValueCodec.write(List.of(value.value())));
        });
    }

    private static String negate(String sql, boolean negated) {
        return negated ? "not (" + sql + ")" : sql;
    }

    private static SqlTermHandler oracleExists(String id, boolean negated) {
        // 与无方言的输入解析器共用描述器；Oracle 的实际绑定数为零。
        return handler(id, 1, (term, context) -> {
            JsonConditionValue value = requireKind(term.value(), JsonConditionValue.Kind.EXISTS);
            return SqlFragment.of(negate(oracleFilter(context.identifier(term.field()),
                    jsonPath(value, true), "", List.of()), negated));
        });
    }

    private static SqlTermHandler oracleComparison(String id, String operator) {
        JsonConditionValue.Kind kind = operator.equals("like") ? JsonConditionValue.Kind.PATH_PATTERN
                : id.equals(JsonStructuredConditions.JSON_PATH_EQUALS)
                ? JsonConditionValue.Kind.PATH_EQUALS : JsonConditionValue.Kind.PATH_COMPARE;
        return handler(id, 2, (term, context) -> {
            JsonConditionValue value = requireKind(term.value(), kind);
            List<Object> parameters = new ArrayList<>(1);
            StringBuilder filter = new StringBuilder();
            // 空 LIKE 模式等价于空字符串相等，避免 Oracle 原生 LIKE "" 漏匹配。
            appendOracleComparison(filter, operator.equals("like") && "".equals(value.value()) ? "==" : operator,
                    value.value(), parameters);
            if (id.equals(JsonStructuredConditions.JSON_PATH_NOT_LIKE)) {
                filter.insert(0, "exists(@.stringOnly()) && !(").append(')');
            }
            return new SqlFragment(oracleFilter(context.identifier(term.field()),
                    jsonPath(value, true), filter.toString(), parameters), parameters);
        });
    }

    private static SqlTermHandler oraclePathNull(String id, boolean negated) {
        return handler(id, 1, (term, context) -> {
            JsonConditionValue value = requireKind(term.value(), JsonConditionValue.Kind.PATH_NULL);
            String field = context.identifier(term.field());
            String present = oracleFilter(field, jsonPath(value, true), "@.type() != \"null\"", List.of());
            return SqlFragment.of(negated ? "(" + field + " is not null and " + present + ")"
                    : "(" + field + " is null or not (" + present + "))");
        });
    }

    private static SqlTermHandler oracleMembership(String id, boolean negated) {
        return handler(id, Integer.MAX_VALUE, (term, context) -> {
            JsonConditionValue value = requireKind(term.value(), JsonConditionValue.Kind.PATH_MEMBERSHIP);
            List<?> members = (List<?>) value.value();
            if (members.isEmpty()) {
                return SqlFragment.of(negated ? "1 = 1" : "1 = 0");
            }
            List<Object> parameters = new ArrayList<>(members.size());
            StringBuilder filter = new StringBuilder();
            for (Object member : members) {
                if (!filter.isEmpty()) {
                    filter.append(" || ");
                }
                appendOracleComparison(filter, "==", member, parameters);
            }
            String field = context.identifier(term.field());
            String path = jsonPath(value, true);
            String sql = oracleFilter(field, path, filter.toString(), parameters);
            // 非空 NOT IN 不能把不存在的路径当成集合外的值。
            return new SqlFragment(negated
                    ? oracleFilter(field, path, "", List.of()) + " and not (" + sql + ")" : sql, parameters);
        });
    }

    private static void appendOracleComparison(StringBuilder filter, String operator,
                                                Object value, List<Object> parameters) {
        filter.append(value instanceof Number ? "@.numberOnly()" : value instanceof Boolean ? "@.booleanOnly()"
                : value == null ? "@" : "@.stringOnly()").append(' ').append(operator).append(' ');
        // SQL 空字符串会变为 NULL；只为这些有限的 JSON 类型字面量使用固定语法。
        if (value == null || value instanceof Boolean) {
            filter.append(value);
        } else if ("".equals(value)) {
            filter.append("\"\"");
        } else {
            filter.append("$v").append(parameters.size());
            parameters.add(value);
        }
    }

    private static String oracleFilter(String field, String path, String filter, List<Object> parameters) {
        StringBuilder sql = new StringBuilder("json_exists(").append(field).append(", '")
                .append(path.replace("'", "''"));
        if (!filter.isEmpty()) {
            sql.append("?(").append(filter).append(')');
        }
        sql.append('\'');
        for (int index = 0; index < parameters.size(); index++) {
            sql.append(index == 0 ? " passing " : ", ").append("? as \"v").append(index).append('"');
        }
        return sql.append(')').toString();
    }

    private static String jsonPath(JsonConditionValue value, boolean quotedNames) {
        StringBuilder path = new StringBuilder("$");
        for (String segment : value.path()) {
            if (segment.charAt(0) != '[') {
                path.append('.');
            }
            path.append(quotedNames && segment.charAt(0) != '[' && segment.charAt(0) != '"'
                    ? JsonValueCodec.writeLiteral(segment) : segment);
        }
        return path.toString();
    }

    private static String[] postgresqlPath(JsonConditionValue value) {
        String[] path = new String[value.path().size()];
        for (int index = 0; index < path.length; index++) {
            String segment = value.path().get(index);
            path[index] = switch (segment.charAt(0)) {
                case '[' -> segment.substring(1, segment.length() - 1);
                case '"' -> (String) JsonValueCodec.read(segment);
                default -> segment;
            };
        }
        return path;
    }

}
