package com.flying.orm.rdb.json;

import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRenderContext;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.core.sql.render.SqlTermPackage;

import java.util.List;

/**
 * JSON 条件的方言扩展入口。所有路径和值都作为参数绑定，SQL 里只放固定模板。
 *
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
public final class JsonTermHandlers {

    private JsonTermHandlers() {
    }

    public static SqlTermPackage mysql() {
        return SqlTermPackage.of("json-mysql",
                                 SqlTermHandler.of(JsonStructuredConditions.JSON_PATH_EQUALS,
                                                   JsonTermHandlers::renderMysqlPathEquals),
                                 SqlTermHandler.of(JsonStructuredConditions.JSON_CONTAINS,
                                                   JsonTermHandlers::renderMysqlContains),
                                 SqlTermHandler.of(JsonStructuredConditions.JSON_EXISTS,
                                                   JsonTermHandlers::renderMysqlExists),
                                 SqlTermHandler.of(JsonStructuredConditions.JSON_ARRAY_CONTAINS,
                                                   JsonTermHandlers::renderMysqlArrayContains));
    }

    public static SqlTermPackage postgresql() {
        return SqlTermPackage.of("json-postgresql",
                                 SqlTermHandler.of(JsonStructuredConditions.JSON_PATH_EQUALS,
                                                   JsonTermHandlers::renderPostgresqlPathEquals),
                                 SqlTermHandler.of(JsonStructuredConditions.JSON_CONTAINS,
                                                   JsonTermHandlers::renderPostgresqlContains),
                                 SqlTermHandler.of(JsonStructuredConditions.JSON_EXISTS,
                                                   JsonTermHandlers::renderPostgresqlExists),
                                 SqlTermHandler.of(JsonStructuredConditions.JSON_ARRAY_CONTAINS,
                                                   JsonTermHandlers::renderPostgresqlArrayContains));
    }

    private static SqlFragment renderMysqlPathEquals(TermCondition term, SqlRenderContext context) {
        JsonConditionValue value = conditionValue(term.value(), JsonConditionValue.Kind.PATH_EQUALS);
        return SqlFragment.of("json_unquote(json_extract(" + context.identifier(term.field()) + ", ?)) = ?",
                              mysqlPath(value), value.value());
    }

    private static SqlFragment renderPostgresqlPathEquals(TermCondition term, SqlRenderContext context) {
        JsonConditionValue value = conditionValue(term.value(), JsonConditionValue.Kind.PATH_EQUALS);
        return SqlFragment.of(context.identifier(term.field()) + " #>> ? = ?", postgresqlPath(value), value.value());
    }

    private static SqlFragment renderMysqlContains(TermCondition term, SqlRenderContext context) {
        JsonConditionValue value = conditionValue(term.value(), JsonConditionValue.Kind.CONTAINS);
        return SqlFragment.of("json_contains(" + context.identifier(term.field()) + ", cast(? as json))",
                              JsonValueCodec.write(value.value()));
    }

    private static SqlFragment renderPostgresqlContains(TermCondition term, SqlRenderContext context) {
        JsonConditionValue value = conditionValue(term.value(), JsonConditionValue.Kind.CONTAINS);
        return SqlFragment.of(context.identifier(term.field()) + " @> cast(? as jsonb)",
                              JsonValueCodec.write(value.value()));
    }

    private static SqlFragment renderMysqlExists(TermCondition term, SqlRenderContext context) {
        JsonConditionValue value = conditionValue(term.value(), JsonConditionValue.Kind.EXISTS);
        return SqlFragment.of("json_contains_path(" + context.identifier(term.field()) + ", 'one', ?)",
                              mysqlPath(value));
    }

    private static SqlFragment renderPostgresqlExists(TermCondition term, SqlRenderContext context) {
        JsonConditionValue value = conditionValue(term.value(), JsonConditionValue.Kind.EXISTS);
        return SqlFragment.of(context.identifier(term.field()) + " #> ? is not null",
                              (Object) postgresqlPath(value));
    }

    private static SqlFragment renderMysqlArrayContains(TermCondition term, SqlRenderContext context) {
        JsonConditionValue value = conditionValue(term.value(), JsonConditionValue.Kind.ARRAY_CONTAINS);
        return SqlFragment.of("json_contains(json_extract(" + context.identifier(term.field())
                                      + ", ?), cast(? as json))",
                              mysqlPath(value),
                              JsonValueCodec.writeLiteral(value.value()));
    }

    private static SqlFragment renderPostgresqlArrayContains(TermCondition term, SqlRenderContext context) {
        JsonConditionValue value = conditionValue(term.value(), JsonConditionValue.Kind.ARRAY_CONTAINS);
        return SqlFragment.of("(" + context.identifier(term.field()) + " #> ?) @> cast(? as jsonb)",
                              postgresqlPath(value),
                              JsonValueCodec.write(List.of(value.value())));
    }

    private static String mysqlPath(JsonConditionValue value) {
        return "$." + String.join(".", value.path());
    }

    private static String[] postgresqlPath(JsonConditionValue value) {
        return value.path().toArray(String[]::new);
    }

    private static JsonConditionValue conditionValue(Object value, JsonConditionValue.Kind expectedKind) {
        if (value instanceof JsonConditionValue conditionValue && conditionValue.kind() == expectedKind) {
            return conditionValue;
        }
        throw new IllegalArgumentException("json term value must be " + expectedKind);
    }

}
