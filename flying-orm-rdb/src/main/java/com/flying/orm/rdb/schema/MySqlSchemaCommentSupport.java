package com.flying.orm.rdb.schema;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.internal.template.SqlLexicalScanner;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Function;

/**
 * MySQL 注释中的反斜线由会话 {@code sql_mode} 决定含义。渲染器只给确实依赖该模式的 DDL 打内部标记，
 * 执行器据此在第一条 DDL 前读取模式；flying-orm 不修改上层管理的数据库会话。
 */
final class MySqlSchemaCommentSupport {

    private static final String MODE_MARKER = "/*flying-orm:mysql-comment-no-backslash-escapes*/";
    private static final String REQUIRED_MODE = "NO_BACKSLASH_ESCAPES";
    private static final SqlRequest MODE_QUERY = new SqlRequest(
            "select @@SESSION.sql_mode as sql_mode", List.of());
    private static final SqlLexicalScanner.Rules MYSQL_RULES = SqlLexicalScanner.rulesFor("mysql");
    private static final SqlLexicalScanner.Rules COMMENT_LITERAL_RULES = SqlLexicalScanner.rulesFor("standard");

    private MySqlSchemaCommentSupport() {
    }

    static String literal(SchemaDialectTypeSupport types, String comment, boolean mysql) {
        String literal = types.quoteLiteral(comment);
        return mysql && comment.indexOf('\\') >= 0 ? MODE_MARKER + " " + literal : literal;
    }

    /** Reads only the existing marker and its following NO_BACKSLASH_ESCAPES string literal. */
    static int markedLiteralEnd(String sql, int offset) {
        if (!sql.startsWith(MODE_MARKER, offset)) {
            return offset;
        }
        int literalStart = offset + MODE_MARKER.length();
        while (literalStart < sql.length() && Character.isWhitespace(sql.charAt(literalStart))) {
            literalStart++;
        }
        if (literalStart == sql.length() || sql.charAt(literalStart) != '\'') {
            return offset;
        }
        return SqlLexicalScanner.segmentEnd(
                SqlLexicalScanner.protectedSegmentAt(sql, literalStart, COMMENT_LITERAL_RULES, false));
    }

    static boolean requiresModeValidation(List<SqlRequest> requests) {
        for (SqlRequest request : requests) {
            if (containsModeMarker(request.sql())) {
                return true;
            }
        }
        return false;
    }

    static <T> T execute(SyncSqlExecutor executor, List<SqlRequest> requests,
                         SqlExecutionOptions options, String planFingerprint,
                         Function<SyncSqlExecutor, T> work) {
        if (!requiresModeValidation(requests)) {
            return work.apply(executor);
        }
        return executor.withConnection(MODE_QUERY, bound -> {
            requireConfigured(bound.query(MODE_QUERY, options), planFingerprint);
            return work.apply(bound);
        });
    }

    static <T> Mono<T> execute(ReactiveSqlExecutor executor, List<SqlRequest> requests,
                               SqlExecutionOptions options, String planFingerprint,
                               Function<ReactiveSqlExecutor, Mono<T>> work) {
        return Mono.defer(() -> {
            if (!requiresModeValidation(requests)) {
                return work.apply(executor);
            }
            return executor.withConnection(MODE_QUERY, bound -> {
                Flux<DynamicRow> rows = options == null
                        ? bound.query(MODE_QUERY) : bound.query(MODE_QUERY, options);
                return rows.take(2).collectList()
                        .doOnNext(result -> requireConfigured(result, planFingerprint))
                        .then(Mono.defer(() -> work.apply(bound)));
            });
        });
    }

    static void requireConfigured(List<DynamicRow> rows, String planFingerprint) {
        if (rows.size() == 1 && rows.getFirst().columnCount() > 0) {
            Object value = rows.getFirst().value(0);
            if (value instanceof CharSequence modes && containsRequiredMode(modes.toString())) {
                return;
            }
        }
        throw new SchemaMigrationRejectedException(
                SchemaMigrationFailureCode.EXECUTOR_CAPABILITY_REQUIRED,
                planFingerprint,
                "MySQL schema comments containing backslash require NO_BACKSLASH_ESCAPES on every schema "
                        + "connection; configure the upper connection provider because flying-orm does not "
                        + "modify session sql_mode");
    }

    private static boolean containsModeMarker(String sql) {
        int searchFrom = 0;
        while (true) {
            int markerStart = sql.indexOf(MODE_MARKER, searchFrom);
            if (markerStart < 0) {
                return false;
            }
            int markerEnd = markerStart + MODE_MARKER.length();
            boolean[] found = new boolean[1];
            try {
                // 只扫描到内部标记。标记后的注释文本允许以反斜线结尾，不能用默认模式解析它。
                SqlLexicalScanner.scan(sql.substring(0, markerEnd), MYSQL_RULES, false, (kind, start, end) -> {
                    if (kind == SqlLexicalScanner.SegmentKind.BLOCK_COMMENT
                            && start == markerStart && end == markerEnd) {
                        found[0] = true;
                    }
                });
            } catch (IllegalArgumentException ignored) {
                // 该候选位于字符串内部；继续查找后续真正由渲染器写入的块注释标记。
            }
            if (found[0]) {
                return true;
            }
            searchFrom = markerEnd;
        }
    }

    private static boolean containsRequiredMode(String modes) {
        for (String mode : modes.split(",")) {
            if (REQUIRED_MODE.equalsIgnoreCase(mode.trim())) {
                return true;
            }
        }
        return false;
    }
}
