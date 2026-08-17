package com.flying.orm.rdb.dialect;

import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.sql.render.SqlRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * PaginationDialect 抽象不同数据库的分页 SQL 片段和分页参数顺序。
 *
 * @author wangr
 * @date 2026-07-22
 * @version v1.0
 */
public interface PaginationDialect {

    /**
     * 将基础查询 SQL 转换为带分页语法的 SQL 请求。
     *
     * @param sql        基础查询 SQL
     * @param parameters 基础查询参数
     * @param page       分页请求
     * @return 带分页语法和分页参数的 SQL 请求
     */
    SqlRequest paginate(String sql, List<Object> parameters, PageQuery page);

    /**
     * 只按当前方言的占位符顺序追加分页参数，不渲染或校验 SQL 文本。
     *
     * <p>结构计划命中时调用该入口，避免为了取得参数顺序重复拼接分页 SQL。SQL Server 的 ORDER BY
     * 校验仍只在缓存未命中的完整 SQL 编译阶段执行。</p>
     *
     * @param parameters 基础查询参数
     * @param page 分页请求
     * @return 包含分页参数的独立列表
     */
    List<Object> paginationParameters(List<Object> parameters, PageQuery page);

    /**
     * 为候选集、内部校验等非业务分页场景附加严格行数上限。
     *
     * <p>该能力不受 {@link PageQuery} 的业务页大小限制，但调用方仍必须传入正数硬上限。</p>
     */
    default SqlRequest limit(String sql, List<Object> parameters, int maxRows) {
        throw new UnsupportedOperationException(
                "pagination dialect does not support bounded internal queries");
    }

    /**
     * 创建 limit/offset 分页方言。
     *
     * @return limit/offset 分页方言
     */
    static PaginationDialect limitOffset() {
        return StandardPaginationDialect.LIMIT_OFFSET;
    }

    /**
     * 创建 offset/fetch 分页方言。
     *
     * @return offset/fetch 分页方言
     */
    static PaginationDialect offsetFetch() {
        return StandardPaginationDialect.OFFSET_FETCH;
    }

    /**
     * SQL Server 的 OFFSET/FETCH 必须跟在 ORDER BY 后面。这里选择在渲染阶段直接拒绝无排序分页，
     * 不偷偷补一个不稳定顺序，避免同一条数据在并发写入时跨页重复或丢失。
     */
    static PaginationDialect sqlServerOffsetFetch() {
        return StandardPaginationDialect.SQL_SERVER_OFFSET_FETCH;
    }
}

/** 内置分页实现放在包内，业务代码始终通过 PaginationDialect 的命名工厂取得。 */
enum StandardPaginationDialect implements PaginationDialect {
    /** H2、MySQL 和 PostgreSQL 共用的 LIMIT/OFFSET。 */
    LIMIT_OFFSET {
        @Override
        public SqlRequest paginate(String sql, List<Object> parameters, PageQuery page) {
            PageQuery safePage = requirePage(page);
            return new SqlRequest(requireSql(sql) + " limit ? offset ?",
                                  append(parameters, safePage.size(), safePage.offset()));
        }

        @Override
        public List<Object> paginationParameters(List<Object> parameters, PageQuery page) {
            PageQuery safePage = requirePage(page);
            return append(parameters, safePage.size(), safePage.offset());
        }

        @Override
        public SqlRequest limit(String sql, List<Object> parameters, int maxRows) {
            return new SqlRequest(requireSql(sql) + " limit ?", appendOne(parameters, requireLimit(maxRows)));
        }
    },

    /** Oracle 12c 及以上可复用的 OFFSET/FETCH。 */
    OFFSET_FETCH {
        @Override
        public SqlRequest paginate(String sql, List<Object> parameters, PageQuery page) {
            PageQuery safePage = requirePage(page);
            return new SqlRequest(requireSql(sql) + " offset ? rows fetch next ? rows only",
                                  append(parameters, safePage.offset(), safePage.size()));
        }

        @Override
        public List<Object> paginationParameters(List<Object> parameters, PageQuery page) {
            PageQuery safePage = requirePage(page);
            return append(parameters, safePage.offset(), safePage.size());
        }

        @Override
        public SqlRequest limit(String sql, List<Object> parameters, int maxRows) {
            return new SqlRequest(requireSql(sql) + " offset 0 rows fetch next ? rows only",
                                  appendOne(parameters, requireLimit(maxRows)));
        }
    },

    /** SQL Server 2012 及以上要求分页前必须有明确排序。 */
    SQL_SERVER_OFFSET_FETCH {
        @Override
        public SqlRequest paginate(String sql, List<Object> parameters, PageQuery page) {
            PageQuery safePage = requirePage(page);
            String safeSql = requireSql(sql).trim();
            if (!hasTopLevelOrderBy(safeSql)) {
                throw new IllegalArgumentException("SQL Server pagination requires an explicit order by");
            }
            return new SqlRequest(safeSql + " offset ? rows fetch next ? rows only",
                                  append(parameters, safePage.offset(), safePage.size()));
        }

        @Override
        public List<Object> paginationParameters(List<Object> parameters, PageQuery page) {
            PageQuery safePage = requirePage(page);
            return append(parameters, safePage.offset(), safePage.size());
        }

        @Override
        public SqlRequest limit(String sql, List<Object> parameters, int maxRows) {
            String safeSql = requireSql(sql).trim();
            if (!hasTopLevelOrderBy(safeSql)) {
                throw new IllegalArgumentException("SQL Server bounded query requires an explicit order by");
            }
            return new SqlRequest(safeSql + " offset 0 rows fetch next ? rows only",
                                  appendOne(parameters, requireLimit(maxRows)));
        }
    };

    private static PageQuery requirePage(PageQuery page) {
        return Objects.requireNonNull(page, "page query must not be null");
    }

    private static String requireSql(String sql) {
        return Objects.requireNonNull(sql, "pagination sql must not be null");
    }

    private static List<Object> append(List<Object> parameters, Object first, Object second) {
        List<Object> result = new ArrayList<>(Objects.requireNonNull(parameters,
                                                                     "sql parameters must not be null"));
        result.add(first);
        result.add(second);
        return result;
    }

    private static List<Object> appendOne(List<Object> parameters, Object value) {
        List<Object> result = new ArrayList<>(Objects.requireNonNull(
                parameters, "sql parameters must not be null"));
        result.add(value);
        return result;
    }

    private static int requireLimit(int maxRows) {
        if (maxRows < 1) {
            throw new IllegalArgumentException("query row limit must be positive");
        }
        return maxRows;
    }

    /** 只识别 SQL Server 分页依赖的最外层 ORDER BY，不把字符串、注释、子查询或窗口函数算进去。 */
    private static boolean hasTopLevelOrderBy(String sql) {
        int depth = 0;
        boolean order = false;
        for (int index = 0; index < sql.length();) {
            char current = sql.charAt(index);
            char next = index + 1 < sql.length() ? sql.charAt(index + 1) : '\0';
            if (current == '\'' || current == '"') {
                index = quotedEnd(sql, index, current);
            } else if (current == '[') {
                index = bracketEnd(sql, index);
            } else if (current == '-' && next == '-') {
                index = lineCommentEnd(sql, index + 2);
            } else if (current == '/' && next == '*') {
                index = blockCommentEnd(sql, index + 2);
            } else if (current == '(') {
                depth++;
                order = false;
                index++;
            } else if (current == ')') {
                depth = Math.max(0, depth - 1);
                order = false;
                index++;
            } else if (depth == 0 && (Character.isLetter(current) || current == '_')) {
                int end = index + 1;
                while (end < sql.length()) {
                    char character = sql.charAt(end);
                    if (!Character.isLetterOrDigit(character) && character != '_') {
                        break;
                    }
                    end++;
                }
                String word = sql.substring(index, end).toUpperCase(Locale.ROOT);
                if (order && "BY".equals(word)) {
                    return true;
                }
                order = "ORDER".equals(word);
                index = end;
            } else {
                index++;
            }
        }
        return false;
    }

    private static int quotedEnd(String sql, int offset, char quote) {
        for (int index = offset + 1; index < sql.length(); index++) {
            if (sql.charAt(index) == quote) {
                if (index + 1 < sql.length() && sql.charAt(index + 1) == quote) {
                    index++;
                } else {
                    return index + 1;
                }
            }
        }
        return sql.length();
    }

    private static int bracketEnd(String sql, int offset) {
        for (int index = offset + 1; index < sql.length(); index++) {
            if (sql.charAt(index) == ']') {
                if (index + 1 < sql.length() && sql.charAt(index + 1) == ']') {
                    index++;
                } else {
                    return index + 1;
                }
            }
        }
        return sql.length();
    }

    private static int lineCommentEnd(String sql, int offset) {
        for (int index = offset; index < sql.length(); index++) {
            char current = sql.charAt(index);
            if (current == '\n' || current == '\r') {
                return index + 1;
            }
        }
        return sql.length();
    }

    private static int blockCommentEnd(String sql, int offset) {
        int depth = 1;
        for (int index = offset; index + 1 < sql.length(); index++) {
            char current = sql.charAt(index);
            char next = sql.charAt(index + 1);
            if (current == '/' && next == '*') {
                depth++;
                index++;
            } else if (current == '*' && next == '/') {
                depth--;
                index++;
                if (depth == 0) {
                    return index + 1;
                }
            }
        }
        return sql.length();
    }
}
