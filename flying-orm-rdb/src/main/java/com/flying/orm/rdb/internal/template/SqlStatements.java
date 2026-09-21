package com.flying.orm.rdb.internal.template;

import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.internal.dialect.DatabaseProduct;

import java.util.Objects;

/**
 * 原生 SQL 和注册模板共用的最小语句边界。
 *
 * <p>这里只确认文本非空且词法上只有一条语句，保证一个入口只交给驱动一条语句。查询是否只读属于注册模板自己的
 * 更严格策略，不能放在这里，否则显式的 unsafe 原生写入口也会被误判成查询模板。</p>
 *
 * @author wangr
 * @date 2026-08-04
 * @version v1.0
 */
public final class SqlStatements {

    private SqlStatements() {
    }

    /**
     * 返回去掉首尾空白的单条 SQL；模板构造阶段只做不依赖方言的词法检查，执行前仍按真实方言复验。
     */
    public static String requireSingle(String sql) {
        // 模板构造时还不知道方言，不能用某个数据库的关键字规则猜测其他方言语法。
        return requireSingle(sql, SqlLexicalScanner.genericRules(), false, false);
    }

    /**
     * 在尚不知道数据库方言的低层执行边界采用可移植词法规则；井号按普通运算符处理，不能隐藏后续语句。
     */
    public static String requirePortableSingle(String sql) {
        return requireSingle(sql, SqlLexicalScanner.portableRules(), false, false);
    }

    /**
     * 按当前方言识别注释词法；只有 SQL Server 额外检查不需要分号的批处理语句边界。
     */
    public static String requireSingle(String sql, RdbDialect dialect) {
        String dialectName = Objects.requireNonNull(dialect, "RDB dialect must not be null").name();
        return requireSingle(sql, dialectName);
    }

    /**
     * 低层 JDBC/R2DBC 原生请求按驱动报告的数据库名复用同一词法边界，避免上层校验后被错误的通用规则二次拒绝。
     */
    @InternalApi
    public static String requireSingleForDatabaseProduct(String sql, String databaseProductName) {
        return requireSingle(sql, Objects.requireNonNullElse(databaseProductName, ""));
    }

    private static String requireSingle(String sql, String dialectName) {
        DatabaseProduct product = DatabaseProduct.detect(dialectName);
        return requireSingle(sql, SqlLexicalScanner.rulesFor(dialectName),
                product == DatabaseProduct.SQL_SERVER, product == DatabaseProduct.ORACLE);
    }

    private static String requireSingle(String sql,
                                         SqlLexicalScanner.Rules lexicalRules,
                                         boolean sqlServerDialect,
                                         boolean oracleDialect) {
        if (sql == null || sql.isBlank()) {
            throw new IllegalArgumentException("SQL statement must not be blank");
        }
        String statement = sql.trim();
        SqlStatementBoundary.validate(statement, lexicalRules, sqlServerDialect, oracleDialect);
        return statement;
    }
}
