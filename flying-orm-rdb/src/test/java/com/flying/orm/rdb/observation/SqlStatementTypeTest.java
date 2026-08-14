package com.flying.orm.rdb.observation;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** 验证观测分类能跳过连续、交错的 SQL 头部注释。 */
class SqlStatementTypeTest {

    @Test
    void skipsInterleavedLeadingCommentsInOnePass() {
        assertEquals(SqlStatementType.SELECT,
                     SqlStatementType.fromSql(" /* trace */ -- tenant\n /* audit */ select id from users"));
        assertEquals(SqlStatementType.UNKNOWN, SqlStatementType.fromSql("/* unfinished"));
    }
}
