package com.flying.orm.rdb.observation;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SqlStatementTypeTest {

    @Test
    void classifiesAfterSharedCommentBoundariesWithoutAllocatingTrimmedSql() {
        assertEquals(SqlStatementType.SELECT,
                SqlStatementType.fromSql(" /* outer /* nested */ end */ -- line\n WITH data AS (SELECT 1) SELECT * FROM data"));
        assertEquals(SqlStatementType.UPDATE, SqlStatementType.fromSql("\n/* audit */ UPDATE form_data SET value=?"));
    }

    @Test
    void malformedOrQuotedLeadingTextNeverBreaksTheObservedExecution() {
        assertEquals(SqlStatementType.UNKNOWN, SqlStatementType.fromSql("/* unclosed"));
        assertEquals(SqlStatementType.UNKNOWN, SqlStatementType.fromSql("'select'"));
        assertEquals(SqlStatementType.UNKNOWN, SqlStatementType.fromSql(null));
    }
}
