package com.flying.orm.rdb.internal.template;

import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SqlStatementsTest {

    @Test
    void rejectsMySqlExecutableCommentsThatCanHideAnotherStatement() {
        assertThrows(
                IllegalArgumentException.class,
                () -> SqlStatements.requireSingle(
                        "select 1 /*! ; delete from victim */",
                        RdbDialect.mysql()));
        assertThrows(
                IllegalArgumentException.class,
                () -> SqlStatements.requireSingle(
                        "select 1 /*M! ; delete from victim */",
                        RdbDialect.mysql()));
        assertThrows(
                IllegalArgumentException.class,
                () -> SqlStatements.requireSingle(
                        "select 1; /*! delete from victim */",
                        RdbDialect.mysql()));
    }

    @Test
    void rejectsExecutableCommentsReportedByMariaDbProductName() {
        assertThrows(
                IllegalArgumentException.class,
                () -> SqlStatements.requireSingleForDatabaseProduct(
                        "select 1 /*! ; delete from victim */",
                        "MariaDB"));
        assertThrows(
                IllegalArgumentException.class,
                () -> SqlStatements.requireSingleForDatabaseProduct(
                        "select 1 /*M! ; delete from victim */",
                        "MariaDB"));
    }

    @Test
    void keepsOrdinaryAndOptimizerHintCommentsAvailableForMySql() {
        assertDoesNotThrow(
                () -> SqlStatements.requireSingle("select /* ordinary */ 1", RdbDialect.mysql()));
        assertDoesNotThrow(
                () -> SqlStatements.requireSingle("select /*+ MAX_EXECUTION_TIME(1000) */ 1", RdbDialect.mysql()));
    }

    @Test
    void rejectsMysqlSqlWhoseQuoteBoundaryDependsOnSessionMode() {
        String ambiguous = "select 'a\\' INTO @leak -- '";

        assertThrows(
                IllegalArgumentException.class,
                () -> SqlStatements.requireSingle(ambiguous, RdbDialect.mysql()));
        assertThrows(
                IllegalArgumentException.class,
                () -> SqlStatements.requireSingleForDatabaseProduct(ambiguous, "MariaDB"));
    }
}
