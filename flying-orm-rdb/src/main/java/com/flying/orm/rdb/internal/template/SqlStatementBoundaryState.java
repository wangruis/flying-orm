package com.flying.orm.rdb.internal.template;

/**
 * SQL 单语句边界的顶层词法状态，只判断后续关键字是否仍属于当前语句。
 * @author wangr
 * @date 2026-08-16
 * @version v2.0
 */
final class SqlStatementBoundaryState {
    private String statement, prefix, previousWord;
    private boolean prefixPayload;
    private boolean prefixSubject;
    private boolean setOperator;
    private boolean createAs, createTrigger, createTriggerTarget;
    private boolean upsertClause, upsertUpdate, upsertSet;
    private boolean insertSource, insertMultiTable, insertMultiTableSelect;
    private boolean updateSet;
    private boolean selectLockClause, selectFetchClause, selectFetchRows;
    private boolean grantPrivileges = true;
    private boolean grantWithOption;
    private boolean createPartition, createPartitionOf, createPartitionFor, createPartitionValues;
    private boolean mergeThen, mergeUpdate, mergeInsert, mergeSource;
    private int caseDepth;
    private boolean previousEndClosedCase;
    private final SqlAlterStatementState alter = new SqlAlterStatementState();
    void comma() {
        if ("ALTER".equals(statement)) {
            alter.comma();
        }
    }
    void accept(String word, boolean mysqlDialect, boolean sqlServerDialect) {
        String before = previousWord;
        previousWord = word;
        boolean endClosedCase = "END".equals(word) && caseDepth > 0;
        if ("CASE".equals(word)) {
            caseDepth++;
        } else if (endClosedCase) {
            caseDepth--;
        }
        if (sqlServerDialect && "CONVERSATION".equals(word) && "END".equals(before)
                && !previousEndClosedCase && statement != null && !"END".equals(statement)) {
            throw multipleStatements();
        }
        previousEndClosedCase = endClosedCase;
        if (grantWithOption && !"GRANT".equals(word)) {
            // WITH GRANT OPTION 只允许 WITH 后紧邻的 GRANT；不能让该状态跨过 ADMIN/OPTION 吞掉下一条语句。
            grantWithOption = false;
        }
        alter.accept(statement, word, before, sqlServerDialect);
        validateSelectFetchTail(word);
        if ("SELECT".equals(statement) && "FOR".equals(word)) {
            // SQL Server 的 FOR 只用于 JSON/XML/BROWSE，不能把后续独立 UPDATE 吸收到当前查询。
            selectLockClause = !sqlServerDialect;
        } else if (selectLockClause && ("JSON".equals(word) || "XML".equals(word) || "BROWSE".equals(word))) {
            // 可移植标记请求在拿到连接前也必须关闭 SQL Server 输出子句留下的伪锁状态。
            selectLockClause = false;
        }
        if (isPrivilegeStatement() && ("ON".equals(word) || "TO".equals(word) || "FROM".equals(word))) {
            grantPrivileges = false;
        }
        if ("GRANT".equals(statement) && "WITH".equals(word)) {
            grantWithOption = true;
        }
        if ("INSERT".equals(statement) && ("ALL".equals(word) || "FIRST".equals(word))) {
            insertMultiTable = true;
        }
        if ("CREATE".equals(statement)) {
            if ("PARTITION".equals(word)) {
                createPartition = true;
            } else if (createPartition && "OF".equals(word)) {
                createPartitionOf = true;
            } else if (createPartitionOf && "FOR".equals(word)) {
                createPartitionFor = true;
            }
        }
        if ("UNION".equals(word) || "INTERSECT".equals(word) || "EXCEPT".equals(word)) {
            setOperator = true;
            return;
        }
        if ("TABLE".equals(word) && setOperator) {
            setOperator = false;
            return;
        }
        if ("AS".equals(word) && "CREATE".equals(statement)) {
            createAs = true;
            return;
        }
        if ("TRIGGER".equals(word) && "CREATE".equals(statement)) {
            createTrigger = true;
            return;
        }
        if (createTrigger && "ON".equals(word)) {
            createTriggerTarget = true;
        }
        if ("CONFLICT".equals(word) || "DUPLICATE".equals(word)) {
            upsertClause = true;
            return;
        }
        acceptMergeWord(word);
        if (!SqlStatementBoundary.isStatementStart(word, sqlServerDialect)) {
            if (statement == null && prefix == null) {
                // 原生 SQL 的单条语句不应被一个永远追不上各方言的首词白名单限制。
                // 这里只固定首条语句；分号、已知的后续语句起点和方言复验仍负责拒绝批处理。
                statement = word;
            } else if (statement == null) {
                prefixPayload = true;
                if (isExplainPrefix() && !isExplainModifier(word)) {
                    prefixSubject = true;
                }
            }
            return;
        }
        if (statement == null) {
            if (isExplainPrefix() && prefixSubject) {
                throw multipleStatements();
            }
            if ("WITH".equals(word) || "EXPLAIN".equals(word)
                    || "DESCRIBE".equals(word) || "DESC".equals(word)) {
                prefix = word;
            } else {
                statement = word;
                prefix = null;
            }
            return;
        }
        if (!isContinuation(word, before, mysqlDialect)) {
            throw multipleStatements();
        }
    }
    private static boolean isExplainModifier(String word) {
        return switch (word) {
            case "ANALYZE", "ANALYSE", "VERBOSE", "COSTS", "SETTINGS", "GENERIC_PLAN",
                    "BUFFERS", "SERIALIZE", "WAL", "TIMING", "SUMMARY", "MEMORY", "FORMAT",
                    "TEXT", "XML", "YAML", "JSON", "PLAN", "FOR", "CONNECTION", "EXTENDED",
                    "PARTITIONS", "TREE", "TRADITIONAL", "TRUE", "FALSE", "ON", "OFF" -> true;
            default -> false;
        };
    }
    void terminate() {
        if (statement == null && !(prefixPayload && isExplainPrefix())) {
            throw multipleStatements();
        }
    }
    private void acceptMergeWord(String word) {
        if (!"MERGE".equals(statement)) {
            return;
        }
        if ("WHEN".equals(word)) {
            mergeThen = false;
            mergeUpdate = false;
            mergeInsert = false;
        } else if ("THEN".equals(word)) {
            mergeThen = true;
        }
    }
    private boolean isContinuation(String word, String before, boolean mysqlDialect) {
        if (setOperator && ("SELECT".equals(word) || "VALUES".equals(word))) {
            setOperator = false;
            return true;
        }
        if ("WITH".equals(word)) {
            // 表提示、存储参数和 CREATE INDEX WITH 均属于已开始语句；裸 WITH 本身不能形成第二条可执行语句。
            return true;
        }
        if ("GRANT".equals(statement) && "GRANT".equals(word) && grantWithOption) {
            grantWithOption = false;
            return true;
        }
        if ("SELECT".equals(statement) && "UPDATE".equals(word) && selectLockClause) {
            selectLockClause = false;
            return true;
        }
        if ("SELECT".equals(statement) && "FETCH".equals(word)) {
            selectFetchClause = true;
            selectFetchRows = false;
            return true;
        }
        if ("INSERT".equals(statement) || "REPLACE".equals(statement) || "UPSERT".equals(statement)) {
            return isInsertContinuation(word);
        }
        if ("UPDATE".equals(statement) && "SET".equals(word)) {
            if (updateSet) {
                return false;
            }
            updateSet = true;
            return true;
        }
        if ("BULK".equals(statement) && "INSERT".equals(word)) {
            return true;
        }
        if ("SHOW".equals(statement) && "CREATE".equals(word) && "SHOW".equals(before)) {
            return true;
        }
        if ("RESTORE".equals(statement) && "REPLACE".equals(word)) {
            return true;
        }
        if ("DROP".equals(statement) && "IF".equals(word)
                && SqlStatementBoundary.isSqlServerDropObject(before)) {
            return true;
        }
        if ("CREATE".equals(statement)) {
            return ("REPLACE".equals(word) || "ALTER".equals(word)) && "OR".equals(before)
                    || mysqlDialect && "REPLACE".equals(word)
                    || ("SELECT".equals(word) || "WITH".equals(word) || "VALUES".equals(word))
                    && (createAs || mysqlDialect)
                    || mysqlDialect && "SET".equals(word) && "CHARACTER".equals(before)
                    || createTrigger && ("INSERT".equals(word) || "UPDATE".equals(word) || "DELETE".equals(word))
                    || mysqlDialect && createTrigger && "SET".equals(word)
                    || createTriggerTarget && "EXECUTE".equals(word)
                    || acceptsCreatePartitionValues(word)
                    || "COMMENT".equals(word);
        }
        if ("ALTER".equals(statement)) {
            return alter.isContinuation(word, before);
        }
        if (isPrivilegeStatement()) {
            return grantPrivileges;
        }
        return isMergeContinuation(word);
    }
    private boolean isMergeContinuation(String word) {
        if (!"MERGE".equals(statement)) {
            return false;
        }
        if ("VALUES".equals(word) && !mergeThen && !mergeUpdate && !mergeInsert && !mergeSource) {
            mergeSource = true;
            return true;
        }
        if (mergeThen && ("UPDATE".equals(word) || "INSERT".equals(word) || "DELETE".equals(word))) {
            mergeThen = false;
            mergeUpdate = "UPDATE".equals(word);
            mergeInsert = "INSERT".equals(word);
            return true;
        }
        return "SET".equals(word) && mergeUpdate || "VALUES".equals(word) && mergeInsert;
    }
    private boolean isInsertContinuation(String word) {
        if (insertMultiTable) {
            if ("VALUES".equals(word)) {
                return !insertMultiTableSelect;
            }
            if ("SELECT".equals(word) && !insertMultiTableSelect) {
                insertMultiTableSelect = true;
                return true;
            }
        }
        if (upsertClause) {
            if ("UPDATE".equals(word) && !upsertUpdate) {
                upsertUpdate = true;
                return true;
            }
            if ("SET".equals(word) && upsertUpdate && !upsertSet) {
                upsertSet = true;
                return true;
            }
            // MySQL 的 VALUES(column) 是 ON DUPLICATE KEY UPDATE 表达式，不是第二条语句。
            if ("VALUES".equals(word) && upsertUpdate) {
                return true;
            }
        }
        if (!("VALUES".equals(word) || "SELECT".equals(word) || "WITH".equals(word)
                || "SET".equals(word) || "EXEC".equals(word) || "EXECUTE".equals(word))) {
            return false;
        }
        if (insertSource) {
            return false;
        }
        insertSource = true;
        return true;
    }
    private boolean acceptsCreatePartitionValues(String word) {
        if (!"VALUES".equals(word) || !createPartitionFor || createPartitionValues) {
            return false;
        }
        createPartitionValues = true;
        return true;
    }
    private boolean isPrivilegeStatement() {
        return "GRANT".equals(statement) || "REVOKE".equals(statement) || "DENY".equals(statement);
    }
    private boolean isExplainPrefix() {
        return "EXPLAIN".equals(prefix) || "DESCRIBE".equals(prefix) || "DESC".equals(prefix);
    }
    private void validateSelectFetchTail(String word) {
        if (!selectFetchClause || "FETCH".equals(word)) {
            return;
        }
        if ("ROW".equals(word) || "ROWS".equals(word)) {
            selectFetchRows = true;
        } else if (("FROM".equals(word) || "IN".equals(word)) && !selectFetchRows) {
            throw multipleStatements();
        } else if ("ONLY".equals(word) && selectFetchRows) {
            selectFetchClause = false;
            selectFetchRows = false;
        }
    }
    private static IllegalArgumentException multipleStatements() {
        return new IllegalArgumentException("SQL must contain exactly one statement");
    }
}
