package com.flying.orm.rdb.internal.template;

/**
 * 跟踪单条 {@code ALTER} 语句内部的动作和列子句，避免把合法子句误判成第二条 SQL。
 *
 * @author wangr
 * @date 2026-08-17
 * @version v2.0
 */
final class SqlAlterStatementState {
    private boolean as;
    private boolean action;
    private boolean column;
    private boolean columnSet;
    private boolean columnDrop;
    private boolean columnComplete;
    private boolean attach;
    private boolean attachPartition;
    private boolean attachFor;
    private boolean attachValues;

    void comma() {
        action = false;
        column = false;
        columnSet = false;
        columnDrop = false;
        columnComplete = false;
    }

    void accept(String statement, String word, String before, boolean sqlServerDialect) {
        validateColumnTail(word);
        if (!"ALTER".equals(statement)) {
            return;
        }
        if ("ATTACH".equals(word)) {
            attach = true;
        } else if (attach && "PARTITION".equals(word)) {
            attachPartition = true;
        } else if (attachPartition && "FOR".equals(word)) {
            attachFor = true;
        }
        if ("AS".equals(word)) {
            as = true;
        }
        if ("ALTER".equals(before) && "COLUMN".equals(word)) {
            column = true;
            columnComplete = false;
        }
        if (!SqlStatementBoundary.isStatementStart(word, sqlServerDialect)
                && SqlStatementBoundary.isAlterAction(word)) {
            action = true;
        }
    }

    boolean isContinuation(String word, String before) {
        if ("SELECT".equals(word) && as
                || ("UPDATE".equals(word) || "DELETE".equals(word)) && "ON".equals(before)) {
            return true;
        }
        if ("SET".equals(word) && ("UPDATE".equals(before) || "DELETE".equals(before))) {
            return true;
        }
        if (column && !columnComplete && "SET".equals(word)) {
            columnSet = true;
            return true;
        }
        if (column && !columnComplete && "DROP".equals(word)) {
            columnDrop = true;
            return true;
        }
        if ("VALUES".equals(word) && attachFor && !attachValues) {
            attachValues = true;
            return true;
        }
        if (!action && ("ALTER".equals(word) || "DROP".equals(word)
                || "COMMENT".equals(word) || "SET".equals(word)
                || SqlStatementBoundary.isAlterAction(word))) {
            action = true;
            return true;
        }
        return false;
    }

    private void validateColumnTail(String word) {
        if (columnDrop) {
            if (!SqlStatementBoundary.isAlterColumnDropClause(word)) {
                throw multipleStatements();
            }
            columnDrop = false;
            columnSet = "NOT".equals(word);
            columnComplete = !columnSet;
        } else if (columnSet && !("NOT".equals(word) || "DATA".equals(word))) {
            columnSet = false;
            columnComplete = true;
        }
    }

    private static IllegalArgumentException multipleStatements() {
        return new IllegalArgumentException("SQL must contain exactly one statement");
    }
}
