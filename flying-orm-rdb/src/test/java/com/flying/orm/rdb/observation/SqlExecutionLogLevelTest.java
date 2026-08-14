package com.flying.orm.rdb.observation;

import org.junit.jupiter.api.Test;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证日志级别选择和关闭 DEBUG 后的短路行为。
 *
 * <p>这里故意使用一个会在读取参数时抛错的列表，确保测试的是“先探测级别、后格式化”的顺序，
 * 而不是只验证最后没有写出日志。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
class SqlExecutionLogLevelTest {

    /** 默认阈值的边界是包含 1 秒：小于 1 秒是 DEBUG，正好 1 秒已经是 WARN。 */
    @Test
    void classifiesFastAndBoundarySlowSqlByLevel() {
        LevelSink sink = new LevelSink();
        SqlExecutionLogObserver observer = SqlExecutionLogObserver.create(
                SqlExecutionLogOptions.defaults(), sink);

        observer.onExecution(observationWithDuration(999_999_999L));
        observer.onExecution(observationWithDuration(1_000_000_000L));

        assertEquals(List.of(SqlExecutionLogLevel.DEBUG, SqlExecutionLogLevel.WARN), sink.levels);
    }

    /** 取消、超时、连接中断和 UNKNOWN 都是 WARN；已经明确的 SQL 错误才提升到 ERROR。 */
    @Test
    void classifiesFailureCategoriesByLevel() {
        LevelSink sink = new LevelSink();
        SqlExecutionLogObserver observer = SqlExecutionLogObserver.create(
                SqlExecutionLogOptions.defaults(), sink);

        observer.onExecution(observationWithStatus(SqlExecutionStatus.CANCELLED, SqlFailureCategory.CANCELLED));
        observer.onExecution(observationWithStatus(SqlExecutionStatus.ERROR, SqlFailureCategory.TIMEOUT));
        observer.onExecution(observationWithStatus(SqlExecutionStatus.ERROR, SqlFailureCategory.CONNECTION));
        observer.onExecution(observationWithStatus(SqlExecutionStatus.ERROR, SqlFailureCategory.UNKNOWN));
        observer.onExecution(observationWithStatus(SqlExecutionStatus.ERROR, SqlFailureCategory.BAD_SQL));

        assertEquals(List.of(SqlExecutionLogLevel.WARN,
                             SqlExecutionLogLevel.WARN,
                             SqlExecutionLogLevel.WARN,
                             SqlExecutionLogLevel.WARN,
                             SqlExecutionLogLevel.ERROR), sink.levels);
    }

    /** DEBUG 关闭时，observer 在格式化前直接返回，连参数集合的读取方法都不会被调用。 */
    @Test
    void skipsFormattingWhenDebugIsDisabled() {
        LevelSink sink = new LevelSink();
        sink.debugEnabled = false;
        SqlExecutionLogObserver observer = SqlExecutionLogObserver.create(
                SqlExecutionLogOptions.defaults().withSql(true).withParameters(true), sink);
        List<Object> unreadableParameters = new AbstractList<>() {
            @Override
            public Object get(int index) {
                throw new AssertionError("parameters must not be formatted when DEBUG is disabled");
            }

            @Override
            public int size() {
                return 1;
            }
        };

        assertDoesNotThrow(() -> observer.onExecution(observationWithDuration(1), unreadableParameters));
        assertTrue(sink.messages.isEmpty());
        assertTrue(sink.probedLevels.contains(SqlExecutionLogLevel.DEBUG));
    }

    private static SqlExecutionObservation observationWithDuration(long durationNanos) {
        return new SqlExecutionObservation(SqlExecutionOperation.QUERY,
                                           SqlStatementType.SELECT,
                                           SqlExecutionStatus.SUCCESS,
                                           SqlFailureCategory.NONE,
                                           "select id from Users",
                                           0,
                                           0,
                                           1,
                                           durationNanos,
                                           null);
    }

    private static SqlExecutionObservation observationWithStatus(SqlExecutionStatus status,
                                                                  SqlFailureCategory failureCategory) {
        return new SqlExecutionObservation(SqlExecutionOperation.UPDATE,
                                           SqlStatementType.UPDATE,
                                           status,
                                           failureCategory,
                                           "update Users set name = ?",
                                           1,
                                           0,
                                           0,
                                           1,
                                           status == SqlExecutionStatus.ERROR
                                                   ? new IllegalStateException("test failure")
                                                   : null);
    }

    private static final class LevelSink implements SqlExecutionLogSink {

        private final List<SqlExecutionLogLevel> levels = new ArrayList<>();

        private final List<SqlExecutionLogLevel> probedLevels = new ArrayList<>();

        private final List<String> messages = new ArrayList<>();

        private boolean debugEnabled = true;

        @Override
        public void write(String message) {
            messages.add(message);
        }

        @Override
        public boolean isEnabled(SqlExecutionLogLevel level) {
            probedLevels.add(level);
            return level != SqlExecutionLogLevel.DEBUG || debugEnabled;
        }

        @Override
        public void write(SqlExecutionLogLevel level, String message) {
            levels.add(level);
            messages.add(message);
        }
    }
}
