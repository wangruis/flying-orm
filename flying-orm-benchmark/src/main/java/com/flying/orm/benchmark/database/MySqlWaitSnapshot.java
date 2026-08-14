package com.flying.orm.benchmark.database;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** MySQL 文件等待的实例级累计快照；最大值只能报告采样结束时的全局峰值。 */
record MySqlWaitSnapshot(long binlogCount,
                         long binlogWaitPicoseconds,
                         long binlogMaxPicoseconds,
                         long redoCount,
                         long redoWaitPicoseconds,
                         long redoMaxPicoseconds) {

    private static final String BINLOG_WAIT = "wait/io/file/sql/binlog";
    private static final String REDO_WAIT = "wait/io/file/innodb/innodb_log_file";

    static MySqlWaitSnapshot fromRows(List<? extends Map<String, Object>> rows) {
        long binlogCount = 0;
        long binlogWait = 0;
        long binlogMax = 0;
        long redoCount = 0;
        long redoWait = 0;
        long redoMax = 0;
        for (Map<String, Object> row : rows) {
            String eventName = String.valueOf(value(row, "EVENT_NAME"));
            if (BINLOG_WAIT.equals(eventName)) {
                binlogCount = number(row, "COUNT_STAR");
                binlogWait = number(row, "SUM_TIMER_WAIT");
                binlogMax = number(row, "MAX_TIMER_WAIT");
            } else if (REDO_WAIT.equals(eventName)) {
                redoCount = number(row, "COUNT_STAR");
                redoWait = number(row, "SUM_TIMER_WAIT");
                redoMax = number(row, "MAX_TIMER_WAIT");
            }
        }
        return new MySqlWaitSnapshot(binlogCount, binlogWait, binlogMax, redoCount, redoWait, redoMax);
    }

    MySqlWaitSnapshot minus(MySqlWaitSnapshot before) {
        MySqlWaitSnapshot safeBefore = Objects.requireNonNull(before, "MySQL wait snapshot must not be null");
        return new MySqlWaitSnapshot(
                Math.max(0, binlogCount - safeBefore.binlogCount),
                Math.max(0, binlogWaitPicoseconds - safeBefore.binlogWaitPicoseconds),
                binlogMaxPicoseconds,
                Math.max(0, redoCount - safeBefore.redoCount),
                Math.max(0, redoWaitPicoseconds - safeBefore.redoWaitPicoseconds),
                redoMaxPicoseconds);
    }

    double binlogWaitMillis() { return picosecondsToMillis(binlogWaitPicoseconds); }
    double binlogMaxMillis() { return picosecondsToMillis(binlogMaxPicoseconds); }
    double redoWaitMillis() { return picosecondsToMillis(redoWaitPicoseconds); }
    double redoMaxMillis() { return picosecondsToMillis(redoMaxPicoseconds); }

    private static Object value(Map<String, Object> row, String name) {
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(name)) {
                return entry.getValue();
            }
        }
        throw new IllegalArgumentException("MySQL wait snapshot is missing column " + name);
    }

    private static long number(Map<String, Object> row, String name) {
        Object value = value(row, name);
        return value instanceof Number number ? number.longValue() : Long.parseLong(String.valueOf(value));
    }

    private static double picosecondsToMillis(long picoseconds) {
        return picoseconds / 1_000_000_000.0;
    }
}
