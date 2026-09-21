package microsoft.sql;

import java.sql.Timestamp;

/** SQL Server JDBC 厂商类型的无依赖测试替身。 */
public final class DateTimeOffset {

    private final Timestamp timestamp;
    private final int minutesOffset;

    public DateTimeOffset(Timestamp timestamp, int minutesOffset) {
        this.timestamp = timestamp;
        this.minutesOffset = minutesOffset;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public int getMinutesOffset() {
        return minutesOffset;
    }
}
