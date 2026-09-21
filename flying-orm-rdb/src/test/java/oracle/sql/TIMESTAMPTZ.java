package oracle.sql;

import java.sql.Connection;
import java.time.OffsetDateTime;

/** Oracle JDBC TIMESTAMPTZ 的无依赖测试替身。 */
public final class TIMESTAMPTZ {

    private final OffsetDateTime value;
    private final RuntimeException failure;
    private Connection connection;

    public TIMESTAMPTZ(OffsetDateTime value) {
        this(value, null);
    }

    public TIMESTAMPTZ(OffsetDateTime value, RuntimeException failure) {
        this.value = value;
        this.failure = failure;
    }

    public OffsetDateTime offsetDateTimeValue(Connection connection) {
        this.connection = connection;
        if (failure != null) {
            throw failure;
        }
        return value;
    }

    public Connection connection() {
        return connection;
    }
}
