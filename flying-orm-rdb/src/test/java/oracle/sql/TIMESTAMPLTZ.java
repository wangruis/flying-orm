package oracle.sql;

import java.sql.Connection;
import java.time.OffsetDateTime;

/** Oracle JDBC TIMESTAMPLTZ 的无依赖测试替身。 */
public final class TIMESTAMPLTZ {

    private final OffsetDateTime value;
    private Connection connection;

    public TIMESTAMPLTZ(OffsetDateTime value) {
        this.value = value;
    }

    public OffsetDateTime offsetDateTimeValue(Connection connection) {
        this.connection = connection;
        return value;
    }

    public Connection connection() {
        return connection;
    }
}
