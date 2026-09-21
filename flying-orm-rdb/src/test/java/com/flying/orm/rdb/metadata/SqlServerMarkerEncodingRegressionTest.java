package com.flying.orm.rdb.metadata;

import java.util.List;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SqlServerMarkerEncodingRegressionTest {
    @Test void bothMarkerKindsUseUnicodeOnBothBinaryComparisonOperands() {
        String sql = SqlServerMetadataQueries.queries().columnQuery().create("dbo", "t").sql();
        for (String marker : List.of("[[flying-orm:v1:OFFSET_TIME]]", "[[flying-orm:v1:COMMENT]]")) {
            String expected = "= convert(varbinary(128), N'" + marker + "')";
            assertEquals(2, occurrences(sql, expected), "DATA_TYPE and REMARKS must agree for " + marker);
            assertFalse(sql.contains("= convert(varbinary(128), '" + marker + "')"));
        }
        assertEquals(4, occurrences(sql, "left(cast(ep.value as nvarchar(4000)),"));
    }

    @Test void markerFixDoesNotChangeScopeBindingOrPlainColumnBranches() {
        var explicit = SqlServerMetadataQueries.queries().columnQuery().create("dbo", "t");
        var implicit = SqlServerMetadataQueries.queries().columnQuery().create(null, "t");
        assertEquals(List.of("t", "dbo"), explicit.parameters());
        assertEquals(List.of("t", "t"), implicit.parameters());
        assertTrue(explicit.sql().contains("else c.DATA_TYPE"));
        assertTrue(explicit.sql().contains("else cast(ep.value as nvarchar(4000))"));
    }

    private static int occurrences(String sql, String part) {
        int count = 0;
        for (int start = 0; (start = sql.indexOf(part, start)) >= 0; start += part.length()) count++;
        return count;
    }
}
