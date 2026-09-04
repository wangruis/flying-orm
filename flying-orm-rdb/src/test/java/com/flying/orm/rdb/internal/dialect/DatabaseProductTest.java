package com.flying.orm.rdb.internal.dialect;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DatabaseProductTest {

    @Test
    void resolvesConfiguredAliasesWithoutGuessingUnknownNames() {
        assertEquals(DatabaseProduct.POSTGRESQL, DatabaseProduct.fromName("postgres"));
        assertEquals(DatabaseProduct.SQL_SERVER, DatabaseProduct.fromName("sql-server"));
        assertEquals(DatabaseProduct.UNKNOWN, DatabaseProduct.fromName("not-mysql"));
    }

    @Test
    void detectsStandardDriverAndMetadataProductNames() {
        assertEquals(DatabaseProduct.MYSQL, DatabaseProduct.detect("MariaDB ConnectionFactory"));
        assertEquals(DatabaseProduct.POSTGRESQL, DatabaseProduct.detect("R2DBC PostgreSQL"));
        assertEquals(DatabaseProduct.ORACLE, DatabaseProduct.detect("Oracle Database"));
        assertEquals(DatabaseProduct.SQL_SERVER, DatabaseProduct.detect("Microsoft SQL Server"));
        assertEquals(DatabaseProduct.UNKNOWN, DatabaseProduct.detect(null));
    }
}
