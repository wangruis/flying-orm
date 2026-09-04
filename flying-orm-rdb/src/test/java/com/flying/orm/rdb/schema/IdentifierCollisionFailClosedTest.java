package com.flying.orm.rdb.schema;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;

class IdentifierCollisionFailClosedTest {

    @Test
    void rejectsNamesThatCollideAfterDatabaseCaseFolding() {
        RelationalObjectNameGenerator names = new RelationalObjectNameGenerator(63);

        assertThrows(IllegalArgumentException.class,
                     () -> names.requireNoCollisions(List.of("IX_accounts_name", "ix_ACCOUNTS_NAME")));
    }
}
