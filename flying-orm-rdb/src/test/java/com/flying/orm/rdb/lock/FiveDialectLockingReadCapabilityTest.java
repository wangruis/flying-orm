package com.flying.orm.rdb.lock;

import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FiveDialectLockingReadCapabilityTest {

    @Test
    void builtInDialectsExposeOnlyControlledUpdateLockForms() {
        List<RdbDialect> suffixDialects = List.of(
                RdbDialect.h2(), RdbDialect.mysql(), RdbDialect.postgresql(), RdbDialect.oracle());

        for (RdbDialect dialect : suffixDialects) {
            LockingReadDialect locking = dialect.lockingReadDialect();
            assertTrue(locking.supports(ReadLock.update()));
            assertTrue(locking.supports(ReadLock.updateNowait()));
            assertTrue(locking.supports(ReadLock.updateSkipLocked()));
            assertEquals(" FOR UPDATE", locking.suffix(ReadLock.update()));
            assertEquals(" FOR UPDATE NOWAIT", locking.suffix(ReadLock.updateNowait()));
            assertEquals(" FOR UPDATE SKIP LOCKED", locking.suffix(ReadLock.updateSkipLocked()));
            assertEquals("", locking.tableHint(ReadLock.update()));
        }
    }

    @Test
    void sqlServerUsesControlledTableHintsInsteadOfAppendingForeignSyntax() {
        LockingReadDialect locking = RdbDialect.sqlServer().lockingReadDialect();

        assertEquals(" WITH (UPDLOCK, ROWLOCK)", locking.tableHint(ReadLock.update()));
        assertEquals(" WITH (UPDLOCK, ROWLOCK, NOWAIT)",
                     locking.tableHint(ReadLock.updateNowait()));
        assertEquals(" WITH (UPDLOCK, ROWLOCK, READPAST)",
                     locking.tableHint(ReadLock.updateSkipLocked()));
        assertEquals("", locking.suffix(ReadLock.updateSkipLocked()));
    }

    @Test
    void legacyCustomDialectFailsClosed() {
        RdbDialect custom = RdbDialect.of(
                "custom",
                RdbDialect.h2().schema(),
                RdbDialect.h2().pagination(),
                RdbDialect.h2().upsert());

        assertFalse(custom.lockingReadDialect().supports(ReadLock.update()));
        assertThrows(UnsupportedOperationException.class,
                     () -> custom.lockingReadDialect().suffix(ReadLock.update()));
    }
}
