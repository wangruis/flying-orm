package com.flying.orm.rdb.exception;

import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcTimeoutException;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

class RdbExceptionTranslatorTimeoutTest {

    @Test
    void classifiesStandardR2dbcTimeoutWithoutSqlState() {
        R2dbcTimeoutException timeout = new R2dbcTimeoutException("driver timeout");

        RdbException translated = assertInstanceOf(
                RdbException.class, RdbExceptionTranslator.translate(timeout));

        assertEquals(RdbErrorKind.TIMEOUT, translated.kind());
        assertSame(timeout, translated.getCause());
    }

    @Test
    void timeoutTypeTakesPrecedenceOverCancellationSqlState() {
        R2dbcTimeoutException timeout = new R2dbcTimeoutException("driver timeout", "57014");

        RdbException translated = assertInstanceOf(
                RdbException.class, RdbExceptionTranslator.translate(timeout));

        assertEquals(RdbErrorKind.TIMEOUT, translated.kind());
        assertSame(timeout, translated.getCause());
    }

    @Test
    void cancellationSqlStateRemainsCancelledForNonTimeoutErrors() {
        R2dbcException cancelled = new R2dbcException("cancelled", "57014") {
        };

        RdbException translated = assertInstanceOf(
                RdbException.class, RdbExceptionTranslator.translate(cancelled));

        assertEquals(RdbErrorKind.CANCELLED, translated.kind());
        assertSame(cancelled, translated.getCause());
    }

    @Test
    void unwrapsEveryJdkAsyncWrapperBeforeTranslatingTheDriverFailure() {
        SQLException driverFailure = new SQLException("driver failure");
        Throwable wrapped = driverFailure;
        for (int depth = 0; depth < 12; depth++) {
            wrapped = new CompletionException(wrapped);
        }

        RdbException translated = assertInstanceOf(
                RdbException.class, RdbExceptionTranslator.translate(wrapped));
        assertSame(driverFailure, translated.getCause());
    }
}
