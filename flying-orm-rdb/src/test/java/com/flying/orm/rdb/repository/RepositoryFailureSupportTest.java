package com.flying.orm.rdb.repository;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.CompletionException;

import org.junit.jupiter.api.Test;

class RepositoryFailureSupportTest {

    @Test
    void generatedKeyReadFailurePreservesWriteFactsAndFatalIdentity() {
        IllegalArgumentException cause = new IllegalArgumentException("key conversion failed");
        com.flying.orm.rdb.execution.GeneratedKeyReadException failure =
                new com.flying.orm.rdb.execution.GeneratedKeyReadException(3L, cause);
        GeneratedKeyResolutionException result = (GeneratedKeyResolutionException)
                RepositoryFailureSupport.generatedKeyReadFailure(failure);
        assertEquals(3L, result.affectedRows());
        assertSame(failure, result.getCause());
        OutOfMemoryError fatal = new OutOfMemoryError("fatal");
        assertSame(fatal, assertThrows(OutOfMemoryError.class, () ->
                RepositoryFailureSupport.generatedKeyReadFailure(
                        new com.flying.orm.rdb.execution.GeneratedKeyReadException(
                                1L, new CompletionException(fatal)))));
    }

}
