package com.flying.orm.core.internal.error;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Test;

class ThrowableGraphTest {

    @Test
    void doesNotMineSuppressedFailuresForFatalErrors() {
        RuntimeException root = new RuntimeException("root");
        OutOfMemoryError fatal = new OutOfMemoryError("fatal");
        root.addSuppressed(fatal);

        assertNull(ThrowableGraph.findVirtualMachineError(root));
        assertNull(ThrowableGraph.findVirtualMachineError(new RuntimeException("ordinary")));
    }

    @Test
    void doesNotSearchSuppressedFailuresForRequestedTypes() {
        RuntimeException root = new RuntimeException("root");
        IllegalStateException target = new IllegalStateException("target");
        root.addSuppressed(target);

        assertNull(ThrowableGraph.find(root, IllegalStateException.class));
        assertNull(ThrowableGraph.find(root, IllegalArgumentException.class));
        assertNull(ThrowableGraph.find(null, IllegalStateException.class));
    }

    @Test
    void findsCauseWithoutInspectingSuppressedFailures() {
        IllegalStateException cause = new IllegalStateException("cause");
        RuntimeException root = new RuntimeException("root", cause);
        root.addSuppressed(new IllegalArgumentException("suppressed"));

        assertSame(cause, ThrowableGraph.findCause(root, IllegalStateException.class));
        assertNull(ThrowableGraph.findCause(root, IllegalArgumentException.class));
    }

    @Test
    void rethrowsDirectVirtualMachineError() {
        OutOfMemoryError fatal = new OutOfMemoryError("fatal");

        assertSame(fatal, assertThrows(OutOfMemoryError.class,
                () -> ThrowableGraph.rethrowVirtualMachineError(fatal)));
    }

    @Test
    void unwrapsOnlyKnownReflectionAndAsyncWrappers() {
        OutOfMemoryError reflectionFatal = new OutOfMemoryError("reflection fatal");
        InvocationTargetException reflection = new InvocationTargetException(reflectionFatal);
        InternalError asyncFatal = new InternalError("async fatal");
        CompletionException async = new CompletionException(new ExecutionException(asyncFatal));

        assertSame(reflectionFatal, ThrowableGraph.findVirtualMachineError(reflection));
        assertSame(asyncFatal, ThrowableGraph.findVirtualMachineError(async));
        assertNull(ThrowableGraph.findVirtualMachineError(
                new RuntimeException("application wrapper", asyncFatal)));
    }

    @Test
    void addsOnlyNewAcyclicSuppressedEdges() {
        RuntimeException primary = new RuntimeException("primary");
        RuntimeException cleanup = new RuntimeException("cleanup");

        ThrowableGraph.addSuppressedIfAcyclic(primary, cleanup);
        ThrowableGraph.addSuppressedIfAcyclic(primary, cleanup);
        ThrowableGraph.addSuppressedIfAcyclic(cleanup, primary);

        assertEquals(1, primary.getSuppressed().length);
        assertSame(cleanup, primary.getSuppressed()[0]);
        assertEquals(0, cleanup.getSuppressed().length);
    }

    @Test
    void rejectsADirectCauseBackEdgeWithoutTraversingTheCauseGraph() {
        RuntimeException primary = new RuntimeException("primary");
        RuntimeException cleanup = new RuntimeException("cleanup", primary);

        ThrowableGraph.addSuppressedIfAcyclic(primary, cleanup);

        assertEquals(0, primary.getSuppressed().length);
    }

    @Test
    void promotesPrimaryFatalBeforeCleanupFatalWithoutCreatingCycles() {
        OutOfMemoryError primaryFatal = new OutOfMemoryError("primary fatal");
        InternalError cleanupFatal = new InternalError("cleanup fatal");

        assertSame(primaryFatal, ThrowableGraph.promoteVirtualMachineError(primaryFatal, cleanupFatal));
        assertSame(cleanupFatal, primaryFatal.getSuppressed()[0]);
    }
}
