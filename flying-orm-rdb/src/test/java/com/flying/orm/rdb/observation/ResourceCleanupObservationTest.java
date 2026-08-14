package com.flying.orm.rdb.observation;

import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.Deque;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证清理观测在处理外来异常图时只保留有界且脱敏的诊断结构。
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
class ResourceCleanupObservationTest {

    /** 循环 suppressed 图不能让仅用于观测的构造过程栈溢出，也不得把外来消息带入公开事件。 */
    @Test
    void sanitizesCyclicSuppressedGraphWithoutLeakingDriverMessages() {
        IllegalStateException primary = new IllegalStateException("password=primary-secret");
        IllegalStateException cleanup = new IllegalStateException("tenant=secondary-secret");
        primary.addSuppressed(cleanup);
        cleanup.addSuppressed(primary);

        ResourceCleanupObservation observation = assertDoesNotThrow(
                () -> new ResourceCleanupObservation(SqlExecutionOperation.UPDATE,
                                                     ResourceCleanupObservation.Phase.CONNECTION_CLOSE,
                                                     true,
                                                     primary));

        assertEquals(ResourceCleanupObservation.FailureKind.FAILURE, observation.failureKind());
        assertEquals("resource cleanup failed", observation.error().getMessage());
        assertFalse(containsMessage(observation.error(), "primary-secret"));
        assertFalse(containsMessage(observation.error(), "secondary-secret"));
        assertTrue(nodeCount(observation.error()) <= 33);
    }

    /** 深层外来 suppressed 图必须在固定节点预算内截断，并用稳定分类而非外来文本表达截断事实。 */
    @Test
    void truncatesDeepSuppressedGraphAtStableSanitizedBoundary() {
        Throwable primary = new IllegalStateException("secret-0");
        Throwable current = primary;
        for (int index = 1; index <= 96; index++) {
            Throwable next = new IllegalStateException("secret-" + index);
            current.addSuppressed(next);
            current = next;
        }

        ResourceCleanupObservation observation = new ResourceCleanupObservation(
                SqlExecutionOperation.UPDATE,
                ResourceCleanupObservation.Phase.CONNECTION_INVALIDATE,
                false,
                primary);

        assertEquals(ResourceCleanupObservation.FailureKind.FAILURE, observation.failureKind());
        assertTrue(nodeCount(observation.error()) <= 33);
        assertTrue(containsMessage(observation.error(), "resource cleanup details truncated"));
        assertFalse(containsMessage(observation.error(), "secret-96"));
    }

    private static int nodeCount(Throwable root) {
        int count = 0;
        Deque<Throwable> pending = new ArrayDeque<>();
        pending.add(root);
        while (!pending.isEmpty()) {
            Throwable current = pending.removeFirst();
            count++;
            for (Throwable suppressed : current.getSuppressed()) {
                pending.addLast(suppressed);
            }
        }
        return count;
    }

    private static boolean containsMessage(Throwable root, String fragment) {
        Deque<Throwable> pending = new ArrayDeque<>();
        pending.add(root);
        while (!pending.isEmpty()) {
            Throwable current = pending.removeFirst();
            if (current.getMessage() != null && current.getMessage().contains(fragment)) {
                return true;
            }
            for (Throwable suppressed : current.getSuppressed()) {
                pending.addLast(suppressed);
            }
        }
        return false;
    }
}
