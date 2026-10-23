package com.flying.orm.rdb.metadata;

import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.internal.cache.SchemaCacheInvalidationCoordinator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemaCacheInvalidationCoordinatorTest {
    @Test
    void nestedOwnersNotifyEachCacheOnceAndPreserveSegmentedNames() {
        RecordingCache first = new RecordingCache();
        RecordingCache second = new RecordingCache();
        SchemaCacheInvalidationCoordinator nested = SchemaCacheInvalidationCoordinator.of(first, second);
        SchemaCacheInvalidationCoordinator owner = SchemaCacheInvalidationCoordinator.of(
                first, nested, second, MetadataCacheInvalidator.none());
        assertSame(owner, owner.with(first, first));
        RelationIdentity relation = RelationIdentity.of(null, "tenant", "orders");
        owner.invalidate("orders");
        owner.invalidate("tenant", "orders");
        owner.invalidate(relation);
        owner.invalidateAll();
        assertEquals(List.of(RelationIdentity.table("orders"), relation, relation, "all"), first.calls);
        assertEquals(first.calls, second.calls);
    }

    @Test
    void emptyCompositionReusesTheEmptyOwner() {
        assertSame(SchemaCacheInvalidationCoordinator.of(),
                SchemaCacheInvalidationCoordinator.of(MetadataCacheInvalidator.none()));
    }

    @Test
    void failureStillNotifiesRemainingCachesAndRetainsBothCauses() {
        IllegalStateException firstFailure = new IllegalStateException("first cache");
        IllegalArgumentException secondFailure = new IllegalArgumentException("second cache");
        RecordingCache first = new RecordingCache();
        RecordingCache second = new RecordingCache();
        first.failure = firstFailure;
        second.failure = secondFailure;
        RecordingCache last = new RecordingCache();
        SchemaCacheInvalidationCoordinator owner = SchemaCacheInvalidationCoordinator.of(first, second, last);
        RdbException failure = assertThrows(RdbException.class, () -> owner.invalidate("orders"));
        assertSame(firstFailure, failure.getCause());
        assertEquals(List.of(secondFailure), List.of(failure.getSuppressed()));
        assertEquals(List.of(RelationIdentity.table("orders")), last.calls);
    }

    private static final class RecordingCache implements MetadataCacheInvalidator {
        private final List<Object> calls = new ArrayList<>();
        private RuntimeException failure;

        @Override
        public void invalidate(RelationIdentity relation) {
            calls.add(relation);
            if (failure != null) throw failure;
        }

        @Override
        public void invalidateAll() {
            calls.add("all");
        }
    }
}
