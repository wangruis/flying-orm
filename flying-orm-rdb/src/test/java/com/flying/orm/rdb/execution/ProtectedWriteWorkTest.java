package com.flying.orm.rdb.execution;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProtectedWriteWorkTest {

    @Test
    void ownerRestrictionReadsTheWriteParametersOnlyOnce() {
        AtomicInteger copies = new AtomicInteger();
        CountingDate value = new CountingDate(1L, copies);
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.UPDATE,
                new SqlRequest("update users set seen_at = ? where id = ?", List.of(value, 1L)),
                new SqlRequest("select id from users where id = ?", List.of(1L)),
                List.of("id"), Map.of(), "id = ?",
                "delete from user_tokens where id = ? and field = ?",
                "insert into user_tokens (id, field, token) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("phone", List.of(new byte[]{1}))));
        copies.set(0);

        work.writeRequestForOwners(List.of(Map.of("id", 1L)));

        assertEquals(1, copies.get(),
                "only the new SqlRequest ownership boundary may snapshot the mutable value");
    }

    @Test
    void centralizesOwnerResolutionAndSideIndexParameterOrder() {
        Map<String, Object> knownOwner = new LinkedHashMap<>();
        knownOwner.put("tenant_id", 7L);
        knownOwner.put("id", null);
        ProtectedWriteWork work = work(ProtectedWriteWork.Kind.INSERT,
                                       List.of("tenant_id", "id"), knownOwner);
        SqlWriteResult result = new SqlWriteResult(
                1L, List.of(DynamicRow.copyOf(Map.of("id", 42L))));

        Map<String, Object> owner = work.resolveInsertOwner(result);
        byte[] token = {1, 2};
        List<Object> parameters = work.sideIndexParameters(owner, work.fields().getFirst(), token);

        assertEquals("id", work.generatedOwnerField());
        assertEquals(Map.of("tenant_id", 7L, "id", 42L), owner);
        assertEquals(List.of(7L, 42L, "phone"), parameters.subList(0, 3));
        assertArrayEquals(token, (byte[]) parameters.get(3));
    }

    @Test
    void rejectsAmbiguousGeneratedOwnerAndChangedUpdateSet() {
        ProtectedWriteWork insert = work(ProtectedWriteWork.Kind.INSERT,
                                         List.of("tenant_id", "id"), Map.of());
        ProtectedWriteWork update = work(ProtectedWriteWork.Kind.UPDATE,
                                         List.of("id"), Map.of("id", 1L));

        assertThrows(IllegalArgumentException.class, insert::generatedOwnerField);
        assertThrows(IllegalStateException.class, () -> update.requireStableOwnerSet(
                List.of(Map.of("id", 1L), Map.of("id", 2L)),
                new SqlWriteResult(1L, List.of())));
    }

    @Test
    void ownerReadsHonorDeveloperOptionsWithoutAnInternalCeiling() {
        SqlExecutionOptions unlimited = SqlExecutionOptions.unlimited();
        assertSame(unlimited, ProtectedWriteWork.ownerReadOptions(unlimited));
        SqlExecutionOptions large = SqlExecutionOptions.safeDefaults().withMaxRows(10_000L)
                .withMaxResultBytes(128L * 1024 * 1024)
                .withMaxLargeObjectBytes(32L * 1024 * 1024).withMaxLargeObjectChars(32_000_000L);
        assertSame(large, ProtectedWriteWork.ownerReadOptions(large));
        SqlExecutionOptions strict = SqlExecutionOptions.safeDefaults()
                .withMaxRows(25L)
                .withMaxResultBytes(1_024L)
                .withMaxLargeObjectBytes(512L)
                .withMaxLargeObjectChars(256L);
        assertSame(strict, ProtectedWriteWork.ownerReadOptions(strict));
    }

    @Test
    void ownerRestrictionDoesNotImposeAPortableParameterCeiling() {
        ProtectedWriteWork work = work(ProtectedWriteWork.Kind.UPDATE, List.of("id"), Map.of());
        List<Map<String, Object>> owners = java.util.stream.IntStream.rangeClosed(1, 2500)
                .mapToObj(id -> Map.<String, Object>of("id", id)).toList();
        SqlRequest request = work.writeRequestForOwners(owners);
        assertEquals(2502, request.parameters().size());
        assertEquals(2500, request.parameters().getLast());
    }

    private static ProtectedWriteWork work(ProtectedWriteWork.Kind kind,
                                             List<String> ownerFields,
                                             Map<String, Object> knownOwner) {
        SqlRequest ownerQuery = kind == ProtectedWriteWork.Kind.UPDATE
                ? new SqlRequest("select id from users where id = ?", List.of(1L))
                : null;
        return new ProtectedWriteWork(
                kind,
                new SqlRequest("update users set name = ? where id = ?", List.of("name", 1L)),
                ownerQuery,
                ownerFields,
                knownOwner,
                "id = ?",
                "delete from user_tokens where id = ? and field = ?",
                "insert into user_tokens (id, field, token) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("phone", List.of(new byte[]{1}))));
    }

    private static final class CountingDate extends Date {
        private final AtomicInteger copies;

        private CountingDate(long time, AtomicInteger copies) {
            super(time);
            this.copies = copies;
        }

        @Override
        public Object clone() {
            copies.incrementAndGet();
            return new CountingDate(getTime(), copies);
        }
    }
}
