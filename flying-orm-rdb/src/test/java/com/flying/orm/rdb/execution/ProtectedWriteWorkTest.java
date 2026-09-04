package com.flying.orm.rdb.execution;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProtectedWriteWorkTest {

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
    void appliesOneOwnerReadLimitForJdbcAndR2dbc() {
        SqlExecutionOptions bounded = ProtectedWriteWork.ownerReadOptions(SqlExecutionOptions.unlimited());

        assertEquals(2_000L, bounded.maxRows());
        assertEquals(SqlExecutionOptions.DEFAULT_MAX_RESULT_BYTES, bounded.maxResultBytes());
        assertEquals(SqlExecutionOptions.DEFAULT_MAX_LARGE_OBJECT_BYTES, bounded.maxLargeObjectBytes());
        assertEquals(SqlExecutionOptions.DEFAULT_MAX_LARGE_OBJECT_CHARS, bounded.maxLargeObjectChars());

        SqlExecutionOptions strict = SqlExecutionOptions.safeDefaults()
                .withMaxRows(25L)
                .withMaxResultBytes(1_024L)
                .withMaxLargeObjectBytes(512L)
                .withMaxLargeObjectChars(256L);
        assertSame(strict, ProtectedWriteWork.ownerReadOptions(strict));
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
}
