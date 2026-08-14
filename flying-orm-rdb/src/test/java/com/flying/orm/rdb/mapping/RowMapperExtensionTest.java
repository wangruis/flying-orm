package com.flying.orm.rdb.mapping;

import com.flying.orm.core.codec.DriverValueAdapter;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.rdb.result.DuplicateColumnLabelException;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RowMapperExtensionTest {

    @Test
    void mapsQualifiedLabelsAndExplicitAliases() {
        RowMapper<UserRow> mapper = RowMapper.of(UserRow.class)
                .withAliases(Map.of("display_name", "name"));

        UserRow row = mapper.map(DynamicRow.copyOf(Map.of("u.id", 7L, "DISPLAY_NAME", "Alice")));

        assertEquals(7L, row.id());
        assertEquals("Alice", row.name());
    }

    @Test
    void aliasMappingKeepsTheCompactRowRepresentation() {
        RowMapper<DynamicRow> identity = row -> assertInstanceOf(DynamicRow.class, row);

        DynamicRow renamed = identity.withAliases(Map.of("display_name", "name"))
                                     .map(DynamicRow.copyOf(Map.of("display_name", "Alice")));

        assertEquals("Alice", renamed.get("name"));
    }

    @Test
    void rejectsCaseInsensitiveAliasConflictsBeforeMappingRows() {
        assertThrows(IllegalArgumentException.class,
                     () -> RowMapper.of(UserRow.class)
                                    .withAliases(Map.of("DISPLAY_NAME", "name",
                                                        "display_name", "id")));
    }

    /** 归一化冲突不能把调用方提供的无界别名键拼进公开异常。 */
    @Test
    void rejectsConflictingAliasWithStableMessageWithoutRawCallerKey() {
        String rawKey = " ".repeat(4_096) + "secret_alias" + " ".repeat(4_096);
        Map<String, String> aliases = new LinkedHashMap<>();
        aliases.put("secret_alias", "name");
        aliases.put(rawKey, "id");

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> RowMapper.of(UserRow.class).withAliases(aliases));

        assertEquals("row aliases contain conflicting column", error.getMessage());
        assertFalse(error.getMessage().contains(rawKey));
    }

    /** 调用方直接提供的重复归一化列标签也不能进入映射异常文本。 */
    @Test
    void rejectsAmbiguousCallerLabelsWithStableMessageWithoutRawColumnLabel() {
        String rawLabel = " ".repeat(4_096) + "name" + " ".repeat(4_096);
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("name", "Alice");
        row.put(rawLabel, "Mallory");

        MappingException error = assertThrows(MappingException.class,
                () -> RowMapper.of(UserRow.class).map(row));

        assertEquals("column names become ambiguous after normalization", error.getMessage());
        assertFalse(error.getMessage().contains(rawLabel));
    }

    /** 两个别名目标重名时，公开错误消息不能回显无界目标名称。 */
    @Test
    void rejectsDuplicateAliasTargetsWithoutRawTargetInMessage() {
        String rawTarget = "secret_target_" + "x".repeat(8_192);
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("left", "A");
        row.put("right", "B");
        RowMapper<DynamicRow> identity = source -> source;

        DuplicateColumnLabelException error = assertThrows(DuplicateColumnLabelException.class,
                () -> identity.withAliases(Map.of("left", rawTarget, "right", rawTarget))
                              .map(DynamicRow.copyOf(row)));

        assertEquals("query result contains duplicate column label at indexes 0 and 1", error.getMessage());
        assertFalse(error.getMessage().contains(rawTarget));
        assertEquals(rawTarget, error.columnLabel());
    }

    @Test
    void unwrapsOptionalDriverSpecificValuesBeforeStandardConversion() {
        ValueCodecRegistry codecs = ValueCodecRegistry.standard().withDriverAdapter(new DriverValueAdapter() {
            @Override
            public boolean supports(Object value) { return value instanceof DriverNumber; }
            @Override
            public Object unwrap(Object value) { return ((DriverNumber) value).text; }
        });

        assertEquals(42L, codecs.read(new DriverNumber("42"), Long.class));
    }

    private record UserRow(Long id, String name) {}
    private record DriverNumber(String text) {}
}
