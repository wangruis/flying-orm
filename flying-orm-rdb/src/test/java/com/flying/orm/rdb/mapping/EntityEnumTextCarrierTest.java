package com.flying.orm.rdb.mapping;

import com.flying.orm.core.annotation.EnumValue;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EntityEnumTextCarrierTest {

    @TestFactory
    List<DynamicTest> readsTextMembersByContentForRecordsAndBeans() {
        return List.of(
                DynamicTest.dynamicTest("record/char[]", () -> assertSame(Characters.ACTIVE,
                        assertDoesNotThrow(() -> RowMapper.of(TextRecord.class)
                                .map(Map.of("characters", "A"))).characters())),
                DynamicTest.dynamicTest("bean/char[]", () -> assertSame(Characters.ACTIVE,
                        assertDoesNotThrow(() -> RowMapper.of(TextBean.class)
                                .map(Map.of("characters", "A"))).characters)),
                DynamicTest.dynamicTest("record/StringBuilder", () -> assertSame(Builder.ACTIVE,
                        assertDoesNotThrow(() -> RowMapper.of(TextRecord.class)
                                .map(Map.of("builder", "A"))).builder())),
                DynamicTest.dynamicTest("bean/StringBuilder", () -> assertSame(Builder.ACTIVE,
                        assertDoesNotThrow(() -> RowMapper.of(TextBean.class)
                                .map(Map.of("builder", "A"))).builder)),
                DynamicTest.dynamicTest("record/CharSequence", () -> assertSame(Sequence.ACTIVE,
                        assertDoesNotThrow(() -> RowMapper.of(TextRecord.class)
                                .map(Map.of("sequence", "A"))).sequence())),
                DynamicTest.dynamicTest("bean/CharSequence", () -> assertSame(Sequence.ACTIVE,
                        assertDoesNotThrow(() -> RowMapper.of(TextBean.class)
                                .map(Map.of("sequence", "A"))).sequence)));
    }

    @TestFactory
    List<DynamicTest> rejectsDuplicateTextCodesBeforeMapping() {
        return List.of(DuplicateCharactersRecord.class, DuplicateBuilderRecord.class, DuplicateSequenceRecord.class)
                .stream().map(type -> DynamicTest.dynamicTest(type.getSimpleName(),
                        () -> assertThrows(MappingException.class, () -> RowMapper.of(type)))).toList();
    }

    @Test
    void preservesNullAndDistinguishesDifferentText() {
        RowMapper<TextRecord> mapper = RowMapper.of(TextRecord.class);
        assertEquals(new TextRecord(null, null, null), mapper.map(Map.of()));
        assertThrows(MappingException.class, () -> mapper.map(Map.of("characters", "a")));
        assertThrows(MappingException.class, () -> mapper.map(Map.of("builder", "A ")));
    }

    private record TextRecord(Characters characters, Builder builder, Sequence sequence) {
    }

    private static final class TextBean {
        private Characters characters;
        private Builder builder;
        private Sequence sequence;
    }

    private enum Characters {
        ACTIVE;
        @EnumValue private final char[] value = {'A'};
    }

    private enum Builder {
        ACTIVE;
        @EnumValue private final StringBuilder value = new StringBuilder("A");
    }

    private enum Sequence {
        ACTIVE;
        @EnumValue private final CharSequence value = new StringBuilder("A");
    }

    private record DuplicateCharactersRecord(DuplicateCharacters value) {
    }

    private enum DuplicateCharacters {
        FIRST, SECOND;
        @EnumValue private final char[] value = {'A'};
    }

    private record DuplicateBuilderRecord(DuplicateBuilder value) {
    }

    private enum DuplicateBuilder {
        FIRST, SECOND;
        @EnumValue private final StringBuilder value = new StringBuilder("A");
    }

    private record DuplicateSequenceRecord(DuplicateSequence value) {
    }

    private enum DuplicateSequence {
        FIRST, SECOND;
        @EnumValue private final CharSequence value = new StringBuilder("A");
    }
}
