package com.flying.orm.rdb.mapping;

import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.EnumValue;
import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.IntFunction;

import static org.junit.jupiter.api.Assertions.*;

class NestedRowMapperTest {
    @Test
    void nestedMapsPreservePathsNullsAndColumnOrderWithoutChangingTheRow() {
        RowMapper<Map<String, Object>> mapper = RowMapper.nested();
        Map<String, Object> source = row("id", 1L, "department.id", 10L,
                "department.name", null, "department.owner.name", "Alice", "label", "L");
        Map<String, Object> result = mapper.map(source);
        assertEquals(row("id", 1L, "department", row("id", 10L, "name", null,
                "owner", row("name", "Alice")), "label", "L"), result);
        assertEquals(java.util.List.of("id", "department", "label"),
                java.util.List.copyOf(result.keySet()));
        assertTrue(source.containsKey("department.id"));
        assertFalse(source.containsKey("department"));
        assertEquals(row("department", row("id", null)), mapper.map(row("department.id", null)));
        assertEquals(Map.of(), mapper.map(Map.of()));
    }

    @Test
    void boundMapPathsReadCurrentValuesAndReturnIndependentMaps() {
        RowMapper<Map<String, Object>> mapper = RowMapper.nested();
        Function<IntFunction<Object>, Map<String, Object>> bound =
                mapper.bindColumns(DynamicRow.copyOf(row("child.id", 1L, "id", 2L)));
        assertNotNull(bound);
        Object[] values = {3L, 4L};
        Map<String, Object> first = bound.apply(index -> values[index]);
        values[0] = null;
        Map<String, Object> second = bound.apply(index -> values[index]);
        assertEquals(row("child", row("id", 3L), "id", 4L), first);
        assertEquals(row("child", row("id", null), "id", 4L), second);
        assertNotSame(first.get("child"), second.get("child"));
        assertEquals(row("other", row("name", "D")), mapper.map(row("other.name", "D")));
    }

    @Test
    void mapPathsRejectScalarBranchCollisionsBeforeReadingValues() {
        RowMapper<Map<String, Object>> mapper = RowMapper.nested();
        for (Map<String, Object> source : java.util.List.of(
                row("child", "D", "child.id", 1L), row("child.id", 1L, "child", "D"),
                row("a.b", 1L, "a.b.c", 2L), row(".id", 1L), row("child.", 1L), row("a..b", 1L))) {
            assertThrows(MappingException.class, () -> mapper.bindColumns(DynamicRow.copyOf(source)));
        }
    }

    @Test
    void mapPathsComposeWithAliasesAndKeepLiteralMapValues() {
        RowMapper<Map<String, Object>> mapper = RowMapper.nested()
                .withAliases(Map.of("child_id", "child.id", "label", "child.label"));
        DynamicRow source = DynamicRow.copyOf(row("child_id", 7L, "label", "D"));
        assertEquals(row("child", row("id", 7L, "label", "D")), mapper.map(source));
        assertEquals(row("child", row("id", 8L, "label", "E")),
                mapper.bindColumns(source).apply(index -> index == 0 ? 8L : "E"));
        Map<String, Object> payload = row("literal.key", "value");
        assertSame(payload, RowMapper.nested().map(row("payload", payload)).get("payload"));
        java.util.stream.IntStream.range(0, 100).parallel().forEach(index ->
                assertEquals(row("child", row("id", 7L, "label", "D")), mapper.map(source)));
    }

    @Test
    void boundNestedMappingUsesCurrentRowValuesAndKeepsAbsentChildrenNull() {
        Function<IntFunction<Object>, Person> bound = RowMapper.nested(Person.class).bindColumns(
                DynamicRow.copyOf(row("id", 1L, "department.id", 10L, "department.name", "first")));
        assertNotNull(bound);
        Object[] values = {2L, 20L, "second"};
        IntFunction<Object> reader = index -> values[index];
        assertEquals(new Person(2L, new Department(20L, "second")), bound.apply(reader));
        values[1] = null;
        values[2] = null;
        assertEquals(new Person(2L, null), bound.apply(reader));
    }
    @Test
    void nestedPresenceChecksOnlyMappedLeavesAcrossSiblingBranches() {
        RowMapper<PairRoot> mapper = RowMapper.nested(PairRoot.class);
        assertEquals(new PairRoot(1L, new Pair(null, new Department(null, "D"))),
                mapper.map(row("id", 1L, "pair.left.id", null, "pair.right.name", "D")));
        assertEquals(new PairRoot(1L, null), mapper.map(row("id", 1L,
                "pair.left.id", null, "pair.right.name", null, "pair.unknown", "ignored")));
        assertEquals(new PairRoot(1L, null), mapper.map(row("id", 1L, "pair.left.unknown", "ignored")));
    }

    @Test
    void recordPathsKeepParentAndChildColumnsSeparate() {
        RowMapper<Person> mapper = RowMapper.nested(Person.class);
        assertEquals(new Person(1L, new Department(10L, "engineering")),
                mapper.map(row("id", 1L, "department.id", 10L, "department.name", "engineering")));
        assertThrows(MappingException.class, () -> RowMapper.of(Person.class)
                .map(row("id", 1L, "department.id", 10L)));
    }

    @Test
    void allNullChildIsAbsentButProjectedNullDoesNotKeepBeanInitializer() {
        RowMapper<Bean> mapper = RowMapper.nested(Bean.class);
        assertNull(mapper.map(row("id", 1L, "department.id", null, "department.name", null)).department);
        Child present = mapper.map(row("department.id", 10L, "department.name", null)).department;
        assertEquals(10L, present.id);
        assertNull(present.name);
        assertEquals("initial", mapper.map(row("department.id", 10L)).department.name);
    }

    @Test
    void recursivelyMapsRecordAndBeanAndSupportsSelfRecursiveType() {
        assertEquals(new Report(new Person(1L, new Department(10L, "D"))),
                RowMapper.nested(Report.class).map(row("person.id", 1L,
                        "person.department.id", 10L, "person.department.name", "D")));
        Node result = RowMapper.nested(Node.class).map(row("id", 1L, "next.id", 2L, "next.next.id", 3L));
        assertEquals(1L, result.id);
        assertEquals(2L, result.next.id);
        assertEquals(3L, result.next.next.id);
        assertNull(result.next.next.next);
    }

    @Test
    void mapperCanBeReusedWithDifferentProjectionLayouts() {
        RowMapper<Person> mapper = RowMapper.nested(Person.class);
        assertEquals(new Person(1L, null), mapper.map(row("id", 1L)));
        assertEquals(new Person(null, new Department(2L, null)), mapper.map(row("department.id", 2L)));
        assertEquals(new Person(3L, null), mapper.map(row("department.id", null, "id", 3L)));
    }

    @Test
    void matchingUsesAnnotationsAndPhysicalColumnPrecedenceWithinEachPath() {
        RowMapper<Annotated> mapper = RowMapper.nested(Annotated.class);
        assertEquals(new Annotated(new Label("actual")), mapper.map(row(
                "detail.DISPLAY_NAME", "actual", "detail.name", "fallback")));
        assertEquals(new Annotated(new Label("property")), mapper.map(row("department.name", "property")));
    }

    @Test
    void rejectsAmbiguousPathsAndDirectValueMixedWithDescendants() {
        RowMapper<Person> mapper = RowMapper.nested(Person.class);
        assertThrows(MappingException.class, () -> mapper.map(row("department.id", 1L, "DEPARTMENT.ID", 2L)));
        assertThrows(MappingException.class, () -> mapper.map(row("department", new Department(1L, "D"),
                "department.id", 2L)));
    }

    @Test
    void existingObjectValuesAndUnknownColumnsKeepExistingMappingRules() {
        Department department = new Department(2L, "D");
        assertSame(department, RowMapper.nested(Person.class).map(row("department", department)).department());
        assertEquals(new Person(1L, null), RowMapper.nested(Person.class).map(row("id", 1L, "extra.name", "X")));
        assertNull(RowMapper.nested(Ignored.class).map(row("department.id", 2L)).department());
    }

    @Test
    void nestedValuesReuseEnumsAndApplicationCodecs() {
        ValueCodecRegistry codecs = ValueCodecRegistry.standard().withFirst(new ValueCodec() {
            @Override
            public boolean supports(Class<?> type) {
                return type == Token.class;
            }

            @Override
            public Object read(Object value, Class<?> type) {
                return value == null ? null : new Token(value.toString());
            }
        });
        assertEquals(new Encoded(new EncodedChild(State.DISABLED, new Token("12"))),
                RowMapper.nested(Encoded.class, codecs).map(row("child.state", "D", "child.token", 12)));
    }

    @Test
    void setterAndAliasCompositionRemainUsableWithoutPerRowMutableState() {
        RowMapper<SetterRoot> mapper = RowMapper.nested(SetterRoot.class)
                .withAliases(Map.of("child_id", "child.id"));
        DynamicRow source = DynamicRow.copyOf(row("child_id", 7L));
        java.util.stream.IntStream.range(0, 100).parallel().forEach(index -> {
            SetterRoot result = mapper.map(source);
            assertEquals(7L, result.child.id);
            assertEquals(1, result.assignments);
        });
        assertEquals(7L, source.get("child_id"));
        assertFalse(source.containsKey("child.id"));
    }

    @Test
    void unprojectedChildAndUnknownDescendantsPreserveBeanDefault() {
        RowMapper<DefaultParent> mapper = RowMapper.nested(DefaultParent.class);
        assertEquals(99L, mapper.map(row("id", 1L)).department.id);
        assertEquals(99L, mapper.map(row("department.unknown", 1L)).department.id);
        assertNull(mapper.map(row("department.id", null)).department);
    }

    @Test
    void unknownPhysicalPrefixDoesNotHideKnownLogicalPath() {
        assertEquals(new Annotated(new Label("D")), RowMapper.nested(Annotated.class)
                .map(row("detail.unknown", 1L, "department.name", "D")));
    }

    @Test
    void parentAliasesCanSupplyDifferentChildrenWithoutDroppingEitherValue() {
        assertEquals(new Mixed(new Department(7L, "D")), RowMapper.nested(Mixed.class)
                .map(row("detail.id", 7L, "department.name", "D")));
    }

    private static Map<String, Object> row(Object... pairs) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int index = 0; index < pairs.length; index += 2) {
            row.put((String) pairs[index], pairs[index + 1]);
        }
        return row;
    }

    record Person(Long id, Department department) { }
    record PairRoot(Long id, Pair pair) { }
    record Pair(Department left, Department right) { }
    record Department(Long id, String name) { }
    record Report(Person person) { }
    record Annotated(@TableField("detail") Label department) { }
    record Mixed(@TableField("detail") Department department) { }
    record Label(@TableField("display_name") String name) { }
    record Ignored(Long id, @TableField(exist = false) Department department) { }
    record Encoded(EncodedChild child) { }
    record EncodedChild(State state, Token token) { }
    record Token(String text) { }

    enum State {
        DISABLED("D");
        @EnumValue
        private final String value;
        State(String value) { this.value = value; }
    }

    static class SetterRoot {
        private Child child;
        @TableField(exist = false)
        int assignments;
        public Child getChild() { return child; }
        public void setChild(Child child) { this.child = child; assignments++; }
    }

    static class DefaultParent {
        Long id;
        Child department = new Child();
        DefaultParent() { department.id = 99L; }
    }

    static class Bean {
        Long id;
        Child department;
    }

    static class Child {
        Long id;
        String name = "initial";
    }

    static class Node {
        Long id;
        Node next;
    }
}
