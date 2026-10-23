package com.flying.orm.core.expression;

import com.flying.orm.core.field.FieldIdentity;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinSource;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ValueExpressionTest {

    @Test
    void arithmeticAndFunctionCompositionPreservesOrderAndDoesNotMutateOperands() {
        ValueExpression field = ValueExpression.field(" amount ");
        ValueExpression fallback = field.coalesce(ValueExpression.value(0));
        ValueExpression product = fallback.multiply(ValueExpression.field("quantity"));
        ValueExpression.Arithmetic multiplication = (ValueExpression.Arithmetic) product;
        assertEquals(ValueExpression.Operator.MULTIPLY, multiplication.operator());
        assertSame(fallback, multiplication.left());
        assertEquals(new ValueExpression.Field(FieldIdentity.of("quantity"), null), multiplication.right());
        ValueExpression.Call call = (ValueExpression.Call) fallback;
        assertEquals("coalesce", call.id());
        assertEquals(List.of(field, ValueExpression.value(0)), call.arguments());
        assertEquals("amount", ((ValueExpression.Field) field).identity().name());
    }

    @Test
    void allArithmeticOperatorsAreExplicitStructures() {
        ValueExpression left = ValueExpression.field("x");
        ValueExpression right = ValueExpression.value(2);
        List<ValueExpression> expressions = List.of(left.add(right), left.subtract(right),
                left.multiply(right), left.divide(right));
        assertEquals(List.of(ValueExpression.Operator.values()), expressions.stream()
                .map(expression -> ((ValueExpression.Arithmetic) expression).operator()).toList());
        for (ValueExpression expression : expressions) {
            assertSame(left, ((ValueExpression.Arithmetic) expression).left());
            assertSame(right, ((ValueExpression.Arithmetic) expression).right());
        }
        assertNotSame(left.add(right), left.add(right));
        assertEquals(left.add(right), left.add(right));
    }

    @Test
    void selfJoinFieldsRetainTheirSourceIdentity() {
        DynamicForm people = DynamicForm.builder("people", "people")
                .addField(DynamicField.of("id", "BIGINT")).build();
        JoinSource employee = new JoinSource(0, people);
        JoinSource manager = new JoinSource(1, people);
        ValueExpression.Field employeeId = (ValueExpression.Field) ValueExpression.field(employee.field("id"));
        ValueExpression.Field managerId = (ValueExpression.Field) ValueExpression.field(manager.field("id"));
        assertSame(employee, employeeId.source());
        assertSame(manager, managerId.source());
        assertNotEquals(employeeId, managerId);
        assertEquals(employeeId.identity(), managerId.identity());
        assertThrows(IllegalArgumentException.class,
                () -> new ValueExpression.Field(FieldIdentity.of("absent"), employee));
    }

    @Test
    void copiesFunctionArgumentsAndPreservesRepeatedOccurrences() {
        ValueExpression parameter = ValueExpression.value("not SQL: ? ' OR 1=1 --");
        ValueExpression[] input = {parameter, parameter};
        ValueExpression.Call call = (ValueExpression.Call) ValueExpression.call(" MY-FUNCTION ", input);
        input[0] = ValueExpression.value("changed");
        assertEquals("my-function", call.id());
        assertSame(parameter, call.arguments().get(0));
        assertSame(parameter, call.arguments().get(1));
        assertThrows(UnsupportedOperationException.class, () -> call.arguments().clear());
        List<ValueExpression> mutable = new ArrayList<>(List.of(parameter));
        ValueExpression.Call direct = new ValueExpression.Call("CUSTOM", mutable);
        mutable.clear();
        assertEquals(List.of(parameter), direct.arguments());
        assertEquals(List.of(), ((ValueExpression.Call) ValueExpression.call("current-time")).arguments());
    }

    @Test
    void nullIsAnExplicitParameterNotAnAbsentNode() {
        ValueExpression.Value value = (ValueExpression.Value) ValueExpression.value(null);
        assertNull(value.value());
        assertEquals(value, ValueExpression.value(null));
        assertEquals(1, ((ValueExpression.Call) ValueExpression.call("nullable", value)).arguments().size());
        assertThrows(NullPointerException.class, () -> ValueExpression.call("f", (ValueExpression) null));
        assertThrows(NullPointerException.class, () -> ValueExpression.field("a").add(null));
    }

    @Test
    void takesAndReturnsIndependentArraySnapshots() {
        byte[] bytes = {1, 2};
        ValueExpression.Value value = (ValueExpression.Value) ValueExpression.value(bytes);
        bytes[0] = 9;
        byte[] first = (byte[]) value.value();
        assertArrayEquals(new byte[]{1, 2}, first);
        first[1] = 8;
        assertArrayEquals(new byte[]{1, 2}, (byte[]) value.value());
    }

    @Test
    void preservesLogicalMutableScalarTypesAndDefendsTheirPublishedValues() {
        Timestamp timestamp = Timestamp.valueOf("2026-10-05 01:02:03.123456789");
        ValueExpression.Value time = (ValueExpression.Value) ValueExpression.value(timestamp);
        timestamp.setNanos(0);
        Timestamp published = (Timestamp) time.value();
        assertEquals(123456789, published.getNanos());
        published.setNanos(1);
        assertEquals(123456789, ((Timestamp) time.value()).getNanos());
        StringBuilder text = new StringBuilder("before");
        ValueExpression.Value string = (ValueExpression.Value) ValueExpression.value(text);
        text.append(" changed");
        StringBuilder copy = (StringBuilder) string.value();
        assertEquals("before", copy.toString());
        copy.append(" changed");
        assertEquals("before", string.value().toString());
    }

    @Test
    void byteBufferSnapshotPreservesReadableRangeWithoutMovingCallerPosition() {
        ByteBuffer input = ByteBuffer.wrap(new byte[]{4, 5, 6});
        input.position(1);
        ValueExpression.Value value = (ValueExpression.Value) ValueExpression.value(input);
        input.put(1, (byte) 9);
        assertEquals(1, input.position());
        ByteBuffer read = (ByteBuffer) value.value();
        assertTrue(read.isReadOnly());
        assertEquals(5, read.get());
        assertEquals(5, ((ByteBuffer) value.value()).get());
    }

    @Test
    void customImmutableValuesStayUnconvertedUntilTheFieldAwareBoundary() {
        record Token(String code) { }
        Token token = new Token("D");
        assertSame(token, ((ValueExpression.Value) ValueExpression.value(token)).value());
        assertThrows(IllegalArgumentException.class, () -> ValueExpression.call(" "));
        assertThrows(IllegalArgumentException.class, () -> ValueExpression.field(" "));
        assertThrows(NullPointerException.class, () -> new ValueExpression.Arithmetic(null,
                ValueExpression.value(1), ValueExpression.value(2)));
    }

    @Test
    void deepTreesHaveStackSafeStructuralHashingAndEquality() {
        ValueExpression first = ValueExpression.field("amount");
        ValueExpression second = ValueExpression.field("amount");
        for (int i = 0; i < 5000; i++) {
            first = first.add(ValueExpression.value(i));
            second = second.add(ValueExpression.value(i));
        }
        assertEquals(first.hashCode(), second.hashCode());
        assertEquals(first, second);
        assertEquals(ValueExpression.call("outer", first), ValueExpression.call("outer", second));
        assertNotEquals(first, second.add(ValueExpression.value(0)));
    }

    @Test
    void structuralEqualityDoesNotTreatHashCollisionsAsEqual() {
        ValueExpression first = ValueExpression.value("FB").add(ValueExpression.value(1));
        ValueExpression second = ValueExpression.value("Ea").add(ValueExpression.value(1));
        assertEquals(first.hashCode(), second.hashCode());
        assertNotEquals(first, second);
        assertNotEquals(ValueExpression.call("f", first), ValueExpression.call("f", second));
        assertNotEquals(first, null);
        assertNotEquals(first, "not an expression");
    }

    @Test
    void fieldTraversalVisitsSharedSourcesOnceWithoutRecursion() {
        ValueExpression shared = ValueExpression.field("amount");
        ValueExpression expression = shared;
        for (int index = 0; index < 5000; index++) expression = expression.add(shared);
        java.util.List<String> fields = new java.util.ArrayList<>();
        expression.forEachField(field -> fields.add(field.identity().name()));
        assertEquals(java.util.List.of("amount"), fields);
    }
}
