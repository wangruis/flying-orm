package com.flying.orm.core.sql.expression;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.type.LogicalType;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class SqlExpressionCompilerTest {
    @Test
    void mixedPriorityStructuralRanksAreNotBusinessCodecValues() {
        AtomicInteger numbers = new AtomicInteger();
        ValueExpression expression = ValueExpression.firstMatch(
                ValueExpression.lastMatch(ValueExpression.field("amount").priority(7)),
                ValueExpression.field("other").priority("A"));
        SqlExpressionCompiler.Operand compiled = SqlExpressionCompiler.compile(expression,
                field -> field(field.identity().name()), (parameter, type) -> {
                    Object input = parameter.value();
                    if (input instanceof Integer number) {
                        numbers.incrementAndGet();
                        return value(new ValueExpression.Value(-number));
                    }
                    return value(parameter);
                }, SqlExpressionFunctions.defaults(), "postgresql");
        assertEquals(List.of(-7, "A"), compiled.fragment().parameters());
        assertEquals(1, numbers.get());
        assertEquals("case when amount = ? then 3 when other = ? then 1 else 2 end", compiled.fragment().sql());
    }

    @Test
    void firstMatchCanPlaceAnEarlierMatchingRuleAfterUnmatchedRows() {
        ValueExpression expression = assertDoesNotThrow(() -> ValueExpression.firstMatch(
                ValueExpression.lastMatch(ValueExpression.field("amount").priority(7)),
                ValueExpression.field("other").priority("A", "B")));
        SqlExpressionCompiler.Operand compiled = compile(expression, SqlExpressionFunctions.defaults());
        assertEquals("case when amount = ? then 4 when other = ? then 1 when other = ? then 2 else 3 end",
                compiled.fragment().sql());
        assertEquals(List.of(7, "A", "B"), compiled.fragment().parameters());
        assertEquals(LogicalType.INTEGER, compiled.type());
        assertThrows(IllegalArgumentException.class, () -> ValueExpression.firstMatch(
                ValueExpression.lastMatch(ValueExpression.field("amount").priority(1)), ValueExpression.value(2)));
        assertThrows(IllegalArgumentException.class, () -> compile(ValueExpression.call("priority-mixed-match"),
                SqlExpressionFunctions.defaults()));
    }

    @Test
    void mixedPriorityDoesNotCollideWithUnmatchedRankAfterTenThousandRules() {
        Object[] preferred = java.util.stream.IntStream.range(0, 10002).boxed().toArray();
        ValueExpression expression = ValueExpression.firstMatch(
                ValueExpression.lastMatch(ValueExpression.field("amount").priority(7)),
                ValueExpression.field("other").priority(preferred));
        SqlFragment fragment = compile(expression, SqlExpressionFunctions.defaults()).fragment();
        assertTrue(fragment.sql().startsWith("case when amount = ? then 10004"));
        assertTrue(fragment.sql().endsWith("then 10002 else 10003 end"));
        assertEquals(10003, fragment.parameters().size());
    }

    @Test
    void orderedMatchesUseTheFirstMatchingFieldAndSupportLastPriority() {
        ValueExpression[] pairs = { ValueExpression.field("amount"), ValueExpression.value(7),
                ValueExpression.field("other"), ValueExpression.value("x'); drop table users; --") };
        SqlExpressionCompiler.Operand first = assertDoesNotThrow(() -> compile(
                ValueExpression.call("priority-first-match", pairs), SqlExpressionFunctions.defaults()));
        assertEquals("case when amount = ? then 0 when other = ? then 1 else 2 end", first.fragment().sql());
        assertEquals(List.of(7, "x'); drop table users; --"), first.fragment().parameters());
        SqlExpressionCompiler.Operand last = assertDoesNotThrow(() -> compile(
                ValueExpression.call("priority-last-match", pairs), SqlExpressionFunctions.defaults()));
        assertEquals("case when amount = ? then 1 when other = ? then 2 else 0 end", last.fragment().sql());
        assertEquals(first.fragment().parameters(), last.fragment().parameters());
        assertEquals(LogicalType.INTEGER, first.type());
        assertEquals(LogicalType.INTEGER, last.type());
    }

    @Test
    void priorityUsesBoundValuesAndEvaluatesItsSourceOnlyOnce() {
        String input = "x'); delete from users; --";
        ValueExpression expression = ValueExpression.call("priority",
                ValueExpression.field("amount").add(ValueExpression.value(3)),
                ValueExpression.value(input), ValueExpression.value(7), ValueExpression.value(7));
        SqlExpressionCompiler.Operand result = assertDoesNotThrow(
                () -> compile(expression, SqlExpressionFunctions.defaults()));
        assertEquals("case (amount + ?) when ? then 0 when ? then 1 when ? then 2 else 3 end",
                result.fragment().sql());
        assertEquals(List.of(3, input, 7, 7), result.fragment().parameters());
        assertEquals(LogicalType.INTEGER, result.type());
    }

    @Test
    void orderedMatchGroupsFlattenInDeclarationOrderAndRejectMalformedInput() {
        ValueExpression a = ValueExpression.field("amount").priority(7, 8);
        ValueExpression b = ValueExpression.field("other").priority("x");
        assertEquals("case when amount = ? then 0 when amount = ? then 1 when other = ? then 2 else 3 end",
                compile(ValueExpression.firstMatch(a, b), SqlExpressionFunctions.defaults()).fragment().sql());
        assertEquals(List.of(7, 8, "x"),
                compile(ValueExpression.lastMatch(a, b), SqlExpressionFunctions.defaults()).fragment().parameters());
        assertThrows(IllegalArgumentException.class, () -> ValueExpression.firstMatch());
        assertThrows(IllegalArgumentException.class, () -> ValueExpression.lastMatch(ValueExpression.value(1)));
        for (String id : List.of("priority-first-match", "priority-last-match")) {
            assertThrows(IllegalArgumentException.class, () -> compile(ValueExpression.call(id), SqlExpressionFunctions.defaults()));
            assertThrows(IllegalArgumentException.class, () -> compile(ValueExpression.call(id, a), SqlExpressionFunctions.defaults()));
        }
        List<LogicalType> types = new java.util.ArrayList<>();
        SqlExpressionCompiler.Operand result = SqlExpressionCompiler.compile(ValueExpression.firstMatch(
                ValueExpression.field("amount").priority((Object) null),
                ValueExpression.value(null).priority("x")), field -> field(field.identity().name()),
                (parameter, type) -> { if (type != null) types.add(type); return value(parameter); },
                SqlExpressionFunctions.defaults(), "postgresql");
        assertEquals(java.util.Arrays.asList(null, null, "x"), result.fragment().parameters());
        assertEquals(List.of(LogicalType.DECIMAL, LogicalType.TEXT), types);
    }

    @Test
    void priorityInfersNullBindingTypesFromTheComparedOperands() {
        List<LogicalType> boundTypes = new java.util.ArrayList<>();
        ValueExpression expression = ValueExpression.call("priority", ValueExpression.value(null),
                ValueExpression.value("active"), ValueExpression.value(null));
        SqlExpressionCompiler.Operand result = assertDoesNotThrow(() -> SqlExpressionCompiler.compile(expression,
                field -> field(field.identity().name()), (parameter, type) -> {
                    if (type != null) boundTypes.add(type);
                    return value(parameter);
                }, SqlExpressionFunctions.defaults(), "postgresql"));
        assertEquals("case ? when ? then 0 when ? then 1 else 2 end", result.fragment().sql());
        assertEquals(List.of(LogicalType.TEXT, LogicalType.TEXT), boundTypes);
        assertEquals(java.util.Arrays.asList(null, "active", null), result.fragment().parameters());
    }

    @Test
    void priorityRequiresASourceAndAtLeastOnePreferredValue() {
        for (ValueExpression expression : List.of(ValueExpression.call("priority"),
                ValueExpression.call("priority", ValueExpression.field("amount")))) {
            IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                    () -> compile(expression, SqlExpressionFunctions.defaults()));
            assertEquals("priority requires a source and at least one preferred value", error.getMessage());
        }
    }

    @Test
    void fixedFunctionRegistrationBindsValuesAndKeepsItsDeclaredResultType() {
        SqlExpressionFunctions functions = SqlExpressionFunctions.builder().register("text-size", "length",
                LogicalType.INTEGER, LogicalType.TEXT).build();
        String input = "x'); delete from users; --";
        SqlExpressionCompiler.Operand result = compile(ValueExpression.call("text-size", ValueExpression.value(input)), functions);
        assertEquals("length(?)", result.fragment().sql());
        assertEquals(List.of(input), result.fragment().parameters());
        assertEquals(LogicalType.INTEGER, result.type());
        assertThrows(IllegalArgumentException.class, () -> compile(ValueExpression.call("text-size"), functions));
    }

    @Test
    void fixedFunctionRegistrationFreezesTheSignatureAndTypesNestedNullValues() {
        LogicalType[] types = {LogicalType.TEXT};
        SqlExpressionFunctions functions = SqlExpressionFunctions.builder().register("size", "length",
                LogicalType.INTEGER, types).build();
        types[0] = LogicalType.INTEGER;
        List<LogicalType> boundTypes = new java.util.ArrayList<>();
        ValueExpression nulls = ValueExpression.value(null).coalesce(ValueExpression.value(null));
        SqlExpressionCompiler.Operand result = SqlExpressionCompiler.compile(ValueExpression.call("size", nulls),
                field -> field(field.identity().name()), (parameter, type) -> {
                    if (type != null) boundTypes.add(type);
                    return value(parameter);
                }, functions, "postgresql");
        assertEquals("length(coalesce(?, ?))", result.fragment().sql());
        assertEquals(List.of(LogicalType.TEXT, LogicalType.TEXT), boundTypes);
        assertEquals(java.util.Arrays.asList(null, null), result.fragment().parameters());
    }

    @Test
    void fixedFunctionSupportsZeroArgumentsAndFreezesRegistryOverrides() {
        SqlExpressionFunctions.Builder builder = SqlExpressionFunctions.builder().register("random", "random",
                LogicalType.DECIMAL);
        SqlExpressionFunctions first = builder.build();
        builder.register("random", "rand", LogicalType.DECIMAL);
        assertEquals("random()", compile(ValueExpression.call("random"), first).fragment().sql());
        assertEquals("rand()", compile(ValueExpression.call("random"), builder.build()).fragment().sql());
        assertThrows(IllegalArgumentException.class, () -> SqlExpressionFunctions.builder().register("bad",
                "random(); delete from users", LogicalType.DECIMAL));
    }

    @Test
    void fixedFunctionKeepsFieldResolutionAndNonNullArgumentTypes() {
        SqlExpressionFunctions functions = SqlExpressionFunctions.builder()
                .register("label", "app.label", LogicalType.TEXT, LogicalType.TEXT, LogicalType.TEXT).build();
        AtomicInteger fields = new AtomicInteger();
        SqlExpressionCompiler.Operand result = SqlExpressionCompiler.compile(ValueExpression.call("label",
                ValueExpression.field("amount"), ValueExpression.value("suffix")), field -> {
                    fields.incrementAndGet();
                    return field(field.identity().name());
                }, (parameter, type) -> value(parameter), functions, "postgresql");
        assertEquals(1, fields.get());
        assertEquals("app.label(amount, ?)", result.fragment().sql());
        assertEquals(List.of("suffix"), result.fragment().parameters());
        assertEquals(LogicalType.TEXT, result.type());
    }

    @Test
    void repeatsAndReordersArgumentsWithoutLosingBindings() {
        SqlExpressionFunctions functions = SqlExpressionFunctions.builder()
                .register("reordered", SqlExpressionFunction.numeric(2, LogicalType.DECIMAL, call -> call
                        .sql("(").argument(1).sql(" + ").argument(0).sql(" + ").argument(1).sql(")"))).build();
        ValueExpression expression = ValueExpression.call("reordered", ValueExpression.value(3),
                ValueExpression.value(7).add(ValueExpression.value(2)));
        SqlExpressionCompiler.Operand result = compile(expression, functions);
        assertEquals("((? + ?) + ? + (? + ?))", result.fragment().sql());
        assertEquals(List.of(7, 2, 3, 7, 2), result.fragment().parameters());
        assertEquals(LogicalType.DECIMAL, result.type());
    }

    @Test
    void registrationIsAnImmutableSnapshotAndRendererCopiesPreserveIt() {
        SqlExpressionFunctions.Builder builder = SqlExpressionFunctions.builder()
                .register("f", SqlExpressionFunction.numeric(1, LogicalType.DECIMAL, call -> call.sql("abs(").argument(0).sql(")")));
        SqlExpressionFunctions first = builder.build();
        builder.register("f", SqlExpressionFunction.numeric(1, LogicalType.DECIMAL, call -> call.sql("ceil(").argument(0).sql(")")));
        assertEquals("abs(?)", compile(ValueExpression.call("f", ValueExpression.value(2)), first).fragment().sql());
        assertEquals("ceil(?)", compile(ValueExpression.call("f", ValueExpression.value(2)), builder.build()).fragment().sql());
        SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().expressionFunctions(first).build();
        assertSame(first, renderer.withIdentifierRenderer(name -> "\"" + name + "\"").expressionFunctions());
        assertSame(first, renderer.withFieldIdentifierRenderer(name -> "t." + name,
                name -> DynamicField.of(name, "VARCHAR")).expressionFunctions());
        assertSame(first, renderer.withValueCodecs(ValueCodecRegistry.standard()).expressionFunctions());
        assertThrows(IllegalArgumentException.class, () -> compile(ValueExpression.call("f", ValueExpression.value(2)),
                SqlExpressionFunctions.defaults()));
    }

    @Test
    void typedFunctionsCanConsumeTextAndChooseDialectSyntaxWithoutSeeingValues() {
        SqlExpressionFunctions functions = SqlExpressionFunctions.builder().register("length", call -> {
            call.requireArity(1);
            assertEquals(LogicalType.TEXT, call.type(0));
            call.sql(call.dialect().equals("sqlserver") ? "len(" : "length(").argument(0).sql(")");
            return LogicalType.INTEGER;
        }).build();
        String injection = "x'); delete from users; --";
        SqlExpressionCompiler.Operand result = SqlExpressionCompiler.compile(
                ValueExpression.call("length", ValueExpression.value(injection)),
                field -> field(field.identity().name()), (parameter, type) -> value(parameter), functions, "sqlserver");
        assertEquals("len(?)", result.fragment().sql());
        assertEquals(List.of(injection), result.fragment().parameters());
        assertEquals(LogicalType.INTEGER, result.type());
    }

    @Test
    void rejectsUnknownCallsWrongArityAndNonNumericArithmetic() {
        SqlExpressionFunctions functions = SqlExpressionFunctions.builder()
                .register("abs", SqlExpressionFunction.numeric(1, LogicalType.DECIMAL, call -> call.sql("abs(").argument(0).sql(")"))).build();
        for (ValueExpression expression : List.of(ValueExpression.call("abs"),
                ValueExpression.call("missing", ValueExpression.value(1)),
                ValueExpression.call("abs", ValueExpression.value("text")),
                ValueExpression.value("text").add(ValueExpression.value(1)),
                ValueExpression.call("coalesce", ValueExpression.value(1)))) {
            assertThrows(IllegalArgumentException.class, () -> compile(expression, functions));
        }
    }

    @Test
    void builtInCoalesceRetainsNullOccurrencesAndCompatibleResultTypes() {
        SqlExpressionCompiler.Operand result = compile(ValueExpression.value(null).coalesce(ValueExpression.value("x")),
                SqlExpressionFunctions.defaults());
        assertEquals("coalesce(?, ?)", result.fragment().sql());
        assertEquals(java.util.Arrays.asList(null, "x"), result.fragment().parameters());
        assertEquals(LogicalType.TEXT, result.type());
        assertThrows(IllegalArgumentException.class, () -> compile(ValueExpression.value("text")
                .coalesce(ValueExpression.value(1)), SqlExpressionFunctions.defaults()));
    }

    @Test
    void resolvesEverySourceEvenWhenAFunctionOmitsAnArgument() {
        AtomicInteger resolved = new AtomicInteger();
        SqlExpressionFunctions functions = SqlExpressionFunctions.builder().register("constant", call -> {
            call.sql("1");
            return LogicalType.INTEGER;
        }).build();
        SqlExpressionCompiler.Operand result = SqlExpressionCompiler.compile(
                ValueExpression.call("constant", ValueExpression.field("secret")), field -> {
                    resolved.incrementAndGet();
                    return field(field.identity().name());
                }, (parameter, type) -> value(parameter), functions, "postgresql");
        assertEquals(1, resolved.get());
        assertEquals("1", result.fragment().sql());
    }

    @Test
    void deepExpressionsCompileWithoutRecursiveCallsOrDepthLimits() {
        ValueExpression expression = ValueExpression.field("amount");
        for (int i = 0; i < 5000; i++) expression = expression.add(ValueExpression.value(i));
        SqlExpressionCompiler.Operand result = compile(expression, SqlExpressionFunctions.defaults());
        assertEquals(5000, result.fragment().parameters().size());
        assertEquals(0, result.fragment().parameters().getFirst());
        assertEquals(4999, result.fragment().parameters().getLast());
    }

    @Test
    void nestedNullArgumentsInheritTheFunctionInputTypeNotItsOutputType() {
        List<LogicalType> nullTypes = new java.util.ArrayList<>();
        SqlExpressionFunctions functions = SqlExpressionFunctions.builder().register("length", call -> {
            call.requireArity(1);
            call.sql("length(").argument(0, LogicalType.TEXT).sql(")");
            return LogicalType.INTEGER;
        }).build();
        ValueExpression nulls = ValueExpression.value(null).coalesce(ValueExpression.value(null));
        SqlExpressionCompiler.Operand result = SqlExpressionCompiler.compile(ValueExpression.call("length", nulls),
                field -> field(field.identity().name()), (parameter, type) -> {
                    if (type != null) nullTypes.add(type);
                    return value(parameter);
                }, functions, "postgresql");
        assertEquals(List.of(LogicalType.TEXT, LogicalType.TEXT), nullTypes);
        assertEquals("length(coalesce(?, ?))", result.fragment().sql());
        assertEquals(2, result.fragment().parameters().size());
    }

    @Test
    void integerArithmeticRetainsItsTypeForEnclosingFunctionsAndAggregates() {
        SqlExpressionFunctions functions = SqlExpressionFunctions.builder().register("integer", call -> {
            call.sql("1100000000");
            return LogicalType.INTEGER;
        }).build();
        ValueExpression integer = ValueExpression.call("integer");
        assertEquals(LogicalType.INTEGER, compile(integer.add(integer), functions).type());
        assertEquals(LogicalType.INTEGER, compile(integer.divide(integer), functions).type());
        assertEquals(LogicalType.INTEGER, compile(integer.coalesce(integer), functions).type());
    }

    private static SqlExpressionCompiler.Operand compile(ValueExpression expression, SqlExpressionFunctions functions) {
        return SqlExpressionCompiler.compile(expression, field -> field(field.identity().name()),
                (parameter, type) -> value(parameter), functions, "postgresql");
    }

    private static SqlExpressionCompiler.Operand field(String name) {
        return new SqlExpressionCompiler.Operand(new SqlFragment(name, List.of()), LogicalType.DECIMAL);
    }

    private static SqlExpressionCompiler.Operand value(ValueExpression.Value expression) {
        Object value = expression.value();
        return new SqlExpressionCompiler.Operand(new SqlFragment("?", Collections.singletonList(value)),
                value == null ? null : value instanceof Number ? LogicalType.DECIMAL : LogicalType.TEXT);
    }
}
