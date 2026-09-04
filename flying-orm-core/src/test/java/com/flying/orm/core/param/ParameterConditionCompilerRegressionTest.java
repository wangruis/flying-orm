package com.flying.orm.core.param;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionValueException;
import com.flying.orm.core.condition.ConditionValueShape;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.internal.condition.ConditionValueNormalizer;
import com.flying.orm.core.internal.condition.ConditionValuePolicy;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParameterConditionCompilerRegressionTest {

    @Test
    void builtInIdentityKeepsCleaningAndDefaultFallbackInAndAndOrGroups() {
        for (boolean orGroup : List.of(false, true)) {
            ParameterConditionSpec spec = ParameterConditionSpec.builder("states", "state", "in")
                    .defaultValue(Arrays.asList(null, " ", " fallback "))
                    .build();
            ParameterConditionCompiler compiler = compiler(spec, orGroup);
            List<Map<String, ?>> emptyInputs = List.of(
                    Map.of(), Collections.singletonMap("states", null),
                    Map.of("states", List.of()), Map.of("states", Arrays.asList(null, " ")));

            for (Map<String, ?> input : emptyInputs) {
                assertEquals(List.of("fallback"), firstTerm(compiler.compile(input), orGroup).value());
            }
            AtomicInteger iterations = new AtomicInteger();
            Iterable<Object> input = () -> {
                assertEquals(1, iterations.incrementAndGet(), "request iterable must be consumed once");
                return Arrays.<Object>asList(null, " ", " active ").iterator();
            };

            assertEquals(List.of("active"),
                         firstTerm(compiler.compile(Map.of("STATES", input)), orGroup).value());
            assertEquals(1, iterations.get());
        }
    }

    @Test
    void builtInIdentitySkipsEmptyCollectionsWithoutDefaultsInAndAndOrGroups() {
        for (boolean orGroup : List.of(false, true)) {
            ParameterConditionCompiler compiler = compiler(
                    ParameterConditionSpec.of("states", "state", "in"), orGroup);
            List<Map<String, ?>> emptyInputs = List.of(
                    Map.of(), Collections.singletonMap("states", null),
                    Map.of("states", List.of()), Map.of("states", Arrays.asList(null, " ")));

            for (Map<String, ?> input : emptyInputs) {
                assertTrue(compiler.compile(input).children().isEmpty());
            }
        }
    }

    @Test
    void builtInIdentityKeepsMutableScalarSnapshotsForInputAndDefault() {
        Date sourceDate = new Date(123L);
        byte[] sourceDefault = {1, 2};
        ParameterConditionCompiler compiler = ParameterConditionCompiler.builder()
                .add(ParameterConditionSpec.of("created", "created_at", "="))
                .addOrGroup(ParameterConditionSpec.builder("digest", "digest", "=")
                                   .defaultValue(sourceDefault).build())
                .build();
        sourceDefault[0] = 9;

        ConditionGroup result = compiler.compile(Map.of("created", sourceDate));
        sourceDate.setTime(999L);
        TermCondition created = assertInstanceOf(TermCondition.class, result.children().getFirst());
        ConditionGroup alternatives = assertInstanceOf(ConditionGroup.class, result.children().get(1));
        TermCondition digest = assertInstanceOf(TermCondition.class, alternatives.children().getFirst());

        assertEquals(new Date(123L), created.value());
        assertArrayEquals(new byte[]{1, 2}, assertInstanceOf(byte[].class, digest.value()));
        ((byte[]) digest.value())[0] = 8;
        assertArrayEquals(new byte[]{1, 2}, assertInstanceOf(byte[].class, digest.value()));
        assertArrayEquals(new byte[]{1, 2}, assertInstanceOf(byte[].class,
                firstTerm(compiler.compile(Map.of()), true).value()));
    }

    @Test
    void userConverterReceivesCleanedInputOnceAndItsOutputIsCleanedInBothGroups() {
        for (boolean orGroup : List.of(false, true)) {
            List<Object> convertedInputs = new ArrayList<>();
            ParameterConditionSpec spec = ParameterConditionSpec.builder("states", "state", "in")
                    .defaultValue(List.of(" fallback "))
                    .convert(value -> {
                        convertedInputs.add(value);
                        return Arrays.asList(null, " ", " converted ");
                    }).build();
            ParameterConditionCompiler compiler = compiler(spec, orGroup);

            assertEquals(List.of("converted"), firstTerm(compiler.compile(
                    Map.of("states", Arrays.asList(null, " ", " active "))), orGroup).value());
            assertEquals(List.of(List.of("active")), convertedInputs);
            assertEquals(List.of("converted"), firstTerm(compiler.compile(Map.of()), orGroup).value());
            assertEquals(List.of(List.of("active"), List.of("fallback")), convertedInputs);
        }
    }

    @Test
    void userConverterOutputStillObeysTheConfiguredLimitsBeforeAstConstruction() {
        AtomicInteger conversions = new AtomicInteger();
        ParameterConditionCompiler strings = ParameterConditionCompiler.builder()
                .maxStringLength(3)
                .add(ParameterConditionSpec.builder("state", "state", "=")
                             .convert(value -> {
                                 conversions.incrementAndGet();
                                 return "long";
                             }).build())
                .build();
        ConditionValueException tooLong = assertThrows(ConditionValueException.class,
                () -> strings.compile(Map.of("state", "ok")));
        assertEquals(ConditionValueException.Error.STRING_TOO_LONG, tooLong.error());
        assertEquals(1, conversions.get());

        ConditionValueException invalidInput = assertThrows(ConditionValueException.class,
                () -> strings.compile(Map.of("state", "long")));
        assertEquals(ConditionValueException.Error.STRING_TOO_LONG, invalidInput.error());
        assertEquals(1, conversions.get(), "invalid input must fail before invoking the converter");

        ParameterConditionCompiler collections = ParameterConditionCompiler.builder()
                .maxCollectionSize(1)
                .addOrGroup(ParameterConditionSpec.builder("states", "state", "in")
                                    .convert(value -> List.of("one", "two")).build())
                .build();
        ConditionValueException tooMany = assertThrows(ConditionValueException.class,
                () -> collections.compile(Map.of("states", List.of("one"))));
        assertEquals(ConditionValueException.Error.COLLECTION_TOO_LARGE, tooMany.error());
    }

    @Test
    void explicitUserIdentityStillReceivesAnIsolatedMutableDefault() {
        ParameterConditionSpec spec = ParameterConditionSpec.builder("digest", "digest", "=")
                .defaultValue(new byte[]{1, 2}).convert(Function.identity()).build();
        ParameterConditionCompiler compiler = ParameterConditionCompiler.builder().add(spec).build();
        ParameterConditionSpec compiled = compiler.specs().getFirst();
        Object ownedDefault = compiled.ownedDefaultValue();
        Object conversionInput = compiled.isolateOwnedDefaultForConverter(ownedDefault);

        assertNotSame(ownedDefault, conversionInput);
        assertSame(conversionInput, compiled.convert(conversionInput));
        ((byte[]) conversionInput)[0] = 9;
        assertArrayEquals(new byte[]{1, 2}, assertInstanceOf(byte[].class,
                firstTerm(compiler.compile(Map.of()), false).value()));
    }

    private static ParameterConditionCompiler compiler(ParameterConditionSpec spec, boolean orGroup) {
        ParameterConditionCompiler.Builder builder = ParameterConditionCompiler.builder();
        return (orGroup ? builder.addOrGroup(spec) : builder.add(spec)).build();
    }

    private static TermCondition firstTerm(ConditionGroup result, boolean orGroup) {
        ConditionGroup group = orGroup
                ? assertInstanceOf(ConditionGroup.class, result.children().getFirst()) : result;
        assertEquals(1, group.children().size());
        return assertInstanceOf(TermCondition.class, group.children().getFirst());
    }

    @Test
    void reusesDeclaredFieldIdentityWhenCompilingConditions() {
        ParameterConditionSpec spec = ParameterConditionSpec.of("customer", "CustomerID", "=");
        ParameterConditionCompiler compiler = ParameterConditionCompiler.builder().add(spec).build();

        TermCondition condition = assertInstanceOf(
                TermCondition.class, compiler.compile(Map.of("customer", 42L)).children().getFirst());

        assertSame(spec.identity(), condition.identity());
        assertEquals("CustomerID", condition.field());
    }

    @Test
    void skipsAbsentNoValueParameterWithoutAnExplicitDefault() {
        ParameterConditionCompiler compiler = ParameterConditionCompiler.builder()
                                                                        .add(ParameterConditionSpec.of(
                                                                                "archived",
                                                                                "archived_at",
                                                                                "is-null"))
                                                                        .build();

        ConditionGroup where = compiler.compile(Map.of());

        assertTrue(where.children().isEmpty());
    }

    @Test
    void skipsAbsentNoValueParametersInsideAnOrGroup() {
        ParameterConditionCompiler compiler = ParameterConditionCompiler.builder()
                                                                        .addOrGroup(
                                                                                ParameterConditionSpec.of(
                                                                                        "missing",
                                                                                        "archived_at",
                                                                                        "is-null"),
                                                                                ParameterConditionSpec.of(
                                                                                        "missing",
                                                                                        "deleted_at",
                                                                                        "is-null"))
                                                                        .build();

        ConditionGroup where = compiler.compile(Map.of());

        assertTrue(where.children().isEmpty());
    }

    @Test
    void keepsExplicitNullDefaultForNoValueParameter() {
        ParameterConditionCompiler compiler = ParameterConditionCompiler.builder()
                                                                        .add(ParameterConditionSpec.builder(
                                                                                "archived",
                                                                                "archived_at",
                                                                                "is-null")
                                                                                                   .defaultValue(null)
                                                                                                   .build())
                                                                        .build();

        ConditionGroup where = compiler.compile(Map.of());

        TermCondition condition = assertInstanceOf(TermCondition.class, where.children().getFirst());
        assertEquals("archived_at", condition.field());
        assertEquals("is-null", condition.operator());
        assertNull(condition.value());
    }

    @Test
    void keepsExplicitNullDefaultForNoValueParameterInsideAnOrGroup() {
        ParameterConditionCompiler compiler = ParameterConditionCompiler.builder()
                                                                        .addOrGroup(ParameterConditionSpec.builder(
                                                                                "archived",
                                                                                "archived_at",
                                                                                "is-not-null")
                                                                                                          .defaultValue(null)
                                                                                                          .build())
                                                                        .build();

        ConditionGroup where = compiler.compile(Map.of());
        ConditionGroup alternatives = assertInstanceOf(ConditionGroup.class, where.children().getFirst());
        TermCondition condition = assertInstanceOf(TermCondition.class, alternatives.children().getFirst());

        assertEquals("archived_at", condition.field());
        assertEquals("is-not-null", condition.operator());
        assertNull(condition.value());
    }

    @Test
    void freezesMutableElementsInsideDefaultCollections() {
        ByteBuffer source = ByteBuffer.wrap(new byte[]{1, 2, 3});
        ParameterConditionCompiler compiler = ParameterConditionCompiler.builder()
                                                                        .add(ParameterConditionSpec.builder(
                                                                                "digest",
                                                                                "digest",
                                                                                "in")
                                                                                                   .defaultValue(List.of(source))
                                                                                                   .build())
                                                                        .build();

        source.put(0, (byte) 9);
        @SuppressWarnings("unchecked")
        List<Object> exposed = (List<Object>) compiler.specs().getFirst().defaultValue();
        assertThrows(java.nio.ReadOnlyBufferException.class,
                     () -> ((ByteBuffer) exposed.getFirst()).put(1, (byte) 8));

        TermCondition condition = assertInstanceOf(
                TermCondition.class, compiler.compile(Map.of()).children().getFirst());
        ByteBuffer actual = assertInstanceOf(ByteBuffer.class,
                                             ((List<?>) condition.value()).getFirst());
        assertEquals(1, actual.get(0));
        assertEquals(2, actual.get(1));
        assertEquals(3, actual.get(2));
    }

    @Test
    void acceptsPrimitiveArrayScalarsInsideCollectionConditions() {
        byte[] first = {1, 2};
        byte[] second = {3, 4};

        ConditionValueNormalizer.Result result = ConditionValueNormalizer.normalize(
                ConditionValueShape.COLLECTION,
                List.of(first, second),
                ConditionValuePolicy.REJECT_EMPTY);

        List<?> values = assertInstanceOf(List.class, result.value());
        assertTrue(java.util.Arrays.equals(first, assertInstanceOf(byte[].class, values.get(0))));
        assertTrue(java.util.Arrays.equals(second, assertInstanceOf(byte[].class, values.get(1))));
    }

    @Test
    void isolatesTheCompiledDefaultBeforeEveryUntrustedConverterInvocation() {
        byte[] source = {1, 2, 3};
        List<Object> convertedDefaults = new ArrayList<>();
        ParameterConditionCompiler compiler = ParameterConditionCompiler.builder()
                                                                        .add(ParameterConditionSpec.builder(
                                                                                "digest",
                                                                                "digest",
                                                                                "=")
                                                                                                   .defaultValue(source)
                                                                                                   .convert(value -> {
                                                                                                       convertedDefaults.add(value);
                                                                                                       ((byte[]) value)[0]++;
                                                                                                       return value;
                                                                                                   })
                                                                                                   .build())
                                                                        .build();
        source[0] = 9;

        TermCondition first = assertInstanceOf(
                TermCondition.class, compiler.compile(Map.of()).children().getFirst());
        TermCondition second = assertInstanceOf(
                TermCondition.class, compiler.compile(Map.of()).children().getFirst());

        assertTrue(java.util.Arrays.equals(new byte[]{2, 2, 3}, (byte[]) first.value()));
        assertTrue(java.util.Arrays.equals(new byte[]{2, 2, 3}, (byte[]) second.value()));
        assertTrue(java.util.Arrays.equals(
                new byte[]{1, 2, 3}, (byte[]) compiler.specs().getFirst().defaultValue()));
        assertNotSame(convertedDefaults.get(0), convertedDefaults.get(1));
    }

    @Test
    void keepsCompiledSpecRecordEqualityAndHashBasedOnTheNormalizedDefaultValue() {
        Function<Object, Object> converter = value -> value;
        ParameterConditionSpec source = new ParameterConditionSpec(
                "state", "state", "=", " active ", true, converter);
        ParameterConditionSpec expected = new ParameterConditionSpec(
                "state", "state", "=", "active", true, converter);

        ParameterConditionSpec actual = ParameterConditionCompiler.builder()
                                                                    .add(source)
                                                                    .build()
                                                                    .specs()
                                                                    .getFirst();

        assertEquals(expected, actual);
        assertEquals(expected.hashCode(), actual.hashCode());
    }
}
