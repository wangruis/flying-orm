package com.flying.orm.rdb.result;

import com.flying.orm.rdb.mapping.RowMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RowLayoutMappingConvergenceTest {

    @Test
    void positionalAndExactReadsDoNotPrepareEntityMapping() throws ReflectiveOperationException {
        java.lang.reflect.Field keys = RowLayout.class.getDeclaredField("mappingKeys");
        keys.setAccessible(true);
        for (List<String> names : List.of(List.of("user_name", "age"),
                List.of("id", "user_name", "age", "email", "phone", "city", "state", "zip", "country"))) {
            RowLayout layout = RowLayout.of(names);
            DynamicRow row = DynamicRow.owned(layout, new Object[names.size()]);

            assertNull(row.value(0));
            assertNull(row.get("user_name"));
            assertNull(keys.get(layout), "ordinary and aggregate rows do not use entity mapping");
            assertEquals(names.indexOf("user_name"), row.mappingIndexOf("username"));
            assertEquals("username", row.mappingKey(names.indexOf("user_name")));
            Object prepared = keys.get(layout);
            assertFalse(row.hasAmbiguousMappingColumns());
            assertSame(prepared, keys.get(layout));
        }
    }

    @Test
    void normalizedMappingLayoutIsSharedByEveryRowInTheResult() {
        RowLayout layout = RowLayout.of(List.of("account.\"user_name\"", "age"));
        DynamicRow first = DynamicRow.owned(layout, new Object[]{"first", 20});
        DynamicRow second = DynamicRow.owned(layout, new Object[]{"second", 21});

        assertSame(first.mappingKey(0), second.mappingKey(0));
        assertEquals("username", first.mappingKey(0));
        assertEquals(0, first.mappingIndexOf("username"));
        assertFalse(first.hasAmbiguousMappingColumns());
    }

    @Test
    void concurrentFirstMappingReadsSeeCompleteIndexesAndAmbiguity() {
        for (List<String> names : List.of(List.of("zip_code", "zip-code"),
                List.of("id", "name", "age", "email", "phone", "city", "state", "zip_code", "zip-code"))) {
            RowLayout layout = RowLayout.of(names);
            int first = names.indexOf("zip_code");
            java.util.stream.IntStream.range(0, 64).parallel().forEach(index -> {
                switch (index % 3) {
                    case 0 -> assertEquals(first, layout.mappingIndexOf("zipcode"));
                    case 1 -> assertEquals("zipcode", layout.mappingKey(first));
                    default -> assertTrue(layout.hasAmbiguousMappingColumns());
                }
            });
        }
    }

    @Test
    void normalizedAmbiguityIsAnalyzedOnceWithoutChangingExactDynamicRowLookup() {
        RowLayout layout = RowLayout.of(List.of("user_name", "user-name"));
        DynamicRow row = DynamicRow.owned(layout, new Object[]{"first", "second"});

        assertTrue(row.hasAmbiguousMappingColumns());
        assertEquals("first", row.get("user_name"));
        assertEquals("second", row.get("user-name"));
    }

    @Test
    void indexedLayoutPreservesExactAndNormalizedLookupSemantics() {
        RowLayout layout = RowLayout.of(List.of(
                "id", "name", "age", "email", "phone", "city", "state", "zip_code", "zip-code"));

        assertEquals(7, layout.indexOf("zip_code"));
        assertEquals(8, layout.indexOf("zip-code"));
        assertEquals(7, layout.mappingIndexOf("zipcode"));
        assertTrue(layout.hasAmbiguousMappingColumns());
    }

    @Test
    void indexedLayoutStillReportsTheFirstDuplicateColumnIndex() {
        DuplicateColumnLabelException failure = assertThrows(
                DuplicateColumnLabelException.class,
                () -> RowLayout.of(List.of(
                        "id", "name", "age", "email", "phone", "city", "state", "zip", "name")));

        assertEquals("name", failure.columnLabel());
        assertEquals(1, failure.firstIndex());
        assertEquals(8, failure.duplicateIndex());
    }

    @Test
    void planBindingIsCreatedOnceAndLivesOnlyWithTheSharedResultLayout() {
        RowLayout layout = RowLayout.of(List.of("user_name"));
        DynamicRow first = DynamicRow.owned(layout, new Object[]{"first"});
        DynamicRow second = DynamicRow.owned(layout, new Object[]{"second"});
        Object plan = new Object();
        AtomicInteger bindings = new AtomicInteger();

        int firstBinding = first.mappingBinding(plan, bindings::incrementAndGet);
        int secondBinding = second.mappingBinding(plan, bindings::incrementAndGet);

        assertEquals(1, firstBinding);
        assertEquals(firstBinding, secondBinding);
        assertEquals(1, bindings.get());
    }

    @Test
    void publishedBindingReadsDoNotWaitForAnotherPlanToBePrepared() throws Exception {
        RowLayout layout = RowLayout.of(List.of("id"));
        Object plan = new Object();
        Object binding = layout.mappingBinding(plan, Object::new);
        CountDownLatch preparing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch read = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            Future<Object> pending = executor.submit(() -> layout.mappingBinding(new Object(), () -> {
                preparing.countDown();
                try {
                    release.await();
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(failure);
                }
                return new Object();
            }));
            try {
                assertTrue(preparing.await(5, TimeUnit.SECONDS));
                Future<Object> reused = executor.submit(() -> {
                    Object value = layout.mappingBinding(plan, () -> {
                        throw new AssertionError("published binding must be reused");
                    });
                    read.countDown();
                    return value;
                });
                assertTrue(read.await(5, TimeUnit.SECONDS), "published reads must not wait for binding creation");
                assertSame(binding, reused.get());
            } finally {
                release.countDown();
            }
            pending.get();
        }
    }

    @Test
    void concurrentBindingsAreCreatedOncePerIdentityAndRetainedAcrossPlans() {
        RowLayout layout = RowLayout.of(List.of("id"));
        Object[] plans = java.util.stream.IntStream.range(0, 32)
                .mapToObj(index -> new String("same name")).toArray();
        AtomicInteger created = new AtomicInteger();
        java.util.stream.IntStream.range(0, 256).parallel().forEach(index -> {
            Object plan = plans[index % plans.length];
            assertSame(plan, layout.mappingBinding(plan, () -> {
                created.incrementAndGet();
                return plan;
            }));
        });

        assertEquals(plans.length, created.get());
        for (Object plan : plans) {
            assertSame(plan, layout.mappingBinding(plan, () -> {
                throw new AssertionError("earlier plan binding must be retained");
            }));
        }
    }

    @Test
    void aliasMapperReusesTheBoundTargetLayoutForEveryRowInTheResult() {
        RowLayout sourceLayout = RowLayout.of(List.of("account_name", "age"));
        DynamicRow first = DynamicRow.owned(sourceLayout, new Object[]{"first", 20});
        DynamicRow second = DynamicRow.owned(sourceLayout, new Object[]{"second", 21});
        RowMapper<DynamicRow> mapper = ((RowMapper<DynamicRow>) row -> row)
                .withAliases(Map.of("account_name", "name"));

        DynamicRow mappedFirst = mapper.map(first);
        DynamicRow mappedSecond = mapper.map(second);

        assertSame(mappedFirst.layout(), mappedSecond.layout());
        assertEquals("first", mappedFirst.get("name"));
        assertEquals("second", mappedSecond.get("name"));
    }

    @Test
    void publicRenameColumnsStillInvokesAStatefulRenamerForEveryCall() {
        RowLayout sourceLayout = RowLayout.of(List.of("name"));
        DynamicRow row = DynamicRow.owned(sourceLayout, new Object[]{"value"});
        AtomicInteger calls = new AtomicInteger();

        DynamicRow first = row.renameColumns(name -> name + calls.incrementAndGet());
        DynamicRow second = row.renameColumns(name -> name + calls.incrementAndGet());

        assertEquals("value", first.get("name1"));
        assertEquals("value", second.get("name2"));
        assertEquals(2, calls.get());
    }

    @Test
    void unchangedColumnNamesReuseTheRowWithoutSuppressingPublicCallbacks() {
        for (List<String> names : List.of(List.<String>of(), List.of("name"),
                List.of("id", "name", "age", "email", "phone", "city", "state", "zip", "country"))) {
            DynamicRow row = DynamicRow.owned(RowLayout.of(names), new Object[names.size()]);
            AtomicInteger calls = new AtomicInteger();
            UnaryOperator<String> renamer = name -> {
                calls.incrementAndGet();
                return new String(name);
            };

            assertSame(row, row.renameColumns(renamer));
            assertSame(row, row.renameColumns(renamer));
            assertEquals(names.size() * 2, calls.get());
        }
    }

    @Test
    void unchangedBoundNamesReuseEachSourceRowAndOnlyPrepareOnce() {
        RowLayout layout = RowLayout.of(List.of("id", "name"));
        DynamicRow first = DynamicRow.owned(layout, new Object[]{1, "first"});
        DynamicRow second = DynamicRow.owned(layout, new Object[]{2, "second"});
        AtomicInteger calls = new AtomicInteger();
        UnaryOperator<String> renamer = name -> {
            calls.incrementAndGet();
            return name;
        };

        assertSame(first, first.renameColumnsBound(renamer));
        assertSame(second, second.renameColumnsBound(renamer));
        assertEquals(2, calls.get());
    }

    @Test
    void aliasesOutsideTheProjectionDoNotWrapTheResult() {
        DynamicRow row = DynamicRow.copyOf(Map.of("id", 7));
        RowMapper<DynamicRow> mapper = ((RowMapper<DynamicRow>) value -> value)
                .withAliases(Map.of("account_name", "name", "id", "id"));

        assertSame(row, mapper.map(row));
    }

    @Test
    void renamingAfterAnUnchangedPrefixPreservesValuesAndValidatesTheWholeLayout() {
        DynamicRow row = DynamicRow.owned(RowLayout.of(List.of("id", "name", "age")),
                new Object[]{7, "first", 20});
        DynamicRow renamed = row.renameColumns(name -> name.equals("name") ? "title" : name);

        assertEquals(List.of("id", "title", "age"), List.copyOf(renamed.keySet()));
        assertEquals("first", renamed.get("title"));
        assertEquals("first", row.get("name"));
        assertThrows(DuplicateColumnLabelException.class,
                () -> row.renameColumns(name -> name.equals("age") ? "id" : name));
        assertThrows(NullPointerException.class,
                () -> row.renameColumns(name -> name.equals("name") ? null : name));
        assertThrows(IllegalArgumentException.class,
                () -> row.renameColumns(name -> name.equals("name") ? " " : name));
    }
}
