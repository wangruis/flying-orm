package com.flying.orm.rdb.protection;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.scope.DataScope;
import org.junit.jupiter.api.Test;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProtectedFieldReprotectionLookupBudgetTest {

    private static final int WIDE_ROW_WIDTH = 32;
    private static final String UNICODE_IGNORE_CASE_ALIAS = "F\u0130ELD";

    @Test
    void widePublicMigrationHelpersVisitEachInputEntryAtMostOnce() {
        DynamicForm form = protectedForm(WIDE_ROW_WIDTH);
        Map<String, Object> legacySource = new LinkedHashMap<>();
        Map<String, Object> targetSource = new LinkedHashMap<>();
        Map<String, Object> expected = new LinkedHashMap<>();
        for (int index = 0; index < WIDE_ROW_WIDTH; index++) {
            String field = "field_" + index;
            String inputName = field.toUpperCase(Locale.ROOT);
            String value = "value_" + index;
            legacySource.put(inputName, value);
            targetSource.put(inputName, null);
            expected.put(field, value);
        }
        CountingMap legacy = new CountingMap(legacySource);
        CountingMap target = new CountingMap(targetSource);

        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(1))) {
            ProtectedFieldReprotection migration = ProtectedFieldReprotection.create(keys);
            Map<String, Object> plaintext = migration.valuesNeedingPlaintextMigration(form, legacy, target);
            long plaintextLegacyVisits = legacy.entryVisits();
            long plaintextTargetVisits = target.entryVisits();

            target.resetEntryVisits();
            Map<String, Object> reprotection = migration.valuesNeedingReprotection(
                    form, target, DataScope.none(), ValueCodecRegistry.standard());
            long reprotectionVisits = target.entryVisits();

            assertAll(
                    () -> assertEquals(expected, plaintext),
                    () -> assertTrue(reprotection.isEmpty()),
                    () -> assertTrue(plaintextLegacyVisits <= WIDE_ROW_WIDTH,
                            "legacy plaintext lookup visited " + plaintextLegacyVisits
                                    + " entries instead of indexing at most " + WIDE_ROW_WIDTH),
                    () -> assertTrue(plaintextTargetVisits <= WIDE_ROW_WIDTH,
                            "target ciphertext lookup visited " + plaintextTargetVisits
                                    + " entries instead of indexing at most " + WIDE_ROW_WIDTH),
                    () -> assertTrue(reprotectionVisits <= WIDE_ROW_WIDTH,
                            "reprotection lookup visited " + reprotectionVisits
                                    + " entries instead of indexing at most " + WIDE_ROW_WIDTH));
        }
    }

    @Test
    void preservesStringEqualsIgnoreCaseForUnicodeAliases() {
        DynamicForm form = protectedForm("field", "other");
        byte[] oldCiphertext = ciphertext(form, "v1", key(1), "old secret");
        Map<String, Object> target = new LinkedHashMap<>();
        target.put(UNICODE_IGNORE_CASE_ALIAS, null);

        assertTrue("field".equalsIgnoreCase(UNICODE_IGNORE_CASE_ALIAS));
        try (ProtectedFieldKeyRing keys = rotatingKeys()) {
            ProtectedFieldReprotection migration = ProtectedFieldReprotection.create(keys);

            assertEquals(Map.of("field", "trusted legacy"),
                    migration.valuesNeedingPlaintextMigration(
                            form, Map.of(UNICODE_IGNORE_CASE_ALIAS, "trusted legacy"), target));
            assertEquals(Map.of("field", "old secret"),
                    migration.valuesNeedingReprotection(
                            form, Map.of(UNICODE_IGNORE_CASE_ALIAS, oldCiphertext),
                            DataScope.none(), ValueCodecRegistry.standard()));
        }
    }

    @Test
    void ignoresNullInputKeysWhenBuildingWideLookups() {
        DynamicForm form = protectedForm("field", "other");
        byte[] oldCiphertext = ciphertext(form, "v1", key(1), "old secret");
        Map<String, Object> legacy = new LinkedHashMap<>();
        legacy.put(null, "ignored");
        legacy.put("FIELD", "trusted legacy");
        Map<String, Object> target = new LinkedHashMap<>();
        target.put(null, "ignored");
        target.put("FIELD", null);
        Map<String, Object> physical = new LinkedHashMap<>();
        physical.put(null, "ignored");
        physical.put("FIELD", oldCiphertext);

        try (ProtectedFieldKeyRing keys = rotatingKeys()) {
            ProtectedFieldReprotection migration = ProtectedFieldReprotection.create(keys);

            assertAll(
                    () -> assertEquals(Map.of("field", "trusted legacy"),
                            migration.valuesNeedingPlaintextMigration(form, legacy, target)),
                    () -> assertEquals(Map.of("field", "old secret"),
                            migration.valuesNeedingReprotection(
                                    form, physical, DataScope.none(), ValueCodecRegistry.standard())));
        }
    }

    @Test
    void rejectsAmbiguityOnlyForProtectedFieldsThatAreAccessed() {
        DynamicForm form = protectedForm("field", "unused");
        Map<String, Object> ambiguousField = new LinkedHashMap<>();
        ambiguousField.put("field", null);
        ambiguousField.put("FIELD", null);
        Map<String, Object> unrelatedAmbiguity = new LinkedHashMap<>();
        unrelatedAmbiguity.put("FIELD", null);
        unrelatedAmbiguity.put("other", "first");
        unrelatedAmbiguity.put("OTHER", "second");

        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(1))) {
            ProtectedFieldReprotection migration = ProtectedFieldReprotection.create(keys);

            IllegalArgumentException plaintextAmbiguity = assertThrows(
                    IllegalArgumentException.class,
                    () -> migration.valuesNeedingPlaintextMigration(
                            form, Map.of("field", "trusted legacy"), ambiguousField));
            IllegalArgumentException reprotectionAmbiguity = assertThrows(
                    IllegalArgumentException.class,
                    () -> migration.valuesNeedingReprotection(
                            form, ambiguousField, DataScope.none(), ValueCodecRegistry.standard()));

            assertAll(
                    () -> assertEquals("protected migration column is ambiguous",
                            plaintextAmbiguity.getMessage()),
                    () -> assertEquals("protected migration column is ambiguous",
                            reprotectionAmbiguity.getMessage()),
                    () -> assertEquals(Map.of("field", "trusted legacy"),
                            migration.valuesNeedingPlaintextMigration(
                                    form, Map.of("field", "trusted legacy"), unrelatedAmbiguity)),
                    () -> assertTrue(migration.valuesNeedingReprotection(
                            form, unrelatedAmbiguity, DataScope.none(), ValueCodecRegistry.standard()).isEmpty()));
        }
    }

    @Test
    void verifiesTargetCiphertextBeforeConsultingLegacyPlaintext() {
        DynamicForm form = protectedForm("field");
        byte[] currentCiphertext = ciphertext(form, "v2", key(2), "already migrated");

        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v2", key(2))) {
            Map<String, Object> pending = ProtectedFieldReprotection.create(keys)
                    .valuesNeedingPlaintextMigration(
                            form, new ForbiddenIterationMap(), Map.of("FIELD", currentCiphertext));

            assertTrue(pending.isEmpty());
        }
    }

    @Test
    void keepsExplicitNullMigrationAndReprotectionPaths() {
        DynamicForm form = protectedForm("field");
        Map<String, Object> nullTarget = new LinkedHashMap<>();
        nullTarget.put("FIELD", null);

        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(1))) {
            ProtectedFieldReprotection migration = ProtectedFieldReprotection.create(keys);

            assertAll(
                    () -> assertEquals(Map.of("field", "trusted legacy"),
                            migration.valuesNeedingPlaintextMigration(
                                    form, Map.of("FIELD", "trusted legacy"), nullTarget)),
                    () -> assertTrue(migration.valuesNeedingPlaintextMigration(
                            form, Map.of(), nullTarget).isEmpty()),
                    () -> assertTrue(migration.valuesNeedingReprotection(
                            form, nullTarget, DataScope.none(), ValueCodecRegistry.standard()).isEmpty()));
        }
    }

    @Test
    void authenticatesCurrentCiphertextBeforeSkippingReprotection() {
        DynamicForm form = protectedForm("field");
        byte[] tampered = ciphertext(form, "v2", key(2), "current secret");
        tampered[tampered.length - 1] ^= 1;

        try (ProtectedFieldKeyRing keys = rotatingKeys()) {
            assertThrows(ProtectedFieldException.class,
                    () -> ProtectedFieldReprotection.create(keys).valuesNeedingReprotection(
                            form, Map.of("FIELD", tampered),
                            DataScope.none(), ValueCodecRegistry.standard()));
        }
    }

    private static DynamicForm protectedForm(int width) {
        DynamicForm.Builder builder = DynamicForm.builder("migration_scan", "migration_scan");
        for (int index = 0; index < width; index++) {
            String field = "field_" + index;
            builder.addField(DynamicField.of(field, "VARCHAR"));
            builder.encrypted(field, EncryptedFieldDefinition.builder().searchModes().build());
        }
        return builder.build();
    }

    private static DynamicForm protectedForm(String... fields) {
        DynamicForm.Builder builder = DynamicForm.builder("migration_scan", "migration_scan");
        for (String field : fields) {
            builder.addField(DynamicField.of(field, "VARCHAR"));
            builder.encrypted(field, EncryptedFieldDefinition.builder().searchModes().build());
        }
        return builder.build();
    }

    private static byte[] ciphertext(DynamicForm form, String version, byte[] key, String plaintext) {
        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single(version, key))) {
            return (byte[]) runtime.prepareWrite(
                    form, Map.of("field", plaintext), DataScope.none(), ValueCodecRegistry.standard())
                    .values().get("field");
        }
    }

    private static ProtectedFieldKeyRing rotatingKeys() {
        return ProtectedFieldKeyRing.builder()
                                    .current("v2", key(2))
                                    .readable("v1", key(1))
                                    .build();
    }

    private static byte[] key(int value) {
        byte[] key = new byte[32];
        Arrays.fill(key, (byte) value);
        return key;
    }

    private static final class CountingMap extends AbstractMap<String, Object> {

        private final Map<String, Object> source;
        private long entryVisits;

        private CountingMap(Map<String, Object> source) {
            this.source = source;
        }

        private long entryVisits() {
            return entryVisits;
        }

        private void resetEntryVisits() {
            entryVisits = 0;
        }

        @Override
        public Set<Entry<String, Object>> entrySet() {
            return new AbstractSet<>() {
                @Override
                public Iterator<Entry<String, Object>> iterator() {
                    Iterator<Entry<String, Object>> delegate = source.entrySet().iterator();
                    return new Iterator<>() {
                        @Override
                        public boolean hasNext() {
                            return delegate.hasNext();
                        }

                        @Override
                        public Entry<String, Object> next() {
                            entryVisits++;
                            return delegate.next();
                        }
                    };
                }

                @Override
                public int size() {
                    return source.size();
                }
            };
        }
    }

    private static final class ForbiddenIterationMap extends AbstractMap<String, Object> {

        @Override
        public Set<Entry<String, Object>> entrySet() {
            throw new AssertionError("legacy plaintext must not be read after target authentication succeeds");
        }
    }
}
