package com.flying.orm.rdb.form;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProtectedBatchPhysicalFormReuseTest {

    @Test
    void everyBatchPlannerReusesOnePhysicalFormOperation() throws IOException {
        String reactiveInserts = source("ReactiveFormBatchInsertOperations.java");
        String reactiveUpdates = source("ReactiveFormBatchUpdateOperations.java");
        String syncBatches = source("NativeSyncFormBatchOperations.java");
        String scopes = source("FormScopeSupport.java");

        assertContains(reactiveInserts,
                "renderer.protection().writeOperation( safeForm, physicalForm, scope, protectionLayout.contains())");
        assertContains(reactiveInserts, "protection.prepare(logical)");
        assertFalse(reactiveInserts.contains(
                "renderer.protection().prepareWrite(safeForm, logical, scope)"));

        assertContains(syncBatches,
                "renderer.protection().writeOperation( form, physicalForm, scope, protectionLayout.contains())");
        assertContains(syncBatches, "protection.prepare(logical)");
        assertFalse(syncBatches.contains(
                "renderer.protection().prepareWrite(form, logical, scope)"));

        assertContains(reactiveUpdates,
                "scopes.prepareBatchScope( safeForm, physicalForm, effectiveScope)");
        assertContains(reactiveUpdates,
                "scopes.prepareBatchUpdate( form, first.form(), update, batchScope, protection)");
        assertContains(syncBatches,
                "scopes.prepareBatchScope( form, physicalForm, scope)");
        assertContains(syncBatches,
                "scopes.prepareBatchUpdate( form, first.form(), requireUpdate(row), batchScope, protection)");

        assertContains(scopes,
                "PreparedBatchUpdate prepareBatchUpdate(DynamicForm form, DynamicForm physicalForm,");
        assertContains(scopes,
                "FormPreparedWrite write = protection.prepare(logicalValues)");
    }

    private static void assertContains(String source, String expected) {
        assertTrue(source.contains(compact(expected)), () -> "missing batch physical-form reuse: " + expected);
    }

    private static String source(String file) throws IOException {
        Path path = Path.of(System.getProperty("basedir"), "src", "main", "java", "com", "flying", "orm",
                            "rdb", "form", file);
        return compact(Files.readString(path));
    }

    private static String compact(String value) {
        return value.replaceAll("\\s+", "");
    }
}
