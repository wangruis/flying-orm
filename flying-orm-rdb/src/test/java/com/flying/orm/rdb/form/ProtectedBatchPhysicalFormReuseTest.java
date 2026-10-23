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
        String batchRows = source("FormProtectedBatchRows.java");

        assertContains(reactiveUpdates, "FormProtectedBatchRows.updateRequest(");
        assertContains(syncBatches, "FormProtectedBatchRows.updateRequest(");
        assertContains(reactiveInserts, "FormProtectedBatchRows.insertRequest(");
        assertContains(syncBatches, "FormProtectedBatchRows.insertRequest(");
        assertFalse(reactiveUpdates.contains("newBatchUpdatePlan.Cursor("));
        assertFalse(syncBatches.contains("newBatchUpdatePlan.Cursor("));
        assertFalse(reactiveInserts.contains("newBatchInsertPlan.Cursor("));
        assertFalse(syncBatches.contains("newBatchInsertPlan.Cursor("));

        assertContains(batchRows,
                "renderer.protection().writeOperation( form, physicalForm, scope, protectionLayout)");
        assertContains(batchRows, "protection.prepare(logical)");
        assertFalse(reactiveInserts.contains(
                "renderer.protection().prepareWrite(safeForm, logical, scope)"));

        assertFalse(syncBatches.contains(
                "renderer.protection().prepareWrite(form, logical, scope)"));

        assertContains(batchRows, "scopes.prepareBatchScope(form, physicalForm, scope)");
        assertContains(batchRows, "scopes.prepareBatchUpdate( form, physicalForm, update, batchScope, protection)");

        assertContains(scopes,
                "PreparedBatchUpdate prepareBatchUpdate(DynamicForm form, DynamicForm physicalForm,");
        assertContains(scopes,
                "if (protection == null)");
        assertContains(batchRows,
                "FormPreparedWrite first = protection == null ? null : protection.prepare(firstValues)");
        assertContains(batchRows,
                "protection == null ? null : protection.prepare(logical)");
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
