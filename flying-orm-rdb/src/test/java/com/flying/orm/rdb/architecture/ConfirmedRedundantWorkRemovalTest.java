package com.flying.orm.rdb.architecture;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConfirmedRedundantWorkRemovalTest {

    @Test
    void scopePlanningUsesOneStateOwnerWithoutCopyingReadContexts() throws IOException {
        String scopes = compact(source("form", "FormScopeSupport.java"));
        assertFalse(scopes.contains("FormScopeGuard"), "Scope must not pass through a second state owner");
        assertTrue(scopes.contains("newGovernedRead(buildScopedRead(spec.form(),business,effective),business)"));
        assertFalse(scopes.contains("renderBatchUpdate("));
        assertFalse(scopes.contains("parameterCompiler("));
        assertFalse(scopes.contains("batchUpdateWhere("));
        assertTrue(compact(source("form", "SyncFormClient.java"))
                .contains("newNativeSyncFormBatchOperations(this.batchExecutor,configuration.renderer(),operations.scopes,"));
        assertFalse(compact(source("form", "NativeSyncFormBatchOperations.java")).contains("newFormScopeSupport("));
    }

    @Test
    void writePlanningMergesScopeOnceAndGovernanceReusesIt() throws IOException {
        String planner = compact(source("form", "FormOperationPlanner.java"));
        String guard = compact(source("form", "FormScopeSupport.java"));

        assertEquals(3, occurrences(planner, "scopes.effectiveScope(safeSpec.scope())"));
        assertFalse(planner.contains("scopes.writeScope("));
        assertEquals(1, occurrences(planner, "FieldUseGuard.write(plan,renderer,spec,plan.scope()"));
        assertFalse(guard.contains("requireBusinessWhere("));
    }

    @Test
    void batchChunkTransfersItsPrivateListWithoutCopyingMembers() throws IOException {
        String chunks = compact(source("reactive", "R2dbcBatchWriterChunks.java"));

        assertFalse(chunks.contains("List.copyOf(rows)"));
        assertTrue(chunks.contains("Collections.unmodifiableList(rows)"));
    }

    @Test
    void singleWritePlanningReusesOneProtectionOperation() throws IOException {
        String source = compact(source("form", "FormOperationPlanner.java"));

        assertTrue(occurrences(source, "FormProtectionSqlSupport.WriteOperationprotection=") >= 2);
        assertFalse(source.contains("renderer.protection().protectedWrite("));
        assertTrue(source.contains("protection.prepare(values)"));
        assertTrue(source.contains("protection.protectedWrite(write,request,null,ProtectedWriteWork.Kind.INSERT,protection.insertOwner(insert,insert.firstParameters()))"));
        assertTrue(source.contains("protection.protectedWrite(write,request,ownerQuery,ProtectedWriteWork.Kind.UPDATE,Map.of())"));
    }

    @Test
    void joinPlanningDerivesOnePhysicalFormPerSource() throws IOException {
        String source = compact(source("form", "JoinQueryPlanner.java"));

        assertTrue(source.contains("DynamicFormphysicalForm=renderer.protection().physicalForm(source.form())"));
        assertEquals(1, occurrences(source,
                "renderer.protection().prepareQuery(source.form(),physicalForm,"));
        assertTrue(source.contains("key.source().form(),physicalForms.get(key.source()),key.condition()"));
    }

    @Test
    void reactiveSequenceAndProtectedBatchDoNotCollectDiscardedLists() throws IOException {
        String sequence = compact(source("reactive", "R2dbcSequenceExecutor.java"));
        String sideIndex = compact(source("reactive", "R2dbcProtectedBatchSideIndex.java"));
        String chunks = compact(source("reactive", "R2dbcBatchWriterChunks.java"));

        assertFalse(sequence.contains(".collectList().map(ignored->"));
        assertTrue(sequence.contains(".then(Mono.fromSupplier("));
        assertTrue(sideIndex.contains("Mono<Void>executeOwnerRestrictedUpdates("));
        assertTrue(sideIndex.contains(".then();"));
        assertFalse(chunks.contains("sideIndex.executeOwnerRestrictedUpdates(connection,request,window,prepared,sql,facts::completeRow,facts).then()"));
    }

    private static int occurrences(String value, String target) {
        int count = 0;
        for (int offset = 0; (offset = value.indexOf(target, offset)) >= 0; offset += target.length()) {
            count++;
        }
        return count;
    }

    private static String source(String packageName, String file) throws IOException {
        return Files.readString(Path.of(System.getProperty("basedir"), "src", "main", "java",
                "com", "flying", "orm", "rdb", packageName, file));
    }

    private static String compact(String value) {
        return value.replaceAll("\\s+", "");
    }
}
