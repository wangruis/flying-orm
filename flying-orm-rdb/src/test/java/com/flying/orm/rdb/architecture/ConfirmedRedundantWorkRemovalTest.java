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
    void writePlanningMergesScopeOnceAndGovernanceReusesIt() throws IOException {
        String planner = compact(source("form", "FormOperationPlanner.java"));
        String guard = compact(source("form", "FormScopeGuard.java"));

        assertEquals(1, occurrences(planner, "scopes.effectiveScope(safeSpec.scope())"));
        assertEquals(3, occurrences(planner, "scopes.writeScope(safeSpec.where(),safeSpec.scope())"));
        assertEquals(4, occurrences(planner, "FieldUseGuard.write(plan,renderer,safeSpec,plan.scope()"));
        assertTrue(guard.contains("requireBusinessWhere(where);returneffectiveScope(scope);"));
        assertFalse(guard.contains("ConditionGroupbusinessWhere=requireBusinessWhere(where);DataScopeeffectiveScope=effectiveScope(scope);"));
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
        assertTrue(source.contains("protection.protectedWrite(values,request,null,ProtectedWriteWork.Kind.INSERT,protection.insertOwner(write,request))"));
        assertTrue(source.contains("protection.protectedWrite(values,request,ownerQuery,ProtectedWriteWork.Kind.UPDATE,Map.of())"));
    }

    @Test
    void joinPlanningDerivesOnePhysicalFormPerSource() throws IOException {
        String source = compact(source("form", "JoinQueryPlanner.java"));

        assertTrue(source.contains("DynamicFormphysicalForm=renderer.protection().physicalForm(source.form())"));
        assertEquals(2, occurrences(source,
                "renderer.protection().prepareQuery(source.form(),physicalForm,"));
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
