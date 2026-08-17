package com.flying.orm.core.scope;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.TermCondition;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.time.LocalDateTime;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 守住 TenantScope、DataScope、FieldScope 和 TimeScope 合并时只能收紧、不能放宽的契约。 */
class DataScopeTest {

    @Test
    void tenantScopeBuildsStableCondition() {
        ConditionGroup condition = DataScope.tenant("tenant_id", "t1").condition().orElseThrow();

        assertEquals(1, condition.children().size());
    }

    @Test
    void dataScopesCanBeMerged() {
        DataScope first = DataScope.tenant("tenant_id", "t1");
        DataScope second = DataScope.where(ConditionGroup.and().where("org_id", "=", "o1").build());

        DataScope merged = first.and(second);

        assertFalse(merged.empty());
        assertEquals(2, merged.condition().orElseThrow().children().size());
    }

    @Test
    void allScopeDoesNotRemoveTenantBoundaryWhenMerged() {
        DataScope merged = DataScope.tenant("tenant_id", "t1").and(DataScope.all());

        assertFalse(merged.empty());
        assertEquals(1, merged.condition().orElseThrow().children().size());
    }

    @Test
    void presetScopesBuildParameterizedConditionTerms() {
        TermCondition orgAndChildren = (TermCondition) DataScope.orgAndChildren("org_id", "o1")
                                                               .condition()
                                                               .orElseThrow()
                                                               .children()
                                                               .get(0);
        TermCondition orgOnly = (TermCondition) DataScope.orgOnly("org_id", "o1")
                                                        .condition()
                                                        .orElseThrow()
                                                        .children()
                                                        .get(0);
        TermCondition self = (TermCondition) DataScope.self("creator_id", "u1")
                                                      .condition()
                                                      .orElseThrow()
                                                      .children()
                                                      .get(0);

        assertEquals("org_id", orgAndChildren.field());
        assertEquals(DataScope.ORG_AND_CHILDREN_OPERATOR, orgAndChildren.operator());
        assertEquals("o1", orgAndChildren.value());
        assertEquals("=", orgOnly.operator());
        assertEquals("=", self.operator());
    }

    @Test
    void fieldScopeUsesExplicitReadableFields() {
        FieldScope fields = FieldScope.readable("id", "name");

        assertFalse(fields.unrestrictedRead());
        assertTrue(fields.canRead("id"));
        assertTrue(fields.canRead("Name"));
        assertFalse(fields.canRead("password"));
        assertTrue(fields.unrestrictedWrite());
    }

    @Test
    void fieldScopeCanProtectWritableFields() {
        FieldScope fields = FieldScope.writable("name");

        assertTrue(fields.unrestrictedRead());
        assertTrue(fields.canWrite("NAME"));
        assertFalse(fields.canWrite("password"));
    }

    @Test
    void mergedFieldScopesIntersectReadAndWriteFields() {
        DataScope first = DataScope.none().withFields(FieldScope.readWrite("id", "name"));
        DataScope second = DataScope.none().withFields(new FieldScope(Set.of("name", "email"),
                                                                      Set.of("name", "password")));

        FieldScope merged = first.and(second).fields();

        assertTrue(merged.canRead("name"));
        assertFalse(merged.canRead("id"));
        assertTrue(merged.canWrite("name"));
        assertFalse(merged.canWrite("password"));
    }

    @Test
    void disjointFieldScopesDenyEveryFieldInsteadOfBecomingUnrestricted() {
        DataScope first = DataScope.none().withFields(FieldScope.readWrite("id"));
        DataScope second = DataScope.none().withFields(FieldScope.readWrite("name"));

        FieldScope merged = first.and(second).fields();

        assertFalse(merged.unrestrictedRead());
        assertFalse(merged.unrestrictedWrite());
        assertFalse(merged.canRead("id"));
        assertFalse(merged.canRead("name"));
        assertFalse(merged.canWrite("id"));
        assertFalse(merged.canWrite("name"));
    }

    /** 连续追加字段范围只能交集收窄，即使最后追加不受限范围也不能恢复权限。 */
    @Test
    void repeatedFieldScopesCannotWidenAccess() {
        FieldScope fields = DataScope.none()
                                     .withFields(FieldScope.readWrite("id"))
                                     .withFields(FieldScope.readWrite("id", "name"))
                                     .withFields(FieldScope.unrestricted())
                                     .fields();

        assertTrue(fields.canRead("id"));
        assertFalse(fields.canRead("name"));
        assertTrue(fields.canWrite("id"));
        assertFalse(fields.canWrite("name"));
    }

    @Test
    void tenantTimeAndFieldScopesComposeWithoutWideningAccess() {
        LocalDateTime start = LocalDateTime.of(2026, 7, 1, 0, 0);
        LocalDateTime end = LocalDateTime.of(2026, 8, 1, 0, 0);

        DataScope scope = DataScope.tenant("tenant_id", "t1")
                                   .and(DataScope.time(TimeScope.between("created_at", start, end)))
                                   .withFields(FieldScope.readable("id", "name", "created_at"));

        assertEquals(3, scope.condition().orElseThrow().children().size());
        assertEquals("t1", scope.tenantScope("tenant_id").orElseThrow().value());
        assertTrue(scope.fields().canRead("created_at"));
        assertFalse(scope.fields().canRead("secret"));
    }

    @Test
    void securityScopesNormalizeValuesAndRejectBlankBoundaries() {
        TenantScope tenant = TenantScope.of("tenant_id", "  t1  ");

        assertEquals("t1", tenant.value());
        assertThrows(IllegalArgumentException.class, () -> TenantScope.of("tenant_id", " "));
        assertThrows(IllegalArgumentException.class, () -> DataScope.orgOnly("org_id", " "));
        assertThrows(IllegalArgumentException.class, () -> TimeScope.from("created_at", " "));
    }

    /** 验证数组租户标识在构造和访问时都不能改写已发布的 Scope。 */
    @Test
    void tenantScopeSnapshotsScalarArrayValue() {
        byte[] tenantId = {7};
        TenantScope scope = TenantScope.of("tenant_id", tenantId);

        tenantId[0] = 8;
        byte[] exposed = (byte[]) scope.value();
        assertEquals(7, exposed[0]);

        exposed[0] = 9;
        TermCondition condition = (TermCondition) scope.toCondition().children().getFirst();
        assertEquals(7, ((byte[]) condition.value())[0]);
    }

    /** 嵌套数组租户身份也必须与源值和访问器隔离，并能安全保存自引用数组图。 */
    @Test
    void tenantScopeSnapshotsNestedArrayGraph() {
        byte[][] tenantId = {{7, 8}};
        TenantScope scope = TenantScope.of("tenant_id", tenantId);
        tenantId[0][0] = 9;

        byte[][] exposed = (byte[][]) scope.value();
        exposed[0][1] = 6;
        assertArrayEquals(new byte[]{7, 8}, ((byte[][]) scope.value())[0]);

        Object[] cycle = new Object[1];
        cycle[0] = cycle;
        Object[] cycleSnapshot = (Object[]) TenantScope.of("tenant_id", cycle).value();
        assertTrue(cycleSnapshot == cycleSnapshot[0]);
    }

    /** 二进制租户标识在构造和访问边界都必须冻结，不能改变后续租户条件。 */
    @Test
    void tenantScopeSnapshotsByteBufferValue() {
        ByteBuffer tenantId = ByteBuffer.wrap(new byte[]{7, 8});
        TenantScope scope = TenantScope.of("tenant_id", tenantId);

        tenantId.put(0, (byte) 9);
        ByteBuffer exposed = (ByteBuffer) scope.value();
        assertEquals(7, exposed.get(0));
        assertThrows(ReadOnlyBufferException.class, () -> exposed.put(0, (byte) 6));

        exposed.position(1);
        ByteBuffer conditionValue = (ByteBuffer) ((TermCondition) scope.toCondition()
                                                                        .children()
                                                                        .getFirst()).value();
        assertEquals(0, conditionValue.position());
        assertEquals(7, conditionValue.get(0));
    }

    /** 验证内容相同的数组租户约束能够合并，避免防御性副本破坏 Scope 组合。 */
    @Test
    void combinesTenantScopesWithEquivalentArrayValues() {
        DataScope scope = DataScope.tenant("tenant_id", new byte[]{7, 8})
                                   .and(DataScope.tenant("tenant_id", new byte[]{7, 8}));

        assertArrayEquals(new byte[]{7, 8}, (byte[]) scope.tenantScope("tenant_id").orElseThrow().value());
    }

    /** 验证内容不同的数组租户约束仍被稳定拒绝。 */
    @Test
    void rejectsTenantScopesWithDifferentArrayValues() {
        DataScope left = DataScope.tenant("tenant_id", new byte[]{7, 8});
        DataScope right = DataScope.tenant("tenant_id", new byte[]{7, 9});

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                                                      () -> left.and(right).tenantScope("tenant_id"));

        assertEquals("conflicting tenant scope values", error.getMessage());
    }

    /** 验证租户范围冲突不会在公开异常中回显未限长的字段输入。 */
    @Test
    void tenantScopeConflictDoesNotExposeConfiguredField() {
        String secretField = "tenant-secret-field-" + "x".repeat(5_000);
        DataScope left = DataScope.tenant(secretField, "tenant-a");
        DataScope right = DataScope.tenant(secretField, "tenant-b");

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> left.and(right).tenantScope(secretField));

        assertEquals("conflicting tenant scope values", error.getMessage());
    }
}
