package com.flying.orm.core.scope;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ScopeIdentityReuseTest {

    @Test
    void tenantComparisonUsesOneRuleForTextAndArrayValues() {
        TenantScope text = TenantScope.of("tenant_id", new char[]{'a', 'b'});
        assertTrue(text.matches("ab"));
        assertTrue(text.matches(new StringBuilder("ab")));
        assertFalse(text.matches(" ab"));
        assertFalse(text.matches(null));
        TenantScope bytes = TenantScope.of("tenant_id", new byte[]{1, 2});
        assertTrue(bytes.matches(new byte[]{1, 2}));
        assertFalse(bytes.matches(new byte[]{1, 3}));
        assertTrue(DataScope.tenant("tenant_id", new char[]{'a', 'b'})
                .and(DataScope.tenant("tenant_id", "ab")).tenantScope("tenant_id").isPresent());
    }

    @Test
    void neutralDataScopeReusesTheOtherImmutableScope() {
        DataScope tenant = DataScope.tenant("tenant_id", 7L)
                .withFields(FieldScope.readWrite("id", "name"));
        DataScope empty = DataScope.none().withFields(new FieldScope(Set.of(), Set.of()));

        assertSame(DataScope.none(), DataScope.none().and(DataScope.none()));
        assertSame(tenant, DataScope.none().and(tenant));
        assertSame(tenant, tenant.and(DataScope.none()));
        assertSame(tenant, empty.and(tenant));
        assertSame(tenant, tenant.and(empty));
        assertTrue(tenant.tenantScope("tenant_id").isPresent());
        assertFalse(tenant.fields().canRead("secret"));
        assertThrows(NullPointerException.class, () -> DataScope.none().and(null));
    }

    @Test
    void unrestrictedFieldScopeIsSharedAndNeutralInAnIntersection() {
        FieldScope restricted = FieldScope.readWrite("id", "name");
        FieldScope empty = new FieldScope(Set.of(), Set.of());

        assertSame(FieldScope.unrestricted(), FieldScope.unrestricted());
        assertSame(restricted, FieldScope.intersect(FieldScope.unrestricted(), restricted));
        assertSame(restricted, FieldScope.intersect(restricted, empty));
        assertSame(restricted, FieldScope.intersect(restricted, restricted));
        assertThrows(NullPointerException.class, () -> FieldScope.intersect(null, restricted));
        assertThrows(NullPointerException.class, () -> FieldScope.intersect(restricted, null));
    }

    @Test
    void disjointPermissionsRemainDenyAllNotUnrestricted() {
        FieldScope denied = FieldScope.intersect(FieldScope.readWrite("id"), FieldScope.readWrite("name"));
        DataScope scope = DataScope.none().withFields(denied);

        assertFalse(scope.empty());
        assertFalse(denied.unrestrictedRead());
        assertFalse(denied.unrestrictedWrite());
        assertFalse(scope.and(DataScope.none()).fields().canRead("id"));
        assertFalse(DataScope.none().and(scope).fields().canWrite("name"));
        assertFalse(FieldScope.intersect(denied, FieldScope.unrestricted()).canRead("id"));
    }

    @Test
    void twoNonemptyScopesStillRetainBothRowsAndTenantDeclarations() {
        DataScope left = DataScope.tenant("tenant_id", 7L).withFields(FieldScope.readable("id"));
        DataScope right = DataScope.tenant("tenant_id", 8L).withFields(FieldScope.writable("name"));
        DataScope merged = left.and(right);

        assertNotSame(left, merged);
        assertNotSame(right, merged);
        assertEquals(2, merged.condition().orElseThrow().children().size());
        assertThrows(IllegalArgumentException.class, () -> merged.tenantScope("tenant_id"));
        assertTrue(merged.fields().canRead("id"));
        assertFalse(merged.fields().canRead("name"));
        assertTrue(merged.fields().canWrite("name"));
        assertFalse(merged.fields().canWrite("id"));
    }
}
