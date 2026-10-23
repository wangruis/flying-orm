package com.flying.orm.core.metadata;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 外键元数据只保存业务输入，规范名由唯一规则派生。 */
class ForeignKeyDefinitionTest {

    @Test
    void equalityIncludesReferenceNamespaceAndActions() {
        ForeignKeyDefinition plain = key(RelationIdentity.table("tenant"), ReferentialAction.NO_ACTION);
        ForeignKeyDefinition equal = key(RelationIdentity.table("tenant"), ReferentialAction.NO_ACTION);
        assertEquals(plain, equal);
        assertEquals(plain.hashCode(), equal.hashCode());
        assertNotEquals(plain, key(RelationIdentity.of(null, "auth", "tenant"), ReferentialAction.NO_ACTION));
        assertNotEquals(plain, key(RelationIdentity.table("tenant"), ReferentialAction.CASCADE));
    }

    private static ForeignKeyDefinition key(RelationIdentity reference, ReferentialAction onDelete) {
        return ForeignKeyDefinition.builder("fk_tenant").addColumn("tenant_id")
                .reference(reference).addReferenceColumn("id").onDelete(onDelete).build();
    }

    @Test
    void derivesNormalizedNamesAndFreezesColumns() {
        List<String> columns = new ArrayList<>(List.of(" Tenant_ID "));
        ForeignKeyDefinition.Builder builder = ForeignKeyDefinition.builder(" FK_Order_Tenant ")
                .reference(RelationIdentity.table(" Tenant ")).addReferenceColumn(" ID ");
        columns.forEach(builder::addColumn);
        ForeignKeyDefinition metadata = builder.build();

        columns.set(0, "changed");

        assertEquals("FK_Order_Tenant", metadata.name());
        assertEquals("fk_order_tenant", metadata.normalizedName());
        assertEquals(List.of("Tenant_ID"), metadata.columns());
        assertEquals(RelationIdentity.table("Tenant"), metadata.reference());
        assertEquals(List.of("ID"), metadata.referenceColumns());
        assertThrows(UnsupportedOperationException.class, () -> metadata.columns().add("other"));
    }
}
