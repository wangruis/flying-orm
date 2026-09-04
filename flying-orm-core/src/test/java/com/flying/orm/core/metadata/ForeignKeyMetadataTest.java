package com.flying.orm.core.metadata;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 外键元数据只保存业务输入，规范名由唯一规则派生。 */
class ForeignKeyMetadataTest {

    @Test
    void derivesNormalizedNamesAndFreezesColumns() {
        List<String> columns = new ArrayList<>(List.of(" Tenant_ID "));
        ForeignKeyMetadata metadata = new ForeignKeyMetadata(
                " FK_Order_Tenant ", columns, " Tenant ", List.of(" ID "));

        columns.set(0, "changed");

        assertEquals("FK_Order_Tenant", metadata.name());
        assertEquals("fk_order_tenant", metadata.normalizedName());
        assertEquals(List.of("Tenant_ID"), metadata.columns());
        assertEquals("Tenant", metadata.referenceTable());
        assertEquals("tenant", metadata.normalizedReferenceTable());
        assertEquals(List.of("ID"), metadata.referenceColumns());
        assertThrows(UnsupportedOperationException.class, () -> metadata.columns().add("other"));
    }
}
