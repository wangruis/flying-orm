package com.flying.orm.rdb.mapping;

import com.flying.orm.core.annotation.TableCheck;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableForeignKey;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableIndex;
import com.flying.orm.core.annotation.TableIndexColumn;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.annotation.TablePrimaryKey;
import com.flying.orm.core.annotation.TableUnique;
import com.flying.orm.core.metadata.CheckPredicate;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.IndexKeyPart;
import com.flying.orm.core.metadata.ReferentialAction;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EntityCompositeConstraintCompilationTest {

    @Test
    void compilesCompositeConstraintsFromJavaPropertiesToPhysicalColumns() {
        RelationalTableDefinition table = EntitySchemaDescriptor.builder(OrderLine.class)
                                                                .build()
                                                                .table();

        assertEquals(List.of("order_no", "tenant_key"),
                     table.primaryKey().orElseThrow().columns());
        assertEquals(List.of("tenant_key", "order_no", "sku_code"),
                     table.uniqueConstraints().getFirst().columns());

        IndexDefinition index = table.indexes().getFirst();
        assertEquals("ix_order_lines_sku_tenant", index.name());
        assertEquals(List.of(IndexKeyPart.desc("sku_code"), IndexKeyPart.asc("tenant_key")),
                     index.keys());

        ForeignKeyDefinition foreignKey = table.foreignKeys().getFirst();
        assertEquals(List.of("tenant_key", "order_no"), foreignKey.columns());
        assertEquals(RelationIdentity.of(null, "sales", "order_headers"),
                     foreignKey.referencedTable());
        assertEquals(List.of("tenant_key", "order_no"), foreignKey.referenceColumns());
        assertEquals(ReferentialAction.RESTRICT, foreignKey.onUpdate());
        assertEquals(ReferentialAction.CASCADE, foreignKey.onDelete());

        assertEquals(CheckPredicate.compare(
                             "quantity_value",
                             CheckPredicate.ComparisonOperator.GREATER_THAN,
                             0),
                     table.checks().getFirst().predicate());
    }

    @Test
    void mergesRepeatableConstraintsFromEveryEntityTypeInInheritanceOrder() {
        EntitySchemaDescriptor<RegionalAccount> descriptor =
                EntitySchemaDescriptor.builder(RegionalAccount.class).build();
        RelationalTableDefinition table = descriptor.table();

        assertEquals(List.of("uk_accounts_external_code", "uk_accounts_region_code"),
                     table.uniqueConstraints().stream().map(constraint -> constraint.name()).toList());
        assertEquals(List.of("ix_accounts_external_code", "ix_accounts_region_code"),
                     table.indexes().stream().map(IndexDefinition::name).toList());
        assertEquals(List.of("fk_accounts_home_tenant", "fk_accounts_region_tenant"),
                     table.foreignKeys().stream().map(ForeignKeyDefinition::name).toList());
        assertEquals(List.of("ck_accounts_enabled", "ck_accounts_region_enabled"),
                     table.checks().stream().map(constraint -> constraint.name()).toList());
        assertTrue(descriptor.form().field("external_code").unique());
        assertTrue(descriptor.form().field("region_code").unique());
    }

    @TableName(value = "order_lines", schema = "sales")
    @TablePrimaryKey(name = "pk_order_lines", properties = {"orderId", "tenantId"})
    @TableUnique(
            id = "order-line-natural-key",
            name = "uk_order_lines_tenant_order_sku",
            properties = {"tenantId", "orderId", "sku"})
    @TableIndex(
            id = "order-line-sku-tenant",
            name = "ix_order_lines_sku_tenant",
            columns = {
                    @TableIndexColumn(property = "sku", direction = TableIndexColumn.Direction.DESC),
                    @TableIndexColumn(property = "tenantId")
            })
    @TableForeignKey(
            id = "order-line-header",
            name = "fk_order_lines_header",
            localProperties = {"tenantId", "orderId"},
            targetEntity = OrderHeader.class,
            targetProperties = {"tenantId", "orderId"},
            onUpdate = ReferentialAction.RESTRICT,
            onDelete = ReferentialAction.CASCADE)
    @TableCheck(
            id = "order-line-positive-quantity",
            name = "ck_order_lines_positive_quantity",
            property = "quantity",
            operator = TableCheck.Operator.GREATER_THAN,
            literalValues = "0")
    private static final class OrderLine {

        @TableId
        @TableField("tenant_key")
        private Long tenantId;

        @TableId
        @TableField("order_no")
        private Long orderId;

        @TableField("sku_code")
        private String sku;

        @TableField("quantity_value")
        private Integer quantity;
    }

    @TableName(value = "order_headers", schema = "sales")
    @TablePrimaryKey(name = "pk_order_headers", properties = {"tenantId", "orderId"})
    private static final class OrderHeader {

        @TableId
        @TableField("tenant_key")
        private Long tenantId;

        @TableId
        @TableField("order_no")
        private Long orderId;
    }

    @TableUnique(
            id = "account-external-code",
            name = "uk_accounts_external_code",
            properties = "externalCode")
    @TableIndex(
            id = "account-external-code-search",
            name = "ix_accounts_external_code",
            columns = @TableIndexColumn(property = "externalCode"))
    @TableForeignKey(
            id = "account-home-tenant",
            name = "fk_accounts_home_tenant",
            localProperties = "homeTenantId",
            targetEntity = AccountTenant.class,
            targetProperties = "id")
    @TableCheck(
            id = "account-enabled",
            name = "ck_accounts_enabled",
            property = "enabled",
            operator = TableCheck.Operator.EQUAL,
            literalValues = "true")
    private static class AccountBase {

        @TableId
        private Long id;

        private String externalCode;

        private Long homeTenantId;

        private Boolean enabled;
    }

    @TableName("regional_accounts")
    @TableUnique(
            id = "account-region-code",
            name = "uk_accounts_region_code",
            properties = "regionCode")
    @TableIndex(
            id = "account-region-code-search",
            name = "ix_accounts_region_code",
            columns = @TableIndexColumn(property = "regionCode"))
    @TableForeignKey(
            id = "account-region-tenant",
            name = "fk_accounts_region_tenant",
            localProperties = "regionTenantId",
            targetEntity = AccountTenant.class,
            targetProperties = "id")
    @TableCheck(
            id = "account-region-enabled",
            name = "ck_accounts_region_enabled",
            property = "regionEnabled",
            operator = TableCheck.Operator.EQUAL,
            literalValues = "true")
    private static final class RegionalAccount extends AccountBase {

        private String regionCode;

        private Long regionTenantId;

        private Boolean regionEnabled;
    }

    @TableName("account_tenants")
    private static final class AccountTenant {

        @TableId
        private Long id;
    }
}
