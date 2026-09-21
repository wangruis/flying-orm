package com.flying.orm.rdb.mapping;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.internal.mapping.RepositoryUpsertValues;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NonPersistentEntityPropertyContractTest {

    @Test
    void excludesNonPersistentBeanPropertyFromReadWriteBatchAndSchemaModels() {
        EntitySchemaDescriptor<BeanAccount> descriptor = EntitySchemaDescriptor.builder(BeanAccount.class).build();
        assertModelExcludesDisplayName(descriptor);

        BeanAccount mapped = MappingPlan.createUncached(
                        BeanAccount.class, descriptor.metadata(), descriptor.valueCodecs())
                .map(DynamicRow.copyOf(row()));
        assertEquals(7L, mapped.id);
        assertEquals("stored", mapped.accountName);
        assertNull(mapped.displayName);

        BeanAccount entity = new BeanAccount(7L, "stored", "computed");
        assertWriteLayoutsExcludeDisplayName(
                EntityValues.createUncached(BeanAccount.class, descriptor.metadata()), entity);
    }

    @Test
    void excludesNonPersistentRecordComponentFromReadWriteBatchAndSchemaModels() {
        EntitySchemaDescriptor<RecordAccount> descriptor = EntitySchemaDescriptor.builder(RecordAccount.class).build();
        assertModelExcludesDisplayName(descriptor);

        RecordAccount mapped = MappingPlan.createUncached(
                        RecordAccount.class, descriptor.metadata(), descriptor.valueCodecs())
                .map(DynamicRow.copyOf(row()));
        assertEquals(new RecordAccount(7L, "stored", null), mapped);

        RecordAccount entity = new RecordAccount(7L, "stored", "computed");
        assertWriteLayoutsExcludeDisplayName(
                EntityValues.createUncached(RecordAccount.class, descriptor.metadata()), entity);
    }

    private static void assertModelExcludesDisplayName(EntitySchemaDescriptor<?> descriptor) {
        assertTrue(descriptor.metadata().findField("displayName").isEmpty());
        assertEquals(List.of("id", "account_name"),
                     descriptor.form().fields().stream().map(DynamicField::name).toList());
        assertEquals(List.of("id", "account_name"),
                     descriptor.table().columns().stream().map(ColumnDefinition::name).toList());
    }

    private static <T> void assertWriteLayoutsExcludeDisplayName(EntityValues<T> values, T entity) {
        assertEquals(List.of("id", "account_name"), List.copyOf(values.readForInsert(entity).keySet()));
        assertEquals(List.of("account_name"), List.copyOf(values.readForUpdate(entity).keySet()));
        assertEquals(List.of("id", "account_name"), List.copyOf(values.readForUpsert(entity).keySet()));

        RepositoryUpsertValues batchUpsert = values.repositoryUpsertValues(entity);
        assertEquals(List.of("id", "account_name"), List.copyOf(batchUpsert.keySet()));
        assertEquals(List.of("id", "account_name"), List.copyOf(batchUpsert.insertValues().keySet()));
        assertEquals(List.of("account_name"), List.copyOf(batchUpsert.updateValues().keySet()));
    }

    private static Map<String, Object> row() {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", 7L);
        row.put("account_name", "stored");
        row.put("display_name", "must be ignored");
        return row;
    }

    @TableName("bean_accounts")
    private static final class BeanAccount {

        @TableId(type = IdType.INPUT)
        private Long id;

        @TableField("account_name")
        private String accountName;

        @TableField(exist = false)
        private String displayName;

        private BeanAccount() {
        }

        private BeanAccount(Long id, String accountName, String displayName) {
            this.id = id;
            this.accountName = accountName;
            this.displayName = displayName;
        }
    }

    @TableName("record_accounts")
    private record RecordAccount(
            @TableId(type = IdType.INPUT) Long id,
            @TableField("account_name") String accountName,
            @TableField(exist = false) String displayName) {
    }
}
