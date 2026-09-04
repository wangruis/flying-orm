package com.flying.orm.rdb.mapping;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableColumn;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.type.DatabaseType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EntityExplicitTypeCodecTest {

    @Test
    void usesOneExplicitTypeMappingForMetadataSchemaBindingAndDecoding() {
        DatabaseType accountNumberType = DatabaseType.of("VARCHAR(32)");
        AccountNumberCodec codec = new AccountNumberCodec();
        EntityTypeMappingRegistry typeMappings = EntityTypeMappingRegistry.builder()
                .register("account-number", AccountNumber.class, accountNumberType, codec)
                .build();

        EntitySchemaDescriptor<Account> descriptor = EntitySchemaDescriptor.builder(Account.class)
                .typeMappings(typeMappings)
                .build();

        assertSame(typeMappings, descriptor.typeMappings());
        assertSame(descriptor.form(), descriptor.metadata().toDynamicForm());
        assertEquals(accountNumberType, descriptor.metadata().field("accountNumber").databaseType());
        assertEquals("VARCHAR(32)", descriptor.form().field("account_number").dataType());
        assertEquals(accountNumberType, descriptor.table().column("account_number").databaseType());
        assertEquals("account-number", descriptor.table().column("account_number").codecId());
        assertEquals("A-17", descriptor.valueCodecs().write(new AccountNumber("A-17")));
        assertEquals(new AccountNumber("A-17"),
                     descriptor.valueCodecs().read("A-17", AccountNumber.class));
        assertFalse(descriptor.typeMappingsFingerprint().isBlank());
        assertFalse(descriptor.relationalFingerprint().isBlank());
    }

    @Test
    void rejectsASecondMappingForTheSameJavaType() {
        StringValueCodec codec = new StringValueCodec();

        assertThrows(IllegalArgumentException.class,
                     () -> EntityTypeMappingRegistry.builder()
                             .register("case-insensitive-text", String.class,
                                       DatabaseType.of("CITEXT"), codec));
    }

    @TableName("accounts")
    private record Account(
            @TableId(type = IdType.INPUT) Long id,
            @TableColumn(databaseTypeId = "account-number") AccountNumber accountNumber) {
    }

    private record AccountNumber(String value) {
    }

    private static final class AccountNumberCodec implements ValueCodec {

        @Override
        public boolean supports(Class<?> targetType) {
            return targetType == AccountNumber.class;
        }

        @Override
        public Object write(Object value) {
            return value == null ? null : ((AccountNumber) value).value();
        }

        @Override
        public Object read(Object value, Class<?> targetType) {
            return value == null ? null : new AccountNumber(value.toString());
        }
    }

    private static final class StringValueCodec implements ValueCodec {

        @Override
        public boolean supports(Class<?> targetType) {
            return targetType == String.class;
        }

        @Override
        public Object write(Object value) {
            return value;
        }

        @Override
        public Object read(Object value, Class<?> targetType) {
            return value == null ? null : value.toString();
        }
    }
}
