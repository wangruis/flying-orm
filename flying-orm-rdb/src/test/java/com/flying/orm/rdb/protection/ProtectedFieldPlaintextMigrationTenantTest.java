package com.flying.orm.rdb.protection;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.TenantStrategy;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.scope.DataScope;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProtectedFieldPlaintextMigrationTenantTest {

    @Test
    void tenantPlaintextMigrationSkipsAnAlreadyAuthenticatedTarget() {
        DynamicForm form = form();
        DataScope scope = DataScope.tenant("tenant_id", 7L);
        ValueCodecRegistry codecs = ValueCodecRegistry.standard();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", new byte[32]);
             ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(keys)) {
            ProtectedFieldReprotection migration = ProtectedFieldReprotection.create(keys);
            Map<String, Object> pending = migration.valuesNeedingPlaintextMigration(form,
                    Map.of("secret", "trusted legacy"), Map.of(), scope, codecs);
            assertEquals(Map.of("secret", "trusted legacy"), pending);
            Map<String, Object> target = runtime.prepareWrite(form, pending, scope, codecs).values();

            assertTrue(migration.valuesNeedingPlaintextMigration(form,
                    Map.of("secret", "trusted legacy"), target, scope, codecs).isEmpty());
        }
    }

    @Test
    void tenantPlaintextMigrationDoesNotSkipCiphertextFromAnotherTenant() {
        DynamicForm form = form();
        ValueCodecRegistry codecs = ValueCodecRegistry.standard();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", new byte[32]);
             ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(keys)) {
            Map<String, Object> target = runtime.prepareWrite(form, Map.of("secret", "tenant seven"),
                    DataScope.tenant("tenant_id", 7L), codecs).values();

            assertThrows(ProtectedFieldException.class,
                    () -> ProtectedFieldReprotection.create(keys).valuesNeedingPlaintextMigration(form, Map.of(), target,
                            DataScope.tenant("tenant_id", 8L), codecs));
        }
    }

    private static DynamicForm form() {
        return DynamicForm.builder("tenant_migration", "tenant_migration")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("tenant_id", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR"))
                .tenant("tenant_id", TenantStrategy.AUTO)
                .encrypted("secret", EncryptedFieldDefinition.builder().build())
                .build();
    }
}
