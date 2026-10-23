package com.flying.orm.rdb.schema;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RelationalObjectNameGeneratorTest {

    @Test
    void keepsShortNamesAndDeterministicallyShortensGeneratedNames() {
        RelationalObjectNameGenerator names = new RelationalObjectNameGenerator(30);

        assertEquals("idx_orders_customer_id",
                     names.generate(RelationalObjectNameGenerator.Kind.INDEX,
                                    "orders", "customer", "id"));
        String first = names.generate(RelationalObjectNameGenerator.Kind.FOREIGN_KEY,
                                      "very_long_order_history", "customer_identifier");
        String second = names.generate(RelationalObjectNameGenerator.Kind.FOREIGN_KEY,
                                       "very_long_order_history", "customer_identifier");

        assertEquals(first, second);
        assertEquals(30, first.length());
        assertTrue(first.matches(".*_[0-9a-f]{12}"));
    }

    @Test
    void neverSilentlyRenamesTablesOrColumns() {
        RelationalObjectNameGenerator names = new RelationalObjectNameGenerator(8);

        assertThrows(IllegalArgumentException.class, () -> names.table("accounts_table"));
        assertThrows(IllegalArgumentException.class, () -> names.column("account_identifier"));
    }

    @Test
    void unknownDatabaseLimitDoesNotImposeAnOrmLimit() {
        RelationalObjectNameGenerator names = new RelationalObjectNameGenerator(0);
        String table = "accounts".repeat(40);
        assertEquals(table, names.table(table));
        assertEquals("idx_" + table + "_id",
                names.generate(RelationalObjectNameGenerator.Kind.INDEX, table, "id"));
        String qualified = names.generate(RelationalObjectNameGenerator.Kind.INDEX, "app.accounts", "id");
        assertTrue(qualified.startsWith("idx_app_accounts_id_"));
        assertTrue(qualified.matches(".*_[0-9a-f]{12}"));
    }
}
