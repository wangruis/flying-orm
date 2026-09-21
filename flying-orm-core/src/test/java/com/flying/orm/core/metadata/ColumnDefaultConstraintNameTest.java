package com.flying.orm.core.metadata;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ColumnDefaultConstraintNameTest {

    @Test
    void namesAreOptionalAndDoNotChangeDefaultValueSemantics() {
        ColumnDefinition unnamed = ColumnDefinition.builder("recorded_at", "TIMESTAMP")
                .defaultValue(ColumnDefault.currentTimestamp()).build();
        ColumnDefinition named = ColumnDefinition.builder("recorded_at", "TIMESTAMP")
                .defaultValue(ColumnDefault.currentTimestamp())
                .defaultConstraintName(" DF__events__recorded__6E01572D ").build();

        assertNull(unnamed.defaultConstraintName());
        assertEquals("DF__events__recorded__6E01572D", named.defaultConstraintName());
        assertEquals(unnamed.defaultValue(), named.defaultValue());
    }

    @Test
    void aSequenceBackedDefaultAlsoRetainsItsPhysicalConstraintName() {
        ColumnDefinition column = ColumnDefinition.builder("id", "BIGINT")
                .generation(ValueGeneration.sequence("event_seq"))
                .defaultConstraintName("DF_events_id").build();

        assertEquals(ColumnDefault.none(), column.defaultValue());
        assertEquals(ValueGeneration.sequence("event_seq"), column.generation());
        assertEquals("DF_events_id", column.defaultConstraintName());
    }

    @Test
    void namesAreValidatedOnceAsUnqualifiedIdentifiersAtConstruction() {
        for (String invalid : List.of("", " ", "dbo.df_events", "df;drop", "[df_events]", "df--events")) {
            assertThrows(IllegalArgumentException.class, () -> ColumnDefinition.builder("id", "BIGINT")
                    .defaultConstraintName(invalid).build());
        }
    }

    @Test
    void namesChangeFingerprintsWithoutChangingTheUnnamedEncoding() {
        String unnamed = RelationalMetadataFingerprint.of(table(null));

        assertEquals("c96d146e9d9c8ad011dcee0e61d40beab03b35d64cc5aa88a4410f8ae3593d11", unnamed);
        assertNotEquals(unnamed, RelationalMetadataFingerprint.of(table("DF_accounts_email_1")));
        assertNotEquals(RelationalMetadataFingerprint.of(table("DF_accounts_email_1")),
                RelationalMetadataFingerprint.of(table("DF_accounts_email_2")));
    }

    private static RelationalTableDefinition table(String defaultConstraintName) {
        return RelationalTableDefinition.builder(RelationIdentity.table("accounts"))
                .addColumn(ColumnDefinition.builder("email", "VARCHAR")
                        .defaultConstraintName(defaultConstraintName).build())
                .addUnique(UniqueConstraintDefinition.of("uq_email", "email")).build();
    }
}
