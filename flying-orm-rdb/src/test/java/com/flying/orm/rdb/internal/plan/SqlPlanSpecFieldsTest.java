package com.flying.orm.rdb.internal.plan;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.metadata.RelationIdentity;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SqlPlanSpecFieldsTest {
    @Test
    void keyRetainsTextNormalizationAndRequiredTextFailures() {
        SqlPlanSpec key = new SqlPlanSpec(" PostgreSQL ", SqlBindMarkerStyle.CANONICAL,
                " fingerprint ", RelationIdentity.table(" Users "), " UPDATE ", List.of(), "", "", "", "");
        assertEquals("postgresql", key.dialect());
        assertEquals("fingerprint", key.formFingerprint());
        assertEquals(RelationIdentity.table("Users"), key.table());
        assertEquals("update", key.operation());
    }

    @Test
    void shapesAllowEmptyButRejectNullWithTheOriginalMessage() {
        String[] names = {"condition", "group", "sort", "page"};
        for (int index = 0; index < names.length; index++) {
            String[] shapes = {"", "", "", ""};
            shapes[index] = null;
            NullPointerException failure = assertThrows(NullPointerException.class,
                    () -> new SqlPlanSpec("postgresql", SqlBindMarkerStyle.CANONICAL,
                            "fingerprint", RelationIdentity.table("users"), "update", List.of(),
                            shapes[0], shapes[1], shapes[2], shapes[3]));
            assertEquals("sql plan " + names[index] + " shape must not be null", failure.getMessage());
        }
    }

    @Test
    void keyFreezesFieldOrderAndRetainsIdentifierSpelling() {
        List<String> fields = new ArrayList<>(List.of("ID", "name", " spaced "));
        SqlPlanSpec key = key(fields);
        fields.set(0, "changed");
        fields.clear();
        assertEquals(List.of("ID", "name", " spaced "), key.fields());
        assertEquals(key, key(List.of("ID", "name", " spaced ")));
        assertNotEquals(key, key(List.of("name", "ID", " spaced ")));
        assertThrows(UnsupportedOperationException.class, () -> key.fields().set(0, "changed"));
        assertTrue(key(List.of()).fields().isEmpty());
    }

    @Test
    void keyRejectsNullAndBlankFieldsWithoutChangingValidationOrder() {
        assertThrows(NullPointerException.class, () -> key(null));
        assertThrows(NullPointerException.class, () -> key(Arrays.asList("", null)));
        for (String blank : List.of("", " ", "\t\n", "\u2003")) {
            IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                    () -> key(List.of("id", blank)));
            assertEquals("sql plan fields must not contain blank values", failure.getMessage());
        }
    }

    private static SqlPlanSpec key(List<String> fields) {
        return new SqlPlanSpec("postgresql", SqlBindMarkerStyle.CANONICAL,
                "fingerprint", RelationIdentity.table("users"), "insert", fields, "none", "", "", "");
    }
}
