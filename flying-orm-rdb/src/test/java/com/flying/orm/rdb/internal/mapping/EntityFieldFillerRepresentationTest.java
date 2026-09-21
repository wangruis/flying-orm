package com.flying.orm.rdb.internal.mapping;

import com.flying.orm.core.annotation.EnumValue;
import com.flying.orm.core.annotation.FieldFill;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.rdb.mapping.EntityFieldFiller;
import com.flying.orm.rdb.mapping.EntityMetadata;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class EntityFieldFillerRepresentationTest {

    @TestFactory
    List<DynamicTest> fillerReceivesDomainValueBeforeStorageEncoding() {
        List<DynamicTest> tests = new ArrayList<>();
        for (EntityFieldFiller.Operation operation : EntityFieldFiller.Operation.values()) {
            tests.add(DynamicTest.dynamicTest(operation.toString(), () -> {
                EntityFieldFiller filler = (entity, field, actual, currentValue) -> {
                    assertSame(operation, actual);
                    assertSame(Status.ACTIVE, currentValue);
                    return currentValue;
                };

                assertEquals("A", values(filler, operation, new Entry(1L, Status.ACTIVE)).get("status"));
            }));
        }
        return tests;
    }

    @TestFactory
    List<DynamicTest> fillerResultIsEncodedExactlyOnce() {
        List<DynamicTest> tests = new ArrayList<>();
        for (EntityFieldFiller.Operation operation : EntityFieldFiller.Operation.values()) {
            tests.add(DynamicTest.dynamicTest(operation.toString(), () -> {
                EntityFieldFiller filler = (entity, field, actual, currentValue) -> {
                    assertSame(operation, actual);
                    assertNull(currentValue);
                    return Status.DISABLED;
                };

                assertEquals("D", values(filler, operation, new Entry(1L, null)).get("status"));
            }));
        }
        return tests;
    }

    @TestFactory
    List<DynamicTest> fillerMayReturnTheStoredParameterRepresentation() {
        List<DynamicTest> tests = new ArrayList<>();
        for (EntityFieldFiller.Operation operation : EntityFieldFiller.Operation.values()) {
            tests.add(DynamicTest.dynamicTest(operation.toString(), () -> {
                EntityFieldFiller filler = (entity, field, actual, currentValue) -> "D";

                assertEquals("D", values(filler, operation, new Entry(1L, Status.ACTIVE)).get("status"));
            }));
        }
        return tests;
    }

    @Test
    void stagedUpsertKeepsAStoredParameterReturnedByTheFiller() {
        EntityMetadata<Entry> metadata = EntityMetadataResolver.createUncached(Entry.class);
        EntityValues<Entry> values = EntityValues.createUncached(
                Entry.class, metadata, (entity, field, operation, currentValue) -> "D");

        RepositoryUpsertValues upsert = values.repositoryUpsertValues(new Entry(1L, Status.ACTIVE));

        assertEquals("D", upsert.get("status"));
        assertEquals("D", upsert.insertValues().get("status"));
        assertEquals("D", upsert.updateValues().get("status"));
    }

    @TestFactory
    List<DynamicTest> fieldsWithoutFillKeepTheirExistingRepresentation() {
        List<DynamicTest> tests = new ArrayList<>();
        for (Status status : Status.values()) {
            tests.add(DynamicTest.dynamicTest(status.toString(), () -> {
                EntityMetadata<PlainEntry> metadata = EntityMetadataResolver.createUncached(PlainEntry.class);
                EntityValues<PlainEntry> values = EntityValues.createUncached(
                        PlainEntry.class, metadata, EntityFieldFiller.none());

                assertEquals(status.code, values.readForInsert(new PlainEntry(1L, status)).get("status"));
            }));
        }
        return tests;
    }

    private static Map<String, Object> values(EntityFieldFiller filler,
                                              EntityFieldFiller.Operation operation,
                                              Entry entry) {
        EntityMetadata<Entry> metadata = EntityMetadataResolver.createUncached(Entry.class);
        EntityValues<Entry> values = EntityValues.createUncached(Entry.class, metadata, filler);
        return switch (operation) {
            case INSERT -> values.readForInsert(entry);
            case UPDATE -> values.readForUpdate(entry);
            case UPSERT -> values.readForUpsert(entry);
        };
    }

    enum Status {
        ACTIVE("A"), DISABLED("D");

        @EnumValue
        private final String code;

        Status(String code) {
            this.code = code;
        }
    }

    @TableName("filler_entries")
    record Entry(@TableId Long id,
                 @TableField(fill = FieldFill.INSERT_UPDATE) Status status) {
    }

    @TableName("plain_entries")
    record PlainEntry(@TableId Long id, Status status) {
    }

}
