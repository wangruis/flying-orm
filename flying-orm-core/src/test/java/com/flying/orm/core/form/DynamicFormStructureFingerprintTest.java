package com.flying.orm.core.form;

import com.flying.orm.core.metadata.RelationIdentity;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/** 动态表单指纹必须区分缺失定义与内容恰好类似内部标记的合法文本。 */
class DynamicFormStructureFingerprintTest {

    @Test
    void temporaryFormsDoNotComputeFingerprintsUntilRequested() throws ReflectiveOperationException {
        DynamicForm form = DynamicForm.builder("temporary", "audit_rows")
                                      .addField(DynamicField.of("id", "BIGINT"))
                                      .build();
        Field cache = DynamicForm.class.getDeclaredField("structureFingerprint");
        cache.setAccessible(true);

        assertNull(cache.get(form));
        String fingerprint = form.structureFingerprint();

        assertSame(fingerprint, cache.get(form));
        assertSame(fingerprint, form.structureFingerprint());
    }

    @Test
    void distinguishesAbsentLogicDeleteFromAFieldNamedLikeTheOldNullMarker() {
        DynamicField markerField = DynamicField.of("<null>", "BOOLEAN");
        DynamicForm plain = DynamicForm.builder("plain", "audit_rows")
                                       .addField(markerField)
                                       .build();
        DynamicForm logical = DynamicForm.builder("logical", "audit_rows")
                                         .addField(markerField)
                                         .logicDelete("<null>", false, true)
                                         .build();

        assertNotEquals(plain.structureFingerprint(), logical.structureFingerprint());
    }

    @Test
    void distinguishesAutoIndexIdentityWhenNamesContainTheOldSeparator() {
        DynamicForm separatorInTable = DynamicForm.builder("first", "a\0b")
                                                   .addField(DynamicField.of("c", "TEXT").withUnique(true))
                                                   .build();
        DynamicForm separatorInColumn = DynamicForm.builder("second", "a")
                                                    .addField(DynamicField.of("b\0c", "TEXT").withUnique(true))
                                                    .build();

        assertNotEquals(separatorInTable.toTableMetadata().indexes().getFirst().name(),
                        separatorInColumn.toTableMetadata().indexes().getFirst().name());
    }

    @Test
    void distinguishesPhysicalColumnNamesThatDifferOnlyByCase() {
        DynamicForm upperCaseColumn = DynamicForm.builder("upper", "audit_rows")
                                                 .addField(DynamicField.of("CustomerId", "BIGINT"))
                                                 .build();
        DynamicForm lowerCaseColumn = DynamicForm.builder("lower", "audit_rows")
                                                 .addField(DynamicField.of("customerid", "BIGINT"))
                                                 .build();

        assertNotEquals(upperCaseColumn.structureFingerprint(), lowerCaseColumn.structureFingerprint());
    }

    @Test
    void distinguishesLegacyQualificationFromSegmentedLiteralNames() {
        DynamicForm legacy = DynamicForm.builder("legacy", "sales.orders")
                .addField(DynamicField.of("id", "BIGINT"))
                .build();
        DynamicForm segmentedLiteral = DynamicForm.relationalBuilder(
                        "segmented", RelationIdentity.table("sales.orders"))
                .addField(DynamicField.of("id", "BIGINT"))
                .build();

        assertNotEquals(legacy.structureFingerprint(), segmentedLiteral.structureFingerprint());
    }

    @Test
    void distinguishesEqualTableNamesInDifferentSchemas() {
        DynamicForm sales = DynamicForm.relationalBuilder(
                        "sales", RelationIdentity.of(null, "sales", "orders"))
                .addField(DynamicField.of("id", "BIGINT"))
                .build();
        DynamicForm archive = DynamicForm.relationalBuilder(
                        "archive", RelationIdentity.of(null, "archive", "orders"))
                .addField(DynamicField.of("id", "BIGINT"))
                .build();

        assertNotEquals(sales.structureFingerprint(), archive.structureFingerprint());
    }
}
