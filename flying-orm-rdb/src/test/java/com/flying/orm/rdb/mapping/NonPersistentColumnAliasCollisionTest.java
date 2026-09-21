package com.flying.orm.rdb.mapping;

import com.flying.orm.core.annotation.TableField;
import com.flying.orm.rdb.internal.mapping.EntityMetadataResolver;
import com.flying.orm.rdb.internal.mapping.EntityPropertyResolver;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NonPersistentColumnAliasCollisionTest {

    @Test
    void excludedRecordComponentCannotOverwriteAnotherPropertysColumn() {
        EntityValues<RecordValue> values = EntityValues.createUncached(RecordValue.class);
        RecordValue entity = new RecordValue("stored", "temporary");

        assertAll(
                () -> assertEquals(Map.of("shadow", "stored"), values.readForInsert(entity)),
                () -> assertEquals(Map.of("shadow", "stored"), values.readForUpdate(entity)),
                () -> assertEquals(Map.of("shadow", "stored"), values.readForUpsert(entity)),
                () -> assertEquals(Map.of("shadow", "stored"),
                        values.repositoryUpsertValues(entity).insertValues()),
                () -> assertEquals(Map.of("shadow", "stored"),
                        values.repositoryUpsertValues(entity).updateValues()));
    }

    @Test
    void excludedRecordComponentKeepsItsDefaultWhenItsNameMatchesAColumnAlias() {
        RecordValue entity = RowMapper.of(RecordValue.class)
                .map(DynamicRow.copyOf(Map.of("shadow", "stored")));

        assertEquals(new RecordValue("stored", null), entity);
    }

    @Test
    void excludedBeanGetterCannotOverwriteAnotherPropertysColumn() {
        EntityValues<BeanValue> values = EntityValues.createUncached(BeanValue.class);
        BeanValue entity = new BeanValue();
        entity.name = "stored";
        entity.shadow = "temporary";

        assertAll(
                () -> assertEquals(Map.of("shadow", "stored"), values.readForInsert(entity)),
                () -> assertEquals(Map.of("shadow", "stored"), values.readForUpdate(entity)),
                () -> assertEquals(Map.of("shadow", "stored"), values.readForUpsert(entity)),
                () -> assertEquals(Map.of("shadow", "stored"),
                        values.repositoryUpsertValues(entity).insertValues()),
                () -> assertEquals(Map.of("shadow", "stored"),
                        values.repositoryUpsertValues(entity).updateValues()));
    }

    @Test
    void excludedBeanSetterCannotCaptureAnotherPropertysColumn() {
        BeanValue entity = RowMapper.of(BeanValue.class)
                .map(DynamicRow.copyOf(Map.of("shadow", "stored")));

        assertAll(
                () -> assertEquals("stored", entity.name),
                () -> assertNull(entity.shadow));
    }

    @Test
    void excludedDirectFieldCannotCaptureAnotherPropertysColumn() {
        DirectValue entity = new DirectValue();
        entity.name = "stored";
        entity.shadow = "temporary";
        EntityValues<DirectValue> values = EntityValues.createUncached(DirectValue.class);
        DirectValue mapped = RowMapper.of(DirectValue.class)
                .map(DynamicRow.copyOf(Map.of("shadow", "stored")));

        assertAll(
                () -> assertEquals(Map.of("shadow", "stored"), values.readForInsert(entity)),
                () -> assertEquals(Map.of("shadow", "stored"), values.readForUpdate(entity)),
                () -> assertEquals(Map.of("shadow", "stored"), values.readForUpsert(entity)),
                () -> assertEquals("stored", mapped.name),
                () -> assertNull(mapped.shadow));
    }

    @Test
    void excludedChildFieldCannotReplaceAnInheritedPersistentField() {
        ExcludedChildValue entity = new ExcludedChildValue();
        ((PersistentParentValue) entity).name = "stored";
        entity.name = "temporary";
        EntityValues<ExcludedChildValue> values = EntityValues.createUncached(ExcludedChildValue.class);
        ExcludedChildValue mapped = RowMapper.of(ExcludedChildValue.class)
                .map(DynamicRow.copyOf(Map.of("name", "stored")));

        assertAll(
                () -> assertEquals(Map.of("name", "stored"), values.readForInsert(entity)),
                () -> assertEquals(Map.of("name", "stored"), values.readForUpdate(entity)),
                () -> assertEquals("stored", ((PersistentParentValue) mapped).name),
                () -> assertNull(mapped.name));
    }

    @Test
    void excludedParentFieldCannotCaptureAPersistentChildField() {
        PersistentChildValue entity = new PersistentChildValue();
        ((ExcludedParentValue) entity).name = "temporary";
        entity.name = "stored";
        EntityValues<PersistentChildValue> values = EntityValues.createUncached(PersistentChildValue.class);
        PersistentChildValue mapped = RowMapper.of(PersistentChildValue.class)
                .map(DynamicRow.copyOf(Map.of("name", "stored")));

        assertAll(
                () -> assertEquals(Map.of("name", "stored"), values.readForInsert(entity)),
                () -> assertEquals(Map.of("name", "stored"), values.readForUpdate(entity)),
                () -> assertEquals("stored", mapped.name),
                () -> assertNull(((ExcludedParentValue) mapped).name));
    }

    @Test
    void excludedAccessorCannotResolveAnotherPropertysColumnAlias() {
        EntityMetadata<RecordValue> metadata = EntityMetadataResolver.createUncached(RecordValue.class);

        assertAll(
                () -> assertThrows(MappingException.class,
                        () -> EntityPropertyResolver.column(metadata, RecordValue::shadow)),
                () -> assertEquals("shadow", EntityPropertyResolver.column(metadata, RecordValue::name)),
                () -> assertEquals("name", metadata.field("shadow").name()));
    }

    @Test
    void acronymAccessorsKeepCanonicalJavaPropertyMatching() {
        AcronymValue mapped = RowMapper.of(AcronymValue.class)
                .map(DynamicRow.copyOf(Map.of("url", "stored")));
        AcronymValue source = new AcronymValue();
        source.url = "stored";

        assertAll(
                () -> assertEquals("validated:stored", mapped.url),
                () -> assertEquals(Map.of("url", "accessed:stored"),
                        EntityValues.createUncached(AcronymValue.class).readForInsert(source)));
    }

    @Test
    void explicitExcludedAcronymMemberCannotCaptureACanonicalPersistentProperty() {
        ExcludedAcronymValue source = new ExcludedAcronymValue();
        source.url = "stored";
        source.URL = "temporary";
        ExcludedAcronymValue mapped = RowMapper.of(ExcludedAcronymValue.class)
                .map(DynamicRow.copyOf(Map.of("url", "stored")));

        assertAll(
                () -> assertEquals(Map.of("url", "stored"),
                        EntityValues.createUncached(ExcludedAcronymValue.class).readForInsert(source)),
                () -> assertEquals("stored", mapped.url),
                () -> assertNull(mapped.URL),
                () -> assertThrows(MappingException.class,
                        () -> EntityPropertyResolver.column(
                                EntityMetadataResolver.createUncached(ExcludedAcronymValue.class),
                                ExcludedAcronymValue::getURL)));
    }

    private record RecordValue(
            @TableField("shadow") String name,
            @TableField(exist = false) String shadow) {
    }

    private static final class DirectValue {

        @TableField("shadow")
        private String name;
        @TableField(exist = false)
        private String shadow;
    }

    private static final class AcronymValue {

        private String url;

        public String getURL() {
            return "accessed:" + url;
        }

        public void setURL(String value) {
            url = "validated:" + value;
        }
    }

    private static final class ExcludedAcronymValue {

        private String url;
        private transient String URL;

        public String getURL() {
            return URL;
        }

        public void setURL(String value) {
            URL = value;
        }
    }

    private static class PersistentParentValue {

        private String name;
    }

    private static final class ExcludedChildValue extends PersistentParentValue {

        private transient String name;
    }

    private static class ExcludedParentValue {

        @TableField(exist = false)
        private String name;
    }

    private static final class PersistentChildValue extends ExcludedParentValue {

        private String name;
    }

    private static final class BeanValue {

        @TableField("shadow")
        private String name;
        private transient String shadow;

        public String getName() {
            return name;
        }

        public void setName(String value) {
            name = value;
        }

        public String getShadow() {
            return shadow;
        }

        public void setShadow(String value) {
            shadow = value;
        }
    }
}
