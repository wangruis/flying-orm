package com.flying.orm.rdb.mapping;

import com.flying.orm.core.annotation.TableField;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MappingPlanNameResolutionTest {

    @Test
    void resolvesQualifiedQuotedSnakeCaseLabelsToJavaMembers() {
        UserName value = MappingPlan.of(UserName.class)
                                    .map(DynamicRow.copyOf(Map.of("account.\"user_name\"", "wangr")));

        assertEquals("wangr", value.userName());
    }

    @Test
    void rejectsLabelsThatCollapseToTheSameEntityMember() {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("user_name", "first");
        row.put("user-name", "second");

        assertThrows(MappingException.class,
                     () -> MappingPlan.of(UserName.class).map(DynamicRow.copyOf(row)));
    }

    @Test
    void suppliesJavaDefaultsForNonPersistentRecordComponents() {
        UserProjection value = MappingPlan.of(UserProjection.class)
                                          .map(DynamicRow.copyOf(Map.of("user_name", "wangr")));

        assertEquals(new UserProjection("wangr", null, 0), value);
    }

    @Test
    void resolvesExplicitRecordColumnByJavaPropertyLabel() {
        AliasedRecord value = RowMapper.of(AliasedRecord.class)
                .map(DynamicRow.copyOf(Map.of("name", "stored")));

        assertEquals(new AliasedRecord("stored"), value);
    }

    @Test
    void resolvesExplicitRecordColumnByPhysicalLabel() {
        AliasedRecord value = RowMapper.of(AliasedRecord.class)
                .map(DynamicRow.copyOf(Map.of("stored_name", "stored")));

        assertEquals(new AliasedRecord("stored"), value);
    }

    @Test
    void resolvesExplicitAliasToTheRecordJavaProperty() {
        AliasedRecord value = RowMapper.of(AliasedRecord.class)
                .withAliases(Map.of("selected_name", "name"))
                .map(Map.of("selected_name", "stored"));

        assertEquals(new AliasedRecord("stored"), value);
    }

    @Test
    void preservesPhysicalRecordLabelWhenBothLabelsArePresent() {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("stored_name", null);
        row.put("name", "alias value");

        assertEquals(new AliasedRecord(null), RowMapper.of(AliasedRecord.class).map(row));
    }

    @Test
    void resolvesExplicitBeanColumnByJavaPropertyLabel() {
        AliasedBean value = RowMapper.of(AliasedBean.class)
                .map(DynamicRow.copyOf(Map.of("name", "stored")));

        assertEquals("stored", value.name);
    }

    @Test
    void resolvesExplicitBeanColumnByPhysicalLabel() {
        AliasedBean value = RowMapper.of(AliasedBean.class)
                .map(DynamicRow.copyOf(Map.of("stored_name", "stored")));

        assertEquals("stored", value.name);
    }

    private record AliasedRecord(@TableField("stored_name") String name) {
    }

    private static final class AliasedBean {

        @TableField("stored_name")
        private String name;

        public String getName() {
            return name;
        }

        public void setName(String value) {
            name = value;
        }
    }

    private record UserName(String userName) {
    }

    private record UserProjection(String userName,
                                  @TableField(exist = false) String displayName,
                                  @TableField(exist = false) int rank) {
    }
}
