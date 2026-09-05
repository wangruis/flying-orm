package com.flying.orm.rdb.mapping;

import com.flying.orm.core.annotation.EncryptedField;
import com.flying.orm.core.annotation.TableColumn;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.annotation.TablePartition;
import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.metadata.TablePartitionDefinition;
import com.flying.orm.core.type.DatabaseType;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EntityPartitionCompilationTest {

    @Test
    void resolvesEachSupportedTemporalPropertyToItsPhysicalColumn() {
        for (Class<?> entityType : List.of(DateEntity.class, TimestampEntity.class,
                                            OffsetTimestampEntity.class, InheritedEntity.class,
                                            ProtectedSiblingEntity.class)) {
            TablePartitionDefinition partition = EntitySchemaDescriptor.builder(entityType)
                    .build().table().partition().orElseThrow();

            assertEquals(TablePartitionDefinition.Strategy.RANGE, partition.strategy(),
                         entityType.getSimpleName());
            assertEquals("occurred_at", partition.column(), entityType.getSimpleName());
        }
    }

    @Test
    void rejectsBlankMissingExcludedTransientAndExpressionProperties() {
        for (Class<?> entityType : List.of(BlankPropertyEntity.class,
                                           MissingPropertyEntity.class,
                                           ExcludedPropertyEntity.class,
                                           TransientPropertyEntity.class,
                                           ExpressionPropertyEntity.class,
                                           InheritedExcludedShadowEntity.class)) {
            assertThrows(MappingException.class,
                    () -> EntitySchemaDescriptor.builder(entityType).build(),
                    entityType.getSimpleName());
        }
    }

    @Test
    void rejectsUnsupportedTemporalAndNonTemporalTypes() {
        for (Class<?> entityType : List.of(TimeEntity.class,
                                           OffsetTimeEntity.class,
                                           TextEntity.class)) {
            assertThrows(MappingException.class,
                    () -> EntitySchemaDescriptor.builder(entityType).build(),
                    entityType.getSimpleName());
        }
    }

    @Test
    void rejectsArrayAndEncryptedPartitionKeys() {
        ValueCodec arrayCodec = new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                return targetType == LocalDateTime[].class;
            }

            @Override
            public Object write(Object value) {
                return value;
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                return value;
            }
        };
        EntityTypeMappingRegistry mappings = EntityTypeMappingRegistry.builder()
                .register("timestamp-array", LocalDateTime[].class,
                          DatabaseType.of("TIMESTAMP[]"), arrayCodec)
                .build();

        assertThrows(MappingException.class,
                () -> EntitySchemaDescriptor.builder(ArrayEntity.class)
                        .typeMappings(mappings).build());
        assertThrows(MappingException.class,
                () -> EntitySchemaDescriptor.builder(EncryptedEntity.class).build());
    }

    @TableName("date_events")
    @TablePartition(strategy = TablePartition.Strategy.RANGE, property = "occurredAt")
    private static final class DateEntity {
        @TableField("occurred_at")
        private LocalDate occurredAt;
    }

    @TableName("timestamp_events")
    @TablePartition(strategy = TablePartition.Strategy.RANGE, property = "occurredAt")
    private static final class TimestampEntity {
        @TableField("occurred_at")
        private LocalDateTime occurredAt;
    }

    @TableName("offset_timestamp_events")
    @TablePartition(strategy = TablePartition.Strategy.RANGE, property = "occurredAt")
    private static final class OffsetTimestampEntity {
        @TableField("occurred_at")
        private OffsetDateTime occurredAt;
    }

    @TablePartition(strategy = TablePartition.Strategy.RANGE, property = "occurredAt")
    private static class InheritedBase {
        @TableField("occurred_at")
        private LocalDateTime occurredAt;
    }

    @TableName("inherited_events")
    private static final class InheritedEntity extends InheritedBase {
        private Long id;
    }

    @TableName("protected_sibling_events")
    @TablePartition(strategy = TablePartition.Strategy.RANGE, property = "occurredAt")
    private static final class ProtectedSiblingEntity {
        @TableField("occurred_at")
        private LocalDateTime occurredAt;
        @EncryptedField
        private String secret;
    }

    @TableName("blank_partition")
    @TablePartition(strategy = TablePartition.Strategy.RANGE, property = "")
    private static final class BlankPropertyEntity {
        private LocalDateTime occurredAt;
    }

    @TableName("missing_partition")
    @TablePartition(strategy = TablePartition.Strategy.RANGE, property = "missing")
    private static final class MissingPropertyEntity {
        private LocalDateTime occurredAt;
    }

    @TableName("excluded_partition")
    @TablePartition(strategy = TablePartition.Strategy.RANGE, property = "occurredAt")
    private static final class ExcludedPropertyEntity {
        private Long id;
        @TableField(exist = false)
        private LocalDateTime occurredAt;
    }

    @TableName("transient_partition")
    @TablePartition(strategy = TablePartition.Strategy.RANGE, property = "occurredAt")
    private static final class TransientPropertyEntity {
        private Long id;
        private transient LocalDateTime occurredAt;
    }

    @TableName("expression_partition")
    @TablePartition(strategy = TablePartition.Strategy.RANGE,
                    property = "date_trunc('day', occurredAt)")
    private static final class ExpressionPropertyEntity {
        private LocalDateTime occurredAt;
    }

    @TableName("inherited_shadow_partition")
    private static final class InheritedExcludedShadowEntity extends InheritedBase {
        @TableField(exist = false)
        private LocalDateTime occurredAt;
    }

    @TableName("time_partition")
    @TablePartition(strategy = TablePartition.Strategy.RANGE, property = "occurredAt")
    private static final class TimeEntity {
        private LocalTime occurredAt;
    }

    @TableName("offset_time_partition")
    @TablePartition(strategy = TablePartition.Strategy.RANGE, property = "occurredAt")
    private static final class OffsetTimeEntity {
        private OffsetTime occurredAt;
    }

    @TableName("text_partition")
    @TablePartition(strategy = TablePartition.Strategy.RANGE, property = "occurredAt")
    private static final class TextEntity {
        private String occurredAt;
    }

    @TableName("array_partition")
    @TablePartition(strategy = TablePartition.Strategy.RANGE, property = "occurredAt")
    private static final class ArrayEntity {
        @TableColumn(databaseTypeId = "timestamp-array")
        private LocalDateTime[] occurredAt;
    }

    @TableName("encrypted_partition")
    @TablePartition(strategy = TablePartition.Strategy.RANGE, property = "occurredAt")
    private static final class EncryptedEntity {
        @EncryptedField
        private LocalDateTime occurredAt;
    }
}
