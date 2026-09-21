package com.flying.orm.rdb.mapping;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableColumn;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableLogic;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EntityLogicDeleteCustomCodecTest {

    @Test
    void decodesLogicDeleteLiteralsBeforeTheValueIsBoundOnce() {
        TrackingStateCodec codec = new TrackingStateCodec();
        EntityTypeMappingRegistry typeMappings = EntityTypeMappingRegistry.builder()
                .register("deletion-state", DeletionState.class, DatabaseType.of("VARCHAR(16)"), codec)
                .build();

        EntitySchemaDescriptor<ManagedRecord> descriptor = EntitySchemaDescriptor.builder(ManagedRecord.class)
                .typeMappings(typeMappings)
                .build();
        EntityFieldMetadata logicDelete = descriptor.metadata().logicDeleteField().orElseThrow();

        assertEquals(DeletionState.ACTIVE, logicDelete.logicNotDeletedValue());
        assertEquals(DeletionState.DELETED, logicDelete.logicDeletedValue());
        assertEquals(List.of("active", "deleted"), codec.readValues);
        assertEquals(List.of(DeletionState.class, DeletionState.class), codec.readTypes);
        assertEquals(0, codec.writeCount);

        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2())
                .withEntityFieldCodecs(descriptor.customFieldCodecs());
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("id", 1L);
        values.put("deletion_state", logicDelete.logicDeletedValue());
        assertEquals(List.of(1L, "db:deleted"), renderer.insert(descriptor.form(), values).parameters());
        assertEquals(1, codec.writeCount);
    }

    @TableName("managed_records")
    private static final class ManagedRecord {

        @TableId(type = IdType.INPUT)
        private Long id;

        @TableLogic(value = "active", delval = "deleted")
        @TableColumn(databaseTypeId = "deletion-state")
        private DeletionState deletionState;
    }

    private enum DeletionState {
        ACTIVE,
        DELETED
    }

    private static final class TrackingStateCodec implements ValueCodec {

        private final List<Object> readValues = new ArrayList<>();
        private final List<Class<?>> readTypes = new ArrayList<>();
        private int writeCount;

        @Override
        public boolean supports(Class<?> targetType) {
            return targetType == DeletionState.class;
        }

        @Override
        public Object write(Object value) {
            writeCount++;
            return value == null
                    ? null
                    : "db:" + ((DeletionState) value).name().toLowerCase(Locale.ROOT);
        }

        @Override
        public Object read(Object value, Class<?> targetType) {
            readValues.add(value);
            readTypes.add(targetType);
            return value == null
                    ? null
                    : DeletionState.valueOf(value.toString().toUpperCase(Locale.ROOT));
        }
    }
}
