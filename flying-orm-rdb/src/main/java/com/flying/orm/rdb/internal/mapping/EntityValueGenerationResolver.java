package com.flying.orm.rdb.internal.mapping;

import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.KeySequence;
import com.flying.orm.core.annotation.TableId;
import java.lang.reflect.Field;
import java.util.Optional;

/** 把 flying-orm 主键策略和可选序列声明转成跨方言生成策略。 */
final class EntityValueGenerationResolver {

    private EntityValueGenerationResolver() {
    }

    static ValueGeneration resolve(Class<?> entityType, Field field) {
        Optional<TableId> flyingId = FlyingAnnotationReader.tableId(field);
        if (flyingId.isPresent()) {
            return resolveFlying(entityType, flyingId.orElseThrow());
        }
        return ValueGeneration.none();
    }

    private static ValueGeneration resolveFlying(Class<?> entityType, TableId tableId) {
        Optional<ValueGeneration> sequence = flyingSequence(entityType);
        if (sequence.isPresent()) {
            return sequence.orElseThrow();
        }
        if (tableId.type() == IdType.AUTO) {
            return ValueGeneration.identity();
        }
        // NONE 按安全的 INPUT 处理；ASSIGN_ID/ASSIGN_UUID 在 Repository 的 SQL 前阶段生成或明确拒绝。
        // 元数据阶段只保留策略，绝不能偷偷生成一个不可靠的分布式 ID。
        return ValueGeneration.none();
    }

    private static Optional<ValueGeneration> flyingSequence(Class<?> entityType) {
        return FlyingAnnotationReader.keySequence(entityType)
                                      .map(KeySequence::value)
                                      .map(FlyingAnnotationReader::text)
                                      .map(ValueGeneration::sequence);
    }

}
