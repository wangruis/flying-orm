package com.flying.orm.rdb.internal.mapping;

import com.flying.orm.core.annotation.EncryptedField;
import com.flying.orm.core.annotation.MaskedField;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.FieldProtectionRegistry;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import com.flying.orm.rdb.mapping.EntityFieldMetadata;

import java.lang.reflect.Field;

/**
 * 把实体字段保护注解编译成 DynamicForm 共用的不可变定义。
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
final class EntityFieldProtectionCompiler {

    private EntityFieldProtectionCompiler() {
    }

    static void compile(Field field,
                        EntityFieldMetadata metadata,
                        FieldProtectionRegistry.Builder protections) {
        if (metadata.version() && FlyingAnnotationReader.encryptedField(field).isPresent()) {
            // 乐观锁必须让数据库直接比较并递增明文版本；密文列无法同时满足这两个原子语义。
            throw new IllegalArgumentException("entity version field must not be encrypted");
        }
        if (metadata.version() && FlyingAnnotationReader.maskedField(field).isPresent()) {
            // Repository 会把查询结果中的版本值继续用于下一次乐观锁比较，脱敏值不能承担并发控制职责。
            throw new IllegalArgumentException("entity version field must not be masked");
        }
        FlyingAnnotationReader.encryptedField(field)
                              .map(EntityFieldProtectionCompiler::encrypted)
                              .ifPresent(definition -> protections.encrypted(metadata.columnName(), definition));
        FlyingAnnotationReader.maskedField(field)
                              .map(EntityFieldProtectionCompiler::masked)
                              .ifPresent(definition -> protections.masked(metadata.columnName(), definition));
    }

    private static EncryptedFieldDefinition encrypted(EncryptedField annotation) {
        return EncryptedFieldDefinition.builder()
                                       .searchModes(annotation.search())
                                       .normalizer(annotation.normalizer())
                                       .suffixLengths(annotation.suffixLengths())
                                       .maxNormalizedLength(annotation.maxNormalizedLength())
                                       .containsMinLength(annotation.containsMinLength())
                                       .build();
    }

    private static MaskedFieldDefinition masked(MaskedField annotation) {
        return MaskedFieldDefinition.builder(annotation.policy())
                                    .prefix(annotation.prefix())
                                    .suffix(annotation.suffix())
                                    .display(annotation.display())
                                    .build();
    }
}
