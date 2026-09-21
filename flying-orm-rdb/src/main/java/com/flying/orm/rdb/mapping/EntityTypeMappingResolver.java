package com.flying.orm.rdb.mapping;

import java.util.ArrayList;
import java.util.List;

/** 从已冻结映射中选择最具体 Java 类型；只在实体元数据编译时执行。 */
final class EntityTypeMappingResolver {

    private EntityTypeMappingResolver() {
    }

    static EntityTypeMappingRegistry.Mapping resolveRegistered(
            List<EntityTypeMappingRegistry.Mapping> source,
            Class<?> javaType,
            boolean preferStandardDefault,
            String requestedId,
            List<EntityTypeMappingRegistry.Mapping> standardMappings) {
        List<EntityTypeMappingRegistry.Mapping> compatible = new ArrayList<>();
        for (EntityTypeMappingRegistry.Mapping mapping : source) {
            if (mapping.javaType().isAssignableFrom(javaType)) {
                compatible.add(mapping);
            }
        }
        if (compatible.isEmpty()) {
            return null;
        }

        List<EntityTypeMappingRegistry.Mapping> mostSpecific = new ArrayList<>();
        for (EntityTypeMappingRegistry.Mapping candidate : compatible) {
            boolean shadowed = false;
            for (EntityTypeMappingRegistry.Mapping other : compatible) {
                if (candidate.javaType() != other.javaType()
                        && candidate.javaType().isAssignableFrom(other.javaType())) {
                    shadowed = true;
                    break;
                }
            }
            if (!shadowed) {
                mostSpecific.add(candidate);
            }
        }

        Class<?> selectedType = mostSpecific.getFirst().javaType();
        for (EntityTypeMappingRegistry.Mapping candidate : mostSpecific) {
            if (candidate.javaType() != selectedType) {
                throw ambiguousMapping(javaType, requestedId, mostSpecific);
            }
        }
        if (preferStandardDefault) {
            for (EntityTypeMappingRegistry.Mapping candidate : mostSpecific) {
                if (isStandard(candidate, standardMappings)) {
                    return candidate.forResolvedType(javaType);
                }
            }
        }
        if (mostSpecific.size() == 1) {
            return mostSpecific.getFirst().forResolvedType(javaType);
        }
        throw ambiguousMapping(javaType, requestedId, mostSpecific);
    }

    private static boolean isStandard(
            EntityTypeMappingRegistry.Mapping candidate,
            List<EntityTypeMappingRegistry.Mapping> standardMappings) {
        for (EntityTypeMappingRegistry.Mapping standard : standardMappings) {
            if (candidate == standard) {
                return true;
            }
        }
        return false;
    }

    private static MappingException ambiguousMapping(
            Class<?> javaType,
            String requestedId,
            List<EntityTypeMappingRegistry.Mapping> candidates) {
        StringBuilder message = new StringBuilder(
                "multiple entity type mappings match Java type ")
                .append(javaType.getTypeName());
        if (requestedId == null) {
            message.append("; declare @TableColumn(databaseTypeId=...) explicitly");
        } else {
            message.append(" for mapping id ").append(requestedId);
        }
        message.append(": ");
        for (int index = 0; index < candidates.size(); index++) {
            if (index > 0) {
                message.append(", ");
            }
            EntityTypeMappingRegistry.Mapping candidate = candidates.get(index);
            message.append(candidate.id())
                    .append('(')
                    .append(candidate.javaType().getTypeName())
                    .append(')');
        }
        return new MappingException(message.toString());
    }
}
