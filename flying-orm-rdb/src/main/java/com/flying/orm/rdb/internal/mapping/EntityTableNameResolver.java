package com.flying.orm.rdb.internal.mapping;

import com.flying.orm.core.annotation.TableCatalog;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.rdb.mapping.MappingException;

import java.util.Optional;

/** 从 flying-orm 的表注解或命名约定确定分段表身份和旧版限定表名。 */
final class EntityTableNameResolver {

    private EntityTableNameResolver() {
    }

    /**
     * 保留旧实体映射使用的限定字符串。
     *
     * <p>旧字符串只能表达 {@code schema.table}，不能表达 catalog。需要完整关系身份的 3.2
     * 调用链必须使用 {@link #resolveIdentity(Class, EntityNamingStrategy)}。</p>
     */
    static String resolve(Class<?> type, EntityNamingStrategy namingStrategy) {
        return resolveTable(type, namingStrategy);
    }

    /**
     * 直接从实体注解构造分段关系身份。
     *
     * <p>{@link TableName#value()} 中即使含有点号，也只会被当作完整表名保存。</p>
     */
    static RelationIdentity resolveIdentity(Class<?> type, EntityNamingStrategy namingStrategy) {
        Optional<TableName> tableDefinition = FlyingAnnotationReader.tableName(type);
        String defaultName = namingStrategy.tableName(type);
        String table = tableDefinition.map(TableName::value)
                                      .map(FlyingAnnotationReader::text)
                                      .orElse(null);
        String schema = tableDefinition.map(TableName::schema)
                                       .map(FlyingAnnotationReader::text)
                                       .orElse(null);
        TableCatalog catalogDefinition = type.getAnnotation(TableCatalog.class);
        String catalog = catalogDefinition == null
                ? null
                : catalogDefinition.value();
        return RelationIdentity.of(catalog, schema, table == null ? defaultName : table);
    }

    /**
     * 为完整关系模型保留旧版点分表名的物理含义。
     *
     * <p>旧 CRUD 会把 {@code schema.table} 交给标识符渲染器逐段引用。完整关系模型不能再把这个
     * 字符串塞进单个 table 段，否则 Repository 与 Schema 会指向不同的物理对象。显式 schema/catalog
     * 已经没有歧义，只有旧式两段 value 才在这里按原有语义转换；更多段没有受支持的旧模型，严格
     * 关系入口会直接拒绝而不是猜测 catalog。</p>
     */
    static RelationIdentity resolveRelationalIdentity(Class<?> type, EntityNamingStrategy namingStrategy) {
        RelationIdentity identity = resolveIdentity(type, namingStrategy);
        if (hasExplicitNamespace(type) || !identity.table().contains(".")) {
            return identity;
        }
        String[] parts = identity.table().split("\\.", -1);
        for (String part : parts) {
            if (part.isBlank()) {
                throw new MappingException("legacy qualified table name must not contain blank segments: "
                                                   + type.getName());
            }
        }
        return switch (parts.length) {
            case 2 -> RelationIdentity.of(null, parts[0], parts[1]);
            default -> throw new MappingException(
                    "legacy qualified table name must contain exactly two segments: " + type.getName());
        };
    }

    /**
     * 判断实体是否显式启用了 3.2 分段命名空间。
     *
     * <p>不能根据解析后的字符串猜测：旧 {@code @TableName("schema.table")} 必须继续走原有
     * String 语义，只有 catalog 注解或非空 schema 属性才切换 Repository 的分段渲染。</p>
     */
    static boolean hasExplicitNamespace(Class<?> type) {
        if (type.getAnnotation(TableCatalog.class) != null) {
            return true;
        }
        return FlyingAnnotationReader.tableName(type)
                                     .map(TableName::schema)
                                     .map(FlyingAnnotationReader::text)
                                     .isPresent();
    }

    /** 解析实体并返回兼容旧 API 的限定表名。 */
    static String resolveTable(Class<?> type, EntityNamingStrategy namingStrategy) {
        return resolveTable(resolveIdentity(type, namingStrategy));
    }

    /**
     * 把已经分段的关系身份转成旧实体映射使用的限定字符串。
     *
     * <p>旧公开访问器历来只认识 {@code schema.table}，所以这里不加入 catalog，也绝不按点号
     * 反向猜测名称段。完整 SQL 渲染同时持有 {@link RelationIdentity}。</p>
     */
    static String resolveTable(RelationIdentity identity) {
        String schema = identity.schema().orElse(null);
        StringBuilder table = new StringBuilder(identity.table().length() + 32);
        if (schema != null) {
            table.append(schema).append('.');
        }
        return table.append(identity.table()).toString();
    }
}
