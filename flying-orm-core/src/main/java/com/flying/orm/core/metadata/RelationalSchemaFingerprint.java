package com.flying.orm.core.metadata;

import com.flying.orm.core.internal.hash.StableDigest;
import com.flying.orm.core.internal.hash.StableEncoder;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * 为一组最终物理关系生成稳定指纹。
 *
 * <p>单表继续复用原有表级指纹，避免普通实体因为新增 Schema 容器发生无意义的审批和缓存失效。
 * 多表只编码稳定排序后的关系身份和各自表级指纹。</p>
 *
 * @author wangr
 * @version v3.3
 */
public final class RelationalSchemaFingerprint {

    private static final StableDigest.Domain DOMAIN = StableDigest.domain("relational-schema/v1");

    private RelationalSchemaFingerprint() {
    }

    /** @return 最终物理关系集合的稳定 SHA-256 指纹 */
    public static String of(RelationalSchemaDefinition schema) {
        RelationalSchemaDefinition source = Objects.requireNonNull(
                schema, "relational schema definition must not be null");
        List<RelationalTableDefinition> tables = source.tables();
        if (tables.size() == 1) {
            return RelationalMetadataFingerprint.of(tables.getFirst());
        }
        StableEncoder encoder = StableDigest.sha256(DOMAIN);
        List<RelationalTableDefinition> ordered = tables.stream()
                .sorted(Comparator.comparing(
                                (RelationalTableDefinition table) ->
                                        table.identity().catalog().orElse(""))
                        .thenComparing(table -> table.identity().schema().orElse(""))
                        .thenComparing(table -> table.identity().table()))
                .toList();
        encoder.integer("TABLE_COUNT", ordered.size());
        for (RelationalTableDefinition table : ordered) {
            RelationIdentity identity = table.identity();
            encoder.marker("TABLE")
                    .nullableText("TABLE_CATALOG", identity.catalog().orElse(null))
                    .nullableText("TABLE_SCHEMA", identity.schema().orElse(null))
                    .text("TABLE_NAME", identity.table())
                    .text("TABLE_FINGERPRINT", RelationalMetadataFingerprint.of(table));
        }
        return encoder.finishHex();
    }
}
