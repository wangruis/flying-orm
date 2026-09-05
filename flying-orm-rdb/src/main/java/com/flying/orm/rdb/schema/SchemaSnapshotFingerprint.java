package com.flying.orm.rdb.schema;

import com.flying.orm.core.internal.hash.StableDigest;
import com.flying.orm.core.internal.hash.StableEncoder;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalMetadataFingerprint;

import java.util.Objects;

/**
 * 为 TOCTOU 审核生成与 JVM、集合迭代顺序无关的 Schema 快照指纹。
 *
 * @author wangr
 * @version v3.2
 */
public final class SchemaSnapshotFingerprint {

    private static final StableDigest.Domain DOMAIN = StableDigest.domain("schema-snapshot/v1");

    private SchemaSnapshotFingerprint() {
    }

    public static String of(SchemaSnapshot snapshot) {
        SchemaSnapshot source = Objects.requireNonNull(snapshot, "schema snapshot must not be null");
        StableEncoder encoder = StableDigest.sha256(DOMAIN);
        encodeIdentity(encoder, source.identity());
        encoder.text("TABLE_STATE", source.tableState().name());
        if (source.tableState() == SchemaSnapshot.State.PRESENT) {
            encoder.text("TABLE_COMMENT_STATE", source.tableComment().state().name())
                    .nullableText("TABLE_COMMENT", source.tableComment().value())
                    .text("COLUMNS_STATE", source.columns().state().name())
                    .text("PRIMARY_KEY_STATE", source.primaryKey().state().name())
                    .text("UNIQUES_STATE", source.uniqueConstraints().state().name())
                    .text("INDEXES_STATE", source.indexes().state().name())
                    .text("FOREIGN_KEYS_STATE", source.foreignKeys().state().name())
                    .text("CHECKS_STATE", source.checks().state().name())
                    .text("TABLE_PARTITION_STATE", source.partition().state().name())
                    .text("KNOWN_DEFINITION", RelationalMetadataFingerprint.of(source.knownDefinition()));
            encoder.integer("UNKNOWN_ATTRIBUTE_COUNT", source.unknownAttributes().size());
            source.unknownAttributes().stream().sorted().forEach(
                    attribute -> encoder.text("UNKNOWN_ATTRIBUTE", attribute.name()));
        }
        return encoder.finishHex();
    }

    private static void encodeIdentity(StableEncoder encoder, RelationIdentity identity) {
        encoder.nullableText("CATALOG", identity.catalog().orElse(null))
                .nullableText("SCHEMA", identity.schema().orElse(null))
                .text("TABLE", identity.table());
    }
}
