package com.flying.orm.rdb.mapping;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.EnumValue;
import com.flying.orm.core.annotation.TableColumn;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.id.IdGenerator;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EntityExplicitTypeCodecTest {

    @Test
    void rawPlansDecodeRecordBeanAndEnumMembersOnceIncludingNullResults() {
        CountingTextCodec codec = new CountingTextCodec();
        EntityTypeMappingRegistry mappings = textMappings(codec);
        try (EntityModelRegistry models = textModels(mappings)) {
            TextRecord record = models.rawRowMapper(TextRecord.class, mappings.valueCodecs())
                    .map(Map.of("text", "db:payload", "member", "db:active"));
            assertEquals(new TextRecord("payload", TextMember.ACTIVE), record);
            assertEquals(2, codec.reads.get());

            codec.reads.set(0);
            TextBean bean = models.rawRowMapper(TextBean.class, mappings.valueCodecs())
                    .map(Map.of("text", "db:payload", "member", "db:active"));
            assertEquals("payload", bean.text);
            assertEquals(TextMember.ACTIVE, bean.member);
            assertEquals(2, codec.reads.get());

            codec.reads.set(0);
            Map<String, Object> nulls = new HashMap<>();
            nulls.put("text", "db:null");
            nulls.put("member", null);
            record = models.rawRowMapper(TextRecord.class, mappings.valueCodecs()).map(nulls);
            bean = models.rawRowMapper(TextBean.class, mappings.valueCodecs()).map(nulls);
            assertEquals(new TextRecord(null, null), record);
            assertNull(bean.text);
            assertNull(bean.member);
            assertEquals(4, codec.reads.get(), "Each raw field is decoded once even when its result is null");
        }
    }

    @Test
    void callerMappersStillAcceptDecodedAliasRowsAndRawPlansShareTheExistingCache() {
        CountingTextCodec codec = new CountingTextCodec();
        EntityTypeMappingRegistry mappings = textMappings(codec);
        ValueCodecRegistry codecs = mappings.valueCodecs();
        try (EntityModelRegistry models = textModels(mappings)) {
            RowMapper<TextRecord> caller = models.rowMapper(TextRecord.class, codecs);
            assertEquals(new TextRecord("payload", TextMember.ACTIVE), caller
                    .withAliases(Map.of("selected_text", "text", "selected_member", "member"))
                    .map(Map.of("selected_text", "payload", "selected_member", "active")));
            assertEquals(0, codec.reads.get(), "Caller-provided mappers retain the decoded-value fast path");
            RowMapper<TextRecord> raw = models.rawRowMapper(TextRecord.class, codecs);
            assertNotSame(caller, raw);
            assertSame(raw, models.rawRowMapper(TextRecord.class, codecs));
            assertSame(caller, models.rowMapper(TextRecord.class, codecs));
            assertSame(models.rowMapper(StandardText.class, codecs), models.rawRowMapper(StandardText.class, codecs));
            assertEquals(4L, models.estimatedMappings(), "Both strategies use the same bounded model region");
        }
    }

    private static EntityTypeMappingRegistry textMappings(CountingTextCodec codec) {
        return EntityTypeMappingRegistry.builder()
                .register("coded-text", CharSequence.class, DatabaseType.of("VARCHAR"), codec).build();
    }

    private static EntityModelRegistry textModels(EntityTypeMappingRegistry mappings) {
        return EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults(), IdGenerator.none(),
                EntityFieldFiller.none(), Map.of(
                        TextRecord.class, EntitySchemaDescriptor.builder(TextRecord.class).typeMappings(mappings).build(),
                        TextBean.class, EntitySchemaDescriptor.builder(TextBean.class).typeMappings(mappings).build()));
    }

    @Test
    void usesOneExplicitTypeMappingForMetadataSchemaBindingAndDecoding() {
        DatabaseType accountNumberType = DatabaseType.of("VARCHAR(32)");
        AccountNumberCodec codec = new AccountNumberCodec();
        EntityTypeMappingRegistry typeMappings = EntityTypeMappingRegistry.builder()
                .register("account-number", AccountNumber.class, accountNumberType, codec)
                .build();

        EntitySchemaDescriptor<Account> descriptor = EntitySchemaDescriptor.builder(Account.class)
                .typeMappings(typeMappings)
                .build();

        assertSame(typeMappings, descriptor.typeMappings());
        assertSame(descriptor.form(), descriptor.metadata().toDynamicForm());
        assertEquals(accountNumberType, descriptor.metadata().field("accountNumber").databaseType());
        assertEquals("VARCHAR(32)", descriptor.form().field("account_number").dataType());
        assertEquals(accountNumberType, descriptor.table().column("account_number").databaseType());
        assertEquals("account-number", descriptor.table().column("account_number").codecId());
        assertEquals("A-17", descriptor.valueCodecs().write(new AccountNumber("A-17")));
        assertEquals(new AccountNumber("A-17"),
                     descriptor.valueCodecs().read("A-17", AccountNumber.class));
        assertFalse(descriptor.typeMappingsFingerprint().isBlank());
        assertFalse(descriptor.relationalFingerprint().isBlank());
    }

    @Test
    void rejectsASecondMappingForTheSameJavaType() {
        StringValueCodec codec = new StringValueCodec();

        assertThrows(IllegalArgumentException.class,
                     () -> EntityTypeMappingRegistry.builder()
                             .register("case-insensitive-text", String.class,
                                       DatabaseType.of("CITEXT"), codec));
    }

    @TableName("accounts")
    private record Account(
            @TableId(type = IdType.INPUT) Long id,
            @TableColumn(databaseTypeId = "account-number") AccountNumber accountNumber) {
    }

    private record AccountNumber(String value) {
    }

    @TableName("coded_text")
    private record TextRecord(@TableColumn(databaseTypeId = "coded-text") CharSequence text,
                              @TableColumn(databaseTypeId = "coded-text") TextMember member) {
    }

    @TableName("coded_text")
    private static final class TextBean {
        @TableColumn(databaseTypeId = "coded-text")
        private CharSequence text;
        @TableColumn(databaseTypeId = "coded-text")
        private TextMember member;

        public CharSequence getText() { return text; }
        public void setText(CharSequence text) { this.text = text; }
    }

    private record StandardText(String text) {
    }

    private enum TextMember {
        ACTIVE("active");

        @EnumValue
        private final CharSequence value;

        TextMember(CharSequence value) { this.value = value; }
    }

    private static final class CountingTextCodec implements ValueCodec {
        private final AtomicInteger reads = new AtomicInteger();

        @Override
        public boolean supports(Class<?> targetType) { return targetType == CharSequence.class; }

        @Override
        public Object write(Object value) { return value == null ? null : "db:" + value; }

        @Override
        public Object read(Object value, Class<?> targetType) {
            reads.incrementAndGet();
            return value == null || "db:null".equals(value) ? null : value.toString().substring(3);
        }
    }

    private static final class AccountNumberCodec implements ValueCodec {

        @Override
        public boolean supports(Class<?> targetType) {
            return targetType == AccountNumber.class;
        }

        @Override
        public Object write(Object value) {
            return value == null ? null : ((AccountNumber) value).value();
        }

        @Override
        public Object read(Object value, Class<?> targetType) {
            return value == null ? null : new AccountNumber(value.toString());
        }
    }

    private static final class StringValueCodec implements ValueCodec {

        @Override
        public boolean supports(Class<?> targetType) {
            return targetType == String.class;
        }

        @Override
        public Object write(Object value) {
            return value;
        }

        @Override
        public Object read(Object value, Class<?> targetType) {
            return value == null ? null : value.toString();
        }
    }
}
