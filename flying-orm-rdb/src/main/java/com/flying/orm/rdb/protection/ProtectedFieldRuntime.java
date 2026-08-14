package com.flying.orm.rdb.protection;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.result.DynamicRow;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * 字段保护的内部运行时，把逻辑值转换为密文/盲索引，并在结果边界完成解密和业务脱敏。
 *
 * <p>公开业务代码只配置主密钥环；该类型由统一客户端装配，不是第二套 CRUD API。</p>
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
@InternalApi
public final class ProtectedFieldRuntime implements AutoCloseable {

    private final ProtectedFieldKeyRing keys;
    private final ProtectedWriteTransformer writes;
    private final ProtectedQueryRewriter queries;
    private final ProtectedResultTransformer results;
    private final MaskedFieldResultTransformer masking;

    private ProtectedFieldRuntime(ProtectedFieldKeyRing keys,
                                  ProtectedValueNormalizerRegistry normalizers,
                                  MaskingPolicyRegistry policies) {
        this.keys = keys;
        this.masking = new MaskedFieldResultTransformer(policies);
        if (keys == null) {
            this.writes = null;
            this.queries = null;
            this.results = null;
        } else {
            ProtectedFieldCipher cipher = new ProtectedFieldCipher(keys);
            ProtectedSearchTokenService tokens = new ProtectedSearchTokenService(keys, normalizers);
            this.writes = new ProtectedWriteTransformer(cipher, tokens);
            this.queries = new ProtectedQueryRewriter(tokens);
            this.results = new ProtectedResultTransformer(cipher, masking);
        }
    }

    /** @return 只提供标准脱敏策略、遇到加密字段时明确拒绝的运行时 */
    public static ProtectedFieldRuntime withoutKeys() {
        return withoutKeys(ProtectedValueNormalizerRegistry.standard(), MaskingPolicyRegistry.standard());
    }

    /**
     * 为只启用脱敏或尚未声明加密字段的客户端装配显式扩展 registry，不要求配置无关的密钥。
     *
     * @param normalizers 保护值规范化器 registry
     * @param policies    脱敏策略 registry
     * @return 不持有密钥的字段保护运行时
     */
    public static ProtectedFieldRuntime withoutKeys(ProtectedValueNormalizerRegistry normalizers,
                                                     MaskingPolicyRegistry policies) {
        return new ProtectedFieldRuntime(
                null,
                Objects.requireNonNull(normalizers, "protected normalizer registry must not be null"),
                Objects.requireNonNull(policies, "masking policy registry must not be null"));
    }

    /** @return 使用标准规范化器和脱敏策略的字段保护运行时 */
    public static ProtectedFieldRuntime create(ProtectedFieldKeyRing keys) {
        return create(keys, ProtectedValueNormalizerRegistry.standard(), MaskingPolicyRegistry.standard());
    }

    /** @return 使用上层显式扩展 registry 的字段保护运行时 */
    public static ProtectedFieldRuntime create(ProtectedFieldKeyRing keys,
                                               ProtectedValueNormalizerRegistry normalizers,
                                               MaskingPolicyRegistry policies) {
        return new ProtectedFieldRuntime(
                Objects.requireNonNull(keys, "protected field key ring must not be null"),
                Objects.requireNonNull(normalizers, "protected normalizer registry must not be null"),
                Objects.requireNonNull(policies, "masking policy registry must not be null"));
    }

    /** 把一行逻辑写入值转换为只供内部 SQL 渲染使用的物理值。 */
    public PreparedWrite prepareWrite(DynamicForm form,
                                      Map<String, Object> values,
                                      DataScope scope,
                                      ValueCodecRegistry codecs) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        Map<String, Object> safeValues = Objects.requireNonNull(values, "dynamic form values must not be null");
        if (safeForm.protections().encryptedFields().isEmpty()) {
            return new PreparedWrite(safeForm, safeValues);
        }
        requireKeys();
        return writes.prepare(safeForm, safeValues, scope, codecs);
    }

    /**
     * 为一行逻辑值生成 CONTAINS 侧索引令牌；没有启用 CONTAINS 时返回空列表。
     *
     * <p>返回值不包含 owner 主键，数据库生成键可以在基础 insert 成功后再与这些不可变令牌组合。</p>
     */
    public List<ContainsFieldTokens> prepareContainsTokens(DynamicForm form,
                                                           Map<String, Object> values,
                                                           DataScope scope,
                                                           ValueCodecRegistry codecs) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        if (safeForm.protections().encryptedFields().values().stream()
                    .noneMatch(definition -> definition.searchModes()
                                                       .contains(com.flying.orm.core.protection.EncryptedSearchMode.CONTAINS))) {
            return List.of();
        }
        requireKeys();
        return writes.containsTokens(safeForm,
                                     Objects.requireNonNull(values, "dynamic form values must not be null"),
                                     scope,
                                     codecs);
    }

    /**
     * 为 R2DBC 回执计算受保护字段的稳定载荷身份。返回值只供内部批量管线立即组装摘要行，
     * 不进入 SQL 参数、结果对象或日志。
     */
    public Map<String, byte[]> prepareReceiptIdentities(DynamicForm form,
                                                        Map<String, Object> values,
                                                        DataScope scope,
                                                        ValueCodecRegistry codecs) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        if (safeForm.protections().encryptedFields().isEmpty()) {
            return Map.of();
        }
        requireKeys();
        return writes.receiptIdentities(
                safeForm,
                Objects.requireNonNull(values, "dynamic form values must not be null"),
                scope,
                codecs);
    }

    /** 提取单表查询中的显式 CONTAINS 条件，供有界候选查询与解密复核编排使用。 */
    public Optional<PreparedContainsQuery> prepareContainsQuery(DynamicForm form,
                                                                ConditionGroup where,
                                                                DataScope scope,
                                                                ValueCodecRegistry codecs) {
        return prepareContainsQuery(form, form, where, scope, codecs);
    }

    /** 提取单表 CONTAINS 条件，并只读取本次 Scope 允许暴露的业务字段。 */
    public Optional<PreparedContainsQuery> prepareContainsQuery(DynamicForm form,
                                                                DynamicForm visibleForm,
                                                                ConditionGroup where,
                                                                DataScope scope,
                                                                ValueCodecRegistry codecs) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        if (safeForm.protections().encryptedFields().isEmpty()) {
            return Optional.empty();
        }
        requireKeys();
        return queries.prepareContains(safeForm,
                                       Objects.requireNonNull(visibleForm, "visible form must not be null"),
                                       where, scope, codecs);
    }

    /** 把显式保护搜索改写为隐藏列 HMAC 条件，并拒绝密文上的普通比较。 */
    public PreparedQuery prepareQuery(DynamicForm form,
                                      DynamicForm visibleForm,
                                      ConditionGroup where,
                                      DataScope scope,
                                      ValueCodecRegistry codecs) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        if (safeForm.protections().encryptedFields().isEmpty()) {
            return new PreparedQuery(safeForm, Objects.requireNonNull(where, "query where must not be null"),
                                     ProtectedFormLayout.visibleFieldNames(visibleForm));
        }
        requireKeys();
        return queries.prepare(safeForm, visibleForm, where, scope, codecs);
    }

    /** 解密已经物化为 byte[] 的字段，并按查询级展示策略处理显式 masked 字段。 */
    public DynamicRow transformResult(DynamicForm form,
                                      DynamicRow row,
                                      DataScope scope,
                                      SensitiveDisplayMode displayMode,
                                      ValueCodecRegistry codecs) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        DynamicRow safeRow = Objects.requireNonNull(row, "dynamic row must not be null");
        if (!safeForm.protections().encryptedFields().isEmpty()) {
            requireKeys();
            return results.transform(safeForm, safeRow, scope, displayMode, codecs);
        }
        return masking.transform(safeForm, safeRow, displayMode);
    }

    /** 对已经解密的候选行执行与令牌生成完全相同的规范化后 substring 复核。 */
    public boolean matchesContains(DynamicForm form,
                                   PreparedContainsQuery query,
                                   DynamicRow decryptedRow) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        PreparedContainsQuery safeQuery = Objects.requireNonNull(
                query, "protected contains query must not be null");
        Object value = Objects.requireNonNull(decryptedRow, "decrypted row must not be null")
                              .get(safeQuery.fieldName());
        if (!(value instanceof String text)) {
            return false;
        }
        com.flying.orm.core.protection.EncryptedFieldDefinition definition = safeForm.protections()
                .encrypted(safeQuery.fieldName())
                .orElseThrow(() -> new IllegalArgumentException("protected search requires an encrypted field"));
        return queries.matchesContains(text, definition, safeQuery.normalizedValue());
    }

    /** 只应用最终展示策略；调用方必须保证行内受保护字段已经解密。 */
    public DynamicRow maskResult(DynamicForm form,
                                 DynamicRow decryptedRow,
                                 SensitiveDisplayMode displayMode) {
        return masking.transform(form, decryptedRow, displayMode);
    }

    /** @return 包含密文和隐藏索引列的内部物理表单 */
    public DynamicForm physicalForm(DynamicForm form) {
        return ProtectedFormLayout.physical(Objects.requireNonNull(form, "dynamic form must not be null"));
    }

    @Override
    public void close() {
        if (keys != null) {
            keys.close();
        }
    }

    private void requireKeys() {
        if (keys == null) {
            throw new IllegalStateException("protected field key ring is not configured");
        }
    }

    /** 物理表单和参数值的不可变写入快照。 */
    public record PreparedWrite(DynamicForm physicalForm, Map<String, Object> values) {
        public PreparedWrite {
            physicalForm = Objects.requireNonNull(physicalForm, "physical form must not be null");
            values = snapshotValues(physicalForm, values);
        }

        /**
         * 每次读取都重新复制 ORM 生成的密文与盲索引；普通二进制业务值仍遵守低层零复制交接契约。
         *
         * @return 不可修改的物理写入值快照
         */
        @Override
        public Map<String, Object> values() {
            return snapshotValues(physicalForm, values);
        }

        private static Map<String, Object> snapshotValues(DynamicForm form, Map<String, Object> source) {
            Map<String, Object> copy = new LinkedHashMap<>();
            Objects.requireNonNull(source, "protected write values must not be null")
                   .forEach((name, value) -> copy.put(name, snapshotValue(form, name, value)));
            return Collections.unmodifiableMap(copy);
        }

        private static Object snapshotValue(DynamicForm form, String name, Object value) {
            if (!(value instanceof byte[] bytes)) {
                return value;
            }
            return form.findField(name)
                       .filter(field -> isProtectedPhysicalType(field.dataType()))
                       .<Object>map(field -> bytes.clone())
                       .orElse(value);
        }

        private static boolean isProtectedPhysicalType(String dataType) {
            return ProtectedFormLayout.CIPHERTEXT_TYPE.equalsIgnoreCase(dataType)
                    || ProtectedFormLayout.HASH_TYPE.equalsIgnoreCase(dataType);
        }
    }

    /** 物理表单、已改写条件和业务可见投影的不可变查询快照。 */
    public record PreparedQuery(DynamicForm physicalForm,
                                ConditionGroup where,
                                List<String> visibleFields) {
        public PreparedQuery {
            physicalForm = Objects.requireNonNull(physicalForm, "physical form must not be null");
            where = Objects.requireNonNull(where, "protected query where must not be null");
            visibleFields = List.copyOf(Objects.requireNonNull(
                    visibleFields, "protected visible fields must not be null"));
        }
    }

    /** 单个逻辑字段对应的稳定字段标签和去重 CONTAINS 令牌快照。 */
    public record ContainsFieldTokens(String fieldName, String fieldTag, List<byte[]> tokens) {
        public ContainsFieldTokens {
            fieldName = Objects.requireNonNull(fieldName, "protected contains field name must not be null");
            fieldTag = Objects.requireNonNull(fieldTag, "protected contains field tag must not be null");
            tokens = copyTokens(tokens);
        }

        @Override
        public List<byte[]> tokens() {
            return copyTokens(tokens);
        }

        private static List<byte[]> copyTokens(List<byte[]> values) {
            List<byte[]> copy = new java.util.ArrayList<>(Objects.requireNonNull(
                    values, "protected contains tokens must not be null").size());
            values.forEach(value -> copy.add(Objects.requireNonNull(
                    value, "protected contains token must not be null").clone()));
            return List.copyOf(copy);
        }
    }

    /** 单个可读密钥版本下必须同时命中的令牌组。 */
    public record ContainsTokenGroup(String keyVersion, List<byte[]> tokens) {
        public ContainsTokenGroup {
            keyVersion = Objects.requireNonNull(keyVersion, "protected contains key version must not be null");
            tokens = ContainsFieldTokens.copyTokens(tokens);
        }

        @Override
        public List<byte[]> tokens() {
            return ContainsFieldTokens.copyTokens(tokens);
        }
    }

    /** 单表 CONTAINS 两阶段查询所需的不可变逻辑计划。 */
    public record PreparedContainsQuery(DynamicForm physicalForm,
                                        ConditionGroup remainingWhere,
                                        List<String> visibleFields,
                                        Set<String> encryptedFields,
                                        String fieldName,
                                        String fieldTag,
                                        String normalizedValue,
                                        List<ContainsTokenGroup> tokenGroups,
                                        int distinctTokenCount,
                                        List<String> primaryKeys,
                                        String tokenTable) {
        public PreparedContainsQuery {
            physicalForm = Objects.requireNonNull(physicalForm, "physical form must not be null");
            remainingWhere = Objects.requireNonNull(remainingWhere, "remaining where must not be null");
            visibleFields = List.copyOf(Objects.requireNonNull(visibleFields, "visible fields must not be null"));
            encryptedFields = Set.copyOf(Objects.requireNonNull(
                    encryptedFields, "protected encrypted fields must not be null"));
            fieldName = Objects.requireNonNull(fieldName, "protected contains field name must not be null");
            fieldTag = Objects.requireNonNull(fieldTag, "protected contains field tag must not be null");
            normalizedValue = Objects.requireNonNull(
                    normalizedValue, "protected contains normalized value must not be null");
            tokenGroups = List.copyOf(Objects.requireNonNull(
                    tokenGroups, "protected contains token groups must not be null"));
            primaryKeys = List.copyOf(Objects.requireNonNull(primaryKeys, "primary keys must not be null"));
            tokenTable = Objects.requireNonNull(tokenTable, "protected contains token table must not be null");
            if (distinctTokenCount < 1 || primaryKeys.isEmpty()) {
                throw new IllegalArgumentException("protected contains query plan is incomplete");
            }
        }
    }
}
