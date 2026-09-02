package com.flying.orm.rdb.execution;

import com.flying.orm.core.internal.value.BindableValueSnapshots;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.result.DynamicRow;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.IntFunction;

/**
 * 描述业务表写入与 CONTAINS 侧索引维护必须共享的原子工作单元。
 *
 * <p>该模型只携带已经完成标识符校验和参数化渲染的请求，不暴露 JDBC/R2DBC 连接。执行内核必须在同一连接、
 * 同一事务中完成 owner 查询、业务写入和令牌替换；普通自定义执行器不得把它降级为若干独立 SQL。</p>
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
@InternalApi
public record ProtectedWriteWork(Kind kind,
                                 SqlRequest writeRequest,
                                 SqlRequest ownerQuery,
                                 List<String> ownerFields,
                                 Map<String, Object> knownOwner,
                                 String ownerPredicateSql,
                                 String deleteSql,
                                 String insertSql,
                                 List<FieldTokens> fields) {

    private static final int MAX_SAFE_BOUND_PARAMETERS = 2_000;
    private static final long MAX_CAPTURED_OWNER_ROWS = 2_000L;

    public ProtectedWriteWork {
        kind = Objects.requireNonNull(kind, "protected write kind must not be null");
        writeRequest = Objects.requireNonNull(
                writeRequest, "protected write request must not be null");
        ownerFields = List.copyOf(Objects.requireNonNull(
                ownerFields, "protected write owner fields must not be null"));
        knownOwner = snapshotOwner(Objects.requireNonNull(
                knownOwner, "protected write known owner must not be null"));
        ownerPredicateSql = requireText(ownerPredicateSql, "protected owner predicate sql");
        deleteSql = requireText(deleteSql, "protected token delete sql");
        insertSql = requireText(insertSql, "protected token insert sql");
        fields = List.copyOf(Objects.requireNonNull(fields, "protected token fields must not be null"));
        if (ownerFields.isEmpty() || fields.isEmpty()) {
            throw new IllegalArgumentException("protected write work is incomplete");
        }
        if (kind == Kind.UPDATE && ownerQuery == null) {
            throw new IllegalArgumentException("protected update requires an owner query");
        }
        Map<String, Object> ownerSnapshot = knownOwner;
        if (kind == Kind.UPSERT && ownerFields.stream().anyMatch(
                field -> !ownerSnapshot.containsKey(field) || ownerSnapshot.get(field) == null)) {
            throw new IllegalArgumentException("protected upsert requires a complete owner key");
        }
    }

    /** @return 是否必须从业务写入的同一 Statement 读取数据库生成键 */
    public boolean requiresGeneratedKeys() {
        return kind == Kind.INSERT && ownerFields.stream()
                .anyMatch(field -> !knownOwner.containsKey(field) || knownOwner.get(field) == null);
    }

    /** 给 owner 预读施加统一容量上限；调用方更严格的限制保持不变。 */
    public static SqlExecutionOptions ownerReadOptions(SqlExecutionOptions options) {
        SqlExecutionOptions safeOptions = Objects.requireNonNull(
                options, "protected write execution options must not be null");
        long maxRows = positiveMinimum(safeOptions.maxRows(), MAX_CAPTURED_OWNER_ROWS);
        long maxResultBytes = positiveMinimum(
                safeOptions.maxResultBytes(), SqlExecutionOptions.DEFAULT_MAX_RESULT_BYTES);
        long maxLargeObjectBytes = positiveMinimum(
                safeOptions.maxLargeObjectBytes(), SqlExecutionOptions.DEFAULT_MAX_LARGE_OBJECT_BYTES);
        long maxLargeObjectChars = positiveMinimum(
                safeOptions.maxLargeObjectChars(), SqlExecutionOptions.DEFAULT_MAX_LARGE_OBJECT_CHARS);
        if (safeOptions.maxRows() == maxRows
                && safeOptions.maxResultBytes() == maxResultBytes
                && safeOptions.maxLargeObjectBytes() == maxLargeObjectBytes
                && safeOptions.maxLargeObjectChars() == maxLargeObjectChars) {
            return safeOptions;
        }
        return safeOptions.withMaxRows(maxRows)
                          .withMaxResultBytes(maxResultBytes)
                          .withMaxLargeObjectBytes(maxLargeObjectBytes)
                          .withMaxLargeObjectChars(maxLargeObjectChars);
    }

    /** 返回 INSERT 唯一缺失、必须由数据库生成的 owner 字段。 */
    public String generatedOwnerField() {
        String missing = null;
        for (String field : ownerFields) {
            if (knownOwner.containsKey(field) && knownOwner.get(field) != null) {
                continue;
            }
            if (missing != null) {
                throw new IllegalArgumentException(
                        "protected insert requires exactly one database-generated owner field");
            }
            missing = field;
        }
        if (missing == null) {
            throw new IllegalArgumentException(
                    "protected insert requires exactly one database-generated owner field");
        }
        return missing;
    }

    /** 把数据库返回的生成键补入 INSERT owner；已有完整 owner 时直接返回它的安全快照。 */
    public Map<String, Object> resolveInsertOwner(SqlWriteResult result) {
        SqlWriteResult safeResult = Objects.requireNonNull(result, "protected write result must not be null");
        Map<String, Object> owner = new LinkedHashMap<>(knownOwner());
        if (!requiresGeneratedKeys()) {
            return Collections.unmodifiableMap(owner);
        }
        String field = generatedOwnerField();
        if (safeResult.generatedKeys().size() != 1) {
            throw new IllegalStateException("protected insert did not return one complete owner key");
        }
        DynamicRow generated = safeResult.generatedKeys().getFirst();
        Object value = generated.containsKey(field) ? generated.get(field) : generated.value(0);
        owner.put(field, Objects.requireNonNull(
                value, "protected insert generated owner key must not be null"));
        return Collections.unmodifiableMap(owner);
    }

    /** 保证 UPDATE 预读到的 owner 集合与实际受影响行集合一致。 */
    public void requireStableOwnerSet(List<? extends Map<String, ?>> owners, SqlWriteResult result) {
        Objects.requireNonNull(owners, "protected write owners must not be null");
        SqlWriteResult safeResult = Objects.requireNonNull(result, "protected write result must not be null");
        if (kind == Kind.UPDATE && safeResult.affectedRows() != owners.size()) {
            throw new IllegalStateException("protected update row set changed concurrently");
        }
    }

    /** 按 ownerFields 的既定顺序读取查询行。 */
    public Map<String, Object> ownerFrom(DynamicRow row) {
        DynamicRow safeRow = Objects.requireNonNull(row, "protected owner row must not be null");
        Map<String, Object> owner = new LinkedHashMap<>(ownerFields.size());
        for (int index = 0; index < ownerFields.size(); index++) {
            owner.put(ownerFields.get(index), safeRow.value(index));
        }
        return Map.copyOf(owner);
    }

    /** 按侧索引 SQL 的 owner、字段标签、可选令牌顺序生成绑定参数。 */
    public List<Object> sideIndexParameters(Map<String, Object> owner,
                                            FieldTokens field,
                                            byte[] token) {
        return sideIndexParameters(owner, field, (Object) (token == null ? null : token.clone()));
    }

    /**
     * 内部 DML 直接绑定工作计划持有的只读令牌视图，避免经过公开数组访问器再次深复制。
     */
    @InternalApi
    public List<Object> sideIndexParameters(Map<String, Object> owner,
                                            FieldTokens field,
                                            int tokenIndex) {
        FieldTokens safeField = Objects.requireNonNull(field, "protected token field must not be null");
        return sideIndexParameters(owner, safeField, safeField.ownedToken(tokenIndex));
    }

    private List<Object> sideIndexParameters(Map<String, Object> owner,
                                             FieldTokens field,
                                             Object token) {
        Map<String, Object> safeOwner = Objects.requireNonNull(owner, "protected write owner must not be null");
        FieldTokens safeField = Objects.requireNonNull(field, "protected token field must not be null");
        List<Object> values = new ArrayList<>(ownerFields.size() + 2);
        ownerFields.forEach(name -> values.add(Objects.requireNonNull(
                safeOwner.get(name), "protected write owner value must not be null")));
        values.add(safeField.fieldTag());
        if (token != null) {
            values.add(token);
        }
        return values;
    }

    /**
     * 把 owner 查询已经捕获的主键集合重新附加到原业务 UPDATE，防止查询与写入之间出现的并发行漂移污染侧索引。
     *
     * @param owners 同一事务中先行读取的 owner 快照
     * @return 保留原业务条件并额外按 owner 收窄的参数化请求
     */
    public SqlRequest writeRequestForOwners(List<? extends Map<String, ?>> owners) {
        Objects.requireNonNull(owners, "protected write owners must not be null");
        if (kind != Kind.UPDATE) {
            return writeRequest;
        }
        if (owners.isEmpty()) {
            throw new IllegalArgumentException("protected update owners must not be empty");
        }
        long ownerParameterCount = Math.multiplyExact((long) owners.size(), ownerFields.size());
        if (ownerParameterCount + writeRequest.parameters().size() > MAX_SAFE_BOUND_PARAMETERS) {
            throw new IllegalArgumentException("protected update owner set exceeds safe parameter limit");
        }
        String restriction = owners.stream()
                .map(ignored -> "(" + ownerPredicateSql + ")")
                .collect(java.util.stream.Collectors.joining(" or "));
        List<Object> parameters = new ArrayList<>(writeRequest.parameters());
        for (Map<String, ?> owner : owners) {
            ownerFields.forEach(field -> parameters.add(Objects.requireNonNull(
                    owner.get(field), "protected write owner value must not be null")));
        }
        return new SqlRequest(writeRequest.sql() + " and (" + restriction + ")",
                              parameters,
                              writeRequest.bindMarkerStyle());
    }

    /**
     * 返回 owner 的独立容器快照；可变可绑定值再次返回不可变快照，避免调用方改写冷执行计划。
     *
     * @return owner 字段和值
     */
    @Override
    public Map<String, Object> knownOwner() {
        return snapshotOwner(knownOwner);
    }

    /**
     * 批量所有权边界只读访问构造期已经自有的 owner，用于在复制前完成预算预检。
     * 仅限本包内部使用；调用方不得修改返回 Map 或其值。
     */
    Map<String, Object> knownOwnerInternal() {
        return knownOwner;
    }

    /** 原子保护写入类型。 */
    public enum Kind {
        INSERT,
        UPDATE,
        UPSERT
    }

    /** 单个受保护字段的新令牌快照；空令牌表示只删除旧索引。 */
    public record FieldTokens(String fieldTag, List<byte[]> tokens) {
        public FieldTokens {
            fieldTag = requireText(fieldTag, "protected token field tag");
            tokens = ProtectedTokenStorage.snapshot(tokens);
        }

        @Override
        public List<byte[]> tokens() {
            return storage().publicCopy();
        }

        /** 从上游已经冻结的令牌视图建立工作计划，不再复制二进制载荷。 */
        @InternalApi
        public static FieldTokens owned(String fieldTag,
                                        int tokenCount,
                                        IntFunction<ByteBuffer> tokenAt) {
            return new FieldTokens(fieldTag, ProtectedTokenStorage.owned(tokenCount, tokenAt));
        }

        /** @return 工作计划持有的令牌数量。 */
        @InternalApi
        public int tokenCount() {
            return storage().size();
        }

        /** @return 指定令牌的独立只读绑定视图。 */
        @InternalApi
        public ByteBuffer ownedToken(int index) {
            return storage().readOnlyBuffer(index);
        }

        /** 供批量预算器直接估算自有载荷，不发布或复制令牌数组。 */
        long estimatedTokenBytes() {
            return storage().estimatedBytes();
        }

        private ProtectedTokenStorage storage() {
            return (ProtectedTokenStorage) tokens;
        }
    }

    private static String requireText(String value, String name) {
        String text = Objects.requireNonNull(value, name + " must not be null").trim();
        if (text.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return text;
    }

    private static Map<String, Object> snapshotOwner(Map<String, Object> owner) {
        Map<String, Object> snapshot = new LinkedHashMap<>(owner.size());
        owner.forEach((field, value) -> snapshot.put(
                Objects.requireNonNull(field, "protected owner field must not be null"),
                BindableValueSnapshots.immutableValue(value)));
        return Collections.unmodifiableMap(snapshot);
    }

    private static long positiveMinimum(long configured, long fallback) {
        return configured <= 0L ? fallback : Math.min(configured, fallback);
    }
}
