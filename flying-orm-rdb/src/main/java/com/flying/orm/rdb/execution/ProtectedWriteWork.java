package com.flying.orm.rdb.execution;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.internal.MutableValueSnapshots;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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

    public ProtectedWriteWork {
        kind = Objects.requireNonNull(kind, "protected write kind must not be null");
        writeRequest = Objects.requireNonNull(writeRequest, "protected write request must not be null");
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
     * 返回 owner 的独立容器快照；数组值再次复制完整数组图，避免调用方改写冷执行计划。
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
            List<byte[]> copy = new ArrayList<>(Objects.requireNonNull(
                    tokens, "protected tokens must not be null").size());
            tokens.forEach(token -> copy.add(Objects.requireNonNull(
                    token, "protected token must not be null").clone()));
            tokens = List.copyOf(copy);
        }

        @Override
        public List<byte[]> tokens() {
            List<byte[]> copy = new ArrayList<>(tokens.size());
            tokens.forEach(token -> copy.add(token.clone()));
            return List.copyOf(copy);
        }

        /** 仅供同包预算器读取构造期自有值，不得把数组引用交给外部调用方。 */
        List<byte[]> tokensInternal() {
            return tokens;
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
                MutableValueSnapshots.arrayGraph(value)));
        return Collections.unmodifiableMap(snapshot);
    }
}
