package com.flying.orm.rdb.form;

import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldUseRequirements;
import com.flying.orm.core.scope.FieldUseSnapshot;
import com.flying.orm.core.scope.JoinFieldDecision;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.scope.ScopeErrorCode;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 在 governed JOIN 的 SQL 生成前完成来源限定字段审批。
 *
 * @author wangr
 * @version v3.3
 */
final class JoinFieldUseGuard {

    private JoinFieldUseGuard() {
    }

    static FieldUseSnapshot approve(
            FieldUseRequirements requirements,
            Map<JoinSource, DataScope> scopes,
            FieldUsePolicy policy) {
        Map<JoinSource, DataScope> safeScopes = Objects.requireNonNull(
                scopes, "join scopes must not be null");
        Map<JoinSource, FieldScope> fieldScopes = new LinkedHashMap<>(safeScopes.size());
        safeScopes.forEach((source, scope) -> fieldScopes.put(
                Objects.requireNonNull(source, "join source must not be null"),
                Objects.requireNonNull(scope, "join data scope must not be null").fields()));
        FieldUseSnapshot snapshot = Objects.requireNonNull(
                policy, "field use policy must not be null")
                .approveJoin(requirements, fieldScopes);
        for (JoinFieldDecision decision : snapshot.joinDecisions()) {
            if (decision.denied()) {
                reject(decision);
            }
        }
        return snapshot;
    }

    private static void reject(JoinFieldDecision decision) {
        JoinSource source = decision.field().source();
        String description = "source[" + source.ordinal() + ':' + source.form().id()
                + "]." + decision.field().field();
        throw new ScopeAccessException(
                decision.use().write()
                        ? ScopeErrorCode.FIELD_NOT_WRITABLE : ScopeErrorCode.FIELD_NOT_READABLE,
                source.form().id(),
                decision.field().field(),
                "join field [" + description + "] is not allowed for "
                        + decision.use() + " from " + decision.origin());
    }
}
