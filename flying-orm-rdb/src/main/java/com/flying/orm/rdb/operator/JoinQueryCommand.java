package com.flying.orm.rdb.operator;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.join.JoinType;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 响应式与同步 DynamicForm JOIN 门面共享的单次命令状态。
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
final class JoinQueryCommand {

    private final JoinQuerySpec.Builder spec;
    private final SqlRenderer renderer;
    private final Map<DynamicForm, JoinSource> sources = new IdentityHashMap<>();
    private final Map<JoinSource, ConditionGroup.Builder> conditions = new java.util.LinkedHashMap<>();
    private final JoinSource root;
    private JoinSource lastJoined;
    private JoinQuerySpec built;

    JoinQueryCommand(DynamicForm rootForm, SqlRenderer renderer) {
        this.spec = JoinQuerySpec.builder(Objects.requireNonNull(rootForm, "join root form must not be null"));
        this.renderer = Objects.requireNonNull(renderer, "join condition renderer must not be null");
        this.root = spec.root();
        sources.put(rootForm, root);
    }

    JoinQueryCommand join(JoinType type, DynamicForm form, String leftField, String rightField) {
        requireMutable();
        JoinSource joined = spec.join(type, form, root, leftField, rightField);
        sources.put(Objects.requireNonNull(form, "joined form must not be null"), joined);
        lastJoined = joined;
        return this;
    }

    JoinQueryCommand andOn(String leftField, String rightField) {
        requireMutable();
        if (lastJoined == null) {
            throw new IllegalStateException("join ON extension requires a preceding join");
        }
        spec.andOn(lastJoined, root, leftField, rightField);
        return this;
    }

    JoinQueryCommand select(DynamicForm form, String field) {
        requireMutable();
        spec.select(source(form), field);
        return this;
    }

    JoinQueryCommand selectAs(DynamicForm form, String field, String alias) {
        requireMutable();
        spec.selectAs(source(form), field, alias);
        return this;
    }

    JoinQueryCommand where(DynamicForm form, String field, String operator, Object value) {
        requireMutable();
        JoinSource source = source(form);
        conditions.computeIfAbsent(source, ignored -> ConditionGroup.and(renderer.terms()))
                  .where(field, operator, value);
        return this;
    }

    JoinQueryCommand scope(DynamicForm form, DataScope scope) {
        requireMutable();
        spec.scope(source(form), scope);
        return this;
    }

    JoinQueryCommand orderBy(DynamicForm form, String field, PageSort.Direction direction) {
        requireMutable();
        spec.orderBy(source(form), field, direction);
        return this;
    }

    JoinQueryCommand declaredDisplay() {
        requireMutable();
        spec.declaredDisplay();
        return this;
    }

    JoinQueryCommand masked() {
        requireMutable();
        spec.masked();
        return this;
    }

    JoinQueryCommand showSensitive() {
        requireMutable();
        spec.showSensitive();
        return this;
    }

    JoinQuerySpec spec() {
        if (built == null) {
            conditions.forEach((source, where) -> spec.where(source, where.build()));
            built = spec.build();
        }
        return built;
    }

    private JoinSource source(DynamicForm form) {
        JoinSource source = sources.get(Objects.requireNonNull(form, "join form must not be null"));
        if (source == null) {
            throw new IllegalArgumentException("join source is not part of the query");
        }
        return source;
    }

    private void requireMutable() {
        if (built != null) {
            throw new IllegalStateException("join query has already been built");
        }
    }
}
