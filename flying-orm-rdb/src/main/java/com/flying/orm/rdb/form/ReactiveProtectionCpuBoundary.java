package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionNode;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.form.DynamicForm;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/** Places only reachable protected-field CPU work on Reactor's bounded parallel workers. */
final class ReactiveProtectionCpuBoundary {

    static final int QUERY_PREFETCH = 8;
    private static final int MAX_BATCH_PREFETCH = 32;

    private ReactiveProtectionCpuBoundary() {
    }

    static <T> Flux<T> batch(Publisher<T> source, boolean enabled, int chunkSize) {
        return sequence(source, enabled, Math.min(chunkSize, MAX_BATCH_PREFETCH));
    }

    static <T> Flux<T> sequence(Publisher<T> source, boolean enabled, int prefetch) {
        Flux<T> flux = Flux.from(Objects.requireNonNull(source, "protected CPU source must not be null"));
        if (!enabled) {
            return flux;
        }
        if (prefetch <= 0) {
            throw new IllegalArgumentException("protected CPU prefetch must be positive");
        }
        return flux.publishOn(Schedulers.parallel(), prefetch);
    }

    static <T> Mono<T> plan(Supplier<? extends T> planner) {
        Supplier<? extends T> safePlanner = Objects.requireNonNull(
                planner, "protected CPU planner must not be null");
        return Mono.just(safePlanner)
                   .publishOn(Schedulers.parallel())
                   .map(Supplier::get);
    }

    static boolean writesEncryptedField(DynamicForm form, Map<String, ?> values) {
        DynamicForm safeForm = Objects.requireNonNull(form, "protected write form must not be null");
        Map<String, ?> safeValues = Objects.requireNonNull(values, "protected write values must not be null");
        if (safeForm.protections().encryptedFields().isEmpty()) {
            return false;
        }
        for (String field : safeValues.keySet()) {
            if (safeForm.findField(field)
                        .flatMap(found -> safeForm.protections().encrypted(found.name()))
                        .isPresent()) {
                return true;
            }
        }
        return false;
    }

    static boolean usesEncryptedCondition(DynamicForm form, ConditionGroup where) {
        DynamicForm safeForm = Objects.requireNonNull(form, "protected condition form must not be null");
        ConditionGroup safeWhere = Objects.requireNonNull(where, "protected condition must not be null");
        if (safeForm.protections().encryptedFields().isEmpty()) {
            return false;
        }
        for (ConditionNode child : safeWhere.children()) {
            if (child instanceof ConditionGroup group && usesEncryptedCondition(safeForm, group)) {
                return true;
            }
            if (child instanceof TermCondition term
                    && safeForm.findField(term.field())
                               .flatMap(found -> safeForm.protections().encrypted(found.name()))
                               .isPresent()) {
                return true;
            }
        }
        return false;
    }

    static boolean usesEncryptedScope(DynamicForm form, com.flying.orm.core.scope.DataScope scope) {
        Objects.requireNonNull(scope, "protected data scope must not be null");
        return scope.condition().map(where -> usesEncryptedCondition(form, where)).orElse(false);
    }

}
