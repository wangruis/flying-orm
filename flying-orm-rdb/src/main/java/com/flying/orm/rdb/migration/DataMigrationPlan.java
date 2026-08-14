package com.flying.orm.rdb.migration;

import com.flying.orm.core.sql.render.SqlRequest;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * 服务端创建的数据迁移计划。每一步必须明确补偿 SQL，避免执行失败后只留下“请人工恢复”的模糊状态。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public record DataMigrationPlan(String id, List<DataMigrationStep> steps) {

    public DataMigrationPlan {
        id = requireText(id, "data migration plan id");
        steps = List.copyOf(Objects.requireNonNull(steps, "data migration steps must not be null"));
        if (steps.isEmpty()) {
            throw new IllegalArgumentException("data migration plan must contain at least one step");
        }
        Set<String> ids = new HashSet<>();
        for (DataMigrationStep step : steps) {
            if (!ids.add(step.id())) {
                throw new IllegalArgumentException("duplicate data migration step id");
            }
        }
    }

    public static Builder builder(String id) {
        return new Builder(id);
    }

    private static String requireText(String value, String name) {
        String text = Objects.requireNonNull(value, name + " must not be null").trim();
        if (text.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return text;
    }

    public static final class Builder {
        private final String id;
        private final List<DataMigrationStep> steps = new ArrayList<>();

        private Builder(String id) {
            this.id = requireText(id, "data migration plan id");
        }

        public Builder step(String id, SqlRequest forward, SqlRequest rollback) {
            steps.add(new DataMigrationStep(id, forward, rollback));
            return this;
        }

        public DataMigrationPlan build() {
            return new DataMigrationPlan(id, steps);
        }
    }
}
