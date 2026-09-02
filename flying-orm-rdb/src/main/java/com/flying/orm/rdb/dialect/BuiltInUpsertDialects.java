package com.flying.orm.rdb.dialect;

import com.flying.orm.rdb.internal.dialect.StagedUpsertDialect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

/** 内置 upsert 方言共享的阶段参数模型和 SQL 片段。 */
final class BuiltInUpsertDialects {

    private BuiltInUpsertDialects() {
    }

    static UpsertDialect h2() {
        return dialect((table, stage) -> "merge into " + table + " target using (values ("
                + stage.parameterExpressions() + ")) source (" + join(stage.parameterColumns()) + ") "
                + mergeBody(stage, false));
    }

    static UpsertDialect mysql() {
        return dialect((table, stage) -> {
            StringJoiner sets = new StringJoiner(", ");
            addMySqlIdentityGuard(sets, stage.conflictColumns());
            for (String column : stage.updateColumns()) {
                String value = stage.insertMembership().contains(column)
                        ? "values(" + column + ")"
                        : stage.expression(column);
                sets.add(column + " = " + value);
            }
            return "insert into " + table + " (" + join(stage.insertColumns()) + ") values ("
                    + stage.expressions(stage.insertColumns()) + ") on duplicate key update " + sets;
        });
    }

    static UpsertDialect postgresql() {
        return dialect((table, stage) -> {
            String sql = "insert into " + table + " (" + join(stage.insertColumns()) + ") values ("
                    + stage.expressions(stage.insertColumns()) + ") on conflict ("
                    + join(stage.conflictColumns()) + ") ";
            if (stage.updateColumns().isEmpty()) {
                return sql + "do nothing";
            }
            StringJoiner sets = new StringJoiner(", ");
            for (String column : stage.updateColumns()) {
                String value = stage.insertMembership().contains(column)
                        ? "excluded." + column
                        : stage.expression(column);
                sets.add(column + " = " + value);
            }
            return sql + "do update set " + sets;
        });
    }

    static UpsertDialect oracle() {
        return dialect((table, stage) -> {
            StringJoiner sourceColumns = new StringJoiner(", ");
            for (int index = 0; index < stage.parameterColumns().size(); index++) {
                sourceColumns.add(stage.valueExpressions().get(index)
                                          + " as " + stage.parameterColumns().get(index));
            }
            return "merge into " + table + " target using (select " + sourceColumns + " from dual) source "
                    + mergeBody(stage, true);
        });
    }

    static UpsertDialect sqlServer() {
        return dialect((table, stage) -> "merge into " + table
                + " with (holdlock) as target using (values (" + stage.parameterExpressions()
                + ")) as source (" + join(stage.parameterColumns()) + ") "
                + mergeBody(stage, false) + ";");
    }

    static List<String> markers(int count) {
        List<String> markers = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            markers.add("?");
        }
        return List.copyOf(markers);
    }

    private static UpsertDialect dialect(StagedRenderer renderer) {
        return new StagedUpsertDialect() {
            @Override
            public String render(String table,
                                 List<String> columns,
                                 List<String> conflictColumns,
                                 List<String> updateColumns,
                                 List<String> valueExpressions) {
                return renderer.render(table, Stage.create(columns,
                                                           conflictColumns,
                                                           updateColumns,
                                                           columns,
                                                           valueExpressions));
            }

            @Override
            public String renderStaged(String table,
                                       List<String> insertColumns,
                                       List<String> conflictColumns,
                                       List<String> updateColumns,
                                       List<String> parameterColumns,
                                       List<String> valueExpressions) {
                return renderer.render(table, Stage.create(insertColumns,
                                                           conflictColumns,
                                                           updateColumns,
                                                           parameterColumns,
                                                           valueExpressions));
            }
        };
    }

    private static String mergeBody(Stage stage, boolean wrapPredicate) {
        StringJoiner predicates = new StringJoiner(" and ");
        for (String column : stage.conflictColumns()) {
            predicates.add("target." + column + " = source." + column);
        }
        String predicate = wrapPredicate ? "(" + predicates + ")" : predicates.toString();
        StringBuilder body = new StringBuilder("on ").append(predicate);
        if (!stage.updateColumns().isEmpty()) {
            StringJoiner sets = new StringJoiner(", ");
            for (String column : stage.updateColumns()) {
                sets.add("target." + column + " = source." + column);
            }
            body.append(" when matched then update set ").append(sets);
        }
        StringJoiner sourceValues = new StringJoiner(", ");
        for (String column : stage.insertColumns()) {
            sourceValues.add("source." + column);
        }
        return body.append(" when not matched then insert (")
                   .append(join(stage.insertColumns()))
                   .append(") values (")
                   .append(sourceValues)
                   .append(')')
                   .toString();
    }

    private static void addMySqlIdentityGuard(StringJoiner sets, List<String> conflictColumns) {
        StringJoiner identity = new StringJoiner(" and ");
        for (String column : conflictColumns) {
            identity.add(column + " <=> values(" + column + ")");
        }
        String guardColumn = conflictColumns.getFirst();
        sets.add(guardColumn + " = if(" + identity + ", " + guardColumn
                         + ", (select null union all select null))");
    }

    private static String join(List<String> values) {
        return String.join(", ", values);
    }

    @FunctionalInterface
    private interface StagedRenderer {
        String render(String table, Stage stage);
    }

    private record Stage(List<String> insertColumns,
                         List<String> conflictColumns,
                         List<String> updateColumns,
                         List<String> parameterColumns,
                         List<String> valueExpressions,
                         Map<String, String> expressionByColumn,
                         Set<String> insertMembership) {

        static Stage create(List<String> insertColumns,
                            List<String> conflictColumns,
                            List<String> updateColumns,
                            List<String> parameterColumns,
                            List<String> valueExpressions) {
            insertColumns = requireColumns(insertColumns, "upsert insert columns must not be empty");
            conflictColumns = requireColumns(conflictColumns, "upsert conflict columns must not be empty");
            updateColumns = List.copyOf(Objects.requireNonNull(
                    updateColumns, "upsert update columns must not be null"));
            parameterColumns = requireColumns(parameterColumns, "upsert parameter columns must not be empty");
            valueExpressions = List.copyOf(Objects.requireNonNull(
                    valueExpressions, "upsert value expressions must not be null"));
            if (parameterColumns.size() != valueExpressions.size()) {
                throw new IllegalArgumentException("upsert value expression count must match parameter column count");
            }
            Map<String, String> expressionByColumn = HashMap.newHashMap(parameterColumns.size());
            // The public dialect SPI retains the first expression for a repeated parameter column.
            for (int index = 0; index < parameterColumns.size(); index++) {
                expressionByColumn.putIfAbsent(parameterColumns.get(index), valueExpressions.get(index));
            }
            Set<String> parameterMembership = expressionByColumn.keySet();
            if (!parameterMembership.containsAll(insertColumns)
                    || !parameterMembership.containsAll(updateColumns)) {
                throw new IllegalArgumentException("upsert parameter columns must cover insert and update columns");
            }
            return new Stage(insertColumns, conflictColumns, updateColumns, parameterColumns, valueExpressions,
                             expressionByColumn, Set.copyOf(insertColumns));
        }

        String parameterExpressions() {
            return String.join(", ", valueExpressions);
        }

        String expressions(List<String> columns) {
            StringJoiner expressions = new StringJoiner(", ");
            columns.forEach(column -> expressions.add(expression(column)));
            return expressions.toString();
        }

        String expression(String column) {
            String expression = expressionByColumn.get(column);
            if (expression == null) {
                throw new IllegalArgumentException("upsert parameter column is missing: " + column);
            }
            return expression;
        }

        private static List<String> requireColumns(List<String> columns, String message) {
            List<String> safeColumns = List.copyOf(Objects.requireNonNull(
                    columns, "upsert columns must not be null"));
            if (safeColumns.isEmpty()) {
                throw new IllegalArgumentException(message);
            }
            return safeColumns;
        }
    }
}
