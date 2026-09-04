package com.flying.orm.rdb.form;

import com.flying.orm.core.field.FieldIdentity;
import com.flying.orm.core.page.CursorPosition;
import com.flying.orm.core.page.KeysetPageResult;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.rdb.result.DynamicRow;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.UnaryOperator;

/**
 * keyset 物理投影与业务可见投影之间的固定布局。
 *
 * <p>排序字段即使没有投影权限，也可在 FieldUse 授权后用保留别名参加游标计算；别名列在生成
 * CursorPosition 后立刻剥离。没有隐藏列时直接返回原 DynamicRow，不给常见路径增加复制。</p>
 *
 * @author wangr
 * @version v3.2
 */
final class HiddenProjectionLayout {

    private static final String HIDDEN_ALIAS_PREFIX = "__fo_ks_";

    private final List<Projection> selections;
    private final List<String> decodingFields;
    private final List<String> positionLabels;
    private final boolean hidden;
    private final UnaryOperator<String> toLogical;
    private final UnaryOperator<String> toPhysical;

    private HiddenProjectionLayout(List<Projection> selections,
                                   List<String> positionLabels,
                                   boolean hidden) {
        this.selections = List.copyOf(selections);
        this.decodingFields = this.selections.stream().map(Projection::field).toList();
        this.positionLabels = List.copyOf(positionLabels);
        this.hidden = hidden;
        if (hidden) {
            Map<String, String> logicalByLabel = new LinkedHashMap<>();
            Map<String, String> labelByLogical = new LinkedHashMap<>();
            for (Projection selection : this.selections) {
                logicalByLabel.put(FieldIdentity.of(selection.label()).key(), selection.field());
                labelByLogical.put(FieldIdentity.of(selection.field()).key(), selection.label());
            }
            Map<String, String> frozenLogicalByLabel = Map.copyOf(logicalByLabel);
            Map<String, String> frozenLabelByLogical = Map.copyOf(labelByLogical);
            this.toLogical = column -> renamed(
                    frozenLogicalByLabel, column, "physical keyset row contains an unknown selected column");
            this.toPhysical = column -> renamed(
                    frozenLabelByLogical, column, "decoded keyset row contains an unknown selected field");
        } else {
            this.toLogical = UnaryOperator.identity();
            this.toPhysical = UnaryOperator.identity();
        }
    }

    static HiddenProjectionLayout of(
            List<String> visibleFields,
            KeysetPageNormalizer.NormalizedKeysetPage page) {
        List<String> safeVisible = Objects.requireNonNull(
                visibleFields, "visible keyset fields must not be null");
        KeysetPageNormalizer.NormalizedKeysetPage safePage = Objects.requireNonNull(
                page, "normalized keyset page must not be null");

        Map<String, String> visibleByKey = new LinkedHashMap<>();
        List<Projection> selections = new ArrayList<>(
                safeVisible.size() + safePage.sorts().size());
        Set<String> usedLabels = new LinkedHashSet<>();
        for (String visible : safeVisible) {
            FieldIdentity identity = FieldIdentity.of(visible);
            if (visibleByKey.putIfAbsent(identity.key(), identity.name()) != null) {
                throw new IllegalArgumentException("visible keyset projection must not contain duplicates");
            }
            usedLabels.add(identity.key());
            selections.add(new Projection(identity.name(), identity.name(), false));
        }

        Map<String, String> hiddenLabels = new LinkedHashMap<>();
        int aliasIndex = 0;
        for (KeysetSort sort : safePage.sorts()) {
            String key = FieldIdentity.of(sort.field()).key();
            if (visibleByKey.containsKey(key) || hiddenLabels.containsKey(key)) {
                continue;
            }
            String alias;
            do {
                alias = HIDDEN_ALIAS_PREFIX + aliasIndex++;
            } while (!usedLabels.add(FieldIdentity.of(alias).key()));
            hiddenLabels.put(key, alias);
            selections.add(new Projection(sort.field(), alias, true));
        }

        List<String> positionLabels = new ArrayList<>(safePage.sorts().size());
        for (KeysetSort sort : safePage.sorts()) {
            String key = FieldIdentity.of(sort.field()).key();
            positionLabels.add(visibleByKey.getOrDefault(key, hiddenLabels.get(key)));
        }
        return new HiddenProjectionLayout(
                selections, positionLabels, !hiddenLabels.isEmpty());
    }

    List<Projection> selections() {
        return selections;
    }

    List<String> decodingFields() {
        return decodingFields;
    }

    boolean hasHiddenSelections() {
        return hidden;
    }

    /** 把 SQL 保留别名临时还原成表单字段名，让隐藏游标值也走同一字段 codec。 */
    DynamicRow logicalRowForDecoding(DynamicRow physicalRow) {
        DynamicRow safeRow = Objects.requireNonNull(
                physicalRow, "physical keyset row must not be null");
        if (!hidden) {
            return safeRow;
        }
        return safeRow.renameColumnsBound(toLogical);
    }

    /** codec/保护处理完成后恢复 SQL 布局，随后统一生成位置并剥离隐藏列。 */
    DynamicRow physicalRowAfterDecoding(DynamicRow logicalRow) {
        DynamicRow safeRow = Objects.requireNonNull(
                logicalRow, "decoded keyset row must not be null");
        if (!hidden) {
            return safeRow;
        }
        return safeRow.renameColumnsBound(toPhysical);
    }

    private static String renamed(Map<String, String> names, String column, String message) {
        String renamed = names.get(FieldIdentity.of(column).key());
        if (renamed == null) {
            throw new IllegalArgumentException(message);
        }
        return renamed;
    }

    CursorPosition nextPosition(DynamicRow physicalRow) {
        DynamicRow safeRow = Objects.requireNonNull(
                physicalRow, "physical keyset row must not be null");
        List<Object> values = new ArrayList<>(positionLabels.size());
        for (String label : positionLabels) {
            if (!safeRow.containsKey(label)) {
                throw new IllegalArgumentException(
                        "physical keyset row does not contain a required cursor column");
            }
            values.add(safeRow.get(label));
        }
        return CursorPosition.of(values);
    }

    DynamicRow visibleRow(DynamicRow physicalRow) {
        DynamicRow safeRow = Objects.requireNonNull(
                physicalRow, "physical keyset row must not be null");
        if (!hidden) {
            return safeRow;
        }
        Map<String, Object> visible = new LinkedHashMap<>();
        for (Projection selection : selections) {
            if (selection.hidden()) {
                continue;
            }
            if (!safeRow.containsKey(selection.label())) {
                throw new IllegalArgumentException(
                        "physical keyset row does not contain a visible projection column");
            }
            visible.put(selection.label(), safeRow.get(selection.label()));
        }
        return DynamicRow.copyOf(visible);
    }

    KeysetPageResult<DynamicRow> finish(
            List<DynamicRow> physicalRows,
            int pageSize,
            UnaryOperator<DynamicRow> publisher) {
        List<DynamicRow> safeRows = Objects.requireNonNull(
                physicalRows, "physical keyset rows must not be null");
        if (pageSize < 1) {
            throw new IllegalArgumentException("keyset page size must be positive");
        }
        UnaryOperator<DynamicRow> safePublisher = Objects.requireNonNull(
                publisher, "keyset row publisher must not be null");
        int resultSize = Math.min(pageSize, safeRows.size());
        boolean hasMore = safeRows.size() > pageSize;
        List<DynamicRow> visibleRows = new ArrayList<>(resultSize);
        for (int index = 0; index < resultSize; index++) {
            visibleRows.add(safePublisher.apply(visibleRow(safeRows.get(index))));
        }
        CursorPosition next = hasMore && resultSize > 0
                ? nextPosition(safeRows.get(resultSize - 1))
                : CursorPosition.first();
        return new KeysetPageResult<>(visibleRows, next, hasMore);
    }

    /** 一项物理 SELECT 投影；hidden 项必须用 label 作为 SQL alias。 */
    record Projection(String field, String label, boolean hidden) {

        Projection {
            field = FieldIdentity.of(field).name();
            label = FieldIdentity.of(label).name();
        }
    }
}
