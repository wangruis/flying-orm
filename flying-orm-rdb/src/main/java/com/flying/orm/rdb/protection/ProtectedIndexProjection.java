package com.flying.orm.rdb.protection;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.rdb.internal.InternalApi;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 两套 Schema 元数据入口共用的受保护索引列投影规则。
 *
 * @author wangr
 * @version v3.3
 */
@InternalApi
public final class ProtectedIndexProjection {

    private ProtectedIndexProjection() {
    }

    /**
     * 把可等价表达的单列 EXACT 索引指向稳定哈希列；普通索引原样返回。
     * 名称、唯一性和排序方向仍由各自元数据类型保留。
     */
    public static List<String> columns(DynamicForm form, List<String> columns) {
        return columns(form, columns, false);
    }

    /** 唯一索引还必须与 CRUD 的稳定令牌声明一致，避免密钥轮换期间接受重复业务值。 */
    public static List<String> columns(DynamicForm form, List<String> columns, boolean unique) {
        DynamicForm safeForm = Objects.requireNonNull(form, "logical form must not be null");
        List<String> safeColumns = List.copyOf(Objects.requireNonNull(
                columns, "index columns must not be null"));
        List<String> protectedColumns = new ArrayList<>(1);
        for (String column : safeColumns) {
            if (safeForm.protections().encrypted(column).isPresent()) {
                protectedColumns.add(column);
            }
        }
        if (protectedColumns.isEmpty()) {
            return safeColumns;
        }
        if (safeColumns.size() != 1) {
            throw new IllegalArgumentException("composite index must not reference an encrypted field");
        }
        String column = protectedColumns.getFirst();
        EncryptedFieldDefinition definition = safeForm.protections().encrypted(column).orElseThrow();
        if (!definition.searchModes().contains(EncryptedSearchMode.EXACT)) {
            throw new IllegalArgumentException("index on an encrypted field requires exact search");
        }
        if (unique && !safeForm.field(column).unique()) {
            throw new IllegalArgumentException("unique encrypted index requires a unique logical field");
        }
        return List.of(ProtectedFormLayout.exactColumn(safeForm, column));
    }
}
