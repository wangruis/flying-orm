package com.flying.orm.rdb.codec;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.condition.ConditionValueShape;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.condition.TermExtensionDescriptor;
import com.flying.orm.core.condition.TermHandler;
import com.flying.orm.core.condition.TermRegistry;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.core.sql.render.SqlTermPackage;
import com.flying.orm.rdb.form.StructuredConditionCustomizer;
import com.flying.orm.rdb.form.StructuredConditionResolver;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.UnaryOperator;

/**
 * 整数位掩码集合条件：mask-any、mask-all、mask-contained-by 及 mask-not-*。
 * 调用方按数据库选择包；全部掩码参数绑定，保留 SQL NULL 语义，不改写空集合条件。
 *
 * @author wangr
 * @version v4.1.2
 */
public final class BitMaskTermHandlers {

    private static final TermRegistry TERMS = postgresql().terms();

    private BitMaskTermHandlers() {
    }

    public static SqlTermPackage postgresql() {
        return terms("postgresql", field -> "(" + field + " & ?)");
    }

    public static SqlTermPackage mysql() {
        // MySQL 数值位运算返回无符号整数；还原 BIGINT 符号才能比较含最高位的掩码。
        return terms("mysql", field -> "cast((" + field + " & ?) as signed)");
    }

    public static SqlTermPackage sqlServer() {
        return terms("sqlserver", field -> "(" + field + " & ?)");
    }

    public static SqlTermPackage oracle() {
        return terms("oracle", field -> "bitand(" + field + ", ?)");
    }

    /** 前端传数值掩码；字段、字符串及条件复杂度仍经过现有结构化条件策略。 */
    public static StructuredConditionResolver conditions() {
        return StructuredConditionResolver.composite(new StructuredConditionCustomizer() {
            @Override
            public StructuredConditionPolicy customize(StructuredConditionPolicy policy) {
                StructuredConditionPolicy configured = StructuredConditionCustomizer.super.customize(policy);
                for (TermHandler handler : TERMS.handlers()) {
                    configured = configured.allowOperator(handler.id());
                }
                return configured.withAdditionalTerms(TERMS);
            }
        });
    }

    private static SqlTermPackage terms(String dialect, UnaryOperator<String> bitAnd) {
        List<SqlTermHandler> handlers = new ArrayList<>();
        for (String operation : List.of("any", "all", "contained-by")) {
            for (boolean negated : new boolean[]{false, true}) {
                String id = "mask-" + (negated ? "not-" : "") + operation;
                boolean all = operation.equals("all");
                boolean any = operation.equals("any");
                String comparison = any != negated ? " <> " : " = ";
                handlers.add(SqlTermHandler.of(TermExtensionDescriptor.filter(id, Set.of(), all ? 2 : 1, 1),
                        ConditionValueShape.SCALAR, (term, context) -> {
                            if (!(term.value() instanceof Number)) {
                                throw new IllegalArgumentException("bit mask condition must be an integer number");
                            }
                            long mask = ValueCodecRegistry.standard().read(term.value(), Long.class);
                            String field = context.identifier(term.field());
                            String sql = bitAnd.apply(field) + comparison + (any ? "0" : all ? "?" : field);
                            return all ? SqlFragment.of(sql, mask, mask) : SqlFragment.of(sql, mask);
                        }));
            }
        }
        return SqlTermPackage.of("bit-mask-" + dialect, handlers);
    }
}
