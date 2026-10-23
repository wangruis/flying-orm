package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.batch.BatchMemoryBudget;
import com.flying.orm.rdb.aggregate.AggregateRow;
import com.flying.orm.rdb.codec.LargeObjectValueCodec;
import com.flying.orm.rdb.codec.ArrayValueCodec;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlResultMemoryLimitExceededException;
import com.flying.orm.rdb.json.JsonValueCodec;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.observation.SqlStatementType;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.vector.VectorValueCodec;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.UnaryOperator;

/**
 * 把数据库驱动返回的值整理成 flying-orm 对外承诺的 Java 值。
 *
 * <p>这个类故意只在 {@code form} 包内使用。客户端负责组织查询、Scope 和执行保护，
 * 解码器只关心“某一列该怎样读”。两边分开后，新增数据库类型时不需要继续把
 * {@link ReactiveFormClient} 撑大，也不容易误碰查询安全流程。</p>
 *
 * <p>实例只保存不可变的 renderer 和实体模型注册表，可以被多个订阅并发复用。
 * 所有真正可能读取 LOB 的工作仍在 Reactor 链内发生，这里不会提前阻塞或订阅。</p>
 */
final class FormResultDecoder {

    static final RowMapper<DynamicRow> IDENTITY = row -> Objects.requireNonNull(row, "form row must not be null");

    private static final DatabaseType PROTECTED_BINARY_TYPE = DatabaseType.of("PROTECTED_BINARY");

    private final FormDataSqlRenderer renderer;

    private final EntityModelRegistry entityModels;
    private final boolean decodeProtectedValues;

    FormResultDecoder(FormDataSqlRenderer renderer, EntityModelRegistry entityModels) {
        this(renderer, entityModels, true);
    }

    private FormResultDecoder(FormDataSqlRenderer renderer, EntityModelRegistry entityModels,
                              boolean decodeProtectedValues) {
        this.renderer = Objects.requireNonNull(renderer, "form data sql renderer must not be null");
        this.entityModels = Objects.requireNonNull(entityModels, "entity model registry must not be null");
        this.decodeProtectedValues = decodeProtectedValues;
    }

    /** CONTAINS 先复核编码后的明文；业务 codec 与脱敏由最终结果阶段处理。 */
    FormResultDecoder forContains() {
        return new FormResultDecoder(renderer, entityModels, false);
    }

    private UnaryOperator<DynamicRow> protection(DynamicForm form, DataScope scope,
                                                 SensitiveDisplayMode displayMode) {
        return decodeProtectedValues ? renderer.protection().resultOperation(form, scope, displayMode)
                : renderer.protection().decryptOperation(form, scope)::transform;
    }

    /**
     * 实体回读和动态表单写入共用同一套 codec，避免同一个字段写进去是一种类型，
     * 查出来却走了另一套转换规则。
     */
    <T> RowMapper<T> rowMapper(Class<T> type, String nullMessage) {
        return entityModels.decodedRowMapper(Objects.requireNonNull(type, nullMessage), renderer.valueCodecs());
    }

    /** JDBC 类型化结果把字段解码与布局映射一次绑定；需要行级保护或预算时沿用普通行链。 */
    <T> RowMapper<T> rowMapper(FormReadPlanner.RowReadPlan plan, RowMapper<T> mapper) {
        RowMapper<DynamicRow> decode = rowDecoder(plan);
        if (decode == IDENTITY) return mapper;
        return new RowMapper<>() {
            @Override
            public T map(DynamicRow row) {
                return mapper.map(decode.map(row));
            }

            @Override
            public Function<IntFunction<Object>, T> bindColumns(DynamicRow columns) {
                if (!plan.form().protections().isEmpty() || plan.options().maxResultBytes() != 0L
                        || plan.joinResultPlan() != null && !plan.joinResultPlan().direct()) return null;
                Function<IntFunction<Object>, T> bound = mapper.bindColumns(columns);
                if (bound == null) return null;
                FormFieldDecodingPlan active = plan.resultDecodingPlan() == null
                        ? decodingPlan(plan.form()) : plan.resultDecodingPlan();
                List<BoundField> fields = bindFields(plan.form(), active, columns);
                if (fields.isEmpty()) return bound;
                Object[] decoded = new Object[columns.columnCount()];
                IntFunction<Object> reader = index -> decoded[index];
                return values -> {
                    for (int index = 0; index < decoded.length; index++) decoded[index] = values.apply(index);
                    for (BoundField field : fields) {
                        Object value = decoded[field.index()];
                        if (value != null) decoded[field.index()] = decodeMaterializedValue(field.field(), field.decoding(), value);
                    }
                    return bound.apply(reader);
                };
            }
        };
    }

    /** 普通行和关联行共用解码入口；只有已声明的关联保护才增加转换阶段。 */
    RowMapper<DynamicRow> rowDecoder(FormReadPlanner.RowReadPlan plan) {
        RowMapper<DynamicRow> decode = rowDecoder(plan.form(), plan.options(), plan.scope(),
                plan.displayMode(), plan.resultDecodingPlan());
        JoinResultProtector.ResultPlan joined = plan.joinResultPlan();
        if (joined == null || joined.direct()) return decode;
        if (plan.options().maxResultBytes() == 0L) {
            return row -> joined.transform(decode.map(row));
        }
        DecodedResultBudget budget = new DecodedResultBudget(plan.options().maxResultBytes());
        return row -> budget.accept(joined.transform(decode.map(row)));
    }

    Flux<DynamicRow> decodeRows(FormReadPlanner.RowReadPlan plan, Flux<DynamicRow> rows) {
        Flux<DynamicRow> decoded = decodeRows(plan.form(), rows, plan.options(), plan.scope(),
                plan.displayMode(), plan.decodingFields(), plan.resultDecodingPlan());
        JoinResultProtector.ResultPlan joined = plan.joinResultPlan();
        if (joined == null || joined.direct()) return decoded;
        return protectDecodedRows(ReactiveProtectionCpuBoundary.sequence(decoded,
                joined.requiresCpuBoundary(), ReactiveProtectionCpuBoundary.QUERY_PREFETCH)
                .map(joined::transform), plan.options());
    }

    /**
     * 创建一次同步查询专用的逐行解码器。计划、累计内存和行号都绑定本次查询，调用方必须按结果顺序单线程使用。
     */
    RowMapper<DynamicRow> rowDecoder(DynamicForm form,
                                     SqlExecutionOptions options,
                                     DataScope scope,
                                     SensitiveDisplayMode displayMode) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options, "sql execution options must not be null");
        DataScope safeScope = Objects.requireNonNull(scope, "data scope must not be null");
        SensitiveDisplayMode safeDisplayMode = Objects.requireNonNull(
                displayMode, "sensitive display mode must not be null");
        FormFieldDecodingPlan decodingPlan = decodingPlan(safeForm);
        return rowDecoder(safeForm, safeOptions, safeScope, safeDisplayMode, decodingPlan);
    }

    RowMapper<DynamicRow> rowDecoder(DynamicForm form,
                                             SqlExecutionOptions options,
                                             DataScope scope,
                                             SensitiveDisplayMode displayMode,
                                             FormFieldDecodingPlan decodingPlan) {
        FormFieldDecodingPlan activePlan = decodingPlan == null ? decodingPlan(form) : decodingPlan;
        boolean needsDecoding = !form.protections().isEmpty() || !activePlan.isEmpty();
        if (!needsDecoding) {
            return IDENTITY;
        }
        if (form.protections().isEmpty() && options.maxResultBytes() == 0L) {
            return row -> decodeMaterializedRow(form, activePlan, row);
        }
        UnaryOperator<DynamicRow> protection = protection(form, scope, displayMode);
        return new RowMapper<>() {
            private final DecodedResultBudget budget = new DecodedResultBudget(options.maxResultBytes());

            @Override
            public DynamicRow map(DynamicRow source) {
                DynamicRow row = Objects.requireNonNull(source, "form row must not be null");
                if (!activePlan.isEmpty()) {
                    row = decodeMaterializedRow(form, activePlan, row);
                }
                row = protection.apply(row);
                return budget.accept(row);
            }
        };
    }

    /**
     * 没有特殊类型时直接返回原始 Flux，不复制行，也不增加逐列扫描。
     * 普通特殊值直接同步映射；只有当前投影确实含 LOB 或保护密文时才使用 {@code concatMap}，
     * 保证异步读取有序且受下游背压约束。
     */
    Flux<DynamicRow> decodeRows(DynamicForm form,
                                Flux<DynamicRow> rows,
                                SqlExecutionOptions options) {
        return decodeRows(form, rows, options, DataScope.none(), SensitiveDisplayMode.DECLARED);
    }

    Flux<DynamicRow> decodeRows(DynamicForm form,
                                Flux<DynamicRow> rows,
                                SqlExecutionOptions options,
                                DataScope scope,
                                SensitiveDisplayMode displayMode) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        return decodeRows(safeForm, rows, options, scope, displayMode, decodingPlan(safeForm));
    }

    Flux<DynamicRow> decodeRows(DynamicForm form,
                                Flux<DynamicRow> rows,
                                SqlExecutionOptions options,
                                DataScope scope,
                                SensitiveDisplayMode displayMode,
                                List<String> projectedFields) {
        return decodeRows(form, rows, options, scope, displayMode, projectedFields, null);
    }

    Flux<DynamicRow> decodeRows(DynamicForm form, Flux<DynamicRow> rows, SqlExecutionOptions options,
                                DataScope scope, SensitiveDisplayMode displayMode, List<String> projectedFields,
                                FormFieldDecodingPlan projectionPlan) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        FormFieldDecodingPlan projectedPlan = projectionPlan == null
                ? decodingPlan(safeForm).projected(safeForm, projectedFields) : projectionPlan;
        return decodeRows(safeForm, rows, options, scope, displayMode, projectedPlan);
    }

    Flux<DynamicRow> decodeRows(DynamicForm safeForm,
                                        Flux<DynamicRow> rows,
                                        SqlExecutionOptions options,
                                        DataScope scope,
                                        SensitiveDisplayMode displayMode,
                                        FormFieldDecodingPlan decodingPlan) {
        Flux<DynamicRow> safeRows = Objects.requireNonNull(rows, "form rows must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options, "sql execution options must not be null");
        boolean needsFieldDecoding = !safeForm.protections().isEmpty() || !decodingPlan.isEmpty();
        if (!needsFieldDecoding) {
            return safeRows;
        }
        Flux<DynamicRow> fieldDecodedRows;
        if (decodingPlan.isEmpty()) {
            fieldDecodedRows = safeRows;
        } else if (decodingPlan.requiresAsync()) {
            fieldDecodedRows = safeRows.concatMap(
                    row -> decodeFields(safeForm, decodingPlan, row, safeOptions));
        } else {
            fieldDecodedRows = safeRows.map(row -> decodeMaterializedRow(safeForm, decodingPlan, row));
        }
        Flux<DynamicRow> decodedRows;
        if (safeForm.protections().isEmpty()) {
            decodedRows = fieldDecodedRows;
        } else {
            UnaryOperator<DynamicRow> protection = protection(safeForm, scope, displayMode);
            Flux<DynamicRow> protectedRows = ReactiveProtectionCpuBoundary.sequence(
                    fieldDecodedRows, decodingPlan.requiresProtectionCpu(),
                    ReactiveProtectionCpuBoundary.QUERY_PREFETCH);
            decodedRows = protectedRows.map(protection);
        }
        return protectDecodedRows(decodedRows, safeOptions);
    }

    /**
     * 驱动执行器只能看到解码前的值。BLOB、CLOB 等值被读成 byte[] 或 String 后可能明显变大，
     * 所以解码结果还要独立过一次总量门禁。两个门禁约束的是前后两种表示，不会把同一份内存相加两次。
     */
    static Flux<DynamicRow> protectDecodedRows(Flux<DynamicRow> rows, SqlExecutionOptions options) {
        if (options.maxResultBytes() == 0L) {
            return rows;
        }
        return Flux.defer(() -> {
            DecodedResultBudget budget = new DecodedResultBudget(options.maxResultBytes());
            return rows.map(budget::accept);
        });
    }

    /** 每次查询独立计费；普通读取、JOIN、CONTAINS 和聚合共用最终解码预算。 */
    static final class DecodedResultBudget {
        private final long maxBytes;
        private long totalBytes;
        private long rowIndex;

        DecodedResultBudget(long maxBytes) {
            this.maxBytes = maxBytes;
        }

        DynamicRow accept(DynamicRow row) {
            if (maxBytes > 0) {
                acceptBytes(BatchMemoryBudget.estimateRowBytes(row));
            }
            return row;
        }

        AggregateRow accept(AggregateRow row) {
            if (maxBytes > 0) {
                acceptBytes(BatchMemoryBudget.estimateValueBytes(row.values()));
            }
            return row;
        }

        private void acceptBytes(long bytes) {
            long attemptedBytes = saturatedAdd(totalBytes, bytes);
            if (attemptedBytes == Long.MAX_VALUE || attemptedBytes > maxBytes) {
                throw new SqlResultMemoryLimitExceededException(
                        SqlStatementType.SELECT, maxBytes, attemptedBytes, rowIndex);
            }
            totalBytes = attemptedBytes;
            rowIndex++;
        }
    }

    private static long saturatedAdd(long left, long right) {
        return Long.MAX_VALUE - left < right ? Long.MAX_VALUE : left + right;
    }

    private Mono<DynamicRow> decodeFields(DynamicForm form,
                                           FormFieldDecodingPlan decodingPlan,
                                           DynamicRow row,
                                           SqlExecutionOptions options) {
        return Mono.defer(() -> {
            DynamicRow safeRow = Objects.requireNonNull(row, "form row must not be null");
            List<BoundField> fields = boundFields(form, decodingPlan, safeRow);
            return Flux.fromIterable(fields)
                       .concatMap(bound -> decodeField(bound, safeRow.value(bound.index()), options))
                       .collectMap(DecodedFieldValue::index, DecodedFieldValue::value)
                       .map(safeRow::withValues);
        });
    }

    private DynamicRow decodeMaterializedRow(DynamicForm form,
                                              FormFieldDecodingPlan decodingPlan,
                                              DynamicRow row) {
        DynamicRow safeRow = Objects.requireNonNull(row, "form row must not be null");
        List<BoundField> fields = boundFields(form, decodingPlan, safeRow);
        if (fields.isEmpty()) {
            return safeRow;
        }
        Map<Integer, Object> replacements = null;
        for (BoundField bound : fields) {
            Object rawValue = safeRow.value(bound.index());
            if (rawValue != null) {
                Object decoded = decodeMaterializedValue(bound.field(), bound.decoding(), rawValue);
                if (decoded != rawValue) {
                    if (replacements == null) {
                        replacements = new HashMap<>(fields.size());
                    }
                    replacements.put(bound.index(), decoded);
                }
            }
        }
        return replacements == null ? safeRow : safeRow.withValues(replacements);
    }

    private List<BoundField> boundFields(DynamicForm form,
                                         FormFieldDecodingPlan decodingPlan,
                                         DynamicRow row) {
        return row.mappingBinding(decodingPlan, () -> bindFields(form, decodingPlan, row));
    }

    private static List<BoundField> bindFields(DynamicForm form,
                                               FormFieldDecodingPlan decodingPlan,
                                               DynamicRow row) {
        List<BoundField> fields = new ArrayList<>(Math.min(row.columnCount(), decodingPlan.size()));
        for (int index = 0; index < row.columnCount(); index++) {
            DynamicField field = form.findField(row.columnName(index)).orElse(null);
            FormFieldDecodingPlan.Decoding decoding = field == null ? null : decodingPlan.decoding(field);
            if (decoding != null) {
                fields.add(new BoundField(index, field, decoding));
            }
        }
        return List.copyOf(fields);
    }

    private Object decodeMaterializedValue(DynamicField field,
                                           FormFieldDecodingPlan.Decoding decoding,
                                           Object rawValue) {
        return switch (decoding.kind()) {
            case ENCRYPTED -> LargeObjectValueCodec.read(rawValue, PROTECTED_BINARY_TYPE);
            case JSON -> JsonValueCodec.read(rawValue);
            case ARRAY -> ArrayValueCodec.readList(
                    rawValue, decoding.arrayType().getComponentType(), renderer.valueCodecs());
            case VECTOR -> VectorValueCodec.read(rawValue, field.length());
            case SCALAR -> decoding.scalar().read(rawValue);
            case LARGE_OBJECT -> LargeObjectValueCodec.read(rawValue, field.databaseType());
            case CUSTOM -> decodeCustom(decoding, rawValue);
            case CUSTOM_LARGE_OBJECT -> decodeCustom(
                    decoding, LargeObjectValueCodec.read(rawValue, field.databaseType()));
        };
    }

    private static Object decodeCustom(FormFieldDecodingPlan.Decoding decoding, Object value) {
        return decoding.customMapping().codec().read(value, decoding.customMapping().javaType());
    }

    /**
     * SQL 投影可能带出不在表单定义里的别名列，这类列必须原样保留。
     * {@link Mono#empty()} 表示“不替换当前列”，不是把这一列的值改成 null。
     */
    private Mono<DecodedFieldValue> decodeField(BoundField bound,
                                                Object rawValue,
                                                SqlExecutionOptions options) {
        if (rawValue == null) {
            return Mono.empty();
        }
        return switch (bound.decoding().kind()) {
            case ENCRYPTED -> LargeObjectValueCodec.readReactive(rawValue, PROTECTED_BINARY_TYPE, options)
                                                         .map(value -> new DecodedFieldValue(bound.index(), value));
            case LARGE_OBJECT -> LargeObjectValueCodec.readReactive(
                    rawValue, bound.field().databaseType(), options)
                    .map(value -> new DecodedFieldValue(bound.index(), value));
            case CUSTOM_LARGE_OBJECT -> LargeObjectValueCodec.readReactive(
                    rawValue, bound.field().databaseType(), options)
                    .map(value -> new DecodedFieldValue(
                            bound.index(), decodeCustom(bound.decoding(), value)));
            // 解码结果允许是 Java null，例如数据库返回 JSON 文本 "null"。把 null 放进包装对象，
            // 不能让 Mono.fromSupplier(null) 把它误解成“这一列无需替换”。
            case JSON, ARRAY, VECTOR, SCALAR, CUSTOM -> Mono.fromSupplier(
                    () -> new DecodedFieldValue(bound.index(), decodeMaterializedValue(
                            bound.field(), bound.decoding(), rawValue)));
        };
    }

    private FormFieldDecodingPlan decodingPlan(DynamicForm form) {
        return renderer.resultDecodingPlan(form);
    }

    private record DecodedFieldValue(int index, Object value) {
    }

    private record BoundField(int index,
                              DynamicField field,
                              FormFieldDecodingPlan.Decoding decoding) {
    }

}
