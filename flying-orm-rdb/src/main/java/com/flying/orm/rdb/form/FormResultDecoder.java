package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.batch.BatchMemoryBudget;
import com.flying.orm.rdb.codec.LargeObjectValueCodec;
import com.flying.orm.rdb.codec.JdbcLegacyTemporalAdapter;
import com.flying.orm.rdb.codec.OffsetTimeValueCodec;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlResultMemoryLimitExceededException;
import com.flying.orm.rdb.json.JsonValueCodec;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.observation.SqlStatementType;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.vector.VectorValueCodec;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

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

    private static final DatabaseType PROTECTED_BINARY_TYPE = DatabaseType.of("PROTECTED_BINARY");

    private final FormDataSqlRenderer renderer;

    private final EntityModelRegistry entityModels;

    FormResultDecoder(FormDataSqlRenderer renderer, EntityModelRegistry entityModels) {
        this.renderer = Objects.requireNonNull(renderer, "form data sql renderer must not be null");
        this.entityModels = Objects.requireNonNull(entityModels, "entity model registry must not be null");
    }

    /**
     * 实体回读和动态表单写入共用同一套 codec，避免同一个字段写进去是一种类型，
     * 查出来却走了另一套转换规则。
     */
    <T> RowMapper<T> rowMapper(Class<T> type, String nullMessage) {
        return entityModels.rowMapper(Objects.requireNonNull(type, nullMessage), renderer.valueCodecs());
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
        boolean needsDecoding = !form.protections().isEmpty() || !decodingPlan.isEmpty();
        if (!needsDecoding) {
            return row -> Objects.requireNonNull(row, "form row must not be null");
        }
        ProtectedFieldRuntime.ResultOperation protection = renderer.protection().resultOperation(
                form, scope, displayMode);
        return new RowMapper<>() {
            private long totalBytes;
            private long rowIndex;

            @Override
            public DynamicRow map(DynamicRow source) {
                DynamicRow row = Objects.requireNonNull(source, "form row must not be null");
                if (!decodingPlan.isEmpty()) {
                    row = decodeMaterializedRow(form, decodingPlan, row);
                }
                row = protection.transform(row);
                if (options.maxResultBytes() > 0) {
                    totalBytes = saturatedAdd(totalBytes, BatchMemoryBudget.estimateRowBytes(row));
                    if (totalBytes == Long.MAX_VALUE || totalBytes > options.maxResultBytes()) {
                        throw new SqlResultMemoryLimitExceededException(
                                SqlStatementType.SELECT, options.maxResultBytes(), totalBytes, rowIndex);
                    }
                    rowIndex++;
                }
                return row;
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
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        FormFieldDecodingPlan projectedPlan = decodingPlan(safeForm).projected(
                safeForm, projectedFields);
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
            ProtectedFieldRuntime.ResultOperation protection = renderer.protection().resultOperation(
                    safeForm, scope, displayMode);
            Flux<DynamicRow> protectedRows = ReactiveProtectionCpuBoundary.sequence(
                    fieldDecodedRows, decodingPlan.requiresProtectionCpu(),
                    ReactiveProtectionCpuBoundary.QUERY_PREFETCH);
            decodedRows = protectedRows.map(protection::transform);
        }
        return protectDecodedRows(decodedRows, safeOptions);
    }

    /**
     * 解码 JDBC 已经物化好的结果行，不创建 Publisher，也不经过 Reactor。
     *
     * <p>JDBC 执行器已经把 Blob/Clob 在 ResultSet 生命周期内读成 byte[]/String，这里只做字段声明对应的
     * JSON、数组、向量、时间和标量转换。转换后再次检查结果内存，是因为 JSON 文本变成对象树后可能明显变大。</p>
     */
    List<DynamicRow> decodeRows(DynamicForm form,
                                List<DynamicRow> rows,
                                SqlExecutionOptions options) {
        return decodeRows(form, rows, options, DataScope.none(), SensitiveDisplayMode.DECLARED);
    }

    List<DynamicRow> decodeRows(DynamicForm form,
                                List<DynamicRow> rows,
                                SqlExecutionOptions options,
                                DataScope scope,
                                SensitiveDisplayMode displayMode) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        List<DynamicRow> safeRows = Objects.requireNonNull(rows, "form rows must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options, "sql execution options must not be null");
        FormFieldDecodingPlan decodingPlan = decodingPlan(safeForm);
        boolean needsFieldDecoding = !safeForm.protections().isEmpty() || !decodingPlan.isEmpty();
        if (!needsFieldDecoding) {
            return safeRows;
        }
        DataScope safeScope = Objects.requireNonNull(scope, "data scope must not be null");
        SensitiveDisplayMode safeDisplayMode = Objects.requireNonNull(
                displayMode, "sensitive display mode must not be null");
        RowMapper<DynamicRow> decoder = rowDecoder(
                safeForm, safeOptions, safeScope, safeDisplayMode, decodingPlan);
        java.util.ArrayList<DynamicRow> decoded = new java.util.ArrayList<>(safeRows.size());
        for (DynamicRow row : safeRows) {
            decoded.add(decoder.map(row));
        }
        return List.copyOf(decoded);
    }

    /**
     * 驱动执行器只能看到解码前的值。BLOB、CLOB 等值被读成 byte[] 或 String 后可能明显变大，
     * 所以解码结果还要独立过一次总量门禁。两个门禁约束的是前后两种表示，不会把同一份内存相加两次。
     */
    private static Flux<DynamicRow> protectDecodedRows(Flux<DynamicRow> rows, SqlExecutionOptions options) {
        if (options.maxResultBytes() == 0L) {
            return rows;
        }
        return Flux.defer(() -> {
            AtomicLong totalBytes = new AtomicLong();
            AtomicLong rowIndex = new AtomicLong();
            return rows.handle((row, sink) -> {
                long index = rowIndex.getAndIncrement();
                long attemptedBytes = saturatedAdd(totalBytes.get(), BatchMemoryBudget.estimateRowBytes(row));
                if (attemptedBytes == Long.MAX_VALUE || attemptedBytes > options.maxResultBytes()) {
                    sink.error(new SqlResultMemoryLimitExceededException(
                            SqlStatementType.SELECT, options.maxResultBytes(), attemptedBytes, index));
                    return;
                }
                totalBytes.set(attemptedBytes);
                sink.next(row);
            });
        });
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
        Map<Integer, Object> replacements = new HashMap<>(fields.size());
        for (BoundField bound : fields) {
            Object rawValue = safeRow.value(bound.index());
            if (rawValue != null) {
                replacements.put(bound.index(), decodeMaterializedValue(
                        bound.field(), bound.decoding(), rawValue));
            }
        }
        return safeRow.withValues(replacements);
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
            case ARRAY -> decodeArray(rawValue, decoding.arrayType());
            case VECTOR -> VectorValueCodec.read(rawValue, field.length());
            case OFFSET_TIME -> OffsetTimeValueCodec.read(rawValue);
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
            case JSON, ARRAY, VECTOR, OFFSET_TIME, SCALAR, CUSTOM -> Mono.fromSupplier(
                    () -> new DecodedFieldValue(bound.index(), decodeMaterializedValue(
                            bound.field(), bound.decoding(), rawValue)));
        };
    }

    private FormFieldDecodingPlan decodingPlan(DynamicForm form) {
        return renderer.resultDecodingPlan(form);
    }

    /** 先按声明的元素类型转换，再返回动态表单统一的只读 List；避免 JDBC 可变时间对象逃逸。 */
    private List<Object> decodeArray(Object rawValue, Class<?> arrayType) {
        if (rawValue == null) {
            return null;
        }
        Class<?> elementType = arrayType.getComponentType();
        if (rawValue instanceof Collection<?> collection) {
            List<Object> target = new ArrayList<>(collection.size());
            for (Object item : collection) {
                target.add(decodeArrayElement(item, elementType));
            }
            return Collections.unmodifiableList(target);
        }
        if (!rawValue.getClass().isArray()) {
            throw new IllegalArgumentException("array value must be a Java array or Collection");
        }
        int length = Array.getLength(rawValue);
        List<Object> target = new ArrayList<>(length);
        for (int index = 0; index < length; index++) {
            target.add(decodeArrayElement(Array.get(rawValue, index), elementType));
        }
        return Collections.unmodifiableList(target);
    }

    private Object decodeArrayElement(Object value, Class<?> elementType) {
        if (value instanceof Collection<?> || value != null && value.getClass().isArray()) {
            throw new IllegalArgumentException("nested SQL arrays are not supported yet");
        }
        return value == null || elementType == Object.class
                ? value : JdbcLegacyTemporalAdapter.read(renderer.valueCodecs(), value, elementType);
    }

    private record DecodedFieldValue(int index, Object value) {
    }

    private record BoundField(int index,
                              DynamicField field,
                              FormFieldDecodingPlan.Decoding decoding) {
    }

}
