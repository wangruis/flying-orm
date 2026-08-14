package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.rdb.batch.BatchMemoryBudget;
import com.flying.orm.rdb.codec.ArrayValueCodec;
import com.flying.orm.rdb.codec.LargeObjectValueCodec;
import com.flying.orm.rdb.codec.OffsetTimeValueCodec;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlResultMemoryLimitExceededException;
import com.flying.orm.rdb.json.JsonValueCodec;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.observation.SqlStatementType;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.vector.VectorValueCodec;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.DataScope;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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
     * 没有特殊类型时直接返回原始 Flux，不复制行，也不增加逐列扫描。
     * 有特殊类型时使用 {@code concatMap} 按行解码，保证 LOB 读取有序且受下游背压约束。
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
        Flux<DynamicRow> safeRows = Objects.requireNonNull(rows, "form rows must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options, "sql execution options must not be null");
        boolean needsFieldDecoding = !safeForm.protections().isEmpty()
                || safeForm.fields().stream().anyMatch(field -> needsDecoding(safeForm, field));
        if (!needsFieldDecoding) {
            return safeRows;
        }
        Flux<DynamicRow> decodedRows = safeRows.concatMap(row -> decodeFields(safeForm, row, safeOptions))
                                              .map(row -> renderer.protection().transform(
                                                      safeForm, row, scope, displayMode));
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
        boolean needsFieldDecoding = !safeForm.protections().isEmpty()
                || safeForm.fields().stream().anyMatch(field -> needsDecoding(safeForm, field));
        if (!needsFieldDecoding) {
            return safeRows;
        }
        long totalBytes = 0L;
        java.util.ArrayList<DynamicRow> decoded = new java.util.ArrayList<>(safeRows.size());
        for (int rowIndex = 0; rowIndex < safeRows.size(); rowIndex++) {
            DynamicRow row = decodeMaterializedRow(safeForm, safeRows.get(rowIndex));
            row = renderer.protection().transform(safeForm, row, scope, displayMode);
            totalBytes = saturatedAdd(totalBytes, BatchMemoryBudget.estimateRowBytes(row));
            if (safeOptions.maxResultBytes() > 0
                    && (totalBytes == Long.MAX_VALUE || totalBytes > safeOptions.maxResultBytes())) {
                throw new SqlResultMemoryLimitExceededException(
                        SqlStatementType.SELECT, safeOptions.maxResultBytes(), totalBytes, rowIndex);
            }
            decoded.add(row);
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
                                          DynamicRow row,
                                          SqlExecutionOptions options) {
        return Mono.defer(() -> {
            DynamicRow safeRow = Objects.requireNonNull(row, "form row must not be null");
            return Flux.range(0, safeRow.columnCount())
                       .concatMap(index -> decodeField(form,
                                                       index,
                                                       safeRow.columnName(index),
                                                       safeRow.value(index),
                                                       options))
                       .collectMap(DecodedFieldValue::index, DecodedFieldValue::value)
                       .map(safeRow::withValues);
        });
    }

    private DynamicRow decodeMaterializedRow(DynamicForm form, DynamicRow row) {
        DynamicRow safeRow = Objects.requireNonNull(row, "form row must not be null");
        Map<Integer, Object> replacements = new HashMap<>();
        for (int index = 0; index < safeRow.columnCount(); index++) {
            DynamicField field = form.findField(safeRow.columnName(index)).orElse(null);
            Object rawValue = safeRow.value(index);
            if (field != null && rawValue != null && needsDecoding(form, field)) {
                replacements.put(index, decodeMaterializedValue(form, field, rawValue));
            }
        }
        return safeRow.withValues(replacements);
    }

    private Object decodeMaterializedValue(DynamicForm form, DynamicField field, Object rawValue) {
        if (form.protections().encrypted(field.name()).isPresent()) {
            return LargeObjectValueCodec.read(rawValue, "PROTECTED_BINARY");
        }
        if (JsonValueCodec.isJsonDataType(field.dataType())) {
            return JsonValueCodec.read(rawValue);
        }
        if (ArrayValueCodec.isArrayDataType(field.dataType())) {
            return ArrayValueCodec.read(rawValue);
        }
        if (VectorValueCodec.isVectorDataType(field.dataType())) {
            return VectorValueCodec.read(rawValue, field.length());
        }
        if (OffsetTimeValueCodec.isOffsetTimeDataType(field.dataType())) {
            return OffsetTimeValueCodec.read(rawValue);
        }
        if (renderer.needsScalarDecoding(field)) {
            return renderer.readScalarValue(field, rawValue);
        }
        return LargeObjectValueCodec.read(rawValue, field.dataType());
    }

    /**
     * SQL 投影可能带出不在表单定义里的别名列，这类列必须原样保留。
     * {@link Mono#empty()} 表示“不替换当前列”，不是把这一列的值改成 null。
     */
    private Mono<DecodedFieldValue> decodeField(DynamicForm form,
                                                int index,
                                                String column,
                                                Object rawValue,
                                                SqlExecutionOptions options) {
        DynamicField field = form.findField(column).orElse(null);
        if (field == null || rawValue == null || !needsDecoding(form, field)) {
            return Mono.empty();
        }
        if (form.protections().encrypted(field.name()).isPresent()) {
            return LargeObjectValueCodec.readReactive(rawValue, "PROTECTED_BINARY", options)
                                        .map(value -> new DecodedFieldValue(index, value));
        }
        if (JsonValueCodec.isJsonDataType(field.dataType())) {
            // 解码结果允许是 Java null，例如数据库返回 JSON 文本 "null"。把 null 放进包装对象，
            // 不能让 Mono.fromSupplier(null) 把它误解成“这一列无需替换”。
            return Mono.fromSupplier(() -> new DecodedFieldValue(index, JsonValueCodec.read(rawValue)));
        }
        if (ArrayValueCodec.isArrayDataType(field.dataType())) {
            return Mono.fromSupplier(() -> new DecodedFieldValue(index, ArrayValueCodec.read(rawValue)));
        }
        if (VectorValueCodec.isVectorDataType(field.dataType())) {
            return Mono.fromSupplier(() -> new DecodedFieldValue(
                    index, VectorValueCodec.read(rawValue, field.length())));
        }
        if (OffsetTimeValueCodec.isOffsetTimeDataType(field.dataType())) {
            return Mono.fromSupplier(() -> new DecodedFieldValue(index, OffsetTimeValueCodec.read(rawValue)));
        }
        if (renderer.needsScalarDecoding(field)) {
            return Mono.fromSupplier(() -> new DecodedFieldValue(
                    index, renderer.readScalarValue(field, rawValue)));
        }
        return LargeObjectValueCodec.readReactive(rawValue, field.dataType(), options)
                                    .map(value -> new DecodedFieldValue(index, value));
    }

    private boolean needsDecoding(DynamicForm form, DynamicField field) {
        return form.protections().protectedField(field.name())
                || JsonValueCodec.isJsonDataType(field.dataType())
                || ArrayValueCodec.isArrayDataType(field.dataType())
                || VectorValueCodec.isVectorDataType(field.dataType())
                || OffsetTimeValueCodec.isOffsetTimeDataType(field.dataType())
                || renderer.needsScalarDecoding(field)
                || LargeObjectValueCodec.isLargeObjectDataType(field.dataType());
    }

    private record DecodedFieldValue(int index, Object value) {
    }
}
