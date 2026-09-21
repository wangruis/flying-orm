package com.flying.orm.rdb.form;

import com.flying.orm.core.page.CursorPageResult;
import com.flying.orm.core.page.KeysetPageResult;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.result.DynamicRow;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;

/**
 * 在这里统一做 DynamicRow 到实体对象的转换，
 * 让查询、分页、游标分页共用一套映射逻辑，避免重复写。
 */
final class FormResultMappingSupport {

    private FormResultMappingSupport() {
    }

    static <T> Flux<T> mapRows(Flux<DynamicRow> rows, RowMapper<T> rowMapper) {
        Objects.requireNonNull(rows, "rows must not be null");
        return rows.map(Objects.requireNonNull(rowMapper, "row mapper must not be null")::map);
    }

    static <T> Mono<PageResult<T>> mapPage(Mono<PageResult<DynamicRow>> source, RowMapper<T> rowMapper) {
        return source.map(page -> mapPage(page, rowMapper));
    }

    static <T> PageResult<T> mapPage(PageResult<DynamicRow> source, RowMapper<T> rowMapper) {
        PageResult<DynamicRow> safeSource = Objects.requireNonNull(source, "page result must not be null");
        RowMapper<T> safeMapper = Objects.requireNonNull(rowMapper, "row mapper must not be null");
        List<T> typedRows = safeSource.rows()
                                     .stream()
                                     .map(safeMapper::map)
                                     .toList();
        return new PageResult<>(typedRows, safeSource.total(), safeSource.page(), safeSource.size());
    }

    static <T> Mono<CursorPageResult<T>> mapCursorPage(Mono<CursorPageResult<DynamicRow>> source,
                                                       RowMapper<T> rowMapper) {
        return source.map(page -> mapCursorPage(page, rowMapper));
    }

    static <T> CursorPageResult<T> mapCursorPage(CursorPageResult<DynamicRow> source, RowMapper<T> rowMapper) {
        CursorPageResult<DynamicRow> safeSource = Objects.requireNonNull(source, "cursor page result must not be null");
        RowMapper<T> safeMapper = Objects.requireNonNull(rowMapper, "row mapper must not be null");
        List<T> typedRows = safeSource.rows()
                                     .stream()
                                     .map(safeMapper::map)
                                     .toList();
        return new CursorPageResult<>(typedRows, safeSource.nextCursor(), safeSource.hasMore());
    }

    static <T> Mono<KeysetPageResult<T>> mapKeysetPage(
            Mono<KeysetPageResult<DynamicRow>> source,
            RowMapper<T> rowMapper) {
        return Objects.requireNonNull(source, "keyset page source must not be null")
                .map(page -> mapKeysetPage(page, rowMapper));
    }

    static <T> KeysetPageResult<T> mapKeysetPage(
            KeysetPageResult<DynamicRow> source,
            RowMapper<T> rowMapper) {
        KeysetPageResult<DynamicRow> safeSource = Objects.requireNonNull(
                source, "keyset page result must not be null");
        RowMapper<T> safeMapper = Objects.requireNonNull(
                rowMapper, "row mapper must not be null");
        List<T> typedRows = safeSource.rows().stream().map(safeMapper::map).toList();
        return new KeysetPageResult<>(typedRows, safeSource.nextPosition(), safeSource.hasMore());
    }
}
