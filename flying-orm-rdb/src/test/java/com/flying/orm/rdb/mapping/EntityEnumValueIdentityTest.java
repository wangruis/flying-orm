package com.flying.orm.rdb.mapping;

import com.flying.orm.core.annotation.EnumValue;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EntityEnumValueIdentityTest {

    @Test
    void mapsEquivalentDecimalCodesForRecordsAndBeans() {
        Map<String, Object> row = Map.of("decimal", new BigDecimal("1.00"));

        assertEquals(DecimalCode.ACTIVE, RowMapper.of(ValueRecord.class).map(row).decimal());
        assertEquals(DecimalCode.ACTIVE, RowMapper.of(ValueBean.class).map(row).decimal);
    }

    @Test
    void mapsBinaryCodesByContentsForRecordsAndBeans() {
        Map<String, Object> row = Map.of(
                "binary", new byte[]{1, -2}, "boxedBinary", new Byte[]{1, -2});

        ValueRecord record = RowMapper.of(ValueRecord.class).map(row);
        assertEquals(BinaryCode.ACTIVE, record.binary());
        assertEquals(BoxedBinaryCode.ACTIVE, record.boxedBinary());
        ValueBean bean = RowMapper.of(ValueBean.class).map(row);
        assertEquals(BinaryCode.ACTIVE, bean.binary);
        assertEquals(BoxedBinaryCode.ACTIVE, bean.boxedBinary);
    }

    @Test
    void preservesNullValuesAndRejectsUnknownCodes() {
        RowMapper<ValueRecord> records = RowMapper.of(ValueRecord.class);
        RowMapper<ValueBean> beans = RowMapper.of(ValueBean.class);

        assertEquals(new ValueRecord(null, null, null), records.map(Map.of()));
        assertThrows(MappingException.class, () -> records.map(Map.of("decimal", new BigDecimal("2.00"))));
        assertThrows(MappingException.class, () -> beans.map(Map.of("binary", new byte[]{2, -2})));
        assertThrows(MappingException.class, () -> records.map(Map.of("boxedBinary", new Byte[]{2, -2})));
    }

    @Test
    void rejectsDuplicateDecimalCodesAtMappingCompilation() {
        assertThrows(MappingException.class, () -> RowMapper.of(DuplicateDecimalRecord.class));
    }

    @Test
    void rejectsDuplicateBinaryCodesAtMappingCompilation() {
        assertThrows(MappingException.class, () -> RowMapper.of(DuplicateBinaryRecord.class));
    }

    @Test
    void rejectsDuplicateBoxedBinaryCodesAtMappingCompilation() {
        assertThrows(MappingException.class, () -> RowMapper.of(DuplicateBoxedBinaryRecord.class));
    }

    private record ValueRecord(DecimalCode decimal, BinaryCode binary, BoxedBinaryCode boxedBinary) {
    }

    private static final class ValueBean {
        private DecimalCode decimal;
        private BinaryCode binary;
        private BoxedBinaryCode boxedBinary;
    }

    private enum DecimalCode {
        ACTIVE("1.0");

        @EnumValue
        private final BigDecimal code;

        DecimalCode(String code) {
            this.code = new BigDecimal(code);
        }
    }

    private enum BinaryCode {
        ACTIVE(new byte[]{1, -2});

        @EnumValue
        private final byte[] code;

        BinaryCode(byte[] code) {
            this.code = code;
        }
    }

    private enum BoxedBinaryCode {
        ACTIVE(new Byte[]{1, -2});

        @EnumValue
        private final Byte[] code;

        BoxedBinaryCode(Byte[] code) {
            this.code = code;
        }
    }

    private record DuplicateDecimalRecord(DuplicateDecimalCode value) {
    }

    private enum DuplicateDecimalCode {
        FIRST("1.0"), SECOND("1.00");

        @EnumValue
        private final BigDecimal code;

        DuplicateDecimalCode(String code) {
            this.code = new BigDecimal(code);
        }
    }

    private record DuplicateBinaryRecord(DuplicateBinaryCode value) {
    }

    private enum DuplicateBinaryCode {
        FIRST(new byte[]{1, -2}), SECOND(new byte[]{1, -2});

        @EnumValue
        private final byte[] code;

        DuplicateBinaryCode(byte[] code) {
            this.code = code;
        }
    }

    private record DuplicateBoxedBinaryRecord(DuplicateBoxedBinaryCode value) {
    }

    private enum DuplicateBoxedBinaryCode {
        FIRST(new Byte[]{1, -2}), SECOND(new Byte[]{1, -2});

        @EnumValue
        private final Byte[] code;

        DuplicateBoxedBinaryCode(Byte[] code) {
            this.code = code;
        }
    }
}
