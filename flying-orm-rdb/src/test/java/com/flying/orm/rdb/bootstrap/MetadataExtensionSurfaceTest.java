package com.flying.orm.rdb.bootstrap;

import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Modifier;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class MetadataExtensionSurfaceTest {
    @Test
    void bothMetadataReadersCanBeSuppliedWithoutInternalTypes() {
        assertAll(
                () -> assertFalse(Modifier.isFinal(JdbcFormMetadataReader.class.getModifiers())),
                () -> assertEquals(Modifier.PROTECTED,
                        JdbcFormMetadataReader.class.getDeclaredConstructor().getModifiers()),
                () -> assertEquals(FlyingOrmClientBuilder.class, FlyingOrmClientBuilder.class
                        .getMethod("metadataReader", JdbcFormMetadataReader.class).getReturnType()),
                () -> assertEquals(FlyingOrmClientBuilder.class, FlyingOrmClientBuilder.class
                        .getMethod("metadataReader", ReactiveFormMetadataReader.class).getReturnType()));
    }
}
