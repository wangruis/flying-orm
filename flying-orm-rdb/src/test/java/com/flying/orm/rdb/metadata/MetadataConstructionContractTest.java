package com.flying.orm.rdb.metadata;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MetadataConstructionContractTest {
    @Test
    void readersOnlyAcceptTheProfileThatOwnsQueryCoverage() {
        assertEquals(1, InformationSchemaFormMetadataReader.class.getDeclaredConstructors().length);
        assertEquals(MetadataQueryProfile.class,
                InformationSchemaFormMetadataReader.class.getDeclaredConstructors()[0].getParameterTypes()[1]);
        assertEquals(1, DefaultJdbcFormMetadataReader.class.getDeclaredConstructors().length);
        assertEquals(MetadataQueryProfile.class,
                DefaultJdbcFormMetadataReader.class.getDeclaredConstructors()[0].getParameterTypes()[1]);
    }
}
