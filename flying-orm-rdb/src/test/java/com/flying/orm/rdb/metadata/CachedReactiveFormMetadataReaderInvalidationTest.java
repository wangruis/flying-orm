package com.flying.orm.rdb.metadata;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.RelationIdentity;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CachedReactiveFormMetadataReaderInvalidationTest {

    @Test
    void unqualifiedRelationIdentityInvalidatesQualifiedFormAndTableEntries() {
        CountingReader delegate = new CountingReader();
        CachedReactiveFormMetadataReader cache = CachedReactiveFormMetadataReader.create(delegate);

        cache.readForm("accounts-form", "public", "accounts").block();
        cache.readTable("public", "accounts").block();
        assertEquals(2, delegate.loads.get());

        cache.invalidate(RelationIdentity.table("accounts"));

        cache.readForm("accounts-form", "public", "accounts").block();
        cache.readTable("public", "accounts").block();
        assertEquals(4, delegate.loads.get(),
                     "an unqualified DDL target must evict every schema-qualified cache entry for that table");
    }

    @Test
    void qualifiedInvalidationAlsoRemovesTheUnqualifiedAlias() {
        CountingReader delegate = new CountingReader();
        CachedReactiveFormMetadataReader cache = CachedReactiveFormMetadataReader.create(delegate);

        cache.readForm("accounts-form", "accounts").block();
        cache.readTable("accounts").block();
        assertEquals(2, delegate.loads.get());

        cache.invalidate("public", "accounts");

        cache.readForm("accounts-form", "accounts").block();
        cache.readTable("accounts").block();
        assertEquals(4, delegate.loads.get(),
                     "qualified DDL must also evict the default-schema alias of the same table");
    }

    private static final class CountingReader implements ReactiveFormMetadataReader {

        private final AtomicInteger loads = new AtomicInteger();

        @Override
        public Mono<DynamicForm> readForm(String formId, String table) {
            return readForm(formId, null, table);
        }

        @Override
        public Mono<DynamicForm> readForm(String formId, String schema, String table) {
            return Mono.fromSupplier(() -> {
                loads.incrementAndGet();
                return DynamicForm.builder(formId, table).build();
            });
        }
    }
}
