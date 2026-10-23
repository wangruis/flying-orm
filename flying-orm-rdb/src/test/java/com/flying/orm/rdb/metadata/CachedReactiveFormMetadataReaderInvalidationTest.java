package com.flying.orm.rdb.metadata;

import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.form.DynamicForm;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.assertEquals;

class CachedReactiveFormMetadataReaderInvalidationTest {

    @Test
    void caseAliasesAreEvictedWithoutMergingTheirReadKeys() {
        CountingReader delegate = new CountingReader();
        ReactiveFormMetadataCache cache = ReactiveFormMetadataReaders.cached(delegate);
        cache.readForm("accounts", "PUBLIC", "ACCOUNTS").block();
        cache.readTable("PUBLIC", "ACCOUNTS").block();
        cache.readTable("public", "accounts").block();
        assertEquals(3, delegate.loads.get(), "read identities remain case-sensitive");
        cache.invalidate(RelationIdentity.of(null, "public", "accounts"));
        cache.readForm("accounts", "PUBLIC", "ACCOUNTS").block();
        cache.readTable("PUBLIC", "ACCOUNTS").block();
        cache.readTable("public", "accounts").block();
        assertEquals(6, delegate.loads.get());
    }

    @Test
    void unqualifiedRelationIdentityInvalidatesQualifiedFormAndTableEntries() {
        CountingReader delegate = new CountingReader();
        ReactiveFormMetadataCache cache = ReactiveFormMetadataReaders.cached(delegate);

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
        ReactiveFormMetadataCache cache = ReactiveFormMetadataReaders.cached(delegate);

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
        public Mono<DynamicForm> readForm(String formId, RelationIdentity relation) {
            String table = relation.table();
            return Mono.fromSupplier(() -> {
                loads.incrementAndGet();
                return DynamicForm.builder(formId, table).build();
            });
        }

        @Override
        public Mono<RelationalTableDefinition> readTable(RelationIdentity relation) {
            return readForm(relation.table(), relation).map(form -> form.toTableDefinition(relation));
        }
    }
}
