package com.flying.orm.rdb.repository;

/** Obtains the existing package-private evidence owner without expanding the public Repository API. */
final class RepositoryBatchEvidenceAccess {
    private RepositoryBatchEvidenceAccess() { }

    @SuppressWarnings("unchecked")
    static <T> ReactiveRepositoryBatchOperations<T> reactive(ReactiveFormRepository<T> repository) {
        return (ReactiveRepositoryBatchOperations<T>) owner(repository, "batchOperations");
    }

    @SuppressWarnings("unchecked")
    static <T> SyncRepositoryBatchCoordinator<T> sync(SyncFormRepository<T> repository) {
        return (SyncRepositoryBatchCoordinator<T>) owner(repository, "batchCoordinator");
    }

    private static Object owner(Object repository, String name) {
        try {
            var field = repository.getClass().getDeclaredField(name);
            field.setAccessible(true);
            return field.get(repository);
        } catch (ReflectiveOperationException failure) {
            throw new AssertionError("repository batch owner unavailable", failure);
        }
    }
}
