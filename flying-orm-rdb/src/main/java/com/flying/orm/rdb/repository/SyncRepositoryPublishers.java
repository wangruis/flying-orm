package com.flying.orm.rdb.repository;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Iterator;
import java.util.Objects;

/** 同步集合批量入口使用的最小 Publisher，按下游需求逐项读取，不借用 Reactor。 */
final class SyncRepositoryPublishers {

    private SyncRepositoryPublishers() {
    }

    static <T> Publisher<T> fromIterable(Iterable<T> rows) {
        Iterable<T> safeRows = Objects.requireNonNull(rows, "repository batch rows must not be null");
        return subscriber -> subscriber.onSubscribe(new IterableSubscription<>(subscriber, safeRows));
    }

    private static final class IterableSubscription<T> implements Subscription {
        private final Subscriber<? super T> downstream;
        private final Iterable<T> source;
        private Iterator<T> rows;
        private boolean cancelled;
        private boolean completed;
        private boolean draining;
        private long requested;

        private IterableSubscription(Subscriber<? super T> downstream, Iterable<T> source) {
            this.downstream = Objects.requireNonNull(downstream, "repository batch subscriber must not be null");
            this.source = Objects.requireNonNull(source, "repository batch rows must not be null");
        }

        @Override
        public synchronized void request(long count) {
            if (cancelled || completed) {
                return;
            }
            if (count <= 0L) {
                cancelled = true;
                downstream.onError(new IllegalArgumentException("batch demand must be positive"));
                return;
            }
            requested = Long.MAX_VALUE - requested < count ? Long.MAX_VALUE : requested + count;
            if (draining) {
                return;
            }
            draining = true;
            try {
                while (!cancelled && requested > 0L) {
                    Iterator<T> currentRows = rows();
                    if (!currentRows.hasNext()) {
                        completed = true;
                        downstream.onComplete();
                        return;
                    }
                    requested--;
                    downstream.onNext(Objects.requireNonNull(currentRows.next(), "repository batch row must not be null"));
                }
            } catch (Error fatal) {
                cancelled = true;
                throw fatal;
            } catch (RuntimeException error) {
                cancelled = true;
                downstream.onError(error);
            } finally {
                draining = false;
            }
        }

        @Override
        public synchronized void cancel() {
            cancelled = true;
        }

        private Iterator<T> rows() {
            if (rows == null) {
                rows = Objects.requireNonNull(source.iterator(), "repository batch iterator must not be null");
            }
            return rows;
        }
    }
}
