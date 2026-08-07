/*
 * Copyright 2026 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.occurrent.dsl.projection.blocking;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.query.blocking.DomainEventQueries;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.retry.RetryStrategy;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

@DisplayNameGeneration(ReplaceUnderscores.class)
class ProjectionsTest {

    private static final URI SOURCE = URI.create("urn:occurrent:test");

    record Counted(String eventId) {
    }

    record Tick(String key) {
    }

    // Models a store that does optimistic locking on a @Version-style field: save compares the version threaded
    // through from findById against what is currently stored, and throws instead of overwriting on a mismatch.
    record VersionedCount(int value, long version) {
    }

    static final class ConflictingWriteException extends RuntimeException {
    }

    @Test
    void project_folds_all_matching_events_on_demand_for_a_singleton_projection() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = countedConverter();
        store.write("s", converter.toCloudEvents(List.of(new Counted("1"), new Counted("2"), new Counted("3"))));
        DomainEventQueries<Counted> queries = new DomainEventQueries<>(store, converter);

        Integer count = Projections.project(singletonProjection(), queries);

        assertThat(count).isEqualTo(3);
    }

    @Test
    void project_without_an_instance_id_rejects_a_keyed_projection() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = countedConverter();
        DomainEventQueries<Counted> queries = new DomainEventQueries<>(store, converter);

        Throwable thrown = catchThrowable(() -> Projections.project(keyedProjection(), queries));

        assertThat(thrown).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("keyed");
    }

    @Test
    void project_with_a_null_instance_id_throws_instead_of_failing_inside_the_filter() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = countedConverter();
        DomainEventQueries<Counted> queries = new DomainEventQueries<>(store, converter);

        Throwable thrown = catchThrowable(() -> Projections.project(keyedProjection(), queries, null));

        assertThat(thrown).isInstanceOf(NullPointerException.class).hasMessageContaining("instanceId cannot be null");
    }

    @Test
    void project_with_an_instance_id_folds_only_the_events_for_that_instance() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = countedConverter();
        store.write("s", converter.toCloudEvents(List.of(new Counted("a-1"), new Counted("b-1"), new Counted("a-2"))));
        DomainEventQueries<Counted> queries = new DomainEventQueries<>(store, converter);

        Integer countForA = Projections.project(keyedProjection(), queries, "a");

        assertThat(countForA).isEqualTo(2);
    }

    @Test
    void materialized_view_loses_a_concurrent_update_to_one_key_without_a_retry_strategy() throws Exception {
        CyclicBarrier bothThreadsHaveReadBeforeEitherSaves = new CyclicBarrier(2);
        OptimisticLockingRepository repository = new OptimisticLockingRepository(bothThreadsHaveReadBeforeEitherSaves);
        repository.save("k", new VersionedCount(0, 0));
        MaterializedView<Tick> view = Projections.materializedView(tickProjection(), repository);

        List<Future<Void>> results = runConcurrently(view, "k");

        // One of the two updates is lost. Both threads read the same state, the first save wins, and the second save
        // conflicts because the store detects the version moved on. This store throws on that conflict, so exactly one
        // future surfaces the exception, and with no retry strategy that exception is the whole failure, not recovery,
        // so the stored value reflects only one tick, not two. A store that never detects the conflict would instead
        // let the second save overwrite the first with no exception at all, losing the update with no signal either.
        long failures = results.stream().filter(ProjectionsTest::failed).count();
        assertThat(failures).isEqualTo(1);
        assertThat(repository.findById("k")).hasValueSatisfying(state -> assertThat(state.value()).isEqualTo(1));
    }

    @Test
    void materialized_view_keeps_both_concurrent_updates_to_one_key_with_a_retry_strategy() throws Exception {
        CyclicBarrier bothThreadsHaveReadBeforeEitherSaves = new CyclicBarrier(2);
        OptimisticLockingRepository repository = new OptimisticLockingRepository(bothThreadsHaveReadBeforeEitherSaves);
        repository.save("k", new VersionedCount(0, 0));
        RetryStrategy retryOnConflict = RetryStrategy.retry().maxAttempts(5).retryIf(ConflictingWriteException.class::isInstance);
        MaterializedView<Tick> view = Projections.materializedView(tickProjection(), repository, retryOnConflict);

        List<Future<Void>> results = runConcurrently(view, "k");

        // Both transitions survive: the losing thread's save conflicts, the retry re-reads what the winner saved,
        // refolds, and saves again, so no future surfaces an exception and the stored value reflects both ticks.
        assertThat(results.stream().filter(ProjectionsTest::failed).count()).isEqualTo(0);
        assertThat(repository.findById("k")).hasValueSatisfying(state -> assertThat(state.value()).isEqualTo(2));
    }

    // invokeAll itself enforces the 5-second bound per task, so a stuck task (a barrier that never fills) fails the
    // test with a timeout instead of the later failed() calls hanging on an unbounded get().
    private static List<Future<Void>> runConcurrently(MaterializedView<Tick> view, String key) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            List<Callable<Void>> tasks = List.of(
                    () -> { view.update(new Tick(key)); return null; },
                    () -> { view.update(new Tick(key)); return null; });
            return pool.invokeAll(tasks, 5, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }
    }

    private static boolean failed(Future<?> result) {
        try {
            result.get(1, TimeUnit.SECONDS);
            return false;
        } catch (Exception e) {
            return true;
        }
    }

    private static Projection<VersionedCount, Tick, String> tickProjection() {
        return Projection.<VersionedCount, Tick, String>builder(new VersionedCount(0, 0))
                .id(Tick::key)
                .on(Tick.class, (state, event) -> new VersionedCount(state.value() + 1, state.version()))
                .build();
    }

    // Rendezvous on the first read from each of the two racing threads only, so a retry's re-read (the third read and
    // later) proceeds immediately instead of blocking on a barrier the sibling thread will never reach again.
    private static final class OptimisticLockingRepository implements ViewStateRepository<VersionedCount, String> {
        private final ConcurrentHashMap<String, VersionedCount> stored = new ConcurrentHashMap<>();
        private final CyclicBarrier rendezvousOnFirstRead;
        private final AtomicInteger reads = new AtomicInteger();

        OptimisticLockingRepository(CyclicBarrier rendezvousOnFirstRead) {
            this.rendezvousOnFirstRead = rendezvousOnFirstRead;
        }

        @Override
        public Optional<VersionedCount> findById(String id) {
            // Read first, then rendezvous, so both threads' reads land before either can proceed to evolve and save.
            // Awaiting before the read only synchronizes arrival at the barrier, not what happens after release, so a
            // thread that races ahead through evolve and save before its sibling even reads would see no conflict at
            // all: exactly the bug this ordering avoids.
            Optional<VersionedCount> result = Optional.ofNullable(stored.get(id));
            if (reads.getAndIncrement() < 2) {
                await(rendezvousOnFirstRead);
            }
            return result;
        }

        @Override
        public void save(String id, VersionedCount state) {
            stored.compute(id, (key, current) -> {
                long expectedVersion = current == null ? 0 : current.version();
                if (state.version() != expectedVersion) {
                    throw new ConflictingWriteException();
                }
                return new VersionedCount(state.value(), expectedVersion + 1);
            });
        }
    }

    private static void await(CyclicBarrier barrier) {
        try {
            barrier.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Projection<Integer, Counted, String> singletonProjection() {
        return Projection.<Integer, Counted>singletonBuilder(0)
                .on(Counted.class, (state, event) -> state + 1)
                .build();
    }

    private static Projection<Integer, Counted, String> keyedProjection() {
        return Projection.<Integer, Counted, String>builder(0)
                .id(event -> event.eventId().split("-")[0])
                .on(Counted.class, (state, event) -> state + 1)
                .build();
    }

    private static CloudEventConverter<Counted> countedConverter() {
        return new JacksonCloudEventConverter.Builder<Counted>(new ObjectMapper(), SOURCE).idMapper(Counted::eventId).build();
    }
}
