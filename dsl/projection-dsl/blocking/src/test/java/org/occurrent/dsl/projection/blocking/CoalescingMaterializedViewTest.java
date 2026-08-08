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

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.projection.MaterializedViewOptions;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

@DisplayNameGeneration(ReplaceUnderscores.class)
class CoalescingMaterializedViewTest {

    private static final URI SOURCE = URI.create("urn:occurrent:test");

    // The key is the part of the event id before the dash, the same convention ProjectionsTest uses, so no CloudEvent
    // extension is needed to carry it through the replay's encode/decode round trip.
    record Ticked(String eventId) {
        String key() {
            return eventId.split("-")[0];
        }
    }

    @Test
    void a_replay_over_several_keys_coalesces_reads_and_writes_to_one_call_per_key_instead_of_one_per_event() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Ticked> converter = tickedConverter();
        // 3 keys, 5 events each: 15 events total, 2N = 30 round trips the old per-event fold would pay.
        List<Ticked> events = new ArrayList<>();
        for (String key : List.of("a", "b", "c")) {
            for (int i = 0; i < 5; i++) {
                events.add(new Ticked(key + "-" + i));
            }
        }
        store.write("s", converter.toCloudEvents(events));

        CountingRepository repository = new CountingRepository();
        MaterializedView<Ticked> view = Projections.materializedView(tickProjection(), repository);
        CatchupProjectionFeed<Ticked> feed = CatchupProjectionFeed.create(
                "ticks", view, org.occurrent.filter.Filter.all(), store, converter, Ticked::eventId, null);

        feed.catchUp();

        assertThat(repository.get("a")).isEqualTo(5);
        assertThat(repository.get("b")).isEqualTo(5);
        assertThat(repository.get("c")).isEqualTo(5);
        // One batch (15 buffered events is under the default batch size), so one bulk read and one bulk write for all
        // 3 keys, not 15 of each.
        assertThat(repository.findAllByIdCalls.get()).isEqualTo(1);
        assertThat(repository.saveAllCalls.get()).isEqualTo(1);
        assertThat(repository.findByIdCalls.get()).isZero();
        assertThat(repository.saveCalls.get()).isZero();
    }

    @Test
    void a_batch_size_of_one_writes_through_per_event_the_same_as_no_coalescing() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Ticked> converter = tickedConverter();
        List<Ticked> events = List.of(new Ticked("a-0"), new Ticked("b-0"), new Ticked("a-1"));
        store.write("s", converter.toCloudEvents(events));

        CountingRepository repository = new CountingRepository();
        MaterializedView<Ticked> view = Projections.materializedView(
                tickProjection(), repository, org.occurrent.retry.RetryStrategy.none(), new MaterializedViewOptions(1));
        CatchupProjectionFeed<Ticked> feed = CatchupProjectionFeed.create(
                "ticks", view, org.occurrent.filter.Filter.all(), store, converter, Ticked::eventId, null);

        feed.catchUp();

        assertThat(repository.get("a")).isEqualTo(2);
        assertThat(repository.get("b")).isEqualTo(1);
        // One flush per event: 3 events, 3 bulk calls, each touching exactly the one key that event belongs to.
        assertThat(repository.findAllByIdCalls.get()).isEqualTo(3);
        assertThat(repository.saveAllCalls.get()).isEqualTo(3);
    }

    @Test
    void a_smaller_batch_size_than_the_buffered_total_flushes_more_than_once_but_still_folds_every_event_correctly() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Ticked> converter = tickedConverter();
        List<Ticked> events = new ArrayList<>();
        for (String key : List.of("a", "b", "c")) {
            for (int i = 0; i < 4; i++) {
                events.add(new Ticked(key + "-" + i));
            }
        }
        store.write("s", converter.toCloudEvents(events)); // 12 events

        CountingRepository repository = new CountingRepository();
        MaterializedView<Ticked> view = Projections.materializedView(
                tickProjection(), repository, org.occurrent.retry.RetryStrategy.none(), new MaterializedViewOptions(5));
        CatchupProjectionFeed<Ticked> feed = CatchupProjectionFeed.create(
                "ticks", view, org.occurrent.filter.Filter.all(), store, converter, Ticked::eventId, null);

        feed.catchUp();

        assertThat(repository.get("a")).isEqualTo(4);
        assertThat(repository.get("b")).isEqualTo(4);
        assertThat(repository.get("c")).isEqualTo(4);
        // 12 events at a batch size of 5 flushes at least 3 times (2 full batches plus a remainder on replayCompleted),
        // strictly fewer than one call per event.
        assertThat(repository.findAllByIdCalls.get()).isBetween(3, 12);
        assertThat(repository.findAllByIdCalls.get()).isLessThan(events.size());
    }

    @Test
    void a_configured_retry_strategy_flushes_one_key_at_a_time_instead_of_using_the_bulk_repository_calls() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Ticked> converter = tickedConverter();
        List<Ticked> events = List.of(new Ticked("a-0"), new Ticked("b-0"), new Ticked("a-1"));
        store.write("s", converter.toCloudEvents(events));

        CountingRepository repository = new CountingRepository();
        MaterializedView<Ticked> view = Projections.materializedView(
                tickProjection(), repository, org.occurrent.retry.RetryStrategy.retry().maxAttempts(3), MaterializedViewOptions.defaults());
        CatchupProjectionFeed<Ticked> feed = CatchupProjectionFeed.create(
                "ticks", view, org.occurrent.filter.Filter.all(), store, converter, Ticked::eventId, null);

        feed.catchUp();

        assertThat(repository.get("a")).isEqualTo(2);
        assertThat(repository.get("b")).isEqualTo(1);
        // A configured retry strategy trades the bulk round trip for per-key retry, so the flush goes through
        // findById/save (one pair per key) rather than findAllById/saveAll.
        assertThat(repository.findAllByIdCalls.get()).isZero();
        assertThat(repository.saveAllCalls.get()).isZero();
        assertThat(repository.findByIdCalls.get()).isEqualTo(2); // one per key (a, b), not one per event
        assertThat(repository.saveCalls.get()).isEqualTo(2);
    }

    @Test
    void a_batch_write_that_fails_partway_leaves_the_already_written_keys_durable_fails_the_catch_up_and_writes_no_marker() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Ticked> converter = tickedConverter();
        // Arrival order determines flush order: "a" is buffered first, "b" second, "c" third.
        List<Ticked> events = List.of(new Ticked("a-0"), new Ticked("b-0"), new Ticked("c-0"));
        store.write("s", converter.toCloudEvents(events));
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();

        PartiallyFailingRepository repository = new PartiallyFailingRepository("b");
        MaterializedView<Ticked> view = Projections.materializedView(tickProjection(), repository);
        CatchupProjectionFeed<Ticked> feed = CatchupProjectionFeed.create(
                "ticks", view, org.occurrent.filter.Filter.all(), store, converter, Ticked::eventId, marker);

        Throwable thrown = catchThrowable(feed::catchUp);

        assertThat(thrown).isInstanceOf(RuntimeException.class).hasMessageContaining("simulated");
        // "a" was written before the batch hit "b" and failed, so it is durable; "b" and "c" (not yet attempted) are not.
        assertThat(repository.get("a")).isEqualTo(1);
        assertThat(repository.get("b")).isNull();
        assertThat(repository.get("c")).isNull();
        assertThat(marker.exists("ticks")).isFalse();
    }

    @Test
    void stopping_a_catch_up_mid_replay_discards_the_buffered_batch_instead_of_writing_a_partial_one() throws InterruptedException {
        InMemoryEventStore store = new InMemoryEventStore();
        CountDownLatch parked = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        AtomicInteger converted = new AtomicInteger();
        // Parks the replay after the second event has been decoded (and, by the time it resumes, buffered), before a
        // third can arrive, so the test can stop a replay that genuinely still has an unflushed batch in flight.
        CloudEventConverter<Ticked> converter = parkingConverter(converted, parked, proceed);
        List<Ticked> events = List.of(new Ticked("a-0"), new Ticked("b-0"), new Ticked("a-1"));
        store.write("s", converter.toCloudEvents(events));
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();

        CountingRepository repository = new CountingRepository();
        // A batch size larger than the whole history, so nothing flushes until replayCompleted() (which a stop skips).
        MaterializedView<Ticked> view = Projections.materializedView(
                tickProjection(), repository, org.occurrent.retry.RetryStrategy.none(), new MaterializedViewOptions(100));
        CatchupProjectionFeed<Ticked> feed = CatchupProjectionFeed.create(
                "ticks", view, org.occurrent.filter.Filter.all(), store, converter, Ticked::eventId, marker);

        Thread replay = new Thread(feed::catchUp);
        replay.start();
        parked.await();
        feed.stopCatchUp();
        proceed.countDown();
        replay.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(replay.isAlive()).isFalse();
        assertThat(marker.exists("ticks")).isFalse();
        // Nothing was flushed: the buffered batch was discarded, not written.
        assertThat(repository.findAllByIdCalls.get()).isZero();
        assertThat(repository.saveAllCalls.get()).isZero();
        assertThat(repository.get("a")).isNull();
        assertThat(repository.get("b")).isNull();

        // The feed stays usable: a later catch-up (with a converter that no longer parks) replays the whole history.
        CatchupProjectionFeed<Ticked> restarted = CatchupProjectionFeed.create(
                "ticks", Projections.materializedView(tickProjection(), repository), org.occurrent.filter.Filter.all(),
                store, tickedConverter(), Ticked::eventId, marker);
        restarted.catchUp();

        assertThat(repository.get("a")).isEqualTo(2);
        assertThat(repository.get("b")).isEqualTo(1);
    }

    private static Projection<Integer, Ticked, String> tickProjection() {
        return Projection.<Integer, Ticked, String>builder(0)
                .id(Ticked::key)
                .on(Ticked.class, (state, event) -> state + 1)
                .build();
    }

    private static CloudEventConverter<Ticked> tickedConverter() {
        return new CloudEventConverter<>() {
            @Override
            public CloudEvent toCloudEvent(Ticked domainEvent) {
                return CloudEventBuilder.v1()
                        .withId(domainEvent.eventId())
                        .withSource(SOURCE)
                        .withType("Ticked")
                        .build();
            }

            @Override
            public Ticked toDomainEvent(CloudEvent cloudEvent) {
                return new Ticked(cloudEvent.getId());
            }

            @Override
            public String getCloudEventType(Class<? extends Ticked> type) {
                return "Ticked";
            }
        };
    }

    private static CloudEventConverter<Ticked> parkingConverter(AtomicInteger converted, CountDownLatch parked, CountDownLatch proceed) {
        CloudEventConverter<Ticked> delegate = tickedConverter();
        return new CloudEventConverter<>() {
            @Override
            public CloudEvent toCloudEvent(Ticked domainEvent) {
                return delegate.toCloudEvent(domainEvent);
            }

            @Override
            public Ticked toDomainEvent(CloudEvent cloudEvent) {
                Ticked event = delegate.toDomainEvent(cloudEvent);
                if (converted.incrementAndGet() == 2) {
                    parked.countDown();
                    awaitUninterruptibly(proceed);
                }
                return event;
            }

            @Override
            public String getCloudEventType(Class<? extends Ticked> type) {
                return delegate.getCloudEventType(type);
            }
        };
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    // Tracks how many times each ViewStateRepository method is called, so a test can assert the coalescing view
    // actually took the bulk route (findAllById/saveAll) rather than looping findById/save.
    private static final class CountingRepository implements ViewStateRepository<Integer, String> {
        private final Map<String, Integer> store = new ConcurrentHashMap<>();
        final AtomicInteger findByIdCalls = new AtomicInteger();
        final AtomicInteger saveCalls = new AtomicInteger();
        final AtomicInteger findAllByIdCalls = new AtomicInteger();
        final AtomicInteger saveAllCalls = new AtomicInteger();

        @Override
        public Optional<Integer> findById(String id) {
            findByIdCalls.incrementAndGet();
            return Optional.ofNullable(store.get(id));
        }

        @Override
        public void save(String id, Integer state) {
            saveCalls.incrementAndGet();
            store.put(id, state);
        }

        @Override
        public Map<String, Integer> findAllById(Collection<String> ids) {
            findAllByIdCalls.incrementAndGet();
            Map<String, Integer> result = new LinkedHashMap<>();
            for (String id : ids) {
                Integer value = store.get(id);
                if (value != null) {
                    result.put(id, value);
                }
            }
            return result;
        }

        @Override
        public void saveAll(Map<String, Integer> states) {
            saveAllCalls.incrementAndGet();
            store.putAll(states);
        }

        Integer get(String id) {
            return store.get(id);
        }
    }

    // A saveAll that writes entries in iteration order and throws once it reaches failOnKey, leaving entries written
    // before it durable and everything from failOnKey onwards untouched, the way an unordered bulk write that fails
    // partway through would.
    private static final class PartiallyFailingRepository implements ViewStateRepository<Integer, String> {
        private final Map<String, Integer> store = new ConcurrentHashMap<>();
        private final String failOnKey;

        PartiallyFailingRepository(String failOnKey) {
            this.failOnKey = failOnKey;
        }

        @Override
        public Optional<Integer> findById(String id) {
            return Optional.ofNullable(store.get(id));
        }

        @Override
        public void save(String id, Integer state) {
            store.put(id, state);
        }

        @Override
        public Map<String, Integer> findAllById(Collection<String> ids) {
            Map<String, Integer> result = new LinkedHashMap<>();
            for (String id : ids) {
                Integer value = store.get(id);
                if (value != null) {
                    result.put(id, value);
                }
            }
            return result;
        }

        @Override
        public void saveAll(Map<String, Integer> states) {
            for (Map.Entry<String, Integer> entry : states.entrySet()) {
                if (entry.getKey().equals(failOnKey)) {
                    throw new RuntimeException("simulated partial write failure on key " + failOnKey);
                }
                store.put(entry.getKey(), entry.getValue());
            }
        }

        Integer get(String id) {
            return store.get(id);
        }
    }
}
