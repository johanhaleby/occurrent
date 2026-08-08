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

package org.occurrent.dsl.projection.reactor;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.projection.MaterializedViewOptions;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@DisplayNameGeneration(ReplaceUnderscores.class)
class CoalescingMaterializedUpdateTest {

    private static final URI SOURCE = URI.create("urn:occurrent:test");

    // The key is the part of the event id before the dash, the same convention the blocking twin of this test uses.
    record Ticked(String eventId) {
        String key() {
            return eventId.split("-")[0];
        }
    }

    @Test
    void a_replay_over_several_keys_coalesces_reads_and_writes_to_one_call_per_key_instead_of_one_per_event() {
        List<String> eventIds = new ArrayList<>();
        for (String key : List.of("a", "b", "c")) {
            for (int i = 0; i < 5; i++) {
                eventIds.add(key + "-" + i);
            }
        }
        CountingRepository repository = new CountingRepository();
        BiFunction<EventMetadata, Ticked, Mono<Void>> fold = Projections.reactiveUpdateWithMetadata(tickProjection(), repository);
        CatchupProjectionFeed<Ticked> feed = CatchupProjectionFeed.create(
                "ticks", fold, Filter.all(), reader(eventIds), tickedConverter(), Ticked::eventId, null);

        feed.catchUp().block(ofSeconds(5));

        assertThat(repository.get("a")).isEqualTo(5);
        assertThat(repository.get("b")).isEqualTo(5);
        assertThat(repository.get("c")).isEqualTo(5);
        assertThat(repository.findAllByIdCalls.get()).isEqualTo(1);
        assertThat(repository.saveAllCalls.get()).isEqualTo(1);
        assertThat(repository.findByIdCalls.get()).isZero();
        assertThat(repository.saveCalls.get()).isZero();
    }

    @Test
    void a_batch_size_of_one_writes_through_per_event_the_same_as_no_coalescing() {
        CountingRepository repository = new CountingRepository();
        BiFunction<EventMetadata, Ticked, Mono<Void>> fold = Projections.reactiveUpdateWithMetadata(
                tickProjection(), repository, new MaterializedViewOptions(1));
        CatchupProjectionFeed<Ticked> feed = CatchupProjectionFeed.create(
                "ticks", fold, Filter.all(), reader(List.of("a-0", "b-0", "a-1")), tickedConverter(), Ticked::eventId, null);

        feed.catchUp().block(ofSeconds(5));

        assertThat(repository.get("a")).isEqualTo(2);
        assertThat(repository.get("b")).isEqualTo(1);
        assertThat(repository.findAllByIdCalls.get()).isEqualTo(3);
        assertThat(repository.saveAllCalls.get()).isEqualTo(3);
    }

    @Test
    void a_batch_write_that_fails_partway_leaves_the_already_written_keys_durable_fails_the_catch_up_and_writes_no_marker() {
        // Arrival order determines flush order: "a" is buffered first, "b" second, "c" third.
        PartiallyFailingRepository repository = new PartiallyFailingRepository("b");
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();
        BiFunction<EventMetadata, Ticked, Mono<Void>> fold = Projections.reactiveUpdateWithMetadata(tickProjection(), repository);
        CatchupProjectionFeed<Ticked> feed = CatchupProjectionFeed.create(
                "ticks", fold, Filter.all(), reader(List.of("a-0", "b-0", "c-0")), tickedConverter(), Ticked::eventId, marker);

        assertThatThrownBy(() -> feed.catchUp().block(ofSeconds(5))).hasMessageContaining("simulated");

        assertThat(repository.get("a")).isEqualTo(1);
        assertThat(repository.get("b")).isNull();
        assertThat(repository.get("c")).isNull();
        assertThat(marker.read("ticks").hasElement().block(ofSeconds(5))).isFalse();
    }

    @Test
    void stopping_a_catch_up_mid_replay_discards_the_buffered_batch_instead_of_writing_a_partial_one() throws InterruptedException {
        CountDownLatch parked = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        AtomicInteger converted = new AtomicInteger();
        CloudEventConverter<Ticked> converter = parkingConverter(converted, parked, proceed);
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();

        CountingRepository repository = new CountingRepository();
        // A batch size larger than the whole history, so nothing flushes until replayCompleted() (which a stop skips).
        BiFunction<EventMetadata, Ticked, Mono<Void>> fold = Projections.reactiveUpdateWithMetadata(
                tickProjection(), repository, new MaterializedViewOptions(100));
        CatchupProjectionFeed<Ticked> feed = CatchupProjectionFeed.create(
                "ticks", fold, Filter.all(), reader(List.of("a-0", "b-0", "a-1")), converter, Ticked::eventId, marker);

        Mono<Void> catchUpSignal = feed.catchUp();
        parked.await();
        feed.stopCatchUp();
        proceed.countDown();
        catchUpSignal.block(ofSeconds(5));

        assertThat(marker.read("ticks").hasElement().block(ofSeconds(5))).isFalse();
        assertThat(repository.findAllByIdCalls.get()).isZero();
        assertThat(repository.saveAllCalls.get()).isZero();
        assertThat(repository.get("a")).isNull();
        assertThat(repository.get("b")).isNull();

        // The feed stays usable: a later catch-up (with a converter that no longer parks) replays the whole history.
        BiFunction<EventMetadata, Ticked, Mono<Void>> restartedFold = Projections.reactiveUpdateWithMetadata(tickProjection(), repository);
        CatchupProjectionFeed<Ticked> restarted = CatchupProjectionFeed.create(
                "ticks", restartedFold, Filter.all(), reader(List.of("a-0", "b-0", "a-1")), tickedConverter(), Ticked::eventId, marker);
        restarted.catchUp().block(ofSeconds(5));

        await().atMost(ofSeconds(5)).untilAsserted(() -> {
            assertThat(repository.get("a")).isEqualTo(2);
            assertThat(repository.get("b")).isEqualTo(1);
        });
    }

    private static Projection<Integer, Ticked, String> tickProjection() {
        return Projection.<Integer, Ticked, String>builder(0)
                .id(Ticked::key)
                .on(Ticked.class, (state, event) -> state + 1)
                .build();
    }

    private static PositionOrderedReader reader(List<String> eventIds) {
        return new PositionOrderedReader() {
            @Override
            public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return Flux.fromIterable(eventIds).map(CoalescingMaterializedUpdateTest::cloudEvent);
            }

            @Override
            public Mono<Long> currentPosition() {
                return Mono.just((long) eventIds.size());
            }

            @Override
            public boolean writesPosition() {
                return true;
            }
        };
    }

    private static CloudEvent cloudEvent(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(SOURCE).withType("Ticked").build();
    }

    private static CloudEventConverter<Ticked> tickedConverter() {
        return new CloudEventConverter<>() {
            @Override
            public CloudEvent toCloudEvent(Ticked domainEvent) {
                return cloudEvent(domainEvent.eventId());
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
