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
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.api.blocking.CheckpointStorage;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupProjectionFeedTest {

    private static final URI SOURCE = URI.create("urn:occurrent:test");

    @Test
    void catches_up_from_the_store_then_folds_live_domain_events() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = countedConverter();
        store.write("s", converter.toCloudEvents(List.of(new Counted("1"), new Counted("2"))));

        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        CatchupProjectionFeed<Counted> feed = feed("counter", store, converter, repo, null);
        feed.catchUp();

        assertThat(repo.get("counter")).isEqualTo(2);

        // A live domain event is folded directly, no CloudEvent involved.
        feed.accept(new Counted("3"));
        assertThat(repo.get("counter")).isEqualTo(3);
    }

    @Test
    void an_event_both_replayed_and_delivered_live_during_catch_up_is_folded_once() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = countedConverter();
        store.write("s", converter.toCloudEvents(List.of(new Counted("1"), new Counted("2"))));

        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        CatchupProjectionFeed<Counted> feed = feed("counter", store, converter, repo, null);

        // "2" also arrives live before the catch-up completes (the replay-to-live overlap).
        feed.accept(new Counted("2"));
        feed.catchUp();

        // Deduped by the domain event id: folded once (via the replay), so the count is 2, not 3.
        assertThat(repo.get("counter")).isEqualTo(2);
    }

    @Test
    void a_live_event_not_in_the_replay_is_folded_after_the_catch_up() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = countedConverter();
        store.write("s", converter.toCloudEvents(List.of(new Counted("1"), new Counted("2"))));

        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        CatchupProjectionFeed<Counted> feed = feed("counter", store, converter, repo, null);

        // "3" is not in history but arrives live during catch-up; it must not be lost.
        feed.accept(new Counted("3"));
        feed.catchUp();

        assertThat(repo.get("counter")).isEqualTo(3);
    }

    @Test
    void a_restart_skips_the_replay_when_the_catchup_marker_exists() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = countedConverter();
        store.write("s", converter.toCloudEvents(List.of(new Counted("1"), new Counted("2"))));

        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();

        feed("counter", store, converter, repo, marker).catchUp();
        assertThat(repo.get("counter")).isEqualTo(2);

        // Restart: a fresh feed over the same store, repository, and marker. The replay is skipped, so the persisted
        // count is not re-folded (which would double it to 4).
        CatchupProjectionFeed<Counted> restarted = feed("counter", store, converter, repo, marker);
        restarted.catchUp();
        assertThat(repo.get("counter")).isEqualTo(2);

        restarted.accept(new Counted("3"));
        assertThat(repo.get("counter")).isEqualTo(3);
    }

    @Test
    void overflowing_the_live_buffer_during_catch_up_fails_loud() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = countedConverter();

        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "counter", projection(), repository, store, converter, Counted::eventId, null, 10, 2);

        feed.accept(new Counted("l1"));
        feed.accept(new Counted("l2"));
        Throwable thrown = catchThrowable(() -> feed.accept(new Counted("l3")));

        assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("buffer overflowed");
    }

    @Test
    void the_live_path_never_encodes_to_a_cloud_event() {
        InMemoryEventStore store = new InMemoryEventStore();
        AtomicInteger toCloudEventCalls = new AtomicInteger();
        AtomicInteger toDomainEventCalls = new AtomicInteger();
        CloudEventConverter<Counted> converter = countingConverter(toCloudEventCalls, toDomainEventCalls);
        store.write("s", converter.toCloudEvents(List.of(new Counted("1"))));
        int encodesAfterHistoryWrite = toCloudEventCalls.get();

        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        CatchupProjectionFeed<Counted> feed = feed("counter", store, converter, repo, null);
        feed.catchUp();
        feed.accept(new Counted("2"));

        // The catch-up decodes the one replayed event; the live path does neither encode nor decode.
        assertThat(toDomainEventCalls.get()).isEqualTo(1);
        assertThat(toCloudEventCalls.get()).isEqualTo(encodesAfterHistoryWrite);
        assertThat(repo.get("counter")).isEqualTo(2);
    }

    @Test
    void a_reader_that_does_not_write_positions_fails_fast_at_construction() {
        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);
        CloudEventConverter<Counted> converter = countedConverter();
        PositionOrderedReader reader = positionlessReader();

        Throwable thrown = catchThrowable(() ->
                CatchupProjectionFeed.create("counter", projection(), repository, reader, converter, Counted::eventId, null));

        assertThat(thrown).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("writesPosition");
    }

    @Test
    void a_null_event_id_fails_fast_instead_of_silently_dropping() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = countedConverter();
        store.write("s", converter.toCloudEvents(List.of(new Counted("1"))));

        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "counter", projection(), repository, store, converter, event -> null, null);

        Throwable thrown = catchThrowable(feed::catchUp);

        assertThat(thrown).isInstanceOf(NullPointerException.class).hasMessageContaining("eventId function returned null");
    }

    // --- helpers ---

    private static CatchupProjectionFeed<Counted> feed(String id, InMemoryEventStore store, CloudEventConverter<Counted> converter,
                                                             Map<String, Integer> repo, CheckpointStorage marker) {
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);
        return CatchupProjectionFeed.create(id, projection(), repository, store, converter, Counted::eventId, marker);
    }

    private static Projection<Integer, Counted, String> projection() {
        return Projection.<Integer, Counted, String>builder(0)
                .id(event -> "counter")
                .on(Counted.class, (state, event) -> state + 1)
                .build();
    }

    private static CloudEventConverter<Counted> countedConverter() {
        return countingConverter(new AtomicInteger(), new AtomicInteger());
    }

    private static CloudEventConverter<Counted> countingConverter(AtomicInteger toCloudEvent, AtomicInteger toDomainEvent) {
        return new CloudEventConverter<>() {
            @Override
            public CloudEvent toCloudEvent(Counted domainEvent) {
                toCloudEvent.incrementAndGet();
                return CloudEventBuilder.v1()
                        .withId(domainEvent.eventId())
                        .withSource(SOURCE)
                        .withType("Counted")
                        .build();
            }

            @Override
            public Counted toDomainEvent(CloudEvent cloudEvent) {
                toDomainEvent.incrementAndGet();
                return new Counted(cloudEvent.getId());
            }

            @Override
            public String getCloudEventType(Class<? extends Counted> type) {
                return "Counted";
            }
        };
    }

    private static PositionOrderedReader positionlessReader() {
        return new PositionOrderedReader() {
            @Override
            public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return Stream.empty();
            }

            @Override
            public long currentPosition() {
                return 0;
            }

            @Override
            public boolean writesPosition() {
                return false;
            }
        };
    }

    record Counted(String eventId) {
    }

    private static final class InMemoryCheckpointStorage implements CheckpointStorage {
        private final Map<String, Checkpoint> checkpoints = new HashMap<>();

        @Override
        public Checkpoint read(String subscriptionId) {
            return checkpoints.get(subscriptionId);
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint) {
            checkpoints.put(subscriptionId, checkpoint);
            return checkpoint;
        }

        @Override
        public void delete(String subscriptionId) {
            checkpoints.remove(subscriptionId);
        }

        @Override
        public boolean exists(String subscriptionId) {
            return checkpoints.containsKey(subscriptionId);
        }
    }
}
