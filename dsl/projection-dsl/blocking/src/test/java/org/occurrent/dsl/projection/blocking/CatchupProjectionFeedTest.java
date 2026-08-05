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
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
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
    void catch_up_threads_event_metadata_into_the_fold() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = countedConverter();
        store.write("s", converter.toCloudEvents(List.of(new Counted("1"), new Counted("2"))));

        ConcurrentHashMap<String, Long> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Long, String> repository = ViewStateRepository.create(repo::get, repo::put);
        // Keyed by the stream id from the metadata and folding the global position: both come from the replayed
        // CloudEvent, so if the catch-up did not thread the metadata, keying on getStreamId() would fail on empty metadata.
        Projection<Long, Counted, String> projection = Projection.<Long, Counted, String>builder(0L)
                .id((metadata, event) -> metadata.getStreamId())
                .on(Counted.class, (state, metadata, event) -> metadata.getPosition())
                .build();
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "positions", projection, repository, store, converter, Counted::eventId, null);

        feed.catchUp();

        // Keyed under the stream id "s" from the metadata, folded to the last replayed event's position rather than to
        // the 0 that an empty-metadata fold would leave behind.
        long lastPosition = store.read("s").eventList().stream()
                .mapToLong(cloudEvent -> EventMetadata.from(cloudEvent).getPosition()).max().orElseThrow();
        assertThat(repo).containsOnlyKeys("s");
        assertThat(repo.get("s")).isEqualTo(lastPosition);
    }

    @Test
    void a_live_domain_event_fed_with_metadata_lands_under_the_metadata_derived_key() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = countedConverter();
        store.write("s", converter.toCloudEvents(List.of(new Counted("1"), new Counted("2"))));

        ConcurrentHashMap<String, Long> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Long, String> repository = ViewStateRepository.create(repo::get, repo::put);
        // Keyed by the stream id from the metadata and folding the global position, same shape as issue 389's
        // headline bug: catches up correctly, then a live event supplies its own metadata via accept(metadata, event).
        Projection<Long, Counted, String> projection = Projection.<Long, Counted, String>builder(0L)
                .id((metadata, event) -> metadata.getStreamId())
                .on(Counted.class, (state, metadata, event) -> metadata.getPosition())
                .build();
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "positions", projection, repository, store, converter, Counted::eventId, null);
        feed.catchUp();

        feed.accept(metadata("live-stream", 99L), new Counted("3"));

        assertThat(repo).containsKey("live-stream");
        assertThat(repo.get("live-stream")).isEqualTo(99L);
    }

    @Test
    void a_live_domain_event_fed_without_metadata_for_a_position_keyed_projection_throws_IllegalStateException() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = countedConverter();
        store.write("s", converter.toCloudEvents(List.of(new Counted("1"))));

        ConcurrentHashMap<Long, Integer> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Integer, Long> repository = ViewStateRepository.create(repo::get, repo::put);
        // Keyed on getPosition(), the accessor that returns null on empty metadata rather than throwing. Before the
        // fix this silently dropped the live event and returned normally instead of surfacing the missing metadata.
        Projection<Integer, Counted, Long> projection = Projection.<Integer, Counted, Long>builder(0)
                .id((metadata, event) -> metadata.getPosition())
                .on(Counted.class, (state, event) -> state + 1)
                .build();
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "positions-key", projection, repository, store, converter, Counted::eventId, null);
        feed.catchUp();

        Throwable thrown = catchThrowable(() -> feed.accept(new Counted("2")));

        assertThat(thrown).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("would have been skipped silently");
    }

    @Test
    void a_projection_declared_metadata_keyed_but_ignoring_the_metadata_does_not_throw_on_a_plain_live_event() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = countedConverter();

        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);
        // Declared through id(BiFunction), so metadataKeyed() is true, but the key itself never reads the metadata,
        // so it always returns a real id and the metadata guard must never trip for it.
        Projection<Integer, Counted, String> projection = Projection.<Integer, Counted, String>builder(0)
                .id((metadata, event) -> event.eventId())
                .on(Counted.class, (state, event) -> state + 1)
                .build();
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "ignoring-metadata", projection, repository, store, converter, Counted::eventId, null);
        feed.catchUp();

        Throwable thrown = catchThrowable(() -> feed.accept(new Counted("live-1")));

        assertThat(thrown).isNull();
        assertThat(repo.get("live-1")).isEqualTo(1);
    }

    @Test
    void an_event_keyed_projection_whose_id_returns_null_skips_that_event_and_still_folds_the_rest() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = countedConverter();

        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);
        // id(Function), not id(BiFunction): metadataKeyed() stays false, so a null id here is the documented "skip",
        // not the metadata guard's failure mode.
        Projection<Integer, Counted, String> projection = Projection.<Integer, Counted, String>builder(0)
                .id(event -> event.eventId().equals("skip-me") ? null : "counter")
                .on(Counted.class, (state, event) -> state + 1)
                .build();
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "skip-null-id", projection, repository, store, converter, Counted::eventId, null);
        feed.catchUp();

        Throwable thrown = catchThrowable(() -> feed.accept(new Counted("skip-me")));
        feed.accept(new Counted("keep-me"));

        assertThat(thrown).isNull();
        assertThat(repo.get("counter")).isEqualTo(1);
    }

    @Test
    void a_hand_written_materialized_view_gets_the_one_argument_form_for_accept_event_and_the_metadata_form_for_accept_metadata_event() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = countedConverter();
        List<String> callsReceived = new CopyOnWriteArrayList<>();
        MaterializedView<Counted> view = overloadRecordingView(callsReceived);
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "split-overload", view, Filter.all(), store, converter, Counted::eventId, null);
        feed.catchUp();

        feed.accept(new Counted("plain"));
        feed.accept(metadata("s", 1L), new Counted("with-metadata"));

        assertThat(callsReceived).containsExactly("event-only:plain", "metadata:with-metadata");
    }

    @Test
    void a_replayed_event_always_gets_the_metadata_form_never_the_one_argument_form() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = countedConverter();
        store.write("s", converter.toCloudEvents(List.of(new Counted("1"), new Counted("2"))));

        List<String> callsReceived = new CopyOnWriteArrayList<>();
        MaterializedView<Counted> view = overloadRecordingView(callsReceived);
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "replay-overload", view, Filter.all(), store, converter, Counted::eventId, null);

        feed.catchUp();

        // A replayed event has a CloudEvent behind it, so it always carries metadata and must take the metadata route.
        // Live and replayed deliveries share one carrier whose metadata is nullable, so this pins that a replayed one is
        // never constructed without it and so can never fall through to the one-argument overload.
        assertThat(callsReceived).containsExactly("metadata:1", "metadata:2");
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
                "counter", projection(), repository, store, converter, Counted::eventId, null, new CatchupThenLiveOptions(10, 2));

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
    void a_catch_up_failure_makes_accept_fail_fast_instead_of_buffering() {
        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);
        CloudEventConverter<Counted> converter = countedConverter();
        PositionOrderedReader reader = failingReader();

        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "counter", projection(), repository, reader, converter, Counted::eventId, null);

        Throwable replayFailure = catchThrowable(feed::catchUp);
        assertThat(replayFailure).isInstanceOf(IllegalStateException.class).hasMessageContaining("replay boom");

        Throwable thrown = catchThrowable(() -> feed.accept(new Counted("x")));

        assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("Catch-up failed");
    }

    @Test
    void go_live_delivers_buffered_events_without_ever_reading_history() {
        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);
        // Throws if replay() is ever called, so a passing test proves goLive() never reads history.
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "counter", projection(), repository, failingReader(), countedConverter(), Counted::eventId, null);

        feed.accept(new Counted("1"));
        feed.goLive();

        assertThat(repo.get("counter")).isEqualTo(1);

        feed.accept(new Counted("2"));
        assertThat(repo.get("counter")).isEqualTo(2);
    }

    @Test
    void go_live_writes_no_completion_marker_so_a_later_catch_up_still_replays_history() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = countedConverter();
        store.write("s", converter.toCloudEvents(List.of(new Counted("1"), new Counted("2"))));
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();

        feed("counter", store, converter, new ConcurrentHashMap<>(), marker).goLive();

        assertThat(marker.exists("counter")).isFalse();

        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        feed("counter", store, converter, repo, marker).catchUp();

        assertThat(repo.get("counter")).isEqualTo(2);
    }

    @Test
    void calling_go_live_twice_is_harmless() {
        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        CatchupProjectionFeed<Counted> feed = feed("counter", new InMemoryEventStore(), countedConverter(), repo, null);

        feed.goLive();
        feed.accept(new Counted("1"));
        feed.goLive();
        feed.accept(new Counted("2"));

        assertThat(repo.get("counter")).isEqualTo(2);
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

    private static EventMetadata metadata(String streamId, long position) {
        Map<String, Object> data = new HashMap<>();
        data.put(OccurrentCloudEventExtension.STREAM_ID, streamId);
        data.put(OccurrentCloudEventExtension.POSITION, position);
        return new EventMetadata(data);
    }

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

    private static PositionOrderedReader failingReader() {
        return new PositionOrderedReader() {
            @Override
            public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                throw new IllegalStateException("replay boom");
            }

            @Override
            public long currentPosition() {
                return 0;
            }

            @Override
            public boolean writesPosition() {
                return true;
            }
        };
    }

    record Counted(String eventId) {
    }

    // Implements both MaterializedView overloads differently, so a test can tell which route a delivery took.
    private static MaterializedView<Counted> overloadRecordingView(List<String> callsReceived) {
        return new MaterializedView<>() {
            @Override
            public void update(Counted event) {
                callsReceived.add("event-only:" + event.eventId());
            }

            @Override
            public void update(EventMetadata metadata, Counted event) {
                callsReceived.add("metadata:" + event.eventId());
            }
        };
    }
}
