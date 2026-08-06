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
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupProjectionFeedTest {

    private static final URI SOURCE = URI.create("urn:occurrent:test");

    @Test
    void catches_up_history_then_folds_live_domain_events() {
        CloudEventConverter<Counted> converter = countedConverter();
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        CatchupProjectionFeed<Counted> feed = feed("counter", reader("1", "2"), converter, repo, null);

        feed.catchUp().block();
        await().atMost(ofSeconds(5)).untilAsserted(() -> assertThat(repo.get("counter")).isEqualTo(2));

        feed.accept(new Counted("3")).block();
        await().atMost(ofSeconds(5)).untilAsserted(() -> assertThat(repo.get("counter")).isEqualTo(3));
    }

    @Test
    void catch_up_threads_event_metadata_into_the_fold() {
        CloudEventConverter<Counted> converter = countedConverter();
        ConcurrentHashMap<String, Long> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Long, String> repository = ViewStateRepository.create(repo::get, repo::put);
        // Keyed by the stream id from the metadata and folding the global position: both come from the replayed
        // CloudEvent, so if the catch-up did not thread the metadata, keying on getStreamId() would fail on empty metadata.
        Projection<Long, Counted, String> projection = Projection.<Long, Counted, String>builder(0L)
                .id((metadata, event) -> metadata.getStreamId())
                .on(Counted.class, (state, metadata, event) -> metadata.getPosition())
                .build();
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "positions", projection, repository, metadataReader("1", "2"), converter, Counted::eventId, null);

        feed.catchUp().block();

        // Keyed under the stream id "s" from the metadata, folded to the last replayed event's position (2), not to the
        // 0 that an empty-metadata fold would leave behind.
        assertThat(repo).containsOnlyKeys("s");
        assertThat(repo.get("s")).isEqualTo(2L);
    }

    @Test
    void a_live_domain_event_fed_with_metadata_lands_under_the_metadata_derived_key() {
        CloudEventConverter<Counted> converter = countedConverter();
        ConcurrentHashMap<String, Long> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Long, String> repository = ViewStateRepository.create(repo::get, repo::put);
        // Keyed by the stream id from the metadata and folding the global position, same shape as issue 389's
        // headline bug: catches up correctly, then a live event supplies its own metadata via accept(metadata, event).
        Projection<Long, Counted, String> projection = Projection.<Long, Counted, String>builder(0L)
                .id((metadata, event) -> metadata.getStreamId())
                .on(Counted.class, (state, metadata, event) -> metadata.getPosition())
                .build();
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "positions", projection, repository, metadataReader("1", "2"), converter, Counted::eventId, null);
        feed.catchUp().block();

        feed.accept(metadata("live-stream", 99L), new Counted("3")).block();

        assertThat(repo).containsKey("live-stream");
        assertThat(repo.get("live-stream")).isEqualTo(99L);
    }

    @Test
    void a_live_domain_event_fed_without_metadata_for_a_position_keyed_projection_throws_IllegalStateException() {
        CloudEventConverter<Counted> converter = countedConverter();
        ConcurrentHashMap<Long, Integer> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Integer, Long> repository = ViewStateRepository.create(repo::get, repo::put);
        // Keyed on getPosition(), the accessor that returns null on empty metadata rather than throwing. Before the
        // fix this silently dropped the live event and completed normally instead of surfacing the missing metadata.
        Projection<Integer, Counted, Long> projection = Projection.<Integer, Counted, Long>builder(0)
                .id((metadata, event) -> metadata.getPosition())
                .on(Counted.class, (state, event) -> state + 1)
                .build();
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "positions-key", projection, repository, metadataReader("1"), converter, Counted::eventId, null);
        feed.catchUp().block();

        StepVerifier.create(feed.accept(new Counted("2")))
                .verifyErrorSatisfies(e -> assertThat(e)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("would have been skipped silently"));
    }

    @Test
    void a_projection_declared_metadata_keyed_but_ignoring_the_metadata_does_not_error_on_a_plain_live_event() {
        CloudEventConverter<Counted> converter = countedConverter();
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);
        // Declared through id(BiFunction), so metadataKeyed() is true, but the key itself never reads the metadata,
        // so it always returns a real id and the metadata guard must never trip for it.
        Projection<Integer, Counted, String> projection = Projection.<Integer, Counted, String>builder(0)
                .id((metadata, event) -> event.eventId())
                .on(Counted.class, (state, event) -> state + 1)
                .build();
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "ignoring-metadata", projection, repository, reader(), converter, Counted::eventId, null);
        feed.catchUp().block();

        feed.accept(new Counted("live-1")).block();

        await().atMost(ofSeconds(5)).untilAsserted(() -> assertThat(repo.get("live-1")).isEqualTo(1));
    }

    @Test
    void an_event_keyed_projection_whose_id_returns_null_skips_that_event_and_still_folds_the_rest() {
        CloudEventConverter<Counted> converter = countedConverter();
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);
        // id(Function), not id(BiFunction): metadataKeyed() stays false, so a null id here is the documented "skip",
        // not the metadata guard's failure mode.
        Projection<Integer, Counted, String> projection = Projection.<Integer, Counted, String>builder(0)
                .id(event -> event.eventId().equals("skip-me") ? null : "counter")
                .on(Counted.class, (state, event) -> state + 1)
                .build();
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "skip-null-id", projection, repository, reader(), converter, Counted::eventId, null);
        feed.catchUp().block();

        feed.accept(new Counted("skip-me")).block();
        feed.accept(new Counted("keep-me")).block();

        await().atMost(ofSeconds(5)).untilAsserted(() -> assertThat(repo.get("counter")).isEqualTo(1));
    }

    @Test
    void an_event_both_replayed_and_delivered_live_during_catch_up_is_folded_once() {
        CloudEventConverter<Counted> converter = countedConverter();
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        CatchupProjectionFeed<Counted> feed = feed("counter", reader("1", "2"), converter, repo, null);

        feed.accept(new Counted("2")).subscribe(); // buffered before catch-up, overlaps the replay
        feed.catchUp().block();

        await().during(ofSeconds(1)).atMost(ofSeconds(5)).untilAsserted(() -> assertThat(repo.get("counter")).isEqualTo(2));
    }

    @Test
    void a_live_event_not_in_the_replay_is_folded_after_the_catch_up() {
        CloudEventConverter<Counted> converter = countedConverter();
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        CatchupProjectionFeed<Counted> feed = feed("counter", reader("1", "2"), converter, repo, null);

        feed.accept(new Counted("3")).subscribe(); // buffered, not in history
        feed.catchUp().block();

        await().atMost(ofSeconds(5)).untilAsserted(() -> assertThat(repo.get("counter")).isEqualTo(3));
    }

    @Test
    void a_restart_skips_the_replay_when_the_catchup_marker_exists() {
        CloudEventConverter<Counted> converter = countedConverter();
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();

        feed("counter", reader("1", "2"), converter, repo, marker).catchUp().block();
        await().atMost(ofSeconds(5)).untilAsserted(() -> assertThat(repo.get("counter")).isEqualTo(2));

        // Restart: the replay is skipped, so the persisted count is not re-folded (which would double it).
        CatchupProjectionFeed<Counted> restarted = feed("counter", reader("1", "2"), converter, repo, marker);
        restarted.catchUp().block();
        await().during(ofSeconds(1)).atMost(ofSeconds(5)).untilAsserted(() -> assertThat(repo.get("counter")).isEqualTo(2));

        restarted.accept(new Counted("3")).block();
        await().atMost(ofSeconds(5)).untilAsserted(() -> assertThat(repo.get("counter")).isEqualTo(3));
    }

    @Test
    void overflowing_the_live_buffer_during_catch_up_fails_loud() {
        CloudEventConverter<Counted> converter = countedConverter();
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "counter", projection(), repository, reader(), converter, Counted::eventId, null, new CatchupThenLiveOptions(10, 2));

        feed.accept(new Counted("l1")).subscribe();
        feed.accept(new Counted("l2")).subscribe();
        StepVerifier.create(feed.accept(new Counted("l3")))
                .verifyErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class).hasMessageContaining("buffer overflowed"));
    }

    @Test
    void a_catch_up_failure_fails_live_acks_instead_of_hanging() {
        CloudEventConverter<Counted> converter = countedConverter();
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        PositionOrderedReader failingReader = new PositionOrderedReader() {
            @Override
            public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return Flux.error(new IllegalStateException("replay boom"));
            }

            @Override
            public Mono<Long> currentPosition() {
                return Mono.just(0L);
            }

            @Override
            public boolean writesPosition() {
                return true;
            }
        };
        CatchupProjectionFeed<Counted> feed = feed("counter", failingReader, converter, repo, null);

        feed.catchUp().subscribe(v -> {
        }, e -> {
        }); // replay fails, so the pipeline terminates
        // A live event fed after the failed catch-up must error rather than hang.
        StepVerifier.create(feed.accept(new Counted("live")))
                .expectErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class).hasMessageContaining("replay boom"))
                .verify(ofSeconds(5));
    }

    @Test
    void catches_up_and_folds_live_events_through_a_caller_supplied_fold() {
        CloudEventConverter<Counted> converter = countedConverter();
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        // The parity overload: drive an existing reactive fold plus a replay filter, instead of a projection and
        // repository. This is what the reactor DomainEventFeed uses to back a MaterializedView store.
        Function<Counted, Mono<Void>> fold = event -> Mono.fromRunnable(() -> repo.merge("counter", 1, Integer::sum));
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "counter", fold, Filter.all(), reader("1", "2"), converter, Counted::eventId, null);

        feed.catchUp().block();
        await().atMost(ofSeconds(5)).untilAsserted(() -> assertThat(repo.get("counter")).isEqualTo(2));

        feed.accept(new Counted("3")).block();
        await().atMost(ofSeconds(5)).untilAsserted(() -> assertThat(repo.get("counter")).isEqualTo(3));
    }

    @Test
    void a_reader_that_does_not_write_positions_fails_fast_at_construction() {
        CloudEventConverter<Counted> converter = countedConverter();
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);

        assertThatThrownBy(() ->
                CatchupProjectionFeed.create("counter", projection(), repository, positionlessReader(), converter, Counted::eventId, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("writesPosition");
    }

    @Test
    void go_live_delivers_buffered_events_without_ever_reading_history() {
        CloudEventConverter<Counted> converter = countedConverter();
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        // Errors if replay() is ever subscribed to, so a passing test proves goLive() never reads history.
        PositionOrderedReader failingReader = new PositionOrderedReader() {
            @Override
            public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return Flux.error(new IllegalStateException("replay boom"));
            }

            @Override
            public Mono<Long> currentPosition() {
                return Mono.just(0L);
            }

            @Override
            public boolean writesPosition() {
                return true;
            }
        };
        CatchupProjectionFeed<Counted> feed = feed("counter", failingReader, converter, repo, null);

        feed.accept(new Counted("1")).subscribe();
        feed.goLive().block();

        await().atMost(ofSeconds(5)).untilAsserted(() -> assertThat(repo.get("counter")).isEqualTo(1));

        feed.accept(new Counted("2")).block();
        assertThat(repo.get("counter")).isEqualTo(2);
    }

    @Test
    void go_live_writes_no_completion_marker_so_a_later_catch_up_still_replays_history() {
        CloudEventConverter<Counted> converter = countedConverter();
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();

        feed("counter", reader("1", "2"), converter, new ConcurrentHashMap<>(), marker).goLive().block();

        assertThat(marker.read("counter").blockOptional()).isEmpty();

        Map<String, Integer> repo = new ConcurrentHashMap<>();
        feed("counter", reader("1", "2"), converter, repo, marker).catchUp().block();

        await().atMost(ofSeconds(5)).untilAsserted(() -> assertThat(repo.get("counter")).isEqualTo(2));
    }

    @Test
    void go_live_completes_normally_the_first_time() {
        CloudEventConverter<Counted> converter = countedConverter();
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        CatchupProjectionFeed<Counted> feed = feed("counter", reader(), converter, repo, null);

        StepVerifier.create(feed.goLive()).verifyComplete();

        feed.accept(new Counted("1")).block();
        assertThat(repo.get("counter")).isEqualTo(1);
    }

    @Test
    void a_null_event_id_fails_fast_instead_of_silently_dropping() {
        CloudEventConverter<Counted> converter = countedConverter();
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "counter", projection(), repository, reader("1"), converter, event -> null, null);

        StepVerifier.create(feed.catchUp())
                .verifyErrorSatisfies(e -> assertThat(e).isInstanceOf(NullPointerException.class).hasMessageContaining("eventId function returned null"));
    }

    // --- helpers ---

    private static CatchupProjectionFeed<Counted> feed(String id, PositionOrderedReader reader, CloudEventConverter<Counted> converter,
                                                             Map<String, Integer> repo, CheckpointStorage marker) {
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);
        return CatchupProjectionFeed.create(id, projection(), repository, reader, converter, Counted::eventId, marker);
    }

    private static Projection<Integer, Counted, String> projection() {
        return Projection.<Integer, Counted, String>builder(0)
                .id(event -> "counter")
                .on(Counted.class, (state, event) -> state + 1)
                .build();
    }

    // A reader whose history is the given event ids, in position order.
    private PositionOrderedReader reader(String... eventIds) {
        return new PositionOrderedReader() {
            @Override
            public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return Flux.fromIterable(List.of(eventIds)).map(CatchupProjectionFeedTest::cloudEvent);
            }

            @Override
            public Mono<Long> currentPosition() {
                return Mono.just((long) eventIds.length);
            }

            @Override
            public boolean writesPosition() {
                return true;
            }
        };
    }

    // A reader whose history carries real stream metadata (stream id "s", a stream version, and a global position),
    // unlike reader(...) above which builds bare CloudEvents with no extensions. Needed to prove the catch-up threads
    // the decoded EventMetadata into the fold rather than EventMetadata.empty().
    private static PositionOrderedReader metadataReader(String... eventIds) {
        return new PositionOrderedReader() {
            @Override
            public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                List<CloudEvent> events = new ArrayList<>();
                for (int i = 0; i < eventIds.length; i++) {
                    long version = i + 1;
                    CloudEvent event = CloudEventBuilder.v1()
                            .withId(eventIds[i])
                            .withSource(SOURCE)
                            .withType("Counted")
                            .withExtension(OccurrentCloudEventExtension.occurrent("s", version))
                            .build();
                    events.add(OccurrentCloudEventExtension.withPosition(event, version));
                }
                return Flux.fromIterable(events);
            }

            @Override
            public Mono<Long> currentPosition() {
                return Mono.just((long) eventIds.length);
            }

            @Override
            public boolean writesPosition() {
                return true;
            }
        };
    }

    private static EventMetadata metadata(String streamId, long position) {
        Map<String, Object> data = new HashMap<>();
        data.put(OccurrentCloudEventExtension.STREAM_ID, streamId);
        data.put(OccurrentCloudEventExtension.POSITION, position);
        return new EventMetadata(data);
    }

    // A reader whose writesPosition() returns false, so the fail-fast guard rejects it before any replay.
    private PositionOrderedReader positionlessReader() {
        return new PositionOrderedReader() {
            @Override
            public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return Flux.empty();
            }

            @Override
            public Mono<Long> currentPosition() {
                return Mono.just(0L);
            }

            @Override
            public boolean writesPosition() {
                return false;
            }
        };
    }

    private static CloudEvent cloudEvent(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(SOURCE).withType("Counted").build();
    }

    private static CloudEventConverter<Counted> countedConverter() {
        return new CloudEventConverter<>() {
            @Override
            public CloudEvent toCloudEvent(Counted domainEvent) {
                return cloudEvent(domainEvent.eventId());
            }

            @Override
            public Counted toDomainEvent(CloudEvent cloudEvent) {
                return new Counted(cloudEvent.getId());
            }

            @Override
            public String getCloudEventType(Class<? extends Counted> type) {
                return "Counted";
            }
        };
    }

    record Counted(String eventId) {
    }

}
