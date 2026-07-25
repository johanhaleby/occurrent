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
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.URI;
import java.util.ArrayList;
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
    void a_live_domain_event_is_folded_with_empty_metadata_so_a_stream_id_keyed_projection_throws() {
        CloudEventConverter<Counted> converter = countedConverter();
        ConcurrentHashMap<String, Long> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Long, String> repository = ViewStateRepository.create(repo::get, repo::put);
        Projection<Long, Counted, String> projection = Projection.<Long, Counted, String>builder(0L)
                .id((metadata, event) -> metadata.getStreamId())
                .on(Counted.class, (state, metadata, event) -> metadata.getPosition())
                .build();
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "positions", projection, repository, metadataReader("1", "2"), converter, Counted::eventId, null);
        feed.catchUp().block();

        // Metadata exists only where an event arrives as a CloudEvent. A live domain event has none, so it folds with
        // EventMetadata.empty(). Keying on getStreamId() throws, because that accessor requires the extension to be
        // present. This is not a general "fails loud" guarantee: getPosition() returns null on empty metadata, and a
        // null id means "skip this event", so a position-keyed projection drops every live event silently instead.
        // Both are the same unsupported combination, tracked in issue 389. The blocking feed behaves identically.
        StepVerifier.create(feed.accept(new Counted("3")))
                .verifyErrorSatisfies(e -> assertThat(e)
                        .isInstanceOf(NullPointerException.class)
                        .hasMessageContaining("streamId extension is absent"));
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
        InMemoryReactiveCheckpointStorage marker = new InMemoryReactiveCheckpointStorage();

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
                "counter", projection(), repository, reader(), converter, Counted::eventId, null, 10, 2);

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

    private static final class InMemoryReactiveCheckpointStorage implements CheckpointStorage {
        private final Map<String, Checkpoint> checkpoints = new ConcurrentHashMap<>();

        @Override
        public Mono<Checkpoint> read(String subscriptionId) {
            return Mono.justOrEmpty(checkpoints.get(subscriptionId));
        }

        @Override
        public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint) {
            checkpoints.put(subscriptionId, checkpoint);
            return Mono.just(checkpoint);
        }

        @Override
        public Mono<Void> delete(String subscriptionId) {
            return Mono.fromRunnable(() -> checkpoints.remove(subscriptionId));
        }
    }
}
