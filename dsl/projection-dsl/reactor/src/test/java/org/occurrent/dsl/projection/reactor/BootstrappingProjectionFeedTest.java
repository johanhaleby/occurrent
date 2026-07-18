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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@DisplayNameGeneration(ReplaceUnderscores.class)
class BootstrappingProjectionFeedTest {

    private static final URI SOURCE = URI.create("urn:occurrent:test");

    @Test
    void bootstraps_history_then_folds_live_domain_events() {
        CloudEventConverter<Counted> converter = countedConverter();
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        BootstrappingProjectionFeed<Counted> feed = feed("counter", reader("1", "2"), converter, repo, null);

        feed.bootstrap().block();
        await().atMost(ofSeconds(5)).untilAsserted(() -> assertThat(repo.get("counter")).isEqualTo(2));

        feed.accept(new Counted("3")).block();
        await().atMost(ofSeconds(5)).untilAsserted(() -> assertThat(repo.get("counter")).isEqualTo(3));
    }

    @Test
    void an_event_both_replayed_and_delivered_live_during_bootstrap_is_folded_once() {
        CloudEventConverter<Counted> converter = countedConverter();
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        BootstrappingProjectionFeed<Counted> feed = feed("counter", reader("1", "2"), converter, repo, null);

        feed.accept(new Counted("2")).subscribe(); // buffered before bootstrap, overlaps the replay
        feed.bootstrap().block();

        await().during(ofSeconds(1)).atMost(ofSeconds(5)).untilAsserted(() -> assertThat(repo.get("counter")).isEqualTo(2));
    }

    @Test
    void a_live_event_not_in_the_replay_is_folded_after_the_bootstrap() {
        CloudEventConverter<Counted> converter = countedConverter();
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        BootstrappingProjectionFeed<Counted> feed = feed("counter", reader("1", "2"), converter, repo, null);

        feed.accept(new Counted("3")).subscribe(); // buffered, not in history
        feed.bootstrap().block();

        await().atMost(ofSeconds(5)).untilAsserted(() -> assertThat(repo.get("counter")).isEqualTo(3));
    }

    @Test
    void a_restart_skips_the_replay_when_the_bootstrap_marker_exists() {
        CloudEventConverter<Counted> converter = countedConverter();
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        InMemoryReactiveCheckpointStorage marker = new InMemoryReactiveCheckpointStorage();

        feed("counter", reader("1", "2"), converter, repo, marker).bootstrap().block();
        await().atMost(ofSeconds(5)).untilAsserted(() -> assertThat(repo.get("counter")).isEqualTo(2));

        // Restart: the replay is skipped, so the persisted count is not re-folded (which would double it).
        BootstrappingProjectionFeed<Counted> restarted = feed("counter", reader("1", "2"), converter, repo, marker);
        restarted.bootstrap().block();
        await().during(ofSeconds(1)).atMost(ofSeconds(5)).untilAsserted(() -> assertThat(repo.get("counter")).isEqualTo(2));

        restarted.accept(new Counted("3")).block();
        await().atMost(ofSeconds(5)).untilAsserted(() -> assertThat(repo.get("counter")).isEqualTo(3));
    }

    @Test
    void overflowing_the_live_buffer_during_bootstrap_fails_loud() {
        CloudEventConverter<Counted> converter = countedConverter();
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);
        BootstrappingProjectionFeed<Counted> feed = BootstrappingProjectionFeed.create(
                "counter", projection(), repository, reader(), converter, Counted::eventId, null, 10, 2);

        feed.accept(new Counted("l1")).subscribe();
        feed.accept(new Counted("l2")).subscribe();
        StepVerifier.create(feed.accept(new Counted("l3")))
                .verifyErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class).hasMessageContaining("buffer overflowed"));
    }

    @Test
    void a_bootstrap_failure_fails_live_acks_instead_of_hanging() {
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
        BootstrappingProjectionFeed<Counted> feed = feed("counter", failingReader, converter, repo, null);

        feed.bootstrap().subscribe(v -> {
        }, e -> {
        }); // replay fails, so the pipeline terminates
        // A live event fed after the failed bootstrap must error rather than hang.
        StepVerifier.create(feed.accept(new Counted("live")))
                .expectErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class).hasMessageContaining("replay boom"))
                .verify(ofSeconds(5));
    }

    // --- helpers ---

    private static BootstrappingProjectionFeed<Counted> feed(String id, PositionOrderedReader reader, CloudEventConverter<Counted> converter,
                                                             Map<String, Integer> repo, CheckpointStorage marker) {
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);
        return BootstrappingProjectionFeed.create(id, projection(), repository, reader, converter, Counted::eventId, marker);
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
                return Flux.fromIterable(List.of(eventIds)).map(BootstrappingProjectionFeedTest::cloudEvent);
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
