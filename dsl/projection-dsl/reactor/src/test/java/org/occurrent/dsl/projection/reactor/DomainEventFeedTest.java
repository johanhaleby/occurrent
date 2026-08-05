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
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import reactor.test.StepVerifier;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

@DisplayNameGeneration(ReplaceUnderscores.class)
class DomainEventFeedTest {

    private static final URI SOURCE = URI.create("urn:occurrent:test");

    record Counted(String eventId) {
    }

    @Test
    void registering_two_projections_with_the_same_id_throws() {
        CloudEventConverter<Counted> converter = countedConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(reader(), converter, Counted::eventId);

        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);
        feed.register("counter", projection(), repository);

        Throwable thrown = catchThrowable(() -> feed.register("counter", projection(), repository));

        // The message comes from SingleConsumerMessages.singleConsumerOnly, shared with the push subscription models,
        // so it names the registered projection and the one refused rather than saying "already registered".
        assertThat(thrown).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("counter")
                .hasMessageContaining("already feeds")
                .hasMessageContaining("refused");
    }

    @Test
    void registering_with_a_null_id_throws_instead_of_failing_inside_the_id_set() {
        CloudEventConverter<Counted> converter = countedConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(reader(), converter, Counted::eventId);

        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);

        Throwable thrown = catchThrowable(() -> feed.register(null, projection(), repository));

        assertThat(thrown).isInstanceOf(NullPointerException.class).hasMessageContaining("id cannot be null");
    }

    @Test
    void a_failed_registration_does_not_permanently_reserve_the_id() {
        CloudEventConverter<Counted> converter = countedConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(readerThatDoesNotWritePosition(), converter, Counted::eventId);

        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);

        Throwable firstAttempt = catchThrowable(() -> feed.register("counter", projection(), repository));
        assertThat(firstAttempt).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("does not write positions");

        // A second attempt with the same id must fail the same way, not with "already registered": the first attempt's
        // feed was never created, so the id must not have been reserved.
        Throwable secondAttempt = catchThrowable(() -> feed.register("counter", projection(), repository));

        assertThat(secondAttempt).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not write positions")
                .hasMessageNotContaining("already registered");
    }

    @Test
    void registering_two_projections_with_different_ids_throws() {
        CloudEventConverter<Counted> converter = countedConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(reader(), converter, Counted::eventId);

        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);
        feed.register("counter-1", projection(), repository);

        Throwable thrown = catchThrowable(() -> feed.register("counter-2", projection(), repository));

        assertThat(thrown).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("counter-1")
                .hasMessageContaining("counter-2");

        // The first registration is unaffected by the refused second one: it still works.
        feed.catchUpAll().block();
        feed.accept(new Counted("1")).block();
        assertThat(repo.get("counter")).isEqualTo(1);
    }

    @Test
    void accept_with_metadata_feeds_the_registered_projection_with_the_metadata_intact() {
        CloudEventConverter<Counted> converter = countedConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(reader(), converter, Counted::eventId);

        ConcurrentHashMap<String, Long> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Long, String> repository = ViewStateRepository.create(repo::get, repo::put);
        feed.register("positions", positionKeyedProjection(), repository);
        feed.catchUpAll().block();

        feed.accept(metadata("stream-1", 7L), new Counted("live")).block();

        assertThat(repo.get("stream-1")).isEqualTo(7L);
    }

    @Test
    void stopping_a_catch_up_mid_replay_does_not_write_the_marker_and_a_later_catch_up_replays_from_the_beginning() throws InterruptedException {
        CloudEventConverter<Counted> converter = countedConverter();
        CountDownLatch parked = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        AtomicInteger deliveries = new AtomicInteger();
        // Blocks the replay's fold for the first event before it returns, so the catch-up is genuinely still running
        // (on boundedElastic) when the test calls stopCatchUp(), landing the stop deterministically rather than by
        // racing a sleep against it. Counts deliveries rather than reading the cumulative counter's final value,
        // because a projection that folds "+1" onto its current state (like this one) double-counts once a second
        // full replay folds on top of what the first, aborted one already wrote.
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, (id, state) -> {
            repo.put(id, state);
            if (deliveries.incrementAndGet() == 1) {
                parked.countDown();
                awaitUninterruptibly(proceed);
            }
        });
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(reader("1", "2"), converter, Counted::eventId, marker);
        feed.register("counter", projection(), repository);

        CountDownLatch catchUpFinished = new CountDownLatch(1);
        // doFinally is a real join on the stopped catch-up, unlike subscribe()'s own return: it counts down only once
        // the Mono has actually terminated, so awaiting it proves the stop landed instead of polling for a side effect
        // (the fold's increment, which is what releases "parked" below) that already happened before stopCatchUp() was
        // even called.
        feed.catchUpAll().doFinally(signal -> catchUpFinished.countDown()).subscribe(); // runs on boundedElastic, so this returns before it finishes
        parked.await();
        feed.stopCatchUp();
        proceed.countDown();

        assertThat(catchUpFinished.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(deliveries.get()).isEqualTo(1);
        // A partial replay is never recorded as a finished one.
        assertThat(marker.read("counter").blockOptional()).isEmpty();

        // The feed is still usable: a later catch-up replays the whole history again, from the beginning, so both
        // events are folded once more rather than only the one the stop skipped. block() already joins the fold, so
        // the assertion right after it needs no additional wait.
        feed.catchUpAll().block();
        assertThat(deliveries.get()).isEqualTo(3);

        feed.accept(new Counted("3")).block();
        assertThat(deliveries.get()).isEqualTo(4);
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }


    @Test
    void register_with_a_metadata_aware_fold_replays_and_folds_live_events_with_metadata_intact() {
        CloudEventConverter<Counted> converter = countedConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(reader(), converter, Counted::eventId);
        ConcurrentHashMap<String, Long> repo = new ConcurrentHashMap<>();
        // The metadata-aware register overload: the caller supplies a BiFunction<EventMetadata, E, Mono<Void>> fold
        // directly instead of a Projection and repository.
        BiFunction<EventMetadata, Counted, Mono<Void>> fold = (metadata, event) ->
                Mono.fromRunnable(() -> repo.put(metadata.getStreamId(), metadata.getPosition()));

        feed.register("positions", fold, Filter.all());
        feed.catchUpAll().block();

        feed.accept(metadata("stream-1", 5L), new Counted("live")).block();

        assertThat(repo.get("stream-1")).isEqualTo(5L);
    }

    @Test
    void handover_options_passed_to_the_constructor_reach_every_registered_projections_catch_up() {
        CloudEventConverter<Counted> converter = countedConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(reader(), converter, Counted::eventId, null, new CatchupThenLiveOptions(10, 2));

        Map<String, Integer> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);
        feed.register("counter", projection(), repository);

        // Buffered before the catch-up runs, so the third one exceeds the cap of two. The message names the cap, which
        // is what proves the constructor's options reached CatchupProjectionFeed rather than the defaults being used.
        feed.accept(new Counted("l1")).subscribe();
        feed.accept(new Counted("l2")).subscribe();
        // verify(Duration) rather than verifyErrorSatisfies(): if the cap were not applied, this ack would stay pending
        // until the catch-up drained it, so an unbounded verify would hang instead of failing.
        StepVerifier.create(feed.accept(new Counted("l3")))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("buffer overflowed")
                        .hasMessageContaining("(cap 2)"))
                .verify(Duration.ofSeconds(5));
    }

    private static Projection<Long, Counted, String> positionKeyedProjection() {
        return Projection.<Long, Counted, String>builder(0L)
                .id((metadata, event) -> metadata.getStreamId())
                .on(Counted.class, (state, metadata, event) -> metadata.getPosition())
                .build();
    }

    private static EventMetadata metadata(String streamId, long position) {
        Map<String, Object> data = new HashMap<>();
        data.put(OccurrentCloudEventExtension.STREAM_ID, streamId);
        data.put(OccurrentCloudEventExtension.POSITION, position);
        return new EventMetadata(data);
    }

    private static Projection<Integer, Counted, String> projection() {
        return Projection.<Integer, Counted, String>builder(0)
                .id(event -> "counter")
                .on(Counted.class, (state, event) -> state + 1)
                .build();
    }

    private static PositionOrderedReader reader() {
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
                return true;
            }
        };
    }

    // A reader whose history is the given event ids, in position order. Used only by the stop-mid-replay test.
    private static PositionOrderedReader reader(String... eventIds) {
        List<CloudEvent> events = new ArrayList<>();
        for (String id : eventIds) {
            events.add(CloudEventBuilder.v1().withId(id).withSource(SOURCE).withType("Counted").build());
        }
        return new PositionOrderedReader() {
            @Override
            public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return Flux.fromIterable(events);
            }

            @Override
            public Mono<Long> currentPosition() {
                return Mono.just((long) events.size());
            }

            @Override
            public boolean writesPosition() {
                return true;
            }
        };
    }

    private static PositionOrderedReader readerThatDoesNotWritePosition() {
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

    private static CloudEventConverter<Counted> countedConverter() {
        return new CloudEventConverter<>() {
            @Override
            public CloudEvent toCloudEvent(Counted domainEvent) {
                return CloudEventBuilder.v1().withId(domainEvent.eventId()).withSource(SOURCE).withType("Counted").build();
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
}
