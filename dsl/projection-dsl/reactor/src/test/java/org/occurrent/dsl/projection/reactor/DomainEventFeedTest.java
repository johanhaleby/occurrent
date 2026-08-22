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
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.RoutingOutcome;
import org.occurrent.subscription.UnreadableLiveFilterException;
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
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.occurrent.condition.Condition.eq;

@DisplayNameGeneration(ReplaceUnderscores.class)
class DomainEventFeedTest {

    private static final URI SOURCE = URI.create("urn:occurrent:test");

    record Counted(String eventId) {
    }

    @Test
    void feeding_an_event_before_a_projection_is_registered_is_refused() {
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(reader(), countedConverter(), Counted::eventId);

        // Completing is what a listener reads as "handled", so it would acknowledge the message and the broker would
        // discard an event nothing received. Under occurrent.subscription.mode=manual registration is deferred, so a
        // listener that starts consuming before startAll() lands exactly here, and refusing is what makes the broker
        // hold the backlog instead. That is ADR 86's withheld-not-lost guarantee on a push stack.
        StepVerifier.create(feed.accept(new Counted("1")))
                .expectErrorSatisfies(e -> assertThat(e)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("has no projection registered")
                        .hasMessageContaining("refused rather than accepted"))
                .verify(Duration.ofSeconds(5));

        StepVerifier.create(feed.accept(EventMetadata.empty(), new Counted("1")))
                .expectErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("has no projection registered"))
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void a_projection_registered_after_the_accept_mono_was_assembled_is_still_found() {
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(reader(), countedConverter(), Counted::eventId);
        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();

        // Assembled while the feed is empty. The lookup is deferred to subscription time, the same lateness
        // catchUp(String) and goLive(String) already have, so this must not have captured the refusal.
        Mono<Void> accept = feed.accept(new Counted("1"));

        feed.register("counter", projection(), ViewStateRepository.create(repo::get, repo::put));
        feed.goLive("counter").block(Duration.ofSeconds(5));

        StepVerifier.create(accept).verifyComplete();
        assertThat(repo).containsEntry("counter", 1);
    }

    @Test
    void catching_up_a_feed_with_no_projection_is_refused() {
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(reader(), countedConverter(), Counted::eventId);

        // It used to complete empty, so a feed nobody had registered on reported a successful catch-up and then
        // silently fed nothing. catchUp(String) already refused, so this is the pair of them agreeing.
        StepVerifier.create(feed.catchUpAll())
                .expectErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("has no projection registered"))
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void a_feed_reports_whether_a_projection_is_registered() {
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(reader(), countedConverter(), Counted::eventId);
        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();

        assertThat(feed.hasProjection()).isFalse();

        feed.register("counter", projection(), ViewStateRepository.create(repo::get, repo::put));

        assertThat(feed.hasProjection()).isTrue();
    }

    @Test
    void stopping_a_catch_up_on_a_feed_with_no_projection_does_nothing() {
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(reader(), countedConverter(), Counted::eventId);

        // A shutdown verb, unlike the two above. One that throws because there was nothing to shut down is a nuisance
        // in a context-close path, not a safeguard.
        assertThat(catchThrowable(feed::stopCatchUp)).isNull();
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
    void go_live_of_the_registered_id_skips_the_replay_and_goes_live() {
        CloudEventConverter<Counted> converter = countedConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(reader("1", "2"), converter, Counted::eventId);

        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        feed.register("counter", projection(), ViewStateRepository.create(repo::get, repo::put));

        feed.goLive("counter").block();
        feed.accept(new Counted("live")).block();

        // The two history events were never replayed, so only the live one was folded.
        assertThat(repo.get("counter")).isEqualTo(1);
    }

    @Test
    void go_live_of_an_unregistered_id_throws() {
        CloudEventConverter<Counted> converter = countedConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(reader(), converter, Counted::eventId);

        StepVerifier.create(feed.goLive("missing"))
                .verifyErrorSatisfies(e -> assertThat(e)
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("missing")
                        .hasMessageContaining("No projection"));
    }

    @Test
    void go_live_of_an_id_that_does_not_match_the_registered_projection_throws() {
        CloudEventConverter<Counted> converter = countedConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(reader(), converter, Counted::eventId);

        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        feed.register("counter", projection(), ViewStateRepository.create(repo::get, repo::put));

        StepVerifier.create(feed.goLive("not-counter"))
                .verifyErrorSatisfies(e -> assertThat(e)
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("not-counter")
                        .hasMessageContaining("No projection"));
    }

    @Test
    void go_live_writes_no_completion_marker_so_a_later_catch_up_still_replays_history() {
        CloudEventConverter<Counted> converter = countedConverter();
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();

        DomainEventFeed<Counted> feed = new DomainEventFeed<>(reader("1", "2"), converter, Counted::eventId, marker);
        ConcurrentHashMap<String, Integer> throwaway = new ConcurrentHashMap<>();
        feed.register("counter", projection(), ViewStateRepository.create(throwaway::get, throwaway::put));
        feed.goLive("counter").block();

        assertThat(marker.read("counter").blockOptional()).isEmpty();

        Map<String, Integer> repo = new ConcurrentHashMap<>();
        DomainEventFeed<Counted> restarted = new DomainEventFeed<>(reader("1", "2"), converter, Counted::eventId, marker);
        restarted.register("counter", projection(), ViewStateRepository.create(repo::get, repo::put));
        restarted.catchUpAll().block();

        assertThat(repo.get("counter")).isEqualTo(2);
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
        CountDownLatch parked = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        AtomicInteger converted = new AtomicInteger();
        // The projection's materialized update coalesces replayed updates rather than writing one through per event
        // (ADR 110), so parking on the store write would never land mid-replay for a two-event history: nothing is
        // written until the replay finishes. Parking on the decode instead still lands after the first event and
        // before the second, which is what lets the test call stopCatchUp() on a replay genuinely still in flight.
        CloudEventConverter<Counted> converter = parkingConverter(converted, parked, proceed);
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        AtomicInteger saves = new AtomicInteger();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, (id, state) -> {
            saves.incrementAndGet();
            repo.put(id, state);
        });
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(reader("1", "2"), converter, Counted::eventId, marker);
        feed.register("counter", projection(), repository);

        CountDownLatch catchUpFinished = new CountDownLatch(1);
        // doFinally is a real join on the stopped catch-up, unlike subscribe()'s own return: it counts down only once
        // the Mono has actually terminated, so awaiting it proves the stop landed instead of polling for a side effect
        // that already happened before stopCatchUp() was even called.
        feed.catchUpAll().doFinally(signal -> catchUpFinished.countDown()).subscribe(); // runs on boundedElastic, so this returns before it finishes
        parked.await();
        feed.stopCatchUp();
        proceed.countDown();

        assertThat(catchUpFinished.await(5, TimeUnit.SECONDS)).isTrue();
        // A stop discards whatever the coalescing update had buffered instead of writing it, so nothing was saved
        // even though the first event was already decoded and folded into the buffer.
        assertThat(saves.get()).isZero();
        assertThat(repo).isEmpty();
        // A partial replay is never recorded as a finished one.
        assertThat(marker.read("counter").blockOptional()).isEmpty();

        // The feed is still usable: a later catch-up (the same converter, which only parks once) replays the whole
        // history again, from the beginning. block() already joins the fold, so the assertion right after it needs no
        // additional wait.
        feed.catchUpAll().block();
        assertThat(repo.get("counter")).isEqualTo(2);

        feed.accept(new Counted("3")).block();
        assertThat(repo.get("counter")).isEqualTo(3);
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static CloudEventConverter<Counted> parkingConverter(AtomicInteger converted, CountDownLatch parked, CountDownLatch proceed) {
        CloudEventConverter<Counted> delegate = countedConverter();
        return new CloudEventConverter<>() {
            @Override
            public CloudEvent toCloudEvent(Counted domainEvent) {
                return delegate.toCloudEvent(domainEvent);
            }

            @Override
            public Counted toDomainEvent(CloudEvent cloudEvent) {
                Counted event = delegate.toDomainEvent(cloudEvent);
                if (converted.incrementAndGet() == 1) {
                    parked.countDown();
                    awaitUninterruptibly(proceed);
                }
                return event;
            }

            @Override
            public String getCloudEventType(Class<? extends Counted> type) {
                return delegate.getCloudEventType(type);
            }
        };
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

    @Test
    void accept_cloud_event_matching_the_registered_filter_delivers_and_reports_delivered() {
        CloudEventConverter<Counted> converter = countedConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(reader(), converter, Counted::eventId);
        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        feed.register("counter", projection(), ViewStateRepository.create(repo::get, repo::put));
        feed.catchUpAll().block();

        RoutingOutcome outcome = feed.acceptCloudEvent(converter.toCloudEvent(new Counted("1"))).block();

        assertThat(outcome).isEqualTo(RoutingOutcome.DELIVERED);
        assertThat(repo.get("counter")).isEqualTo(1);
    }

    /**
     * The one-evaluation fix this unit mirrors from the blocking stack. A matching event that arrives before the
     * registered projection is live must report {@link RoutingOutcome#DEFERRED}, safe to redeliver, rather than
     * being buffered and reported as though it had already reached the view.
     */
    @Test
    void accept_cloud_event_matching_the_registered_filter_reports_deferred_before_catch_up_and_never_buffers() {
        CloudEventConverter<Counted> converter = countedConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(reader(), converter, Counted::eventId);
        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        feed.register("counter", projection(), ViewStateRepository.create(repo::get, repo::put));
        // Deliberately never caught up or gone live. The projection is registered but not yet ready to receive.

        RoutingOutcome outcome = feed.acceptCloudEvent(converter.toCloudEvent(new Counted("1"))).block();

        assertThat(outcome).isEqualTo(RoutingOutcome.DEFERRED);
        assertThat(repo).as("refused outright, never buffered or applied").isEmpty();
    }

    /**
     * A caller that redelivers a {@link RoutingOutcome#DEFERRED} event, or a broker resending a message it never saw
     * acknowledged, must not double-apply it once the projection has gone live. This feed's own de-dup key absorbs
     * the repeat.
     */
    @Test
    void a_deferred_event_redelivered_after_going_live_is_applied_once_not_twice() {
        CloudEventConverter<Counted> converter = countedConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(reader(), converter, Counted::eventId);
        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        feed.register("counter", projection(), ViewStateRepository.create(repo::get, repo::put));
        CloudEvent event = converter.toCloudEvent(new Counted("1"));

        RoutingOutcome beforeLive = feed.acceptCloudEvent(event).block();
        assertThat(beforeLive).isEqualTo(RoutingOutcome.DEFERRED);
        assertThat(repo).isEmpty();

        feed.goLive("counter").block();

        RoutingOutcome firstLiveDelivery = feed.acceptCloudEvent(event).block();
        assertThat(firstLiveDelivery).isEqualTo(RoutingOutcome.DELIVERED);
        assertThat(repo.get("counter")).isEqualTo(1);

        RoutingOutcome retriedDelivery = feed.acceptCloudEvent(event).block();
        assertThat(retriedDelivery).isEqualTo(RoutingOutcome.DELIVERED);
        assertThat(repo.get("counter")).as("applied once, not twice").isEqualTo(1);
    }

    @Test
    void accept_cloud_event_not_matching_the_registered_filter_reports_filtered_and_is_never_decoded() {
        AtomicInteger decodes = new AtomicInteger();
        CloudEventConverter<Counted> converter = countingConverter(decodes);
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(reader(), converter, Counted::eventId);
        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        feed.register("counter", projection(), ViewStateRepository.create(repo::get, repo::put));
        feed.catchUpAll().block();
        CloudEvent nonMatching = CloudEventBuilder.v1().withId("x").withSource(SOURCE).withType("SomethingElseHappened").build();

        RoutingOutcome outcome = feed.acceptCloudEvent(nonMatching).block();

        assertThat(outcome).isEqualTo(RoutingOutcome.FILTERED);
        assertThat(repo).isEmpty();
        assertThat(decodes.get()).as("a non-matching event is never decoded").isZero();
    }

    @Test
    void accept_cloud_event_before_a_projection_is_registered_is_refused() {
        CloudEventConverter<Counted> converter = countedConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(reader(), converter, Counted::eventId);

        StepVerifier.create(feed.acceptCloudEvent(converter.toCloudEvent(new Counted("1"))))
                .expectErrorSatisfies(e -> assertThat(e)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("has no projection registered"))
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void accept_cloud_event_uses_the_same_filter_the_projection_was_registered_with_not_a_second_one() {
        // The live match and the replay share one filter, given once at register(..), so they can never disagree
        // about which events are this projection's the way two independently configured filters could.
        CloudEventConverter<Counted> converter = countedConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(reader(), converter, Counted::eventId);
        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        Function<Counted, Mono<Void>> fold = event -> Mono.fromRunnable(() -> repo.merge("counter", 1, Integer::sum));
        feed.register("counter", fold, Filter.type(converter.getCloudEventType(Counted.class)));
        feed.goLive("counter").block();

        RoutingOutcome delivered = feed.acceptCloudEvent(converter.toCloudEvent(new Counted("1"))).block();
        RoutingOutcome filtered = feed.acceptCloudEvent(CloudEventBuilder.v1().withId("x").withSource(SOURCE).withType("SomethingElseHappened").build()).block();

        assertThat(delivered).isEqualTo(RoutingOutcome.DELIVERED);
        assertThat(filtered).isEqualTo(RoutingOutcome.FILTERED);
        assertThat(repo.get("counter")).isEqualTo(1);
    }

    @Test
    void registering_a_payload_filter_with_no_data_field_reader_still_succeeds_since_the_replay_never_needed_one() {
        // DomainEventFeed.register(..) shipped long before acceptCloudEvent existed, and the store has always
        // evaluated a data payload condition on the replay filter itself, with no DataFieldReader involved. An
        // existing caller who never touches acceptCloudEvent must keep registering exactly the filters it always
        // could, so this must not regress into a startup failure now that acceptCloudEvent needs a DataFieldReader
        // for the very same filter.
        CloudEventConverter<Counted> converter = countedConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(reader(), converter, Counted::eventId);
        Function<Counted, Mono<Void>> fold = event -> Mono.empty();

        Throwable thrown = catchThrowable(() -> feed.register("counter", fold, Filter.data("amount", eq(42))));

        assertThat(thrown).isNull();
        assertThat(feed.hasProjection()).isTrue();
    }

    @Test
    void a_payload_filter_with_no_data_field_reader_is_refused_on_the_first_accept_cloud_event_call_not_at_register() {
        CloudEventConverter<Counted> converter = countedConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(reader(), converter, Counted::eventId);
        Function<Counted, Mono<Void>> fold = event -> Mono.empty();
        feed.register("counter", fold, Filter.data("amount", eq(42)));
        feed.goLive("counter").block();

        Throwable thrown = catchThrowable(() -> feed.acceptCloudEvent(converter.toCloudEvent(new Counted("1"))).block());

        assertThat(thrown).isInstanceOf(UnreadableLiveFilterException.class)
                .hasMessageContaining("DataFieldReader");
    }

    @Test
    void the_first_refusal_is_cached_and_replayed_on_every_later_call_instead_of_rebuilding_the_matcher() {
        // The refusal is a permanent condition of this registration, not a per-message answer: rebuilding and
        // rethrowing on every call would still be the poison loop the finding was about, just slower. A reader that
        // counts how many times it is asked whether it supports payload fields proves the matcher is built once.
        AtomicInteger supportsPayloadFieldsCalls = new AtomicInteger();
        DataFieldReader countingRefusingReader = new DataFieldReader() {
            @Override
            public Optional<Object> read(CloudEvent cloudEvent, String path) {
                throw new AssertionError("a refusing reader must never be asked to read once it has refused");
            }

            @Override
            public boolean supportsPayloadFields() {
                supportsPayloadFieldsCalls.incrementAndGet();
                return false;
            }
        };
        CloudEventConverter<Counted> converter = countedConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(reader(), converter, Counted::eventId, null,
                CatchupThenLiveOptions.defaults(), countingRefusingReader);
        Function<Counted, Mono<Void>> fold = event -> Mono.empty();
        feed.register("counter", fold, Filter.data("amount", eq(42)));
        feed.goLive("counter").block();
        CloudEvent event = converter.toCloudEvent(new Counted("1"));

        Throwable first = catchThrowable(() -> feed.acceptCloudEvent(event).block());
        Throwable second = catchThrowable(() -> feed.acceptCloudEvent(event).block());
        Throwable third = catchThrowable(() -> feed.acceptCloudEvent(event).block());

        assertThat(first).isInstanceOf(UnreadableLiveFilterException.class);
        assertThat(second).as("the same exception instance, not a fresh one rebuilt for this call").isSameAs(first);
        assertThat(third).as("the same exception instance, not a fresh one rebuilt for this call").isSameAs(first);
        assertThat(supportsPayloadFieldsCalls).as("the matcher is built once, on the first call, never again")
                .hasValue(1);
    }

    @Test
    void a_data_field_reader_supplied_to_the_feed_answers_a_payload_condition_on_the_live_path() {
        CloudEventConverter<Counted> converter = countedConverter();
        DataFieldReader reader = (cloudEvent, path) -> path.equals("amount") ? Optional.of(42) : Optional.empty();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(reader(), converter, Counted::eventId, null,
                CatchupThenLiveOptions.defaults(), reader);
        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        Function<Counted, Mono<Void>> fold = event -> Mono.fromRunnable(() -> repo.merge("counter", 1, Integer::sum));
        feed.register("counter", fold, Filter.data("amount", eq(42)));
        feed.goLive("counter").block();

        RoutingOutcome outcome = feed.acceptCloudEvent(converter.toCloudEvent(new Counted("1"))).block();

        assertThat(outcome).isEqualTo(RoutingOutcome.DELIVERED);
        assertThat(repo.get("counter")).isEqualTo(1);
    }

    @Test
    void accept_cloud_event_errors_when_the_live_matcher_itself_throws() {
        CloudEventConverter<Counted> converter = countedConverter();
        DataFieldReader throwingReader = (cloudEvent, path) -> {
            throw new IllegalStateException("payload unreadable");
        };
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(reader(), converter, Counted::eventId, null,
                CatchupThenLiveOptions.defaults(), throwingReader);
        Function<Counted, Mono<Void>> fold = event -> Mono.empty();
        feed.register("counter", fold, Filter.data("amount", eq(42)));

        StepVerifier.create(feed.acceptCloudEvent(converter.toCloudEvent(new Counted("1"))))
                .verifyErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class).hasMessage("payload unreadable"));
    }

    @Test
    void accept_cloud_event_reports_deferred_rather_than_delivered_when_a_stopped_catch_up_drops_the_event() {
        // The one-evaluation fix this unit mirrors from the blocking stack. acceptIfLive refuses an event outright
        // while the registered projection is not live, rather than buffering it and completing as though it might
        // still be folded, so acceptCloudEvent(..) reads that refusal back as DEFERRED, safe to redeliver.
        CountDownLatch parked = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        AtomicInteger converted = new AtomicInteger();
        CloudEventConverter<Counted> converter = parkingConverter(converted, parked, proceed);
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(reader("1", "2"), converter, Counted::eventId);
        feed.register("counter", projection(), ViewStateRepository.create(repo::get, repo::put));

        CountDownLatch catchUpFinished = new CountDownLatch(1);
        feed.catchUpAll().doFinally(signal -> catchUpFinished.countDown()).subscribe();
        awaitUninterruptibly(parked);
        feed.stopCatchUp();
        proceed.countDown();
        assertThat(awaitBoolean(catchUpFinished, 5, TimeUnit.SECONDS)).isTrue();

        RoutingOutcome outcome = feed.acceptCloudEvent(converter.toCloudEvent(new Counted("live"))).block();

        assertThat(outcome).as("the event matched the filter but the stopped handover refused it outright, so it "
                        + "was never actually delivered to the projection, and a redelivery is always safe")
                .isEqualTo(RoutingOutcome.DEFERRED);
        assertThat(repo).doesNotContainKey("counter");
    }

    /**
     * Proves acceptIfLive's refusal is immediate rather than waiting on the replay. An event fed while the replay is
     * still parked (mid-fold, before any stop) reports {@link RoutingOutcome#DEFERRED} right away, never buffered
     * and never left pending on whatever resolves the replay next. This is the behaviour
     * {@code acceptReportingDelivery} did not have, the defect this unit's fix closes.
     */
    @Test
    void accept_cloud_event_reports_deferred_immediately_for_an_event_that_arrives_while_the_replay_is_still_running() {
        CountDownLatch parked = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        AtomicInteger converted = new AtomicInteger();
        CloudEventConverter<Counted> converter = parkingConverter(converted, parked, proceed);
        Map<String, Integer> repo = new ConcurrentHashMap<>();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(reader("1", "2"), converter, Counted::eventId);
        feed.register("counter", projection(), ViewStateRepository.create(repo::get, repo::put));

        CountDownLatch catchUpFinished = new CountDownLatch(1);
        feed.catchUpAll().doFinally(signal -> catchUpFinished.countDown()).subscribe();
        awaitUninterruptibly(parked);

        // Asked while the replay is still parked mid-fold, neither stopped nor finished. Refused immediately rather
        // than waiting on either.
        RoutingOutcome outcome = feed.acceptCloudEvent(converter.toCloudEvent(new Counted("live"))).block(Duration.ofSeconds(5));

        assertThat(outcome).as("refused immediately, never buffered against a replay still in flight")
                .isEqualTo(RoutingOutcome.DEFERRED);
        assertThat(repo).doesNotContainKey("counter");

        proceed.countDown();
        assertThat(awaitBoolean(catchUpFinished, 5, TimeUnit.SECONDS)).isTrue();
    }

    private static boolean awaitBoolean(CountDownLatch latch, long timeout, TimeUnit unit) {
        try {
            return latch.await(timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static CloudEventConverter<Counted> countingConverter(AtomicInteger decodes) {
        CloudEventConverter<Counted> delegate = countedConverter();
        return new CloudEventConverter<>() {
            @Override
            public CloudEvent toCloudEvent(Counted domainEvent) {
                return delegate.toCloudEvent(domainEvent);
            }

            @Override
            public Counted toDomainEvent(CloudEvent cloudEvent) {
                decodes.incrementAndGet();
                return delegate.toDomainEvent(cloudEvent);
            }

            @Override
            public String getCloudEventType(Class<? extends Counted> type) {
                return delegate.getCloudEventType(type);
            }
        };
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

    // A reader whose history is the given event ids, in position order. Used by the stop-mid-replay tests.
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
