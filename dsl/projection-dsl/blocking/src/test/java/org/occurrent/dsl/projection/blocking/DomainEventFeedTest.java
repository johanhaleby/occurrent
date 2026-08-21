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
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.ReplayAware;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.RoutingOutcome;
import org.occurrent.subscription.UnreadableLiveFilterException;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.occurrent.condition.Condition.eq;

@DisplayNameGeneration(ReplaceUnderscores.class)
class DomainEventFeedTest {

    record Counted(String eventId) {
    }

    @Test
    void feeding_an_event_before_a_projection_is_registered_is_refused() {
        InMemoryEventStore store = new InMemoryEventStore();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, counterConverter(), Counted::eventId);

        // Returning normally is what a listener reads as "handled", so it would acknowledge the message and the
        // broker would discard an event nothing received. Under occurrent.subscription.mode=manual registration is
        // deferred, so a listener that starts consuming before startAll() lands exactly here, and refusing is what
        // makes the broker hold the backlog instead. That is ADR 86's withheld-not-lost guarantee on a push stack.
        Throwable thrown = catchThrowable(() -> feed.accept(new Counted("1")));

        assertThat(thrown).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("has no projection registered")
                .hasMessageContaining("refused rather than accepted");
    }

    @Test
    void feeding_an_event_with_metadata_before_a_projection_is_registered_is_refused() {
        InMemoryEventStore store = new InMemoryEventStore();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, counterConverter(), Counted::eventId);

        Throwable thrown = catchThrowable(() -> feed.accept(EventMetadata.empty(), new Counted("1")));

        assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("has no projection registered");
    }

    @Test
    void catching_up_a_feed_with_no_projection_is_refused() {
        InMemoryEventStore store = new InMemoryEventStore();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, counterConverter(), Counted::eventId);

        // It used to be a no-op, so a feed nobody had registered on reported a successful catch-up and then silently
        // fed nothing. catchUp(String) already refused, so this is the pair of them agreeing.
        Throwable thrown = catchThrowable(feed::catchUpAll);

        assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("has no projection registered");
    }

    @Test
    void a_feed_reports_whether_a_projection_is_registered() {
        InMemoryEventStore store = new InMemoryEventStore();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, counterConverter(), Counted::eventId);
        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();

        assertThat(feed.hasProjection()).isFalse();

        feed.register("counter", projection(), ViewStateRepository.create(repo::get, repo::put));

        assertThat(feed.hasProjection()).isTrue();
    }

    @Test
    void stopping_a_catch_up_on_a_feed_with_no_projection_does_nothing() {
        InMemoryEventStore store = new InMemoryEventStore();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, counterConverter(), Counted::eventId);

        // A shutdown verb, unlike the two above. One that throws because there was nothing to shut down is a nuisance
        // in a context-close path, not a safeguard.
        assertThat(catchThrowable(feed::stopCatchUp)).isNull();
    }

    @Test
    void registering_two_projections_with_the_same_id_throws() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = counterConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, converter, Counted::eventId);

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
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = counterConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, converter, Counted::eventId);

        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);

        Throwable thrown = catchThrowable(() -> feed.register(null, projection(), repository));

        assertThat(thrown).isInstanceOf(NullPointerException.class).hasMessageContaining("id cannot be null");
    }

    @Test
    void a_failed_registration_does_not_permanently_reserve_the_id() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = counterConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(readerThatDoesNotWritePosition(store), converter, Counted::eventId);

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
    void handover_options_passed_to_the_constructor_reach_every_registered_projections_catch_up() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = counterConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, converter, Counted::eventId, null, new CatchupThenLiveOptions(10, 2));

        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);
        feed.register("counter", projection(), repository);

        // Buffered before the catch-up runs, so the third one exceeds the cap of two. The message names the cap, which
        // is what proves the constructor's options reached CatchupProjectionFeed rather than the defaults being used.
        feed.accept(new Counted("l1"));
        feed.accept(new Counted("l2"));
        Throwable thrown = catchThrowable(() -> feed.accept(new Counted("l3")));

        assertThat(thrown).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("buffer overflowed")
                .hasMessageContaining("(cap 2)");
    }

    @Test
    void a_failed_catch_up_all_is_terminal_for_the_projection() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = counterConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(failingReader(store), converter, Counted::eventId);

        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        feed.register("counter", projection(), ViewStateRepository.create(repo::get, repo::put));

        Throwable catchUpFailure = catchThrowable(feed::catchUpAll);
        assertThat(catchUpFailure).isInstanceOf(IllegalStateException.class).hasMessageContaining("replay boom");

        // The feed does not drop the poisoned projection, so every event fed afterwards fails fast instead of
        // silently buffering behind a catch-up that never completed. That is the terminal contract catchUpAll
        // documents. A feed drives exactly one projection now, so there is nothing behind it left to block.
        Throwable liveFailure = catchThrowable(() -> feed.accept(new Counted("1")));

        assertThat(liveFailure).isInstanceOf(IllegalStateException.class).hasMessageContaining("Catch-up failed");
    }

    private static PositionOrderedReader failingReader(PositionOrderedReader delegate) {
        return new PositionOrderedReader() {
            @Override
            public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                throw new IllegalStateException("replay boom");
            }

            @Override
            public long currentPosition() {
                return delegate.currentPosition();
            }

            @Override
            public boolean writesPosition() {
                return true;
            }
        };
    }

    private static PositionOrderedReader readerThatDoesNotWritePosition(PositionOrderedReader delegate) {
        return new PositionOrderedReader() {
            @Override
            public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return delegate.readInPositionOrder(filter, range);
            }

            @Override
            public long currentPosition() {
                return delegate.currentPosition();
            }

            @Override
            public boolean writesPosition() {
                return false;
            }
        };
    }

    @Test
    void catch_up_of_the_registered_id_catches_up_the_projection() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = counterConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, converter, Counted::eventId);

        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        feed.register("counter", projection(), ViewStateRepository.create(repo::get, repo::put));

        feed.catchUp("counter");
        feed.accept(new Counted("live"));

        // Caught up (on empty history) and went live, so it saw the live event.
        assertThat(repo.get("counter")).isEqualTo(1);
    }

    @Test
    void catch_up_of_an_unregistered_id_throws() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = counterConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, converter, Counted::eventId);

        Throwable thrown = catchThrowable(() -> feed.catchUp("missing"));

        assertThat(thrown).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("missing").hasMessageContaining("No projection");
    }

    @Test
    void catch_up_of_an_id_that_does_not_match_the_registered_projection_throws() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = counterConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, converter, Counted::eventId);

        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        feed.register("counter", projection(), ViewStateRepository.create(repo::get, repo::put));

        // Distinct from catch_up_of_an_unregistered_id_throws: here a projection IS registered, just under a
        // different id, so this exercises the mismatch branch rather than the nothing-registered one.
        Throwable thrown = catchThrowable(() -> feed.catchUp("not-counter"));

        assertThat(thrown).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not-counter").hasMessageContaining("No projection");
    }

    @Test
    void go_live_of_the_registered_id_skips_the_replay_and_goes_live() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = counterConverter();
        store.write("s", converter.toCloudEvents(List.of(new Counted("1"), new Counted("2"))));
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, converter, Counted::eventId);

        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        feed.register("counter", projection(), ViewStateRepository.create(repo::get, repo::put));

        feed.goLive("counter");
        feed.accept(new Counted("live"));

        // The two history events were never replayed, so only the live one was folded.
        assertThat(repo.get("counter")).isEqualTo(1);
    }

    @Test
    void go_live_of_an_unregistered_id_throws() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = counterConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, converter, Counted::eventId);

        Throwable thrown = catchThrowable(() -> feed.goLive("missing"));

        assertThat(thrown).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("missing").hasMessageContaining("No projection");
    }

    @Test
    void go_live_of_an_id_that_does_not_match_the_registered_projection_throws() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = counterConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, converter, Counted::eventId);

        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        feed.register("counter", projection(), ViewStateRepository.create(repo::get, repo::put));

        Throwable thrown = catchThrowable(() -> feed.goLive("not-counter"));

        assertThat(thrown).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not-counter").hasMessageContaining("No projection");
    }

    @Test
    void go_live_writes_no_completion_marker_so_a_later_catch_up_still_replays_history() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = counterConverter();
        store.write("s", converter.toCloudEvents(List.of(new Counted("1"), new Counted("2"))));
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();

        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, converter, Counted::eventId, marker);
        ConcurrentHashMap<String, Integer> throwaway = new ConcurrentHashMap<>();
        feed.register("counter", projection(), ViewStateRepository.create(throwaway::get, throwaway::put));
        feed.goLive("counter");

        assertThat(marker.exists("counter")).isFalse();

        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        DomainEventFeed<Counted> restarted = new DomainEventFeed<>(store, converter, Counted::eventId, marker);
        restarted.register("counter", projection(), ViewStateRepository.create(repo::get, repo::put));
        restarted.catchUpAll();

        assertThat(repo.get("counter")).isEqualTo(2);
    }

    @Test
    void registering_two_projections_with_different_ids_throws() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = counterConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, converter, Counted::eventId);

        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);
        feed.register("counter-1", projection(), repository);

        Throwable thrown = catchThrowable(() -> feed.register("counter-2", projection(), repository));

        assertThat(thrown).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("counter-1")
                .hasMessageContaining("counter-2");

        // The first registration is unaffected by the refused second one: it still works.
        feed.catchUpAll();
        feed.accept(new Counted("1"));
        assertThat(repo.get("counter")).isEqualTo(1);
    }

    @Test
    void accept_with_metadata_feeds_the_registered_projection_with_the_metadata_intact() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = counterConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, converter, Counted::eventId);

        ConcurrentHashMap<String, Long> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Long, String> repository = ViewStateRepository.create(repo::get, repo::put);
        feed.register("positions", positionKeyedProjection(), repository);
        feed.catchUpAll();

        feed.accept(metadata("stream-1", 7L), new Counted("live"));

        assertThat(repo.get("stream-1")).isEqualTo(7L);
    }

    @Test
    void stopping_a_catch_up_mid_replay_does_not_write_the_marker_and_a_later_catch_up_replays_from_the_beginning() throws InterruptedException {
        InMemoryEventStore store = new InMemoryEventStore();
        store.write("s", counterConverter().toCloudEvents(List.of(new Counted("1"), new Counted("2"))));
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();

        CountDownLatch parked = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        AtomicInteger converted = new AtomicInteger();
        // The projection's materialized view coalesces replayed updates rather than writing one through per event
        // (ADR 110), so parking on the store write would never land mid-replay for a two-event history: nothing is
        // written until the replay finishes. Parking on the decode instead still lands after the first event and
        // before the second, which is what lets the test call stopCatchUp() on a replay genuinely still in flight.
        CloudEventConverter<Counted> converter = parkingConverter(converted, parked, proceed);
        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        AtomicInteger saves = new AtomicInteger();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, (id, state) -> {
            saves.incrementAndGet();
            repo.put(id, state);
        });
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, converter, Counted::eventId, marker);
        feed.register("counter", projection(), repository);

        Thread replay = new Thread(feed::catchUpAll);
        replay.start();
        parked.await();
        feed.stopCatchUp();
        proceed.countDown();
        replay.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(replay.isAlive()).isFalse();
        // A stop discards whatever the coalescing view had buffered instead of writing it, so nothing was saved even
        // though the first event was already decoded and folded into the buffer.
        assertThat(saves.get()).isZero();
        assertThat(repo).isEmpty();
        // A partial replay is never recorded as a finished one.
        assertThat(marker.exists("counter")).isFalse();

        // The feed is still usable: a later catch-up (the same converter, which only parks once) replays the whole
        // history again, from the beginning.
        feed.catchUpAll();
        assertThat(repo.get("counter")).isEqualTo(2);

        feed.accept(new Counted("3"));
        assertThat(repo.get("counter")).isEqualTo(3);
    }

    /**
     * The busy-loop class round 8 first closed for a stopped replay, now proven against the delegating
     * {@code BlockingHandover.isReadyForLiveDelivery()} (round 11) rather than a separately tracked flag. A stop
     * mid-replay leaves the handover dropping every live delivery until a later catch-up revives it, so a bridge
     * polling {@code hasProjection() && isReadyForLiveDelivery()} must not keep consuming and nack-requeue every
     * message in a hot loop for that whole interval. Readiness reads false for the entire replay, running or
     * stopped, since the delegating handover is not live until its drain actually runs, not only for the stopped
     * half round 8's own flag distinguished.
     */
    @Test
    void a_stop_mid_replay_clears_readiness_until_a_later_catch_up_revives_it() throws InterruptedException {
        InMemoryEventStore store = new InMemoryEventStore();
        store.write("s", counterConverter().toCloudEvents(List.of(new Counted("1"), new Counted("2"))));

        CountDownLatch parked = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        AtomicInteger converted = new AtomicInteger();
        CloudEventConverter<Counted> converter = parkingConverter(converted, parked, proceed);
        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, converter, Counted::eventId);
        feed.register("counter", projection(), ViewStateRepository.create(repo::get, repo::put));

        Thread replay = new Thread(feed::catchUpAll);
        replay.start();
        parked.await();
        assertThat(feed.isReadyForLiveDelivery()).as("the replay is still running, not yet live").isFalse();

        feed.stopCatchUp();
        proceed.countDown();
        replay.join(TimeUnit.SECONDS.toMillis(5));
        assertThat(replay.isAlive()).isFalse();

        assertThat(feed.isReadyForLiveDelivery()).as("the stop abandoned the replay, so nothing is ready to receive a live event").isFalse();

        feed.catchUpAll();
        assertThat(feed.isReadyForLiveDelivery()).as("a later catch-up that reaches live revives it").isTrue();
    }

    /**
     * The invariant round 8 only closed for one exit out of {@code catchUp()}: a replay stopped mid-flight. Copilot,
     * and separately an adversarial verifier with a sharper reproduction ({@code replayStarted()} itself throwing,
     * before {@code BlockingHandover} ever marks a replay open), both found the same defect one layer out.
     * {@code handover.catchUp(..)} can fail before, during or after the replay in several distinct ways, and every
     * one of them leaves the handover refusing every later delivery. {@code isReadyForLiveDelivery()} must read
     * false after every one of them, not only the one this suite happened to cover first, so this asserts the
     * invariant against five different failure sources rather than pinning the fix to a single throw site.
     */
    @Test
    void catch_up_clears_readiness_on_every_distinct_way_it_can_fail_to_reach_live() {
        // 1. isAlreadyCaughtUp() throws, before any replay is even attempted.
        InMemoryEventStore store1 = new InMemoryEventStore();
        DomainEventFeed<Counted> feed1 = new DomainEventFeed<>(store1, counterConverter(), Counted::eventId, throwingOnExistsCheckpointStorage());
        feed1.register("counter", projection(), ViewStateRepository.create(new ConcurrentHashMap<String, Integer>()::get, (id, s) -> {
        }));
        assertThat(catchThrowable(feed1::catchUpAll)).isNotNull();
        assertThat(feed1.isReadyForLiveDelivery()).as("isAlreadyCaughtUp() threw before any replay started").isFalse();

        // 2. replayStarted() throws, the moment BlockingHandover tells the view a replay is beginning, before it
        // ever marks the replay open. A plain per-throw-site fix inside replayAbandoned() cannot see this one,
        // since replayAbandoned() itself is never called for it.
        InMemoryEventStore store2 = new InMemoryEventStore();
        DomainEventFeed<Counted> feed2 = new DomainEventFeed<>(store2, counterConverter(), Counted::eventId);
        feed2.register("counter", viewThatThrowsOnReplayStarted(), Filter.type(counterConverter().getCloudEventType(Counted.class)));
        assertThat(catchThrowable(feed2::catchUpAll)).isNotNull();
        assertThat(feed2.isReadyForLiveDelivery()).as("replayStarted() threw before the replay was ever marked open").isFalse();

        // 3. A replayed event's own decode throws, mid-replay.
        InMemoryEventStore store3 = new InMemoryEventStore();
        store3.write("s", counterConverter().toCloudEvents(List.of(new Counted("1"))));
        CloudEventConverter<Counted> throwingConverter = new CloudEventConverter<>() {
            @Override
            public CloudEvent toCloudEvent(Counted domainEvent) {
                return counterConverter().toCloudEvent(domainEvent);
            }

            @Override
            public Counted toDomainEvent(CloudEvent cloudEvent) {
                throw new IllegalStateException("decode boom");
            }

            @Override
            public String getCloudEventType(Class<? extends Counted> type) {
                return counterConverter().getCloudEventType(type);
            }
        };
        DomainEventFeed<Counted> feed3 = new DomainEventFeed<>(store3, throwingConverter, Counted::eventId);
        feed3.register("counter", projection(), ViewStateRepository.create(new ConcurrentHashMap<String, Integer>()::get, (id, s) -> {
        }));
        assertThat(catchThrowable(feed3::catchUpAll)).isNotNull();
        assertThat(feed3.isReadyForLiveDelivery()).as("the replayed event's own decode threw mid-replay").isFalse();

        // 4. markCaughtUp() throws, after the (empty) replay and the buffered drain both succeeded.
        InMemoryEventStore store4 = new InMemoryEventStore();
        DomainEventFeed<Counted> feed4 = new DomainEventFeed<>(store4, counterConverter(), Counted::eventId, throwingOnSaveCheckpointStorage());
        feed4.register("counter", projection(), ViewStateRepository.create(new ConcurrentHashMap<String, Integer>()::get, (id, s) -> {
        }));
        assertThat(catchThrowable(feed4::catchUpAll)).isNotNull();
        assertThat(feed4.isReadyForLiveDelivery()).as("markCaughtUp() threw after the replay and the drain both succeeded").isFalse();

        // 5. The buffered drain itself throws. A live event fed ahead of catchUpAll() folds successfully nowhere
        // near a replay (the store is empty), only when the drain the empty replay's shortcut still runs delivers it.
        InMemoryEventStore store5 = new InMemoryEventStore();
        DomainEventFeed<Counted> feed5 = new DomainEventFeed<>(store5, counterConverter(), Counted::eventId);
        MaterializedView<Counted> throwingView = event -> {
            throw new IllegalStateException("drain boom");
        };
        feed5.register("counter", throwingView, Filter.type(counterConverter().getCloudEventType(Counted.class)));
        feed5.accept(new Counted("buffered"));
        assertThat(catchThrowable(feed5::catchUpAll)).isNotNull();
        assertThat(feed5.isReadyForLiveDelivery()).as("the buffered live event's own fold threw during the drain").isFalse();
    }

    private static CheckpointStorage throwingOnExistsCheckpointStorage() {
        return new CheckpointStorage() {
            @Override
            public Checkpoint read(String subscriptionId) {
                throw new AssertionError("read should not be called in this scenario");
            }

            @Override
            public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                throw new AssertionError("save should not be called in this scenario");
            }

            @Override
            public void delete(String subscriptionId) {
                throw new AssertionError("delete should not be called in this scenario");
            }

            @Override
            public boolean exists(String subscriptionId) {
                throw new IllegalStateException("exists boom");
            }

            @Override
            public java.util.OptionalLong writeVersion(String subscriptionId) {
                throw new AssertionError("writeVersion should not be called in this scenario");
            }
        };
    }

    private static CheckpointStorage throwingOnSaveCheckpointStorage() {
        return new CheckpointStorage() {
            @Override
            public Checkpoint read(String subscriptionId) {
                throw new AssertionError("read should not be called in this scenario");
            }

            @Override
            public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                throw new IllegalStateException("save boom");
            }

            @Override
            public void delete(String subscriptionId) {
                throw new AssertionError("delete should not be called in this scenario");
            }

            @Override
            public boolean exists(String subscriptionId) {
                return false;
            }

            @Override
            public java.util.OptionalLong writeVersion(String subscriptionId) {
                throw new AssertionError("writeVersion should not be called in this scenario");
            }
        };
    }

    private static MaterializedView<Counted> viewThatThrowsOnReplayStarted() {
        class ThrowingView implements MaterializedView<Counted>, ReplayAware {
            @Override
            public void update(Counted event) {
                // Never reached in this scenario. replayStarted() throws before any event is folded.
            }

            @Override
            public void replayStarted() {
                throw new IllegalStateException("replayStarted boom");
            }

            @Override
            public void replayCompleted() {
                throw new AssertionError("replayCompleted should not be called in this scenario");
            }

            @Override
            public void replayAbandoned() {
                throw new AssertionError("replayAbandoned should not be called in this scenario, " +
                        "since replayStarted() throwing must leave the replay never marked open");
            }
        }
        return new ThrowingView();
    }

    private static CloudEventConverter<Counted> parkingConverter(AtomicInteger converted, CountDownLatch parked, CountDownLatch proceed) {
        CloudEventConverter<Counted> delegate = counterConverter();
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

    private static void awaitUninterruptibly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
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

    @Test
    void accept_cloud_event_matching_the_registered_filter_delivers_and_reports_delivered() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = counterConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, converter, Counted::eventId);
        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        feed.register("counter", projection(), ViewStateRepository.create(repo::get, repo::put));
        feed.catchUpAll();

        RoutingOutcome outcome = feed.acceptCloudEvent(converter.toCloudEvent(new Counted("1")));

        assertThat(outcome).isEqualTo(RoutingOutcome.DELIVERED);
        assertThat(repo.get("counter")).isEqualTo(1);
    }

    @Test
    void accept_cloud_event_not_matching_the_registered_filter_reports_filtered_and_is_never_decoded() {
        InMemoryEventStore store = new InMemoryEventStore();
        AtomicInteger decodes = new AtomicInteger();
        CloudEventConverter<Counted> converter = countingConverter(decodes);
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, converter, Counted::eventId);
        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        feed.register("counter", projection(), ViewStateRepository.create(repo::get, repo::put));
        feed.catchUpAll();
        CloudEvent nonMatching = CloudEventBuilder.v1()
                .withId("x")
                .withSource(URI.create("urn:occurrent:test"))
                .withType("SomethingElseHappened")
                .build();

        RoutingOutcome outcome = feed.acceptCloudEvent(nonMatching);

        assertThat(outcome).isEqualTo(RoutingOutcome.FILTERED);
        assertThat(repo).isEmpty();
        assertThat(decodes.get()).as("a non-matching event is never decoded").isZero();
    }

    @Test
    void accept_cloud_event_before_a_projection_is_registered_is_refused() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = counterConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, converter, Counted::eventId);

        Throwable thrown = catchThrowable(() -> feed.acceptCloudEvent(converter.toCloudEvent(new Counted("1"))));

        assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("has no projection registered");
    }

    @Test
    void accept_cloud_event_uses_the_same_filter_the_projection_was_registered_with_not_a_second_one() {
        // The live match and the replay share one filter, given once at register(..), so they can never disagree
        // about which events are this projection's the way two independently configured filters could.
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = counterConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, converter, Counted::eventId);
        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        MaterializedView<Counted> view = event -> repo.merge("counter", 1, Integer::sum);
        feed.register("counter", view, Filter.type(converter.getCloudEventType(Counted.class)));
        feed.goLive("counter");

        RoutingOutcome delivered = feed.acceptCloudEvent(converter.toCloudEvent(new Counted("1")));
        RoutingOutcome filtered = feed.acceptCloudEvent(CloudEventBuilder.v1()
                .withId("x")
                .withSource(URI.create("urn:occurrent:test"))
                .withType("SomethingElseHappened")
                .build());

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
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = counterConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, converter, Counted::eventId);
        MaterializedView<Counted> view = event -> {
        };

        Throwable thrown = catchThrowable(() -> feed.register("counter", view, Filter.data("amount", eq(42))));

        assertThat(thrown).isNull();
        assertThat(feed.hasProjection()).isTrue();
    }

    @Test
    void a_payload_filter_with_no_data_field_reader_is_refused_on_the_first_accept_cloud_event_call_not_at_register() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = counterConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, converter, Counted::eventId);
        MaterializedView<Counted> view = event -> {
        };
        feed.register("counter", view, Filter.data("amount", eq(42)));
        feed.goLive("counter");

        Throwable thrown = catchThrowable(() -> feed.acceptCloudEvent(converter.toCloudEvent(new Counted("1"))));

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
            public java.util.Optional<Object> read(CloudEvent cloudEvent, String path) {
                throw new AssertionError("a refusing reader must never be asked to read once it has refused");
            }

            @Override
            public boolean supportsPayloadFields() {
                supportsPayloadFieldsCalls.incrementAndGet();
                return false;
            }
        };
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = counterConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, converter, Counted::eventId, null,
                CatchupThenLiveOptions.defaults(), countingRefusingReader);
        MaterializedView<Counted> view = event -> {
        };
        feed.register("counter", view, Filter.data("amount", eq(42)));
        feed.goLive("counter");
        CloudEvent event = converter.toCloudEvent(new Counted("1"));

        Throwable first = catchThrowable(() -> feed.acceptCloudEvent(event));
        Throwable second = catchThrowable(() -> feed.acceptCloudEvent(event));
        Throwable third = catchThrowable(() -> feed.acceptCloudEvent(event));

        assertThat(first).isInstanceOf(UnreadableLiveFilterException.class);
        assertThat(second).as("the same exception instance, not a fresh one rebuilt for this call").isSameAs(first);
        assertThat(third).as("the same exception instance, not a fresh one rebuilt for this call").isSameAs(first);
        assertThat(supportsPayloadFieldsCalls).as("the matcher is built once, on the first call, never again")
                .hasValue(1);
    }

    @Test
    void a_data_field_reader_supplied_to_the_feed_answers_a_payload_condition_on_the_live_path() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = counterConverter();
        DataFieldReader reader = (cloudEvent, path) -> path.equals("amount") ? java.util.Optional.of(42) : java.util.Optional.empty();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, converter, Counted::eventId, null,
                CatchupThenLiveOptions.defaults(), reader);
        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        MaterializedView<Counted> view = event -> repo.merge("counter", 1, Integer::sum);
        feed.register("counter", view, Filter.data("amount", eq(42)));
        feed.goLive("counter");

        RoutingOutcome outcome = feed.acceptCloudEvent(converter.toCloudEvent(new Counted("1")));

        assertThat(outcome).isEqualTo(RoutingOutcome.DELIVERED);
        assertThat(repo.get("counter")).isEqualTo(1);
    }

    @Test
    void accept_cloud_event_propagates_when_the_live_matcher_itself_throws() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = counterConverter();
        DataFieldReader throwingReader = (cloudEvent, path) -> {
            throw new IllegalStateException("payload unreadable");
        };
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, converter, Counted::eventId, null,
                CatchupThenLiveOptions.defaults(), throwingReader);
        MaterializedView<Counted> view = event -> {
        };
        feed.register("counter", view, Filter.data("amount", eq(42)));

        Throwable thrown = catchThrowable(() -> feed.acceptCloudEvent(converter.toCloudEvent(new Counted("1"))));

        assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessage("payload unreadable");
    }

    @Test
    void accept_cloud_event_reports_deferred_rather_than_delivered_when_a_stopped_catch_up_drops_the_event() {
        // The exact race Copilot's review of this PR caught: BlockingHandover.accept(..) returns normally for a
        // payload dropped because stopCatchUp() interrupted a replay still in flight (see its own javadoc), so
        // acceptCloudEvent(..) must read that signal back rather than assume delivery from a normal return.
        // acceptIfLive(..) refuses this one outright rather than buffering it, reported DEFERRED, safe to redeliver.
        InMemoryEventStore store = new InMemoryEventStore();
        store.write("s", counterConverter().toCloudEvents(List.of(new Counted("1"), new Counted("2"))));
        CountDownLatch parked = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        AtomicInteger converted = new AtomicInteger();
        CloudEventConverter<Counted> converter = parkingConverter(converted, parked, proceed);
        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, converter, Counted::eventId);
        feed.register("counter", projection(), ViewStateRepository.create(repo::get, repo::put));

        Thread replay = new Thread(feed::catchUpAll);
        replay.start();
        awaitUninterruptibly(parked);
        feed.stopCatchUp();
        proceed.countDown();
        try {
            replay.join(TimeUnit.SECONDS.toMillis(5));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        assertThat(replay.isAlive()).isFalse();

        RoutingOutcome outcome = feed.acceptCloudEvent(converter.toCloudEvent(new Counted("live")));

        assertThat(outcome).as("the event matched the filter but the stopped handover refused it outright rather "
                        + "than buffering it, safe to redeliver")
                .isEqualTo(RoutingOutcome.DEFERRED);
        assertThat(repo).doesNotContainKey("counter");
    }

    @Test
    void a_feed_is_not_ready_for_live_delivery_until_catch_up_all_runs() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = counterConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, converter, Counted::eventId);
        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();

        assertThat(feed.isReadyForLiveDelivery()).isFalse();

        feed.register("counter", projection(), ViewStateRepository.create(repo::get, repo::put));
        assertThat(feed.isReadyForLiveDelivery()).as("registered, but neither catchUpAll() nor goLive() has run yet").isFalse();

        feed.catchUpAll();
        assertThat(feed.isReadyForLiveDelivery()).isTrue();
    }

    @Test
    void go_live_also_marks_the_feed_ready_for_live_delivery() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = counterConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, converter, Counted::eventId);
        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        feed.register("counter", projection(), ViewStateRepository.create(repo::get, repo::put));

        assertThat(feed.isReadyForLiveDelivery()).isFalse();

        feed.goLive("counter");

        assertThat(feed.isReadyForLiveDelivery()).isTrue();
    }

    /**
     * Round 8 had {@code goLive()} flip its own one-shot flag true only after draining the buffer it inherited, to
     * close a window where a poll on another thread could see it as safe to consume while an event fed ahead of
     * {@code goLive()} was still only sitting in that buffer. The round-11 delegate reports {@code true} earlier
     * than that, as soon as {@code BlockingHandover} claims the buffer under its own lock and marks itself live,
     * before the claimed items are actually folded outside that lock. That is safe rather than a reopened window.
     * The claim (under lock, {@code tryReserve}) and the fold (outside it, {@code deliverOutsideLock}) are exactly
     * the same split a concurrent live {@code accept(Object)} already uses (the handover's own class javadoc, #588),
     * so a live event racing this drain either matches the same de-dup key and is dropped as already in flight, or
     * matches a different key and is delivered independently. Nothing is left buffered-but-reported-ready.
     */
    @Test
    void go_live_reports_ready_once_its_drain_claims_the_buffer_even_while_an_inherited_items_fold_is_still_running() throws InterruptedException {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = counterConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, converter, Counted::eventId);
        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        CountDownLatch foldEntered = new CountDownLatch(1);
        CountDownLatch releaseFold = new CountDownLatch(1);
        Projection<Integer, Counted, String> blockingProjection = Projection.<Integer, Counted, String>builder(0)
                .id(event -> "counter")
                .on(Counted.class, (state, event) -> {
                    foldEntered.countDown();
                    awaitUninterruptibly(releaseFold);
                    return state + 1;
                })
                .build();
        feed.register("counter", blockingProjection, ViewStateRepository.create(repo::get, repo::put));
        // Buffered ahead of goLive(), since it is not registered as ready yet.
        feed.accept(new Counted("1"));

        Thread goLive = new Thread(() -> feed.goLive("counter"));
        goLive.start();
        awaitUninterruptibly(foldEntered);

        assertThat(feed.isReadyForLiveDelivery()).as("the drain already claimed the buffer and marked itself live, "
                        + "even though this claimed item's own fold is still running")
                .isTrue();

        releaseFold.countDown();
        goLive.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(goLive.isAlive()).isFalse();
        assertThat(feed.isReadyForLiveDelivery()).isTrue();
        assertThat(repo.get("counter")).isEqualTo(1);
    }

    /**
     * The silent-loss case ADR 133 exists to prevent. {@code hasProjection()} turns true the moment
     * {@code register(..)} returns, well before the application calls {@code goLive(..)}, so a live event arriving
     * in that window used to report {@link RoutingOutcome#DELIVERED} for an event only buffered in memory. A
     * {@code DomainEventFeed} bound for {@code goLive(..)} never replays (its own javadoc: the events are not in the
     * local store), so a crash in that window lost the event for good even though a broker bridge would already have
     * acknowledged it. {@link RoutingOutcome#DEFERRED} is what fixes it now: {@code acceptCloudEvent(..)} refuses
     * this event outright rather than buffering it, so a caller must redeliver instead of acknowledging, and a
     * redelivery once {@code goLive(..)} has actually run is what folds the event, proving it was recoverable
     * rather than dropped. {@code RabbitMqDomainEventBridgeTest} (in the RabbitMQ broker module) exercises the same
     * scenario end to end through the actual bridge; this is the unit-level proof of the feed's own contract.
     */
    @Test
    void accept_cloud_event_reports_deferred_for_an_event_offered_before_catch_up_or_go_live_has_started() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = counterConverter();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, converter, Counted::eventId);
        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        feed.register("counter", projection(), ViewStateRepository.create(repo::get, repo::put));
        CloudEvent cloudEvent = converter.toCloudEvent(new Counted("1"));

        RoutingOutcome outcome = feed.acceptCloudEvent(cloudEvent);

        assertThat(outcome).as("nothing has decided yet whether this registration replays from the store or skips "
                        + "straight to live, so a caller must redeliver rather than acknowledge this delivery")
                .isEqualTo(RoutingOutcome.DEFERRED);
        assertThat(repo).as("refused outright, never buffered, and not dropped either").isEmpty();

        // Proof the event is recoverable rather than lost: goLive(..) makes the feed ready, exactly as it would for
        // a real application that only decides goLive(..) once it starts, after the bridge could already have
        // offered a message, and redelivering the same event once ready is what actually folds it.
        feed.goLive("counter");
        RoutingOutcome redelivered = feed.acceptCloudEvent(cloudEvent);

        assertThat(redelivered).isEqualTo(RoutingOutcome.DELIVERED);
        assertThat(repo.get("counter")).isEqualTo(1);
    }

    /**
     * The counterpart to the test above. An event fed while {@code catchUpAll()} is still replaying is refused
     * outright by {@code acceptIfLive(..)} exactly like one fed before the replay ever started, reported
     * {@link RoutingOutcome#DEFERRED}, even though a replay actually in flight and backed by the store means a
     * <em>different, already-stored</em> event in that same window would have been safe to buffer. This method does
     * not try to tell that case apart from the unsafe one above, see {@code acceptCloudEvent(..)}'s own javadoc for
     * why: refusing unconditionally, rather than buffering, means this event is never folded by the replay's own
     * drain at all, it was never in the store to begin with, and only the caller's own redelivery, once
     * {@code isReadyForLiveDelivery()} reads {@code true}, actually applies it. That redelivery is a genuinely fresh
     * delivery, not a duplicate the de-dup cache recognizes, since nothing folded this event the first time.
     */
    @Test
    void accept_cloud_event_reports_deferred_for_an_event_offered_while_a_catch_up_replay_is_in_flight_and_only_the_redelivery_after_going_live_applies_it() {
        InMemoryEventStore store = new InMemoryEventStore();
        store.write("s", counterConverter().toCloudEvents(List.of(new Counted("1"), new Counted("2"))));
        CountDownLatch parked = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        AtomicInteger converted = new AtomicInteger();
        CloudEventConverter<Counted> converter = parkingConverter(converted, parked, proceed);
        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        DomainEventFeed<Counted> feed = new DomainEventFeed<>(store, converter, Counted::eventId);
        feed.register("counter", projection(), ViewStateRepository.create(repo::get, repo::put));

        Thread replay = new Thread(feed::catchUpAll);
        replay.start();
        awaitUninterruptibly(parked);
        assertThat(feed.isReadyForLiveDelivery()).as("the replay is still running, not yet live").isFalse();

        CloudEvent liveEvent = converter.toCloudEvent(new Counted("live"));
        RoutingOutcome outcome = feed.acceptCloudEvent(liveEvent);

        assertThat(outcome).as("not ready yet, even though the in-flight replay will end up folding this event")
                .isEqualTo(RoutingOutcome.DEFERRED);

        proceed.countDown();
        try {
            replay.join(TimeUnit.SECONDS.toMillis(5));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        assertThat(replay.isAlive()).isFalse();
        assertThat(feed.isReadyForLiveDelivery()).isTrue();
        // The refused event was never buffered, so only the two replayed events have folded so far.
        assertThat(repo.get("counter")).isEqualTo(2);

        // The redelivery a caller must issue on DEFERRED is what actually applies this event, now that the feed is
        // live, since nothing folded it the first time.
        RoutingOutcome redelivered = feed.acceptCloudEvent(liveEvent);

        assertThat(redelivered).isEqualTo(RoutingOutcome.DELIVERED);
        assertThat(repo.get("counter")).isEqualTo(3);
    }

    private static CloudEventConverter<Counted> countingConverter(AtomicInteger decodes) {
        CloudEventConverter<Counted> delegate = counterConverter();
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

    private static Projection<Integer, Counted, String> projection() {
        return Projection.<Integer, Counted, String>builder(0)
                .id(event -> "counter")
                .on(Counted.class, (state, event) -> state + 1)
                .build();
    }

    private static CloudEventConverter<Counted> counterConverter() {
        return new JacksonCloudEventConverter.Builder<Counted>(new ObjectMapper(), URI.create("urn:occurrent:test")).idMapper(Counted::eventId).build();
    }
}
