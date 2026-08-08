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
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.CatchupThenLiveOptions;
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
