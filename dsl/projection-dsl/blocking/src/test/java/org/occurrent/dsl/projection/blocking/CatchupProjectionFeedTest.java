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
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.dsl.projection.MaterializedViewOptions;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.ReplayAware;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.AppendId;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.internal.HandoverMessages;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
    void a_stopped_catch_up_forwards_replay_abandoned_to_a_replay_aware_view_instead_of_replay_completed() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = countedConverter();
        store.write("s", converter.toCloudEvents(List.of(new Counted("1"), new Counted("2"))));

        List<String> calls = new CopyOnWriteArrayList<>();
        ReplayAwareView view = new ReplayAwareView(calls);
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "counter", view, Filter.all(), store, converter, Counted::eventId, null);
        // Stops from inside the first fold, so the stop is in place before the replay considers delivering "2" and
        // the abandon runs on a replay genuinely still in flight.
        view.onUpdate = feed::stopCatchUp;

        feed.catchUp();

        assertThat(calls).containsExactly("replayStarted", "update:1", "replayAbandoned");
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
        FutureTask<Void> fed = feedWaitingForTheCatchUp(() -> feed.accept(new Counted("2")));
        feed.catchUp();

        assertThat(fed).succeedsWithin(Duration.ofSeconds(5));
        // Deduped by the domain event id: folded once (via the replay), so the count is 2, not 3.
        assertThat(repo.get("counter")).isEqualTo(2);
    }

    // The replay applies the overlapping event and the live copy is suppressed, which is right. What used to go
    // missing with it was the recording. An applied append is written down on a live delivery, so a projection wired
    // to record appends applied the event and never recorded it, and waitUntilApplied kept answering false.
    @Test
    void an_event_both_replayed_and_delivered_live_during_catch_up_is_recorded_as_applied() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = countedConverter();
        AppendId appendId = store.write("s", converter.toCloudEvents(List.of(new Counted("1"), new Counted("2"))))
                .appendId().orElseThrow();

        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        AppliedAppendStore appliedAppends = AppliedAppendStore.inMemory();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);
        MaterializedView<Counted> view = Projections.recordingAppliedAppends(
                Projections.materializedView(projection(), repository, "counter"), "counter", appliedAppends);
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "counter", view, Filter.all(), store, converter, Counted::eventId, null);

        // "2" also arrives live before the catch-up completes, carrying the metadata a broker bridge reads off the
        // message rather than the no-metadata overload the test above uses.
        FutureTask<Void> fed = feedWaitingForTheCatchUp(() -> feed.accept(metadataOf(store, "2"), new Counted("2")));
        feed.catchUp();

        assertThat(fed).succeedsWithin(Duration.ofSeconds(5));
        assertThat(repo.get("counter")).isEqualTo(2);
        assertThat(appliedAppends.hasApplied("counter", appendId)).isTrue();
    }

    // Projection.id returning null skips an event, so the replay can deliver an event without applying it. Its live
    // copy is still suppressed as a duplicate of that delivery, and must not record the append, because nothing in
    // the read model came from it.
    @Test
    void an_event_the_replay_skipped_is_not_recorded_as_applied_when_its_live_copy_is_suppressed() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = countedConverter();
        AppendId appendId = store.write("s", converter.toCloudEvents(List.of(new Counted("1")))).appendId().orElseThrow();

        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        AppliedAppendStore appliedAppends = AppliedAppendStore.inMemory();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);
        MaterializedView<Counted> view = Projections.recordingAppliedAppends(
                Projections.materializedView(skippingEveryEvent(), repository, "counter"), "counter", appliedAppends);
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "counter", view, Filter.all(), store, converter, Counted::eventId, null);

        // One copy buffers during the catch-up and one arrives after it, the two moments a copy can be suppressed.
        FutureTask<Void> fed = feedWaitingForTheCatchUp(() -> feed.accept(metadataOf(store, "1"), new Counted("1")));
        feed.catchUp();
        feed.accept(metadataOf(store, "1"), new Counted("1"));

        assertThat(fed).succeedsWithin(Duration.ofSeconds(5));
        assertThat(repo).isEmpty();
        assertThat(appliedAppends.hasApplied("counter", appendId)).isFalse();
    }

    // A stopped replay discards what a coalescing view buffered. The live copy of an event that replay delivered has
    // to be applied after goLive(), not skipped as a duplicate of a delivery whose work was thrown away.
    @Test
    void an_event_a_stopped_replay_delivered_is_applied_when_its_live_copy_arrives_after_go_live() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = countedConverter();
        AppendId appendId = store.write("s", converter.toCloudEvents(List.of(new Counted("1"), new Counted("2"))))
                .appendId().orElseThrow();

        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        AppliedAppendStore appliedAppends = AppliedAppendStore.inMemory();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);
        AtomicReference<CatchupProjectionFeed<Counted>> feedRef = new AtomicReference<>();
        // A batch larger than the history, so the view writes nothing before the stop discards what it buffered.
        MaterializedView<Counted> view = Projections.recordingAppliedAppends(
                Projections.materializedView(stoppingTheReplayAtTheFirstEvent(feedRef), repository, RetryStrategy.none(), new MaterializedViewOptions(100)),
                "counter", appliedAppends);
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "counter", view, Filter.all(), store, converter, Counted::eventId, null);
        feedRef.set(feed);

        feed.catchUp();
        assertThat(repo).isEmpty();

        feed.goLive();
        feed.accept(metadataOf(store, "1"), new Counted("1"));

        assertThat(repo.get("counter")).isEqualTo(1);
        assertThat(appliedAppends.hasApplied("counter", appendId)).isTrue();
    }

    // goLive() after a finished catch-up replays nothing. The live copy of an event that replay applied is still
    // suppressed, and its append is still recorded, because the copy reaches the source whose replay applied it.
    @Test
    void go_live_after_a_finished_catch_up_still_records_an_append_whose_live_copy_is_suppressed() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = countedConverter();
        AppendId appendId = store.write("s", converter.toCloudEvents(List.of(new Counted("1"), new Counted("2"))))
                .appendId().orElseThrow();

        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        AppliedAppendStore appliedAppends = AppliedAppendStore.inMemory();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);
        MaterializedView<Counted> view = Projections.recordingAppliedAppends(
                Projections.materializedView(projection(), repository, "counter"), "counter", appliedAppends);
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "counter", view, Filter.all(), store, converter, Counted::eventId, null);

        feed.catchUp();
        feed.goLive();
        feed.accept(metadataOf(store, "2"), new Counted("2"));

        assertThat(repo.get("counter")).isEqualTo(2);
        assertThat(appliedAppends.hasApplied("counter", appendId)).isTrue();
    }

    // catchUp() after goLive() runs a replay on a live feed. Events that arrive live while it runs, a copy of an event
    // the replay already delivered and one it never reads, must reach the read model when the replay is stopped,
    // though a coalescing view throws away everything it buffered during that replay.
    @Test
    void events_that_arrive_live_while_a_replay_runs_on_a_live_feed_are_applied_after_that_replay_is_stopped() {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = countedConverter();
        store.write("s", converter.toCloudEvents(List.of(new Counted("1"), new Counted("2"), new Counted("3"))));

        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(repo::get, repo::put);
        AtomicReference<CatchupProjectionFeed<Counted>> feedRef = new AtomicReference<>();
        List<FutureTask<Void>> fedDuringTheReplay = new CopyOnWriteArrayList<>();
        // A batch larger than the history, so the view writes nothing before the stop discards what it buffered.
        MaterializedView<Counted> view = Projections.materializedView(
                receivingLiveEventsThenStoppingTheReplayAtTheSecondEvent(feedRef, fedDuringTheReplay), repository, RetryStrategy.none(), new MaterializedViewOptions(100));
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create(
                "counter", view, Filter.all(), store, converter, Counted::eventId, null);
        feedRef.set(feed);

        feed.goLive();
        feed.catchUp();

        assertThat(fedDuringTheReplay).hasSize(2).allSatisfy(fed -> assertThat(fed).succeedsWithin(Duration.ofSeconds(5)));
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
        FutureTask<Void> fed = feedWaitingForTheCatchUp(() -> feed.accept(new Counted("3")));
        feed.catchUp();

        assertThat(fed).succeedsWithin(Duration.ofSeconds(5));
        assertThat(repo.get("counter")).isEqualTo(3);
    }

    @Test
    void accept_during_the_catch_up_returns_only_once_the_event_is_folded() throws InterruptedException {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = countedConverter();
        store.write("s", converter.toCloudEvents(List.of(new Counted("1"))));
        List<String> folded = new CopyOnWriteArrayList<>();
        CountDownLatch replaying = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter", holdingTheReplayAt("1", folded, replaying, releaseReplay, null),
                Filter.all(), store, converter, Counted::eventId, null);
        Thread catchingUp = new Thread(feed::catchUp, "catch-up");
        catchingUp.start();
        try {
            awaitLatch(replaying);

            AtomicReference<List<String>> foldedWhenAcceptReturned = new AtomicReference<>();
            FutureTask<Void> fed = feedWaitingForTheCatchUp(() -> {
                feed.accept(new Counted("live"));
                foldedWhenAcceptReturned.set(List.copyOf(folded));
            });

            assertThat(fed).as("accept(..) still waiting while the replay holds its event").isNotDone();
            releaseReplay.countDown();
            catchingUp.join(5_000);
            assertThat(fed).succeedsWithin(Duration.ofSeconds(5));
            assertThat(foldedWhenAcceptReturned.get()).as("what was folded when accept(..) returned").containsExactly("1", "live");
        } finally {
            releaseReplay.countDown();
        }
    }

    @Test
    void stopping_the_catch_up_while_accept_waits_makes_accept_throw_and_folds_nothing_live() throws InterruptedException {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = countedConverter();
        store.write("s", converter.toCloudEvents(List.of(new Counted("1"), new Counted("2"))));
        List<String> folded = new CopyOnWriteArrayList<>();
        CountDownLatch replaying = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter", holdingTheReplayAt("1", folded, replaying, releaseReplay, null),
                Filter.all(), store, converter, Counted::eventId, null);
        Thread catchingUp = new Thread(feed::catchUp, "catch-up");
        catchingUp.start();
        try {
            awaitLatch(replaying);
            FutureTask<Void> fed = feedWaitingForTheCatchUp(() -> feed.accept(new Counted("live")));

            feed.stopCatchUp();
            releaseReplay.countDown();
            catchingUp.join(5_000);

            Throwable thrownByAccept = catchThrowable(() -> fed.get(5, TimeUnit.SECONDS));
            assertThat(thrownByAccept).as("what accept(..) threw once the catch-up stopped")
                    .isInstanceOf(ExecutionException.class)
                    .cause()
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage(HandoverMessages.notApplied("projection feed"));
            assertThat(folded).containsExactly("1");
        } finally {
            releaseReplay.countDown();
        }
    }

    @Test
    void a_catch_up_failing_while_accept_waits_makes_accept_throw_the_failure() throws InterruptedException {
        InMemoryEventStore store = new InMemoryEventStore();
        CloudEventConverter<Counted> converter = countedConverter();
        store.write("s", converter.toCloudEvents(List.of(new Counted("1"))));
        List<String> folded = new CopyOnWriteArrayList<>();
        CountDownLatch replaying = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        IllegalStateException foldFailure = new IllegalStateException("fold failed");
        CatchupProjectionFeed<Counted> feed = CatchupProjectionFeed.create("counter", holdingTheReplayAt("1", folded, replaying, releaseReplay, foldFailure),
                Filter.all(), store, converter, Counted::eventId, null);
        AtomicReference<Throwable> thrownByCatchUp = new AtomicReference<>();
        Thread catchingUp = new Thread(() -> thrownByCatchUp.set(catchThrowable(feed::catchUp)), "catch-up");
        catchingUp.start();
        try {
            awaitLatch(replaying);
            FutureTask<Void> fed = feedWaitingForTheCatchUp(() -> feed.accept(new Counted("live")));

            releaseReplay.countDown();
            catchingUp.join(5_000);

            assertThat(thrownByCatchUp.get()).isSameAs(foldFailure);
            Throwable thrownByAccept = catchThrowable(() -> fed.get(5, TimeUnit.SECONDS));
            assertThat(thrownByAccept).as("what accept(..) threw once the catch-up failed")
                    .isInstanceOf(ExecutionException.class)
                    .cause()
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage(HandoverMessages.catchUpFailed("projection feed"))
                    .hasCauseReference(foldFailure);
            assertThat(folded).isEmpty();
        } finally {
            releaseReplay.countDown();
        }
    }

    @Test
    void stopping_a_feed_whose_catch_up_never_started_makes_a_waiting_accept_throw() {
        InMemoryEventStore store = new InMemoryEventStore();
        ConcurrentHashMap<String, Integer> repo = new ConcurrentHashMap<>();
        CatchupProjectionFeed<Counted> feed = feed("counter", store, countedConverter(), repo, null);
        FutureTask<Void> fed = feedWaitingForTheCatchUp(() -> feed.accept(new Counted("live")));

        feed.stopCatchUp();

        Throwable thrownByAccept = catchThrowable(() -> fed.get(5, TimeUnit.SECONDS));
        assertThat(thrownByAccept).as("what accept(..) threw once the feed stopped")
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(HandoverMessages.notApplied("projection feed"));
        assertThat(repo).isEmpty();
    }

    // Records every event it folds, and holds the replay at the given event until released. Not replay aware, so it
    // folds each replayed event as the replay delivers it rather than batching them until the replay completes
    private static MaterializedView<Counted> holdingTheReplayAt(String held, List<String> folded, CountDownLatch reached,
                                                               CountDownLatch release, RuntimeException failure) {
        return event -> {
            if (event.eventId().equals(held)) {
                reached.countDown();
                awaitLatch(release);
                if (failure != null) {
                    throw failure;
                }
            }
            folded.add(event.eventId());
        };
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            assertThat(latch.await(5, TimeUnit.SECONDS)).as("latch reached within the timeout").isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
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

        FutureTask<Void> first = feedWaitingForTheCatchUp(() -> feed.accept(new Counted("l1")));
        FutureTask<Void> second = feedWaitingForTheCatchUp(() -> feed.accept(new Counted("l2")));
        Throwable thrown = catchThrowable(() -> feed.accept(new Counted("l3")));

        assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("buffer overflowed");
        feed.stopCatchUp();
        assertThat(first).failsWithin(Duration.ofSeconds(5));
        assertThat(second).failsWithin(Duration.ofSeconds(5));
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

        FutureTask<Void> fed = feedWaitingForTheCatchUp(() -> feed.accept(new Counted("1")));
        feed.goLive();

        assertThat(fed).succeedsWithin(Duration.ofSeconds(5));
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

    // The stored CloudEvent's metadata, which is where the append id lives, so a live copy fed here carries what a
    // broker bridge would have read off the message.
    private static EventMetadata metadataOf(InMemoryEventStore store, String eventId) {
        try (Stream<CloudEvent> events = store.readInPositionOrder(Filter.all(), PositionRange.fromBeginning())) {
            return events.filter(event -> event.getId().equals(eventId))
                    .findFirst()
                    .map(EventMetadata::from)
                    .orElseThrow();
        }
    }

    private static Projection<Integer, Counted, String> projection() {
        return Projection.<Integer, Counted, String>builder(0)
                .id(event -> "counter")
                .on(Counted.class, (state, event) -> state + 1)
                .build();
    }

    // Stops the replay from inside the first event's id lookup, so that event is buffered and then discarded.
    private static Projection<Integer, Counted, String> stoppingTheReplayAtTheFirstEvent(AtomicReference<CatchupProjectionFeed<Counted>> feed) {
        AtomicBoolean stopped = new AtomicBoolean();
        return Projection.<Integer, Counted, String>builder(0)
                .id(event -> {
                    if (stopped.compareAndSet(false, true)) {
                        feed.get().stopCatchUp();
                    }
                    return "counter";
                })
                .on(Counted.class, (state, event) -> state + 1)
                .build();
    }

    // While the replay reads its second event, the feed receives two live events, a copy of the first event and one
    // the replay never reads, and the replay is then stopped. Fed from their own threads, since each waits for the
    // drain this replay's thread runs once it stops.
    private static Projection<Integer, Counted, String> receivingLiveEventsThenStoppingTheReplayAtTheSecondEvent(
            AtomicReference<CatchupProjectionFeed<Counted>> feed, List<FutureTask<Void>> fed) {
        AtomicInteger lookups = new AtomicInteger();
        return Projection.<Integer, Counted, String>builder(0)
                .id(event -> {
                    if (lookups.incrementAndGet() == 2) {
                        fed.add(feedWaitingForTheCatchUp(() -> feed.get().accept(new Counted("1"))));
                        fed.add(feedWaitingForTheCatchUp(() -> feed.get().accept(new Counted("live"))));
                        feed.get().stopCatchUp();
                    }
                    return "counter";
                })
                .on(Counted.class, (state, event) -> state + 1)
                .build();
    }

    // accept(..) returns only once the catch-up has folded the event, so an event fed ahead of the catch-up comes from
    // its own thread, already waiting in the buffer when this returns
    private static FutureTask<Void> feedWaitingForTheCatchUp(Runnable accept) {
        FutureTask<Void> feeding = new FutureTask<>(accept, null);
        Thread thread = new Thread(feeding, "live-delivery");
        thread.start();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (thread.isAlive() && thread.getState() != Thread.State.WAITING && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        return feeding;
    }

    private static Projection<Integer, Counted, String> skippingEveryEvent() {
        return Projection.<Integer, Counted, String>builder(0)
                .id(event -> null)
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

    private static final class ReplayAwareView implements MaterializedView<Counted>, ReplayAware {
        private final List<String> calls;
        private Runnable onUpdate = () -> {
        };

        private ReplayAwareView(List<String> calls) {
            this.calls = calls;
        }

        @Override
        public void update(Counted event) {
            calls.add("update:" + event.eventId());
            onUpdate.run();
        }

        @Override
        public void update(EventMetadata metadata, Counted event) {
            update(event);
        }

        @Override
        public void replayStarted() {
            calls.add("replayStarted");
        }

        @Override
        public void replayCompleted() {
            calls.add("replayCompleted");
        }

        @Override
        public void replayAbandoned() {
            calls.add("replayAbandoned");
        }
    }
}
