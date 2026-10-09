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

package org.occurrent.subscription.reactor.durable.catchup;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.*;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.internal.BoundedIdCache;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Deterministic unit tests for {@link PositionCatchupPipeline} using a fake position reader and a fake live source, so
 * the reserve-low-position-commit-late ordering and the sustained-write reconcile can be reproduced without a database.
 * The pipeline owns the whole bulk-reconcile-live handover, so these tests exercise every phase of the no-loss
 * contract in one place.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class PositionCatchupPipelineTest {

    private static final SubscriptionFilter LIVE_FILTER = StreamSubscriptionFilter.filter(Filter.all());
    private static final Predicate<CloudEvent> DELIVER_EVERYTHING = __ -> true;
    private static final PositionCatchupPipeline.ReplayStart FROM_THE_BEGINNING = new PositionCatchupPipeline.ReplayStart(0, new StringBasedCheckpoint("token"), 0, null);

    @Test
    void a_low_position_event_that_commits_after_the_handover_advanced_past_it_is_still_delivered_exactly_once() {
        // The bulk read sees positions 1..5 but position 2 was reserved before commit (ADR 45) and had not committed
        // yet when the forward-only replay passed it, so the replay never reads it. The head does not move, so
        // reconcile adds nothing. The live stream carries only e2, because the resume checkpoint is taken before the
        // replay and lands strictly past every event committed by then, so e4 and e5 are outside its range. A
        // position-watermark dedup that dropped live events with position <= 5 would drop e2 and lose it. Id-based
        // dedup delivers it exactly once.
        FakeReader reader = FakeReader.withEventsAt(1, 3, 4, 5).head(5);
        FakeLiveSource live = new FakeLiveSource(events("e2"));
        PositionCatchupPipeline pipeline = new PositionCatchupPipeline(reader, 1000, 1000);

        StepVerifier.create(pipeline.catchup(live, LIVE_FILTER, DELIVER_EVERYTHING, GlobalCheckpoint.of(0)).map(CloudEvent::getId))
                .expectNext("e1", "e3", "e4", "e5", "e2")
                .verifyComplete();
    }

    @Test
    void a_history_event_whose_write_committed_after_the_head_read_is_delivered_again_by_the_live_stream() {
        // The #891 shape. e5 held a position at or below the head and committed after the head was read, so the
        // history window reads it even though it is not history. Nothing the history read delivers is recorded, so
        // the live delivery is the only one a recording projection can act on and the dedup must not suppress it.
        // Feed the cache from the history windows again and e5 arrives once, during the replay, and is never
        // recorded, which is the defect.
        FakeReader reader = FakeReader.withEventsInRange(1, 5).head(5);
        FakeLiveSource live = new FakeLiveSource(events("e5"));
        PositionCatchupPipeline pipeline = new PositionCatchupPipeline(reader, 1000, 1000);

        StepVerifier.create(pipeline.catchup(live, LIVE_FILTER, DELIVER_EVERYTHING, GlobalCheckpoint.of(0)).map(CloudEvent::getId))
                .expectNext("e1", "e2", "e3", "e4", "e5", "e5")
                .verifyComplete();
    }

    @Test
    void a_live_event_sharing_only_its_id_with_a_reconciled_event_is_delivered_and_not_suppressed() {
        // e1 from producer A is read during the reconcile phase (the bulk head is 0, so the bulk phase reads
        // nothing, and the reconcile snapshot at 1 picks up e1) and recorded in the dedup cache. A live event that
        // shares only its id, from producer B, is a different event under CloudEvents' (id, source) identity and
        // must still be delivered, not suppressed as a re-delivery of the reconciled one.
        FakeReader reader = FakeReader.withEventsAt(1).headSupplier(headsOf(0, 1));
        CloudEvent fromB = CloudEventBuilder.v1(event("e1")).withSource(URI.create("urn:producer:b")).build();
        FakeLiveSource live = new FakeLiveSource(List.of(fromB));
        PositionCatchupPipeline pipeline = new PositionCatchupPipeline(reader, 1000, 1000);

        StepVerifier.create(pipeline.catchup(live, LIVE_FILTER, DELIVER_EVERYTHING, GlobalCheckpoint.of(0)).map(ce -> ce.getId() + "@" + ce.getSource()))
                .expectNext("e1@urn:test", "e1@urn:producer:b")
                .verifyComplete();
    }

    @Test
    void the_named_catch_up_path_keeps_the_history_ids_out_of_the_cache() {
        // replayApplying is what every named subscription runs through, so it is what a recording projection runs
        // through, and the test above only covers the cold catchup(..) entry point. Cache a history id here and the
        // live delivery of a write that was still in flight when the head was read is dropped, which is #891.
        FakeReader reader = FakeReader.withEventsInRange(1, 4).headSupplier(headsOf(2, 4));
        BoundedIdCache<CatchupEventKey> cache = new BoundedIdCache<>(1000);
        PositionCatchupPipeline pipeline = new PositionCatchupPipeline(reader, 1000, 1000);

        StepVerifier.create(pipeline.replayApplying(FROM_THE_BEGINNING, cache, () -> true, event -> Mono.empty(), () -> {
        })).verifyComplete();

        assertThat(cache.contains(key("e1"))).as("read by a history window").isFalse();
        assertThat(cache.contains(key("e2"))).as("read by a history window").isFalse();
        assertThat(cache.contains(key("e3"))).as("read by the reconciliation window").isTrue();
        assertThat(cache.contains(key("e4"))).as("read by the reconciliation window").isTrue();
    }

    @Test
    void an_overlap_larger_than_the_old_1000_cap_delivers_each_event_exactly_once_when_the_ceiling_covers_it() {
        // The head is 500 when the replay starts and 2000 when reconcile snapshots it, so 1500 events were written
        // during the replay and the reconciliation pass reads them. Those are the events the live stream re-delivers,
        // since they committed after the resume checkpoint, and the reconciliation pass is what fills the cache. The
        // overlap is far past the old fixed 1000 cap, and with a ceiling that covers it every event arrives once.
        FakeReader reader = FakeReader.withEventsInRange(1, 2000).headSupplier(headsOf(500, 2000));
        FakeLiveSource live = new FakeLiveSource(eventsInRange(501, 2000));
        PositionCatchupPipeline pipeline = new PositionCatchupPipeline(reader, 1000, 5000);

        List<String> delivered = pipeline.catchup(live, LIVE_FILTER, DELIVER_EVERYTHING, GlobalCheckpoint.of(0)).map(CloudEvent::getId).collectList().block();

        assertThat(delivered).hasSize(2000);
        assertThat(delivered).doesNotHaveDuplicates();
        assertThat(Set.copyOf(delivered)).isEqualTo(idsInRange(1, 2000));
    }

    @Test
    void an_overlap_beyond_the_ceiling_may_be_delivered_more_than_once_but_is_never_lost() {
        // Everything here was written during the replay, so the head is 0 at the start and 500 when reconcile
        // snapshots it, and all 500 events come through the reconciliation pass that fills the cache. The overlap of
        // 500 re-delivered exceeds the ceiling of 100, so the oldest ids were evicted and are re-delivered. Eviction
        // can only cause a duplicate, never a loss, so every event still appears at least once.
        FakeReader reader = FakeReader.withEventsInRange(1, 500).headSupplier(headsOf(0, 500));
        FakeLiveSource live = new FakeLiveSource(eventsInRange(1, 500));
        PositionCatchupPipeline pipeline = new PositionCatchupPipeline(reader, 1000, 100);

        List<String> delivered = pipeline.catchup(live, LIVE_FILTER, DELIVER_EVERYTHING, GlobalCheckpoint.of(0)).map(CloudEvent::getId).collectList().block();

        assertThat(Set.copyOf(delivered)).isEqualTo(idsInRange(1, 500));
        assertThat(delivered.size()).isGreaterThan(500); // duplicates occurred, which is allowed
    }

    @Test
    void reconcile_hands_over_to_live_in_bounded_time_under_continuous_writes_and_loses_nothing() {
        // currentHead advances by 10 on every call, simulating writes that never stop during the catch-up. The old
        // reconcile re-read the head after every window and would chase this forever, never handing over to live (a
        // livelock). The snapshot-bounded reconcile reads the head exactly once, drains up to it, and completes: the
        // pipeline calls currentHead twice (bulk head 10, reconcile snapshot 20), so the replay drains positions 1..20
        // and then goes live. Everything past the snapshot is covered by the live stream, so nothing is lost.
        FakeReader reader = FakeReader.withEventsInRange(1, 40).headSupplier(advancingBy(10));
        FakeLiveSource live = new FakeLiveSource(events("live-1"));
        PositionCatchupPipeline pipeline = new PositionCatchupPipeline(reader, 1000, 1000);

        StepVerifier.create(pipeline.catchup(live, LIVE_FILTER, DELIVER_EVERYTHING, GlobalCheckpoint.of(0)).map(CloudEvent::getId))
                .expectNextSequence(idsInRangeList(1, 20))
                .expectNext("live-1")
                .verifyComplete();
    }

    // Answers the head reads in order, so a test can put a chosen number of events into the reconciliation pass
    // rather than the history pass. The pipeline reads the head once for the bulk phase and once for reconcile.
    private static LongSupplier headsOf(long bulkHead, long reconcileHead) {
        AtomicBoolean bulkHeadRead = new AtomicBoolean(false);
        return () -> bulkHeadRead.compareAndSet(false, true) ? bulkHead : reconcileHead;
    }

    private static LongSupplier advancingBy(long step) {
        AtomicLong head = new AtomicLong();
        return () -> head.addAndGet(step);
    }

    private static List<CloudEvent> events(String... ids) {
        return java.util.Arrays.stream(ids).map(PositionCatchupPipelineTest::event).collect(Collectors.toList());
    }

    private static List<CloudEvent> eventsInRange(long fromInclusive, long toInclusive) {
        return idsInRangeList(fromInclusive, toInclusive).stream().map(PositionCatchupPipelineTest::event).collect(Collectors.toList());
    }

    private static Set<String> idsInRange(long fromInclusive, long toInclusive) {
        return Set.copyOf(idsInRangeList(fromInclusive, toInclusive));
    }

    private static List<String> idsInRangeList(long fromInclusive, long toInclusive) {
        return LongStream.rangeClosed(fromInclusive, toInclusive).mapToObj(PositionCatchupPipelineTest::id).collect(Collectors.toList());
    }

    private static String id(long position) {
        return "e" + position;
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("type").build();
    }

    private static CatchupEventKey key(String id) {
        return new CatchupEventKey(id, URI.create("urn:test"));
    }

    // Maps a position to an event and answers head reads from a supplier so a test can hold the head still or keep it
    // advancing to simulate sustained writes.
    // Two things make this test able to fail. The history has to be longer than concatMap's default prefetch of 32,
    // and the action has to complete on another thread. With a synchronous action the drain runs inline with the
    // emission, no queue ever builds, and the assertion holds wherever the announcement is made.
    @Test
    void the_history_is_fully_handled_before_the_reconciliation_is_announced() {
        FakeReader reader = FakeReader.withEventsInRange(1, 64).head(64);
        PositionCatchupPipeline pipeline = new PositionCatchupPipeline(reader, 1000, 1000);
        AtomicInteger handled = new AtomicInteger();
        AtomicInteger handledWhenAnnounced = new AtomicInteger(-1);

        StepVerifier.create(pipeline.replayApplying(FROM_THE_BEGINNING, new BoundedIdCache<>(1000), () -> true,
                        event -> Mono.<Void>fromRunnable(handled::incrementAndGet).subscribeOn(Schedulers.single()),
                        () -> handledWhenAnnounced.set(handled.get())))
                .verifyComplete();

        assertThat(handledWhenAnnounced).hasValue(64);
    }

    // A history that stopped part way through is not a history that was read, so nothing may announce that it was.
    // A recording projection told otherwise would record the rest of a rebuild it never finished as though it were
    // live.
    @Test
    void a_truncated_history_never_announces_that_it_was_read() {
        FakeReader reader = FakeReader.withEventsInRange(1, 10).head(10);
        PositionCatchupPipeline pipeline = new PositionCatchupPipeline(reader, 1000, 1000);
        AtomicBoolean keepReplaying = new AtomicBoolean(true);
        AtomicBoolean announced = new AtomicBoolean(false);

        StepVerifier.create(pipeline.replayApplying(FROM_THE_BEGINNING, new BoundedIdCache<>(1000), keepReplaying::get,
                        event -> Mono.fromRunnable(() -> keepReplaying.set(false)),
                        () -> announced.set(true)))
                .verifyComplete();

        assertThat(announced).isFalse();
    }

    @Test
    void a_stop_after_the_history_drained_reads_nothing_more_from_the_store() {
        AtomicLong headReads = new AtomicLong();
        FakeReader reader = FakeReader.withEventsInRange(1, 5).headSupplier(() -> {
            headReads.incrementAndGet();
            return 5L;
        });
        PositionCatchupPipeline pipeline = new PositionCatchupPipeline(reader, 1000, 1000);
        AtomicBoolean keepReplaying = new AtomicBoolean(true);

        StepVerifier.create(pipeline.replayApplying(FROM_THE_BEGINNING, new BoundedIdCache<>(1000), keepReplaying::get,
                        event -> Mono.fromRunnable(() -> keepReplaying.set(false)),
                        () -> {
                        }))
                .verifyComplete();

        // One read, for the history. The reconciliation would have made a second one.
        assertThat(headReads).hasValue(1);
    }

    @Test
    void a_stored_live_start_the_model_still_has_is_kept_and_every_replayed_checkpoint_carries_it() {
        FakeReader reader = FakeReader.withEventsInRange(1, 4).head(4).taggedWithTheirPosition();
        Checkpoint storedLiveStart = new StringBasedCheckpoint("stored live start");
        RecordingLiveSource live = new RecordingLiveSource(true);
        PositionCatchupPipeline pipeline = new PositionCatchupPipeline(reader, 1000, 1000);

        StepVerifier.create(pipeline.catchup(live, LIVE_FILTER, DELIVER_EVERYTHING, GlobalCheckpoint.of(2, storedLiveStart, 0, 4)).map(PositionCatchupPipelineTest::checkpointOf))
                .expectNext(GlobalCheckpoint.of(3, storedLiveStart, 0, 4), GlobalCheckpoint.of(4, storedLiveStart, 0, 4))
                .verifyComplete();
        assertThat(live.resumeProbes).containsExactly(storedLiveStart);
        assertThat(live.liveStarts).containsExactly(StartAt.checkpoint(storedLiveStart).toString());
    }

    @Test
    void a_resume_from_a_stored_live_start_replays_no_further_than_the_stored_replay_end_and_reads_no_head() {
        AtomicLong headReads = new AtomicLong();
        FakeReader reader = FakeReader.withEventsInRange(1, 5).headSupplier(() -> {
            headReads.incrementAndGet();
            return 5L;
        }).taggedWithTheirPosition();
        Checkpoint storedLiveStart = new StringBasedCheckpoint("stored live start");
        RecordingLiveSource live = new RecordingLiveSource(true);
        PositionCatchupPipeline pipeline = new PositionCatchupPipeline(reader, 1000, 1000);

        StepVerifier.create(pipeline.catchup(live, LIVE_FILTER, DELIVER_EVERYTHING, GlobalCheckpoint.of(2, storedLiveStart, 0, 3)).map(PositionCatchupPipelineTest::checkpointOf))
                .expectNext(GlobalCheckpoint.of(3, storedLiveStart, 0, 3))
                .verifyComplete();
        // Positions 4 and 5 lie above the stored end, so they were committed after the stored live start and arrive live
        assertThat(headReads).hasValue(0);
        assertThat(live.liveStarts).containsExactly(StartAt.checkpoint(storedLiveStart).toString());
    }

    @Test
    void a_stored_live_start_the_model_no_longer_has_replays_again_from_the_origin_and_goes_live_from_a_start_read_now() {
        FakeReader reader = FakeReader.withEventsInRange(1, 4).head(4).taggedWithTheirPosition();
        Checkpoint agedOut = new StringBasedCheckpoint("aged out live start");
        RecordingLiveSource live = new RecordingLiveSource(false);
        PositionCatchupPipeline pipeline = new PositionCatchupPipeline(reader, 1000, 1000);

        StepVerifier.create(pipeline.catchup(live, LIVE_FILTER, DELIVER_EVERYTHING, GlobalCheckpoint.of(3, agedOut, 1, 3)).map(PositionCatchupPipelineTest::checkpointOf))
                .expectNext(GlobalCheckpoint.of(2, RecordingLiveSource.READ_NOW, 1, 4), GlobalCheckpoint.of(3, RecordingLiveSource.READ_NOW, 1, 4), GlobalCheckpoint.of(4, RecordingLiveSource.READ_NOW, 1, 4))
                .verifyComplete();
        assertThat(live.resumeProbes).containsExactly(agedOut);
        assertThat(live.liveStarts).containsExactly(StartAt.checkpoint(RecordingLiveSource.READ_NOW).toString());
    }

    @Test
    void a_start_without_a_live_start_reads_one_now_without_asking_the_model() {
        FakeReader reader = FakeReader.withEventsInRange(1, 3).head(3).taggedWithTheirPosition();
        RecordingLiveSource live = new RecordingLiveSource(true);
        PositionCatchupPipeline pipeline = new PositionCatchupPipeline(reader, 1000, 1000);

        StepVerifier.create(pipeline.catchup(live, LIVE_FILTER, DELIVER_EVERYTHING, GlobalCheckpoint.of(1)).map(PositionCatchupPipelineTest::checkpointOf))
                .expectNext(GlobalCheckpoint.of(2, RecordingLiveSource.READ_NOW, 1, 3), GlobalCheckpoint.of(3, RecordingLiveSource.READ_NOW, 1, 3))
                .verifyComplete();
        assertThat(live.resumeProbes).isEmpty();
        assertThat(live.liveStarts).containsExactly(StartAt.checkpoint(RecordingLiveSource.READ_NOW).toString());
    }

    private static Checkpoint checkpointOf(CloudEvent cloudEvent) {
        return ((CheckpointAwareCloudEvent) cloudEvent).getCheckpoint();
    }

    private static final class FakeReader implements CatchupReader {
        private final TreeMap<Long, CloudEvent> byPosition = new TreeMap<>();
        private LongSupplier head = () -> 0L;
        private boolean taggedWithTheirPosition;

        static FakeReader withEventsAt(long... positions) {
            FakeReader reader = new FakeReader();
            for (long position : positions) {
                reader.byPosition.put(position, event(id(position)));
            }
            return reader;
        }

        static FakeReader withEventsInRange(long fromInclusive, long toInclusive) {
            FakeReader reader = new FakeReader();
            LongStream.rangeClosed(fromInclusive, toInclusive).forEach(position -> reader.byPosition.put(position, event(id(position))));
            return reader;
        }

        FakeReader head(long value) {
            this.head = () -> value;
            return this;
        }

        FakeReader headSupplier(LongSupplier supplier) {
            this.head = supplier;
            return this;
        }

        // Wraps each event with its position the way the Mongo readers do
        FakeReader taggedWithTheirPosition() {
            this.taggedWithTheirPosition = true;
            return this;
        }

        @Override
        public Flux<CloudEvent> readWindow(long fromExclusive, long toInclusive) {
            return Flux.fromIterable(byPosition.subMap(fromExclusive, false, toInclusive, true).entrySet())
                    .map(entry -> taggedWithTheirPosition ? new CheckpointAwareCloudEvent(entry.getValue(), GlobalCheckpoint.of(entry.getKey())) : entry.getValue());
        }

        @Override
        public Mono<Long> currentHead() {
            return Mono.fromSupplier(head::getAsLong);
        }
    }

    // A live source whose subscribe replays a fixed, finite list so the handover can be verified deterministically. In
    // production the live stream is unbounded, but a finite list is enough to prove the dedup at the seam.
    private record FakeLiveSource(List<CloudEvent> live) implements CheckpointAwareSubscriptionModel {
        @Override
        public Mono<Checkpoint> globalCheckpoint() {
            return Mono.just(new StringBasedCheckpoint("token"));
        }

        @Override
        // The position never moves, so it is the one at the call
        public Mono<Checkpoint> globalCheckpointAsOfNow() {
            return globalCheckpoint();
        }

        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            return Flux.fromIterable(live);
        }
    }

    // A live source with nothing to deliver that answers canResumeFrom with a fixed value and records what it was
    // asked and where it was subscribed from
    private static final class RecordingLiveSource implements CheckpointAwareSubscriptionModel {
        static final Checkpoint READ_NOW = new StringBasedCheckpoint("live start read now");
        private final boolean canResume;
        private final List<Checkpoint> resumeProbes = new CopyOnWriteArrayList<>();
        private final List<String> liveStarts = new CopyOnWriteArrayList<>();

        RecordingLiveSource(boolean canResume) {
            this.canResume = canResume;
        }

        @Override
        public Mono<Checkpoint> globalCheckpoint() {
            return Mono.just(READ_NOW);
        }

        @Override
        public Mono<Checkpoint> globalCheckpointAsOfNow() {
            return globalCheckpoint();
        }

        @Override
        public Mono<Boolean> canResumeFrom(Checkpoint checkpoint) {
            return Mono.fromSupplier(() -> {
                resumeProbes.add(checkpoint);
                return canResume;
            });
        }

        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            liveStarts.add(startAt.toString());
            return Flux.empty();
        }
    }
}
