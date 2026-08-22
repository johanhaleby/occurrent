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

        StepVerifier.create(pipeline.catchup(live, LIVE_FILTER, DELIVER_EVERYTHING, 0).map(CloudEvent::getId))
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

        StepVerifier.create(pipeline.catchup(live, LIVE_FILTER, DELIVER_EVERYTHING, 0).map(CloudEvent::getId))
                .expectNext("e1", "e2", "e3", "e4", "e5", "e5")
                .verifyComplete();
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

        List<String> delivered = pipeline.catchup(live, LIVE_FILTER, DELIVER_EVERYTHING, 0).map(CloudEvent::getId).collectList().block();

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

        List<String> delivered = pipeline.catchup(live, LIVE_FILTER, DELIVER_EVERYTHING, 0).map(CloudEvent::getId).collectList().block();

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

        StepVerifier.create(pipeline.catchup(live, LIVE_FILTER, DELIVER_EVERYTHING, 0).map(CloudEvent::getId))
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

        StepVerifier.create(pipeline.replayApplying(0, new BoundedIdCache(1000), () -> true,
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

        StepVerifier.create(pipeline.replayApplying(0, new BoundedIdCache(1000), keepReplaying::get,
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

        StepVerifier.create(pipeline.replayApplying(0, new BoundedIdCache(1000), keepReplaying::get,
                        event -> Mono.fromRunnable(() -> keepReplaying.set(false)),
                        () -> {
                        }))
                .verifyComplete();

        // One read, for the history. The reconciliation would have made a second one.
        assertThat(headReads).hasValue(1);
    }

    private static final class FakeReader implements CatchupReader {
        private final TreeMap<Long, CloudEvent> byPosition = new TreeMap<>();
        private LongSupplier head = () -> 0L;

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

        @Override
        public Flux<CloudEvent> readWindow(long fromExclusive, long toInclusive) {
            return Flux.fromIterable(byPosition.subMap(fromExclusive, false, toInclusive, true).values());
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
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            return Flux.fromIterable(live);
        }
    }
}
