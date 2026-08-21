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

package org.occurrent.subscription.blocking.durable.catchup;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.internal.BoundedIdCache;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Deterministic unit tests for the blocking {@link PositionCatchupPipeline}, the mirror of the reactor pipeline's
 * reconcile. The reconcile drains up to a head snapshotted once, so it terminates even while the head keeps advancing.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class PositionCatchupPipelineTest {

    @Test
    void reconcile_terminates_under_continuous_writes_by_draining_up_to_a_snapshot_head() {
        // currentHead advances by 10 on every call, simulating writes that never stop during the catch-up. The old
        // reconcile re-read the head after every window and would loop forever, never returning to hand over to live.
        // The snapshot-bounded reconcile reads the head once for the bulk phase (10) and once for reconcile (20),
        // drains positions 1..20, and returns. assertTimeoutPreemptively fails loudly if the livelock ever returns.
        FakeReader reader = FakeReader.withEventsInRange(1, 100).headSupplier(advancingBy(10));
        CopyOnWriteArrayList<String> delivered = new CopyOnWriteArrayList<>();
        BoundedIdCache cache = new BoundedIdCache(1000);
        PositionCatchupPipeline pipeline = new PositionCatchupPipeline(reader, 1000);

        long cursor = assertTimeoutPreemptively(Duration.ofSeconds(5), () ->
                pipeline.replay(0, () -> true, (events, ignoredCache) -> events.forEach(e -> delivered.add(e.getId())), cache, () -> {
                    }));

        assertThat(cursor).isEqualTo(20L);
        assertThat(delivered).isEqualTo(idsInRange(1, 20));
    }

    @Test
    void reconcile_reads_the_head_only_for_the_bulk_phase_and_one_snapshot() {
        // Two currentHead calls total: the bulk head, then the reconcile snapshot. Proves reconcile does not chase the
        // head across repeated reads (which is what caused the livelock).
        AtomicLong headReads = new AtomicLong();
        FakeReader reader = FakeReader.withEventsInRange(1, 30);
        reader.headSupplier(() -> {
            headReads.incrementAndGet();
            return 30L;
        });
        BoundedIdCache cache = new BoundedIdCache(1000);
        PositionCatchupPipeline pipeline = new PositionCatchupPipeline(reader, 1000);

        long cursor = pipeline.replay(0, () -> true, (events, ignoredCache) -> events.forEach(CloudEvent::getId), cache, () -> {
        });

        assertThat(cursor).isEqualTo(30L);
        assertThat(headReads).hasValue(2);
    }

    private static LongSupplier advancingBy(long step) {
        AtomicLong head = new AtomicLong();
        return () -> head.addAndGet(step);
    }

    private static List<String> idsInRange(long fromInclusive, long toInclusive) {
        return LongStream.rangeClosed(fromInclusive, toInclusive).mapToObj(PositionCatchupPipelineTest::id).toList();
    }

    private static String id(long position) {
        return "e" + position;
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("type").build();
    }

    private static final class FakeReader implements PositionCatchupPipeline.Reader {
        private final TreeMap<Long, CloudEvent> byPosition = new TreeMap<>();
        private LongSupplier head = () -> 0L;

        static FakeReader withEventsInRange(long fromInclusive, long toInclusive) {
            FakeReader reader = new FakeReader();
            LongStream.rangeClosed(fromInclusive, toInclusive).forEach(position -> reader.byPosition.put(position, event(id(position))));
            return reader;
        }

        FakeReader headSupplier(LongSupplier supplier) {
            this.head = supplier;
            return this;
        }

        @Override
        public long currentHead() {
            return head.getAsLong();
        }

        @Override
        public Stream<CloudEvent> readWindow(long fromExclusive, long toInclusive) {
            return byPosition.subMap(fromExclusive, false, toInclusive, true).values().stream();
        }
    }
}
