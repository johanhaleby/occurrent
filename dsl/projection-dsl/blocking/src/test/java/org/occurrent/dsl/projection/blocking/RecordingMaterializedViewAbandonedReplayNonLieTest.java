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
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.dsl.projection.MaterializedViewOptions;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.AppendId;
import org.occurrent.eventstore.api.WriteResult;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The abandoned-replay non-lie (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>,
 * decision 6 / context "What a replay does to a record of membership"): {@code CoalescingMaterializedView} buffers a
 * replayed update in memory and discards the buffer if the replay is abandoned before it flushes. If a recording
 * wrapper recorded an append the moment the delegate's {@code update} returned, it would record an id for state a
 * discarded buffer never wrote, since a coalescing view's {@code applyReportingWhetherApplied} reports {@code true}
 * (queued) the moment an event is buffered, not once it is durable.
 * <p>
 * This wires the real production classes together: {@link CoalescingMaterializedView} (via
 * {@link Projections#materializedView}) wrapped by the real {@link RecordingMaterializedView} (via
 * {@link Projections#recordingAppliedAppends}), fed by a real {@link CatchupProjectionFeed}, using
 * Listening to no subscription model deliberately. On this pull-fed composition the only thing that can suppress
 * recording during the replay is the {@code ReplayAware} lifecycle forwarding
 * ({@code replayStarted}/{@code replayAbandoned}) the feed drives on the recording wrapper itself, exactly the
 * mechanism {@code CatchupProjectionFeed}/{@code DomainEventFeed} compositions rely on in production. A phase that
 * (wrongly) always answers "live" makes this a genuine falsifier of that forwarding, not a redundant re-test of the
 * phase gate {@code RecordingMaterializedViewTest} already covers.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class RecordingMaterializedViewAbandonedReplayNonLieTest {

    private static final URI SOURCE = URI.create("urn:occurrent:test");
    private static final String PROJECTION_ID = "ticks";

    record Ticked(String eventId) {
        String key() {
            return eventId.split("-")[0];
        }
    }

    @Test
    void an_abandoned_replay_records_nothing_for_the_events_its_discarded_buffer_never_wrote() throws InterruptedException {
        InMemoryEventStore store = new InMemoryEventStore();
        CountDownLatch parked = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        AtomicInteger converted = new AtomicInteger();
        // Parks the replay after the second event has been decoded (and, by the time it resumes, buffered), before
        // a third can arrive, so the test can abandon a replay that genuinely still has an unflushed batch in flight.
        CloudEventConverter<Ticked> converter = parkingConverter(converted, parked, proceed);
        List<Ticked> events = List.of(new Ticked("a-0"), new Ticked("b-0"), new Ticked("a-1"));
        WriteResult writeResult = store.write("s", converter.toCloudEvents(events));
        AppendId appendId = writeResult.appendId().orElseThrow();
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();

        Map<String, Integer> state = new ConcurrentHashMap<>();
        ViewStateRepository<Integer, String> repository = ViewStateRepository.create(state::get, state::put);
        AppliedAppendStore appliedAppendStore = AppliedAppendStore.inMemory();
        // A batch size larger than the whole history, so nothing flushes until replayCompleted() (which an abandon skips).
        MaterializedView<Ticked> coalescing = Projections.materializedView(
                tickProjection(), repository, RetryStrategy.none(), new MaterializedViewOptions(100));
        MaterializedView<Ticked> recording = Projections.recordingAppliedAppends(coalescing, PROJECTION_ID, appliedAppendStore);
        CatchupProjectionFeed<Ticked> feed = CatchupProjectionFeed.create(
                PROJECTION_ID, recording, org.occurrent.filter.Filter.all(), store, converter, Ticked::eventId, marker);

        // Captured rather than left to escape the thread uncaught: JUnit never sees an exception a background
        // thread throws, so without this a genuine regression that crashes the replay would leave the state and
        // membership assertions below unexercised and still passing, for the wrong reason.
        AtomicReference<Throwable> replayFailure = new AtomicReference<>();
        Thread replay = new Thread(() -> {
            try {
                feed.catchUp();
            } catch (Throwable t) {
                replayFailure.set(t);
            }
        });
        replay.start();
        try {
            // Bounded rather than a bare await(): a regression that stops the replay from ever reaching the second
            // event would otherwise hang this latch, and with it the whole Maven fork, forever. A bounded wait turns
            // that regression into the test failure it should be instead of an unexplained CI timeout.
            assertThat(parked.await(20, TimeUnit.SECONDS))
                    .as("the replay never reached its second event, so it never parked for this test to abandon it")
                    .isTrue();
            feed.stopCatchUp();
        } finally {
            // Released unconditionally, including when the await above times out or stopCatchUp() throws, so the
            // parked replay thread can never outlive this test method regardless of what failed.
            proceed.countDown();
        }
        replay.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(replay.isAlive()).as("the replay thread is still running after its bounded join").isFalse();
        assertThat(replayFailure.get()).as("the replay thread threw instead of abandoning cleanly").isNull();
        // Nothing was flushed to the read model...
        assertThat(state.get("a")).isNull();
        assertThat(state.get("b")).isNull();
        // ...and nothing was recorded as applied for it either. This is the assertion the pre-U5 test coverage never
        // made: CoalescingMaterializedViewTest proves the repository got no writes, but never wraps the view in a
        // RecordingMaterializedView to check the membership side of the same abandoned buffer.
        assertThat(appliedAppendStore.hasApplied(PROJECTION_ID, appendId)).isFalse();

        // The recording wrapper stays usable afterward: a live delivery (the recorder's replayStarted() marked
        // lifecycleReplaying, but replayAbandoned() cleared it again) records normally. catchUp() itself never
        // exercises this, since a bounded replay run never hands off to a live phase on its own, so this drives the
        // wrapper directly the way a subscription's post-handover delivery would.
        WriteResult liveWrite = store.write("s", tickedConverter().toCloudEvent(new Ticked("c-0")));
        AppendId liveAppendId = liveWrite.appendId().orElseThrow();
        List<CloudEvent> streamEvents = store.read("s").eventList();
        CloudEvent liveCloudEvent = streamEvents.get(streamEvents.size() - 1);
        recording.update(org.occurrent.cloudevents.EventMetadata.from(liveCloudEvent), new Ticked("c-0"));

        assertThat(state.get("c")).isEqualTo(1);
        assertThat(appliedAppendStore.hasApplied(PROJECTION_ID, liveAppendId)).isTrue();
    }

    private static Projection<Integer, Ticked, String> tickProjection() {
        return Projection.<Integer, Ticked, String>builder(0)
                .id(Ticked::key)
                .on(Ticked.class, (state, event) -> state + 1)
                .build();
    }

    private static CloudEventConverter<Ticked> tickedConverter() {
        return new CloudEventConverter<>() {
            @Override
            public CloudEvent toCloudEvent(Ticked domainEvent) {
                return CloudEventBuilder.v1()
                        .withId(domainEvent.eventId())
                        .withSource(SOURCE)
                        .withType("Ticked")
                        .build();
            }

            @Override
            public Ticked toDomainEvent(CloudEvent cloudEvent) {
                return new Ticked(cloudEvent.getId());
            }

            @Override
            public String getCloudEventType(Class<? extends Ticked> type) {
                return "Ticked";
            }
        };
    }

    private static CloudEventConverter<Ticked> parkingConverter(AtomicInteger converted, CountDownLatch parked, CountDownLatch proceed) {
        CloudEventConverter<Ticked> delegate = tickedConverter();
        return new CloudEventConverter<>() {
            @Override
            public CloudEvent toCloudEvent(Ticked domainEvent) {
                return delegate.toCloudEvent(domainEvent);
            }

            @Override
            public Ticked toDomainEvent(CloudEvent cloudEvent) {
                Ticked event = delegate.toDomainEvent(cloudEvent);
                if (converted.incrementAndGet() == 2) {
                    parked.countDown();
                    awaitUninterruptibly(proceed);
                }
                return event;
            }

            @Override
            public String getCloudEventType(Class<? extends Ticked> type) {
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
}
