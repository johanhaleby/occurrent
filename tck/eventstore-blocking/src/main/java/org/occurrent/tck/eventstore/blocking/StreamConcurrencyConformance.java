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

package org.occurrent.tck.eventstore.blocking;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.WriteConditionNotFulfilledException;
import org.occurrent.eventstore.api.blocking.EventStream;
import org.occurrent.tck.ConcurrentRendezvous;
import org.occurrent.tck.ConcurrentRendezvous.Outcome;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.tck.ConformanceEvents.event;
import static org.occurrent.tck.ConformanceEvents.idsOf;

/**
 * Two threads (or more) writing to the same stream at the same time: what a conditional write does when exactly one
 * of them can win, and what an unconditional write does when none of them can lose.
 * <p>
 * This is the replacement for issue 467: five tests existed for this already, in {@code MongoEventStoreTest}'s
 * {@code ParallelWritesToEventStoreReturns}, but every one of them was {@code @EnabledOnOs(MAC)} while every CI run
 * is Linux, so none of them had run anywhere in a long time. They also decided their outcome by having both racing
 * threads write into a single shared {@code AtomicReference}, which is itself a race: whichever thread wrote to the
 * reference last is what the assertion saw, independent of which write actually reached the store first. This suite
 * uses {@link ConcurrentRendezvous} instead, which returns each thread's own outcome as a value, so counting winners
 * and losers does not depend on the order two threads happen to finish in.
 * <p>
 * No fixture declaration governs either behaviour below. Both are contract for every store that declares
 * {@link EventStoreCapability#STREAM}, not a variation a fixture gets to opt into, so a store that fails here has a
 * real defect to report rather than a missing declaration to add.
 */
@NullMarked
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("the stream write-write concurrency contract")
public abstract class StreamConcurrencyConformance extends EventStoreConformance {

    private static final String DEFINED = "NameDefined";

    /** Repetitions per test method. A single race proves little; this is how many independent races each test runs. */
    private static final int ITERATIONS = 5;

    @Override
    protected final Set<EventStoreCapability> requiredCapabilities() {
        return Set.of(EventStoreCapability.STREAM);
    }

    @Test
    @Timeout(60)
    void a_conditional_write_from_two_threads_leaves_exactly_one_winner() throws Exception {
        for (int i = 0; i < ITERATIONS; i++) {
            String streamId = "conditional-" + i;
            String winnerEventIdIfA = "a-" + i;
            String winnerEventIdIfB = "b-" + i;
            WriteCondition condition = WriteCondition.streamVersionEq(0);

            List<Outcome<String>> outcomes = ConcurrentRendezvous.collide(2, index -> {
                String eventId = index == 0 ? winnerEventIdIfA : winnerEventIdIfB;
                CloudEvent event = event(eventId, DEFINED);
                return () -> {
                    eventStore().write(streamId, condition, List.of(event));
                    return eventId;
                };
            });

            long successCount = outcomes.stream().filter(Outcome::succeeded).count();
            assertThat(successCount)
                    .as("Iteration %d: exactly one of the two conditional writes to stream '%s' must succeed", i, streamId)
                    .isEqualTo(1);

            List<Throwable> failures = outcomes.stream()
                    .filter(outcome -> !outcome.succeeded())
                    .map(Outcome::failure)
                    .toList();
            assertThat(failures)
                    .as("Iteration %d: exactly one of the two conditional writes to stream '%s' must fail", i, streamId)
                    .hasSize(1);
            assertThat(failures.getFirst())
                    .as("Iteration %d: the losing write to stream '%s' must fail with WriteConditionNotFulfilledException, "
                            + "not some other exception", i, streamId)
                    .isInstanceOf(WriteConditionNotFulfilledException.class);

            String winnerEventId = outcomes.stream()
                    .filter(Outcome::succeeded)
                    .map(Outcome::value)
                    .findFirst()
                    .orElseThrow();

            EventStream<CloudEvent> stream = eventStore().read(streamId);
            assertThat(idsOf(stream.eventList()))
                    .as("Iteration %d: stream '%s' must hold exactly the winner's event and nothing else", i, streamId)
                    .containsExactly(winnerEventId);
        }
    }

    @Test
    @Timeout(60)
    void unconditional_writes_from_several_threads_all_succeed() throws Exception {
        int threadCount = 6;

        for (int i = 0; i < ITERATIONS; i++) {
            String streamId = "unconditional-" + i;
            int iteration = i;

            List<Outcome<String>> outcomes = ConcurrentRendezvous.collide(threadCount, index -> {
                String eventId = "event-" + iteration + "-" + index;
                CloudEvent event = event(eventId, DEFINED);
                return () -> {
                    // No WriteCondition: WriteCondition.anyStreamVersion() promises that a version race between
                    // concurrent writers can never fail a write, so any failure observed here is a real defect in
                    // the store rather than an expected loser.
                    eventStore().write(streamId, List.of(event));
                    return eventId;
                };
            });

            List<Throwable> failures = outcomes.stream()
                    .filter(outcome -> !outcome.succeeded())
                    .map(Outcome::failure)
                    .toList();
            assertThat(failures)
                    .as("Iteration %d: none of the %d unconditional writes to stream '%s' may fail", i, threadCount, streamId)
                    .isEmpty();

            List<String> writtenEventIds = outcomes.stream().map(Outcome::value).toList();

            EventStream<CloudEvent> stream = eventStore().read(streamId);
            assertThat(idsOf(stream.eventList()))
                    .as("Iteration %d: stream '%s' must hold exactly the %d written events, each exactly once", i, streamId, threadCount)
                    .containsExactlyInAnyOrderElementsOf(writtenEventIds);
        }
    }
}
