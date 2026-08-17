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
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
import org.occurrent.eventstore.api.dcb.DcbAppendConditionNotFulfilledException;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbConsistencyToken;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.tck.ConcurrentRendezvous;
import org.occurrent.tck.ConcurrentRendezvous.Outcome;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.eventstore.api.dcb.DcbAppendCondition.failIfEventsMatch;
import static org.occurrent.tck.ConformanceEvents.extension;
import static org.occurrent.tck.ConformanceEvents.idsOf;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.tag;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.taggedEventWithId;

/**
 * What a DCB append condition is actually for. Two writers decide against the same consistency boundary at the same
 * time, and letting both through corrupts the invariant each of them checked.
 * <p>
 * Write skew is the case worth having tests for. Two writers read boundaries that are not obviously the same, decide
 * independently, and each appends an event the other's boundary would have matched. Both writers are individually
 * correct and the result is wrong, so a store has to reject one of them. The interesting part is that the two
 * boundaries are described differently, one by type and one by tag, or as two overlapping type sets. This suite
 * drives each of those into a real race rather than reasoning about it.
 * <p>
 * Nothing here is fixture-declared, {@link EventStoreFixture#appendConditionModel()} included. Both models exist to
 * be sound, and soundness is exactly what safety under a race means, so a store failing anything below has a real
 * defect rather than a missing declaration. The one thing the two models disagree about, whether a boundary conflicts
 * that arguably should not have, only ever produces extra losers, and every assertion here that could be weakened by
 * an extra loser instead demands a winner.
 * <p>
 * These scenarios put every append into one storage stream on a store that derives placement from tags, so two
 * concurrent appends collide on the next stream version as well as on the DCB condition. That collision is the
 * store's to retry, not the caller's, so a loser here must be a
 * {@link DcbAppendConditionNotFulfilledException} and never a duplicate-key or write-conflict error leaking through.
 */
@NullMarked
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("the DCB write-write concurrency contract")
public abstract class DcbConcurrencyConformance extends EventStoreConformance {

    private static final String RESERVED = "SeatReserved";
    private static final String RELEASED = "SeatReleased";

    /** Repetitions per test method. A single race proves little, so this is how many independent races each test runs. */
    private static final int ITERATIONS = 5;

    @Override
    protected final Set<EventStoreCapability> requiredCapabilities() {
        return Set.of(EventStoreCapability.DCB);
    }

    @Test
    @Timeout(120)
    void appends_to_one_boundary_from_several_threads_leave_exactly_one_winner() {
        int threadCount = 8;

        for (int iteration = 0; iteration < ITERATIONS; iteration++) {
            String show = "show:" + iteration;
            DcbCriteria boundary = DcbCriteria.tags(tag(show));
            // Read once, before the race. Every thread decides against the same observed boundary, which is what two
            // application services handling the same command at the same time actually do.
            DcbConsistencyToken token = dcbEventStore().read(boundary).consistencyToken();

            int currentIteration = iteration;
            List<Outcome<String>> outcomes = ConcurrentRendezvous.collide(threadCount, index -> {
                String eventId = "seat-" + currentIteration + "-" + index;
                CloudEvent event = taggedEventWithId(eventId, RESERVED, show);
                return () -> {
                    dcbEventStore().append(List.of(event), failIfEventsMatch(boundary, token));
                    return eventId;
                };
            });

            String winner = assertExactlyOneWinner(iteration, outcomes,
                    threadCount + " threads appending to the same boundary");
            assertThat(idsOf(dcbEventStore().read(boundary).events()))
                    .as("Iteration %d: boundary '%s' must hold the winner's event and nothing else, so a rejected "
                            + "append leaves no trace", iteration, show)
                    .containsExactly(winner);
        }
    }

    @Test
    @Timeout(120)
    void appends_to_disjoint_boundaries_do_not_falsely_serialise() {
        int threadCount = 4;

        for (int iteration = 0; iteration < ITERATIONS; iteration++) {
            int currentIteration = iteration;

            List<Outcome<String>> outcomes = ConcurrentRendezvous.collide(threadCount, index -> {
                String show = "show:" + currentIteration + "-" + index;
                DcbCriteria boundary = DcbCriteria.tags(tag(show));
                DcbConsistencyToken token = dcbEventStore().read(boundary).consistencyToken();
                String eventId = "seat-" + currentIteration + "-" + index;
                CloudEvent event = taggedEventWithId(eventId, RESERVED, show);
                return () -> {
                    dcbEventStore().append(List.of(event), failIfEventsMatch(boundary, token));
                    return eventId;
                };
            });

            assertThat(failuresOf(outcomes))
                    .as("Iteration %d: %d threads appending to %d boundaries that share no tag must all succeed. "
                            + "Rejecting one would make a DCB append condition behave like a whole-store lock, which "
                            + "is the thing DCB exists to avoid", iteration, threadCount, threadCount)
                    .isEmpty();
        }
    }

    @Test
    @Timeout(120)
    void a_type_scoped_and_a_tag_scoped_condition_over_the_same_event_cannot_both_win() {
        for (int iteration = 0; iteration < ITERATIONS; iteration++) {
            String type = RESERVED + iteration;
            String show = "show:" + iteration;

            // Two boundaries described differently that both match the event either writer is about to append. Each
            // writer checked something true when it read, and both committing makes both of them wrong.
            DcbCriteria byType = DcbCriteria.types(type);
            DcbCriteria byTag = DcbCriteria.tags(tag(show));
            DcbAppendCondition onType = failIfEventsMatch(byType, dcbEventStore().read(byType).consistencyToken());
            DcbAppendCondition onTag = failIfEventsMatch(byTag, dcbEventStore().read(byTag).consistencyToken());

            assertExactlyOneWinnerOfTwo(iteration, "a type-scoped and a tag-scoped condition", byTag,
                    taggedEventWithId("by-type-" + iteration, type, show), onType,
                    taggedEventWithId("by-tag-" + iteration, type, show), onTag);
        }
    }

    @Test
    @Timeout(120)
    void two_overlapping_type_scoped_conditions_cannot_both_win() {
        for (int iteration = 0; iteration < ITERATIONS; iteration++) {
            String reserved = RESERVED + iteration;
            String released = RELEASED + iteration;
            String show = "show:" + iteration;

            // The narrow boundary is a subset of the wide one, so an event of the narrow type matches both. A store
            // that compared boundaries for equality rather than for overlap would let both of these through.
            DcbCriteria narrow = DcbCriteria.types(reserved);
            DcbCriteria wide = DcbCriteria.types(reserved, released);
            DcbAppendCondition onNarrow = failIfEventsMatch(narrow, dcbEventStore().read(narrow).consistencyToken());
            DcbAppendCondition onWide = failIfEventsMatch(wide, dcbEventStore().read(wide).consistencyToken());

            assertExactlyOneWinnerOfTwo(iteration, "a narrow and a wider type-scoped condition",
                    DcbCriteria.tags(tag(show)),
                    taggedEventWithId("narrow-" + iteration, reserved, show), onNarrow,
                    taggedEventWithId("wide-" + iteration, reserved, show), onWide);
        }
    }

    @Test
    @Timeout(120)
    void two_overlapping_tag_scoped_conditions_cannot_both_win() {
        for (int iteration = 0; iteration < ITERATIONS; iteration++) {
            String show = "show:" + iteration;
            String row = "row:" + iteration;

            // One writer guards the show, the other guards the show and the row together. The narrower boundary is
            // inside the wider one, so an event carrying both tags falls in each. Overlap by tag rather than by
            // type, which is the shape a store keying conditions on per-tag markers is most likely to get right for
            // the wrong reason.
            DcbCriteria wide = DcbCriteria.tags(tag(show));
            DcbCriteria narrow = DcbCriteria.tags(tag(show), tag(row));
            DcbAppendCondition onWide = failIfEventsMatch(wide, dcbEventStore().read(wide).consistencyToken());
            DcbAppendCondition onNarrow = failIfEventsMatch(narrow, dcbEventStore().read(narrow).consistencyToken());

            assertExactlyOneWinnerOfTwo(iteration, "a one-tag and a two-tag condition", wide,
                    taggedEventWithId("wide-" + iteration, RESERVED, show, row), onWide,
                    taggedEventWithId("narrow-" + iteration, RESERVED, show, row), onNarrow);
        }
    }

    @Test
    @Timeout(120)
    void appends_to_a_boundary_spanning_two_tags_leave_exactly_one_winner() {
        int threadCount = 8;

        for (int iteration = 0; iteration < ITERATIONS; iteration++) {
            String show = "show:" + iteration;
            String row = "row:" + iteration;
            // A boundary of two tags at once. A store deriving conflicts from per-tag markers has two of them to keep
            // in step here, and keeping one in step is not the same as keeping both.
            DcbCriteria boundary = DcbCriteria.tags(tag(show), tag(row));
            DcbConsistencyToken token = dcbEventStore().read(boundary).consistencyToken();
            int currentIteration = iteration;

            List<Outcome<String>> outcomes = ConcurrentRendezvous.collide(threadCount, index -> {
                String eventId = "spanning-" + currentIteration + "-" + index;
                CloudEvent event = taggedEventWithId(eventId, RESERVED, show, row);
                return () -> {
                    dcbEventStore().append(List.of(event), failIfEventsMatch(boundary, token));
                    return eventId;
                };
            });

            String winner = assertExactlyOneWinner(iteration, outcomes,
                    threadCount + " threads appending to a boundary spanning two tags");
            assertThat(idsOf(dcbEventStore().read(boundary).events()))
                    .as("Iteration %d: a boundary of two tags must admit exactly one of the racing appends",
                            iteration)
                    .containsExactly(winner);
        }
    }

    @Test
    @Timeout(120)
    void an_untokenized_guard_admits_exactly_one_of_several_racing_creates() {
        int threadCount = 8;

        for (int iteration = 0; iteration < ITERATIONS; iteration++) {
            String show = "show:" + iteration;
            DcbCriteria boundary = DcbCriteria.tags(tag(show));
            int currentIteration = iteration;

            // No token, so the condition means "nothing matching may exist". This is the create-once guard, and it is
            // the one case where a store cannot lean on a token read to tell it what changed.
            List<Outcome<String>> outcomes = ConcurrentRendezvous.collide(threadCount, index -> {
                String eventId = "created-" + currentIteration + "-" + index;
                CloudEvent event = taggedEventWithId(eventId, RESERVED, show);
                return () -> {
                    dcbEventStore().append(List.of(event), failIfEventsMatch(boundary));
                    return eventId;
                };
            });

            String winner = assertExactlyOneWinner(iteration, outcomes,
                    threadCount + " threads racing an untokenized create guard");
            assertThat(idsOf(dcbEventStore().read(boundary).events()))
                    .as("Iteration %d: an untokenized guard must admit exactly one create, so boundary '%s' must hold "
                            + "one event", iteration, show)
                    .containsExactly(winner);
        }
    }

    @Test
    @Timeout(120)
    void unconditional_appends_from_several_threads_all_succeed() {
        int threadCount = 4;

        for (int iteration = 0; iteration < ITERATIONS; iteration++) {
            String show = "show:" + iteration;
            int currentIteration = iteration;

            List<Outcome<String>> outcomes = ConcurrentRendezvous.collide(threadCount, index -> {
                String eventId = "unconditional-" + currentIteration + "-" + index;
                CloudEvent event = taggedEventWithId(eventId, RESERVED, show);
                // No condition at all, so nothing can legitimately reject these. They still share a tag, so a store
                // deriving placement from tags puts them in one storage stream and they collide on its version. That
                // is the store's collision to retry.
                return () -> {
                    dcbEventStore().append(List.of(event));
                    return eventId;
                };
            });

            assertThat(failuresOf(outcomes))
                    .as("Iteration %d: an append with no condition has nothing to fail on, so none of the %d may "
                            + "fail even though they all land in the same consistency boundary", iteration, threadCount)
                    .isEmpty();
            assertThat(idsOf(dcbEventStore().read(DcbCriteria.tags(tag(show))).events()))
                    .as("Iteration %d: every unconditional append must be committed exactly once", iteration)
                    .containsExactlyInAnyOrderElementsOf(outcomes.stream().map(Outcome::value).toList());
        }
    }

    @Test
    @Timeout(120)
    void concurrent_unconditional_appends_to_the_same_boundary_each_get_a_distinct_append_id_with_no_cross_contamination() {
        int threadCount = 4;

        for (int iteration = 0; iteration < ITERATIONS; iteration++) {
            String show = "show:" + iteration;
            int currentIteration = iteration;

            List<Outcome<Map.Entry<String, String>>> outcomes = ConcurrentRendezvous.collide(threadCount, index -> {
                String eventId = "append-id-" + currentIteration + "-" + index;
                CloudEvent event = taggedEventWithId(eventId, RESERVED, show);
                return () -> {
                    DcbAppendResult result = dcbEventStore().append(List.of(event));
                    return Map.entry(eventId, result.appendId().orElseThrow().toString());
                };
            });

            List<Map.Entry<String, String>> results = outcomes.stream().map(Outcome::value).toList();

            assertThat(results.stream().map(Map.Entry::getValue).distinct().count())
                    .as("Iteration %d: each of the %d concurrent appends to boundary '%s' must be assigned a distinct append id",
                            iteration, threadCount, show)
                    .isEqualTo(threadCount);

            Map<String, String> appendIdByEventId = dcbEventStore().read(DcbCriteria.tags(tag(show))).events().stream()
                    .collect(Collectors.toMap(CloudEvent::getId, event -> extension(event, OccurrentCloudEventExtension.APPEND_ID)));

            for (Map.Entry<String, String> expected : results) {
                assertThat(appendIdByEventId.get(expected.getKey()))
                        .as("Iteration %d: event '%s' must carry the append id its own append returned, not another writer's",
                                iteration, expected.getKey())
                        .isEqualTo(expected.getValue());
            }
        }
    }

    /**
     * Races two conditioned appends and asserts that exactly one commits, and that the store rejected the other for
     * the documented reason rather than by leaking a storage-level error.
     * <p>
     * {@code committed} is read back rather than the whole store, because a fixture is created per test method and
     * not per iteration, so by iteration 2 the store still holds what iterations 0 and 1 committed. Each iteration
     * therefore tags its two events with something only it uses.
     */
    private void assertExactlyOneWinnerOfTwo(int iteration, String what, DcbCriteria committed,
                                             CloudEvent firstEvent, DcbAppendCondition firstCondition,
                                             CloudEvent secondEvent, DcbAppendCondition secondCondition) {
        List<Outcome<String>> outcomes = ConcurrentRendezvous.collide(2, index -> {
            CloudEvent event = index == 0 ? firstEvent : secondEvent;
            DcbAppendCondition condition = index == 0 ? firstCondition : secondCondition;
            return () -> {
                dcbEventStore().append(List.of(event), condition);
                return event.getId();
            };
        });

        String winner = assertExactlyOneWinner(iteration, outcomes, what);
        assertThat(winner)
                .as("Iteration %d: the winner must be one of the two events raced", iteration)
                .isIn(firstEvent.getId(), secondEvent.getId());
        assertThat(idsOf(dcbEventStore().read(committed).events()))
                .as("Iteration %d: only the winner's event may be committed, since both writers checked a boundary "
                        + "the other's event falls inside", iteration)
                .containsExactly(winner);
    }

    private String assertExactlyOneWinner(int iteration, List<Outcome<String>> outcomes, String what) {
        List<Throwable> failures = failuresOf(outcomes);

        assertThat(outcomes.size() - failures.size())
                .as("Iteration %d: %s must leave exactly one winner. Two winners means the store let a write-skew "
                        + "through, and no winner means it rejected an append it had no reason to", iteration, what)
                .isEqualTo(1);
        assertThat(failures)
                .as("Iteration %d: every loser of %s must be rejected by the append condition. A duplicate-key or "
                        + "write-conflict error reaching the caller is the store failing to retry its own storage "
                        + "collision", iteration, what)
                .allSatisfy(failure -> assertThat(failure).isInstanceOf(DcbAppendConditionNotFulfilledException.class));

        return outcomes.stream().filter(Outcome::succeeded).map(Outcome::value).findFirst().orElseThrow();
    }

    private static List<Throwable> failuresOf(List<Outcome<String>> outcomes) {
        return outcomes.stream().filter(outcome -> !outcome.succeeded()).map(Outcome::failure).toList();
    }
}
