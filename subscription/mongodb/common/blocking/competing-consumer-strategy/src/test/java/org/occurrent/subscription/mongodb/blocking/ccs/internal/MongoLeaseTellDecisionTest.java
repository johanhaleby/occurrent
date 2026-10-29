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

package org.occurrent.subscription.mongodb.blocking.ccs.internal;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.occurrent.subscription.mongodb.blocking.ccs.internal.MongoLeaseCompetingConsumerStrategySupport.Change;
import org.occurrent.subscription.mongodb.blocking.ccs.internal.MongoLeaseCompetingConsumerStrategySupport.Granted;
import org.occurrent.subscription.mongodb.blocking.ccs.internal.MongoLeaseCompetingConsumerStrategySupport.NotGranted;
import org.occurrent.subscription.mongodb.blocking.ccs.internal.MongoLeaseCompetingConsumerStrategySupport.Status;
import org.occurrent.subscription.mongodb.blocking.ccs.internal.MongoLeaseCompetingConsumerStrategySupport.Told;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;
import static org.occurrent.subscription.mongodb.blocking.ccs.internal.MongoLeaseCompetingConsumerStrategySupport.OWN_NOTHING;
import static org.occurrent.subscription.mongodb.blocking.ccs.internal.MongoLeaseCompetingConsumerStrategySupport.OWN_STILL_HELD;
import static org.occurrent.subscription.mongodb.blocking.ccs.internal.MongoLeaseCompetingConsumerStrategySupport.UNSEEN;
import static org.occurrent.subscription.mongodb.blocking.ccs.internal.MongoLeaseCompetingConsumerStrategySupport.isOwn;
import static org.occurrent.subscription.mongodb.blocking.ccs.internal.MongoLeaseCompetingConsumerStrategySupport.whatToTell;

/**
 * What a consumer's listeners are told is worked out from what they were last told, the lease status and two counts,
 * and whether a synchronous call may tell it on its own thread from what its locked step returned. Both are pure, so
 * every case is a row of plain inputs.
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@DisplayName("MongoLeaseTellDecision")
class MongoLeaseTellDecisionTest {

    private static final long LAPSES = 5;
    private static final long LOSSES = 2;
    private static final long TOKEN = 10;
    private static final long OTHER_TOKEN = 11;

    private static final Told NOT_GRANTED_AT_THE_LOSSES = new NotGranted(LOSSES);
    private static final Told NOT_GRANTED_BEFORE_A_LOSS = new NotGranted(LOSSES - 1);
    private static final Told GRANTED_AT_THE_LAPSES = new Granted(TOKEN, LAPSES);
    private static final Told GRANTED_BEFORE_A_LAPSE = new Granted(TOKEN, LAPSES - 1);
    private static final Told GRANTED_AND_BEING_TOLD = new Granted(TOKEN, UNSEEN);

    private static final Status HELD = Status.lockAcquired(TOKEN, 0);
    private static final Status HELD_UNDER_ANOTHER_TOKEN = Status.lockAcquired(OTHER_TOKEN, 0);

    @Nested
    @DisplayName("what is told when the lease is not held")
    class What_is_told_when_the_lease_is_not_held {

        @ParameterizedTest
        @MethodSource("rows")
        @DisplayName("tells what the record and the status ask for")
        void tells_what_the_record_and_the_status_ask_for(Told told, @Nullable Status status, Change expected) {
            // When
            Change change = whatToTell(told, status, LAPSES, LOSSES, true);

            // Then
            assertThat(change).isEqualTo(expected);
        }

        static Stream<Arguments> rows() {
            return Stream.of(
                    row("not granted at the losses, no longer registered", NOT_GRANTED_AT_THE_LOSSES, null, Change.NOTHING),
                    row("not granted at the losses, LOCK_NOT_ACQUIRED", NOT_GRANTED_AT_THE_LOSSES, Status.LOCK_NOT_ACQUIRED, Change.NOTHING),
                    row("not granted at the losses, LOCK_RELEASED", NOT_GRANTED_AT_THE_LOSSES, Status.LOCK_RELEASED, Change.NOTHING),
                    row("not granted at the losses, UNREGISTERING", NOT_GRANTED_AT_THE_LOSSES, Status.UNREGISTERING, Change.NOTHING),

                    row("not granted before a loss, no longer registered", NOT_GRANTED_BEFORE_A_LOSS, null, Change.PROHIBIT),
                    row("not granted before a loss, LOCK_NOT_ACQUIRED", NOT_GRANTED_BEFORE_A_LOSS, Status.LOCK_NOT_ACQUIRED, Change.PROHIBIT),
                    row("not granted before a loss, LOCK_RELEASED", NOT_GRANTED_BEFORE_A_LOSS, Status.LOCK_RELEASED, Change.PROHIBIT),
                    row("not granted before a loss, UNREGISTERING", NOT_GRANTED_BEFORE_A_LOSS, Status.UNREGISTERING, Change.PROHIBIT),

                    row("granted at the lapses, no longer registered", GRANTED_AT_THE_LAPSES, null, Change.PROHIBIT),
                    row("granted at the lapses, LOCK_NOT_ACQUIRED", GRANTED_AT_THE_LAPSES, Status.LOCK_NOT_ACQUIRED, Change.PROHIBIT),
                    row("granted at the lapses, LOCK_RELEASED", GRANTED_AT_THE_LAPSES, Status.LOCK_RELEASED, Change.PROHIBIT),
                    row("granted at the lapses, UNREGISTERING", GRANTED_AT_THE_LAPSES, Status.UNREGISTERING, Change.PROHIBIT),

                    row("granted before a lapse, no longer registered", GRANTED_BEFORE_A_LAPSE, null, Change.PROHIBIT),
                    row("granted before a lapse, LOCK_NOT_ACQUIRED", GRANTED_BEFORE_A_LAPSE, Status.LOCK_NOT_ACQUIRED, Change.PROHIBIT),
                    row("granted before a lapse, LOCK_RELEASED", GRANTED_BEFORE_A_LAPSE, Status.LOCK_RELEASED, Change.PROHIBIT),
                    row("granted before a lapse, UNREGISTERING", GRANTED_BEFORE_A_LAPSE, Status.UNREGISTERING, Change.PROHIBIT),

                    row("a grant being told, no longer registered", GRANTED_AND_BEING_TOLD, null, Change.PROHIBIT),
                    row("a grant being told, LOCK_NOT_ACQUIRED", GRANTED_AND_BEING_TOLD, Status.LOCK_NOT_ACQUIRED, Change.PROHIBIT),
                    row("a grant being told, LOCK_RELEASED", GRANTED_AND_BEING_TOLD, Status.LOCK_RELEASED, Change.PROHIBIT),
                    row("a grant being told, UNREGISTERING", GRANTED_AND_BEING_TOLD, Status.UNREGISTERING, Change.PROHIBIT)
            );
        }
    }

    @Nested
    @DisplayName("what is told when the lease is held and cannot have expired")
    class What_is_told_when_the_lease_is_held_and_cannot_have_expired {

        @ParameterizedTest
        @MethodSource("rows")
        @DisplayName("tells what the record and the status ask for")
        void tells_what_the_record_and_the_status_ask_for(Told told, Status status, Change expected) {
            // When
            Change change = whatToTell(told, status, LAPSES, LOSSES, true);

            // Then
            assertThat(change).isEqualTo(expected);
        }

        static Stream<Arguments> rows() {
            return Stream.of(
                    row("not granted at the losses", NOT_GRANTED_AT_THE_LOSSES, HELD, Change.GRANT),
                    row("not granted before a loss", NOT_GRANTED_BEFORE_A_LOSS, HELD, Change.PROHIBIT),

                    row("granted at the lapses, same token", GRANTED_AT_THE_LAPSES, HELD, Change.NOTHING),
                    row("granted before a lapse, same token", GRANTED_BEFORE_A_LAPSE, HELD, Change.GRANT),
                    row("a grant being told, same token", GRANTED_AND_BEING_TOLD, HELD, Change.GRANT),

                    row("granted at the lapses, another token", GRANTED_AT_THE_LAPSES, HELD_UNDER_ANOTHER_TOKEN, Change.PROHIBIT),
                    row("granted before a lapse, another token", GRANTED_BEFORE_A_LAPSE, HELD_UNDER_ANOTHER_TOKEN, Change.PROHIBIT),
                    row("a grant being told, another token", GRANTED_AND_BEING_TOLD, HELD_UNDER_ANOTHER_TOKEN, Change.PROHIBIT),

                    row("not granted at the losses, another token than any told", NOT_GRANTED_AT_THE_LOSSES, HELD_UNDER_ANOTHER_TOKEN, Change.GRANT),
                    row("not granted before a loss, another token than any told", NOT_GRANTED_BEFORE_A_LOSS, HELD_UNDER_ANOTHER_TOKEN, Change.PROHIBIT),

                    row("a prohibition of the first token being told, the lease now held under a second token",
                            new Granted(TOKEN, UNSEEN), HELD_UNDER_ANOTHER_TOKEN, Change.PROHIBIT)
            );
        }
    }

    @Nested
    @DisplayName("what is told when the lease is held but may have expired")
    class What_is_told_when_the_lease_is_held_but_may_have_expired {

        @ParameterizedTest
        @MethodSource("rows")
        @DisplayName("tells what the record and the status ask for")
        void tells_what_the_record_and_the_status_ask_for(Told told, Status status, Change expected) {
            // When
            Change change = whatToTell(told, status, LAPSES, LOSSES, false);

            // Then
            assertThat(change).isEqualTo(expected);
        }

        @ParameterizedTest
        @MethodSource("recordsOfTheTokenHeld")
        @DisplayName("never asks for a prohibition of the token held")
        void never_asks_for_a_prohibition_of_the_token_held(Told told) {
            // When
            Change change = whatToTell(told, HELD, LAPSES, LOSSES, false);

            // Then
            assertThat(change).isNotEqualTo(Change.PROHIBIT);
        }

        static Stream<Arguments> rows() {
            return Stream.of(
                    row("not granted at the losses", NOT_GRANTED_AT_THE_LOSSES, HELD, Change.NOTHING),
                    row("not granted before a loss", NOT_GRANTED_BEFORE_A_LOSS, HELD, Change.PROHIBIT),

                    row("granted at the lapses, same token", GRANTED_AT_THE_LAPSES, HELD, Change.NOTHING),
                    row("granted before a lapse, same token", GRANTED_BEFORE_A_LAPSE, HELD, Change.NOTHING),
                    row("a grant being told, same token", GRANTED_AND_BEING_TOLD, HELD, Change.NOTHING),

                    row("granted at the lapses, another token", GRANTED_AT_THE_LAPSES, HELD_UNDER_ANOTHER_TOKEN, Change.PROHIBIT),
                    row("granted before a lapse, another token", GRANTED_BEFORE_A_LAPSE, HELD_UNDER_ANOTHER_TOKEN, Change.PROHIBIT),
                    row("a grant being told, another token", GRANTED_AND_BEING_TOLD, HELD_UNDER_ANOTHER_TOKEN, Change.PROHIBIT),

                    row("not granted at the losses, another token than any told", NOT_GRANTED_AT_THE_LOSSES, HELD_UNDER_ANOTHER_TOKEN, Change.NOTHING),
                    row("not granted before a loss, another token than any told", NOT_GRANTED_BEFORE_A_LOSS, HELD_UNDER_ANOTHER_TOKEN, Change.PROHIBIT)
            );
        }

        static Stream<Arguments> recordsOfTheTokenHeld() {
            return Stream.of(
                    argumentSet("granted at the lapses", GRANTED_AT_THE_LAPSES),
                    argumentSet("granted before a lapse", GRANTED_BEFORE_A_LAPSE),
                    argumentSet("a grant being told", GRANTED_AND_BEING_TOLD),
                    argumentSet("not granted at the losses", NOT_GRANTED_AT_THE_LOSSES)
            );
        }
    }

    @Nested
    @DisplayName("whether a change is the one the locked step of a call made")
    class Whether_a_change_is_the_one_the_locked_step_of_a_call_made {

        @ParameterizedTest
        @MethodSource("rows")
        @DisplayName("is the call's own only when the count its step left has not moved")
        void is_the_calls_own_only_when_the_count_its_step_left_has_not_moved(Change change, long own, long lapsesNow, long lossesNow, boolean expected) {
            // When
            boolean isOwn = isOwn(change, own, lapsesNow, lossesNow);

            // Then
            assertThat(isOwn).isEqualTo(expected);
        }

        @ParameterizedTest
        @MethodSource("nothingOwnedAgainstEveryChange")
        @DisplayName("is never the call's own when its step left nothing of its own")
        void is_never_the_calls_own_when_its_step_left_nothing_of_its_own(Change change, long own, long lapsesNow, long lossesNow) {
            // When
            boolean isOwn = isOwn(change, own, lapsesNow, lossesNow);

            // Then
            assertThat(isOwn).isFalse();
        }

        // A grant's own is the lapse count the step left, a prohibition's is -3 - losses
        static Stream<Arguments> rows() {
            long grantOwn = 7;
            long prohibitionOwn = -3 - LOSSES;
            return Stream.of(
                    argumentSet("a grant with the lapses unmoved since the step", Change.GRANT, grantOwn, 7L, LOSSES, true),
                    argumentSet("a grant with the lapses unmoved, however many leases were lost", Change.GRANT, grantOwn, 7L, 99L, true),
                    argumentSet("a grant after an expired answer between the step and the read", Change.GRANT, grantOwn, 8L, LOSSES, false),
                    argumentSet("a grant after the lease was lost between the step and the read", Change.GRANT, grantOwn, 8L, LOSSES + 1, false),

                    argumentSet("a round's commit lands before the counts are read, the prohibition owed is not the call's grant", Change.PROHIBIT, grantOwn, 8L, LOSSES + 1, false),
                    argumentSet("a round's commit lands between the counts and the status, nothing is due", Change.NOTHING, grantOwn, 8L, LOSSES + 1, false),
                    argumentSet("a round's commit lands after the status and after the second read of the lapses, the grant is the call's", Change.GRANT, grantOwn, 7L, LOSSES, true),
                    argumentSet("a round's commit lands after the status and before the second read of the lapses, the grant is handed off", Change.GRANT, grantOwn, 8L, LOSSES + 1, false),

                    argumentSet("a grant told by the notifier, then an expired answer and a regrant of the same shape", Change.GRANT, grantOwn, 9L, LOSSES, false),
                    argumentSet("a prohibition with the losses unmoved since the step", Change.PROHIBIT, prohibitionOwn, LAPSES, LOSSES, true),
                    argumentSet("a prohibition with the losses unmoved, whatever the lapses did", Change.PROHIBIT, prohibitionOwn, 99L, LOSSES, true),
                    argumentSet("a prohibition merged with one owed from before, the first loss", Change.PROHIBIT, -4L, LAPSES, 1L, true),
                    argumentSet("a prohibition after a later loss, the same shape as the own one", Change.PROHIBIT, prohibitionOwn, LAPSES, LOSSES + 1, false),
                    argumentSet("a prohibition after a stand-down, a new grant and a lost commit", Change.PROHIBIT, prohibitionOwn, LAPSES + 2, LOSSES + 1, false),
                    argumentSet("a prohibition after a loss between the counts and the status read", Change.PROHIBIT, prohibitionOwn, LAPSES + 1, LOSSES + 1, false),

                    argumentSet("a grant never matches a prohibition's own", Change.GRANT, prohibitionOwn, LAPSES, LOSSES, false),
                    argumentSet("a prohibition never matches a grant's own", Change.PROHIBIT, grantOwn, LAPSES, LOSSES, false),
                    argumentSet("nothing due is nothing of the call's own", Change.NOTHING, grantOwn, 7L, LOSSES, false),
                    argumentSet("nothing due is nothing of the prohibition's own", Change.NOTHING, prohibitionOwn, LAPSES, LOSSES, false)
            );
        }

        static Stream<Arguments> nothingOwnedAgainstEveryChange() {
            long[][] counts = {{0, 0}, {1, 1}, {LAPSES, LOSSES}, {2, 3}};
            Stream.Builder<Arguments> rows = Stream.builder();
            for (Change change : Change.values()) {
                for (long own : new long[]{OWN_NOTHING, OWN_STILL_HELD}) {
                    for (long[] count : counts) {
                        String ownName = own == OWN_NOTHING ? "OWN_NOTHING" : "OWN_STILL_HELD";
                        rows.add(argumentSet(change + " against " + ownName + " with lapses " + count[0] + " and losses " + count[1],
                                change, own, count[0], count[1]));
                    }
                }
            }
            return rows.build();
        }
    }

    private static Arguments row(String name, Told told, @Nullable Status status, Change expected) {
        return argumentSet(name, told, status, expected);
    }
}
