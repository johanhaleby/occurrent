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

package org.occurrent.subscription.blocking.durable;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.blocking.durable.CheckpointRegistration.Delivery;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel.Read;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * When a registration allows a quiet position to be saved, and what it asks the storage to do before it does, with no
 * model around it. A test plays the storage by holding what is stored, and says what each write and read does.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CheckpointRegistrationTest {

    private enum Write {
        RETURNS, WRITES_THEN_THROWS, THROWS_WITHOUT_WRITING, REFUSED
    }

    private final CheckpointRegistration registration = new CheckpointRegistration();
    // What the registration asked the storage to do, in order
    private final List<String> asked = new ArrayList<>();
    private @Nullable String stored;
    private boolean readFails;

    @Nested
    @DisplayName("deliveries")
    class Deliveries {

        @Test
        void no_quiet_tick_is_allowed_until_a_position_is_stored_or_an_event_delivered() {
            // Given
            boolean beforeAnyPositionIsStored = registration.quietTickAllowed();

            // When
            registration.startPositionStored();

            // Then
            assertThat(beforeAnyPositionIsStored).as("allowed before a position is stored").isFalse();
            assertThat(registration.quietTickAllowed()).as("allowed once a position is stored").isTrue();
        }

        @Test
        void a_quiet_tick_is_allowed_only_once_a_delivery_that_stored_its_checkpoint_has_ended() {
            // Given
            Delivery delivery = registration.delivering();

            // When
            boolean whileItWaits = registration.quietTickAllowed();
            save(delivery, "e1", Write.RETURNS);
            boolean afterItsSave = registration.quietTickAllowed();
            end(delivery, "e1");

            // Then
            assertThat(whileItWaits).as("allowed while the delivery waits").isFalse();
            assertThat(afterItsSave).as("allowed after its save, before it ended").isFalse();
            assertThat(registration.quietTickAllowed()).as("allowed once it ended").isTrue();
        }

        @Test
        void a_delivery_that_stored_no_checkpoint_keeps_quiet_ticks_off_until_a_later_delivery_stores_its_checkpoint() {
            // Given an event stored and one the persist predicate declined, or whose action threw
            deliverStored("e1");
            Delivery declined = registration.delivering();
            end(declined, null);
            boolean afterTheDeclined = registration.quietTickAllowed();
            asked.clear();
            quietTick("q1", Write.RETURNS);

            // When a later event stores its checkpoint
            deliverStored("e3");
            quietTick("q2", Write.RETURNS);

            // Then
            assertThat(afterTheDeclined).as("allowed after the declined delivery").isFalse();
            assertThat(asked).containsExactly("tick off", "save e3", "save q2");
            assertThat(stored).isEqualTo("q2");
        }

        @Test
        void a_delivery_save_that_wrote_and_threw_has_the_next_quiet_tick_read_what_is_stored_and_save_the_quiet_position() {
            // Given
            deliverStored("e1");
            asked.clear();
            Delivery delivery = registration.delivering();
            save(delivery, "e2", Write.WRITES_THEN_THROWS);
            end(delivery, "e2");

            // When
            boolean allowed = registration.quietTickAllowed();
            quietTick("q1", Write.RETURNS);

            // Then
            assertThat(allowed).as("allowed with what is stored unknown").isTrue();
            assertThat(asked).containsExactly("save e2", "read", "save q1");
            assertThat(stored).isEqualTo("q1");
        }

        @Test
        void a_delivery_save_that_threw_without_writing_keeps_quiet_ticks_off_after_a_read_until_a_later_delivery_stores_its_checkpoint() {
            // Given
            deliverStored("e1");
            asked.clear();
            Delivery delivery = registration.delivering();
            save(delivery, "e2", Write.THROWS_WITHOUT_WRITING);
            end(delivery, "e2");

            // When
            quietTick("q1", Write.RETURNS);
            boolean allowedAfterTheRead = registration.quietTickAllowed();
            deliverStored("e3");
            quietTick("q2", Write.RETURNS);

            // Then
            assertThat(allowedAfterTheRead).as("allowed after the read found e1, which does not cover e2").isFalse();
            assertThat(asked).containsExactly("save e2", "read", "save e3", "save q2");
            assertThat(stored).isEqualTo("q2");
        }

        @Test
        void a_delivery_save_the_storage_refused_keeps_quiet_ticks_off_after_a_read_until_a_later_delivery_stores_its_checkpoint() {
            // Given
            deliverStored("e1");
            asked.clear();
            Delivery delivery = registration.delivering();
            save(delivery, "e2", Write.REFUSED);
            end(delivery, "e2");

            // When
            quietTick("q1", Write.RETURNS);
            boolean allowedAfterTheRead = registration.quietTickAllowed();

            // Then
            assertThat(allowedAfterTheRead).as("allowed after the read found e1, which does not cover e2").isFalse();
            assertThat(asked).containsExactly("save e2", "read");
            assertThat(stored).isEqualTo("e1");
        }
    }

    @Nested
    @DisplayName("quiet ticks")
    class QuietTicks {

        @Test
        void a_quiet_position_that_was_written_covers_the_next_tick_without_a_read() {
            // Given
            deliverStored("e1");
            asked.clear();

            // When
            quietTick("q1", Write.RETURNS);
            quietTick("q2", Write.RETURNS);

            // Then
            assertThat(asked).containsExactly("save q1", "save q2");
            assertThat(stored).isEqualTo("q2");
        }

        @Test
        void a_quiet_save_that_wrote_and_threw_is_found_stored_by_a_read_and_the_next_quiet_position_is_saved() {
            // Given
            deliverStored("e1");
            asked.clear();

            // When
            quietTick("q1", Write.WRITES_THEN_THROWS);
            quietTick("q2", Write.RETURNS);

            // Then
            assertThat(asked).containsExactly("save q1", "read", "save q2");
            assertThat(stored).isEqualTo("q2");
        }

        @Test
        void a_quiet_save_that_threw_without_writing_is_followed_by_a_read_that_finds_the_delivery_and_the_next_quiet_position_is_saved() {
            // Given
            deliverStored("e1");
            asked.clear();

            // When
            quietTick("q1", Write.THROWS_WITHOUT_WRITING);
            quietTick("q2", Write.RETURNS);

            // Then
            assertThat(asked).containsExactly("save q1", "read", "save q2");
            assertThat(stored).isEqualTo("q2");
        }

        @Test
        void a_quiet_save_the_storage_refused_is_followed_by_a_read_before_the_next_quiet_position_is_saved() {
            // Given
            deliverStored("e1");
            asked.clear();

            // When
            quietTick("q1", Write.REFUSED);
            quietTick("q2", Write.RETURNS);

            // Then
            assertThat(asked).containsExactly("save q1", "refused", "read", "save q2");
            assertThat(stored).isEqualTo("q2");
        }

        @Test
        void a_quiet_save_that_wrote_and_threw_followed_by_one_that_threw_without_writing_is_recovered_by_a_read() {
            // Given
            deliverStored("e1");
            asked.clear();

            // When
            quietTick("q1", Write.WRITES_THEN_THROWS);
            quietTick("q2", Write.THROWS_WITHOUT_WRITING);
            quietTick("q3", Write.RETURNS);

            // Then
            assertThat(asked).containsExactly("save q1", "read", "save q2", "read", "save q3");
            assertThat(stored).isEqualTo("q3");
        }

        @Test
        void a_quiet_save_that_threw_without_writing_followed_by_one_that_wrote_and_threw_is_recovered_by_a_read() {
            // Given
            deliverStored("e1");
            asked.clear();

            // When
            quietTick("q1", Write.THROWS_WITHOUT_WRITING);
            quietTick("q2", Write.WRITES_THEN_THROWS);
            quietTick("q3", Write.RETURNS);

            // Then
            assertThat(asked).containsExactly("save q1", "read", "save q2", "read", "save q3");
            assertThat(stored).isEqualTo("q3");
        }

        @Test
        void many_quiet_saves_that_fail_in_a_row_leave_at_most_two_positions_remembered() {
            // Given
            deliverStored("e1");
            asked.clear();

            // When
            List<Integer> remembered = new ArrayList<>();
            for (int i = 0; i < 40; i++) {
                quietTick("q" + i, i % 2 == 0 ? Write.WRITES_THEN_THROWS : Write.THROWS_WITHOUT_WRITING);
                remembered.add(registration.positionsRemembered());
            }
            quietTick("recovered", Write.RETURNS);

            // Then
            assertThat(remembered).as("positions remembered after each failed quiet save").allMatch(count -> count <= 2);
            assertThat(asked).as("one save, then a read and a save for each of the 39 failing ticks after it and for the last one").hasSize(1 + 2 * 39 + 2);
            assertThat(stored).isEqualTo("recovered");
        }

        @Test
        void a_read_that_throws_after_a_throwing_write_keeps_the_quiet_position_unsaved_and_the_next_tick_reads_again() {
            // Given a quiet save that wrote and threw
            deliverStored("e1");
            asked.clear();
            quietTick("q1", Write.WRITES_THEN_THROWS);

            // When the read that follows throws twice, and then succeeds
            readFails = true;
            quietTick("q2", Write.RETURNS);
            boolean allowedAfterTheFirstFailedRead = registration.quietTickAllowed();
            quietTick("q3", Write.RETURNS);
            readFails = false;
            quietTick("q4", Write.RETURNS);

            // Then
            assertThat(allowedAfterTheFirstFailedRead).as("allowed, so the next tick reads again").isTrue();
            assertThat(asked).containsExactly("save q1", "read", "read", "read", "save q4");
            assertThat(stored).isEqualTo("q4");
        }

        @Test
        void a_quiet_save_is_not_asked_for_while_a_delivery_is_under_way() {
            // Given
            deliverStored("e1");
            Read read = registration.reading();
            Delivery delivery = registration.delivering();
            asked.clear();

            // When
            registration.saveQuietPositionIfStoredCovers(read.repositions(), at("q1"), () -> {
                asked.add("save q1");
                return true;
            }, () -> {
                asked.add("read");
                return null;
            });
            end(delivery, "e1");

            // Then
            assertThat(asked).isEmpty();
            assertThat(stored).isEqualTo("e1");
        }

        @Test
        void a_quiet_save_is_not_asked_for_when_a_resume_at_a_position_has_stored_its_own_since_the_tick_began() {
            // Given
            deliverStored("p");
            Read read = registration.reading();
            resumeAt("x", "p", Write.RETURNS);
            asked.clear();

            // When
            registration.saveQuietPositionIfStoredCovers(read.repositions(), at("q1"), () -> {
                asked.add("save q1");
                return true;
            }, () -> {
                asked.add("read");
                return null;
            });

            // Then
            assertThat(asked).isEmpty();
            assertThat(stored).isEqualTo("x");
        }
    }

    @Nested
    @DisplayName("resumes at a position and their put-backs")
    class ResumesAtAPosition {

        @Test
        void a_resume_write_that_returned_covers_a_delivery_that_read_before_it_without_a_read() {
            // Given
            deliverStored("p");
            asked.clear();

            // When
            resumeAt("x", "p", Write.RETURNS);
            quietTick("q1", Write.RETURNS);

            // Then
            assertThat(asked).containsExactly("save q1");
            assertThat(stored).isEqualTo("q1");
        }

        @Test
        void a_resume_write_that_wrote_and_threw_is_found_stored_by_a_read_and_covers_a_delivery_that_read_before_it() {
            // Given
            deliverStored("p");
            asked.clear();

            // When
            resumeAt("x", "p", Write.WRITES_THEN_THROWS);
            quietTick("q1", Write.RETURNS);

            // Then
            assertThat(asked).containsExactly("read", "save q1");
            assertThat(stored).isEqualTo("q1");
        }

        @Test
        void a_resume_write_that_threw_without_writing_is_followed_by_a_read_that_finds_the_delivery_covering() {
            // Given
            deliverStored("p");
            asked.clear();

            // When
            resumeAt("x", "p", Write.THROWS_WITHOUT_WRITING);
            quietTick("q1", Write.RETURNS);

            // Then
            assertThat(asked).containsExactly("read", "save q1");
            assertThat(stored).isEqualTo("q1");
        }

        @Test
        void a_delivery_that_read_before_a_resume_at_a_position_is_refused_and_the_position_of_the_resume_stays_stored() {
            // Given
            Delivery delivery = registration.delivering();
            resumeAt("x", null, Write.RETURNS);
            asked.clear();

            // When
            save(delivery, "e1", Write.RETURNS);
            end(delivery, "e1");

            // Then
            assertThat(asked).isEmpty();
            assertThat(stored).isEqualTo("x");
            assertThat(registration.quietTickAllowed()).as("allowed, since the position of the resume covers the delivery").isTrue();
        }

        @Test
        void a_put_back_that_wrote_the_checkpoint_the_delivery_stored_leaves_quiet_ticks_allowed_once_the_delivery_has_ended() {
            // Given a delivery of the resumed run that saved the checkpoint stored before the resume, and a put-back
            // that wrote it again before the delivery ended
            Delivery delivery = aDeliveryOfTheEventStoredBeforeAResumeThatFailsToStart("p");
            putBack("p", Write.RETURNS);
            boolean whileItIsUnderWay = registration.quietTickAllowed();

            // When
            end(delivery, "p");
            asked.clear();
            quietTick("q1", Write.RETURNS);

            // Then
            assertThat(whileItIsUnderWay).as("allowed while the delivery is under way").isFalse();
            assertThat(asked).containsExactly("save q1");
            assertThat(stored).isEqualTo("q1");
        }

        @Test
        void a_put_back_that_wrote_and_threw_leaves_quiet_ticks_allowed_once_the_delivery_has_ended_and_a_read_found_its_checkpoint() {
            // Given
            Delivery delivery = aDeliveryOfTheEventStoredBeforeAResumeThatFailsToStart("p");
            putBack("p", Write.WRITES_THEN_THROWS);

            // When
            end(delivery, "p");
            asked.clear();
            quietTick("q1", Write.RETURNS);

            // Then
            assertThat(asked).containsExactly("save q1");
            assertThat(stored).isEqualTo("q1");
        }

        @Test
        void a_put_back_that_threw_without_writing_leaves_quiet_ticks_allowed_once_the_delivery_has_ended_and_a_read_found_its_checkpoint() {
            // Given
            Delivery delivery = aDeliveryOfTheEventStoredBeforeAResumeThatFailsToStart("p");
            putBack("p", Write.THROWS_WITHOUT_WRITING);

            // When
            end(delivery, "p");
            asked.clear();
            quietTick("q1", Write.RETURNS);

            // Then
            assertThat(asked).containsExactly("save q1");
            assertThat(stored).isEqualTo("q1");
        }

        @Test
        void a_put_back_that_threw_and_whose_read_threw_too_has_the_next_tick_read_before_it_saves_the_quiet_position() {
            // Given
            Delivery delivery = aDeliveryOfTheEventStoredBeforeAResumeThatFailsToStart("p");
            readFails = true;
            putBack("p", Write.THROWS_WITHOUT_WRITING);
            readFails = false;
            end(delivery, "p");
            asked.clear();

            // When
            boolean allowed = registration.quietTickAllowed();
            quietTick("q1", Write.RETURNS);

            // Then
            assertThat(allowed).as("allowed with what is stored unknown").isTrue();
            assertThat(asked).containsExactly("read", "save q1");
            assertThat(stored).isEqualTo("q1");
        }

        @Test
        void a_put_back_that_wrote_a_checkpoint_the_last_delivery_did_not_store_keeps_quiet_ticks_off_until_a_delivery_stores_its_checkpoint() {
            // Given a delivery of the resumed run that stored its own checkpoint, and a put-back that wrote the one
            // stored before the resume over it
            deliverStored("p");
            resumeAt("x", "p", Write.RETURNS);
            registration.reading();
            Delivery delivery = registration.delivering();
            save(delivery, "e2", Write.RETURNS);
            putBack("p", Write.RETURNS);
            end(delivery, "e2");
            asked.clear();

            // When
            quietTick("q1", Write.RETURNS);
            deliverStored("e3");
            quietTick("q2", Write.RETURNS);

            // Then
            assertThat(asked).containsExactly("tick off", "save e3", "save q2");
            assertThat(stored).isEqualTo("q2");
        }

        // The event stored before a resume at an earlier position, delivered again by the resumed run, whose
        // checkpoint it saved after the position of the resume was stored. Answers the delivery, which has not ended
        private Delivery aDeliveryOfTheEventStoredBeforeAResumeThatFailsToStart(String event) {
            deliverStored(event);
            resumeAt("x", event, Write.RETURNS);
            registration.reading();
            Delivery delivery = registration.delivering();
            save(delivery, event, Write.RETURNS);
            return delivery;
        }
    }

    @Nested
    @DisplayName("runs a resume superseded")
    class SupersededRuns {

        @Test
        void the_save_of_a_closed_run_is_refused_once_a_delivery_of_the_run_that_resumed_has_saved() {
            // Given a closed run whose delivery started before the resumed run read, and a resumed run that stored e2
            Delivery closedRun = registration.delivering();
            registration.reading();
            Delivery resumedRun = registration.delivering();
            save(resumedRun, "e2", Write.RETURNS);
            end(resumedRun, "e2");
            asked.clear();

            // When the closed run's action returns and it tries to store e1
            save(closedRun, "e1", Write.RETURNS);
            end(closedRun, "e1");
            quietTick("q1", Write.RETURNS);

            // Then
            assertThat(asked).containsExactly("save q1");
            assertThat(stored).isEqualTo("q1");
        }

        @Test
        void the_stored_checkpoint_stays_the_one_of_the_run_that_resumed_when_the_closed_run_tries_to_store_an_earlier_event() {
            // Given
            Delivery closedRun = registration.delivering();
            registration.reading();
            Delivery resumedRun = registration.delivering();
            save(resumedRun, "e2", Write.RETURNS);
            end(resumedRun, "e2");

            // When
            save(closedRun, "e1", Write.RETURNS);

            // Then
            assertThat(asked).containsExactly("save e2");
            assertThat(stored).isEqualTo("e2");
        }

        @Test
        void a_delivery_that_started_earlier_in_the_same_read_is_refused_once_a_later_one_has_saved() {
            // Given
            Delivery earlier = registration.delivering();
            Delivery later = registration.delivering();
            save(later, "e2", Write.RETURNS);
            end(later, "e2");

            // When
            save(earlier, "e1", Write.RETURNS);
            end(earlier, "e1");

            // Then
            assertThat(asked).containsExactly("save e2");
            assertThat(stored).isEqualTo("e2");
            assertThat(registration.quietTickAllowed()).as("allowed, since e2 is what the last current delivery stored").isTrue();
        }

        @Test
        void deliveries_in_order_in_one_read_and_in_later_reads_are_all_saved() {
            // Given
            registration.reading();
            Delivery first = registration.delivering();
            Delivery second = registration.delivering();

            // When
            save(first, "e1", Write.RETURNS);
            end(first, "e1");
            save(second, "e2", Write.RETURNS);
            end(second, "e2");
            registration.reading();
            Delivery third = registration.delivering();
            save(third, "e3", Write.RETURNS);
            end(third, "e3");

            // Then
            assertThat(asked).containsExactly("save e1", "save e2", "save e3");
            assertThat(stored).isEqualTo("e3");
        }
    }

    @Nested
    @DisplayName("restarts after a lost history")
    class RestartsAfterAHistoryLoss {

        @Test
        void a_restart_position_stored_after_a_quiet_save_that_threw_without_writing_has_the_quiet_ticks_after_it_save_without_a_read() {
            // Given
            deliverStored("e1");
            quietTick("q1", Write.THROWS_WITHOUT_WRITING);
            registration.reading();
            restartPosition("r1", Write.RETURNS);
            asked.clear();

            // When
            boolean allowed = registration.quietTickAllowed();
            quietTick("q2", Write.RETURNS);
            quietTick("q3", Write.RETURNS);

            // Then
            assertThat(allowed).as("allowed with the restart position stored").isTrue();
            assertThat(asked).containsExactly("save q2", "save q3");
            assertThat(stored).isEqualTo("q3");
        }

        @Test
        void a_restart_position_stored_after_a_quiet_save_that_wrote_and_threw_has_the_quiet_ticks_after_it_save_without_a_read() {
            // Given
            deliverStored("e1");
            quietTick("q1", Write.WRITES_THEN_THROWS);
            registration.reading();
            restartPosition("r1", Write.RETURNS);
            asked.clear();

            // When
            quietTick("q2", Write.RETURNS);
            quietTick("q3", Write.RETURNS);

            // Then
            assertThat(asked).containsExactly("save q2", "save q3");
            assertThat(stored).isEqualTo("q3");
        }

        @Test
        void a_restart_position_that_was_written_and_threw_is_found_stored_by_a_read_and_the_quiet_position_is_saved_in_that_attempt() {
            // Given
            deliverStored("e1");
            quietTick("q1", Write.THROWS_WITHOUT_WRITING);
            registration.reading();
            restartPosition("r1", Write.WRITES_THEN_THROWS);
            asked.clear();

            // When
            quietTick("q2", Write.RETURNS);

            // Then
            assertThat(asked).containsExactly("read", "save q2");
            assertThat(stored).isEqualTo("q2");
        }

        @Test
        void a_restart_position_that_threw_without_writing_is_followed_by_a_read_that_finds_the_delivery_and_the_quiet_position_is_saved_in_that_attempt() {
            // Given
            deliverStored("e1");
            quietTick("q1", Write.THROWS_WITHOUT_WRITING);
            registration.reading();
            restartPosition("r1", Write.THROWS_WITHOUT_WRITING);
            asked.clear();

            // When
            quietTick("q2", Write.RETURNS);

            // Then
            assertThat(asked).containsExactly("read", "save q2");
            assertThat(stored).isEqualTo("q2");
        }

        @Test
        void a_restart_position_that_threw_over_a_quiet_save_that_wrote_keeps_quiet_ticks_off_when_the_read_finds_that_quiet_position() {
            // Given a quiet save that wrote and threw, and a restart write that threw without writing
            deliverStored("e1");
            quietTick("q1", Write.WRITES_THEN_THROWS);
            registration.reading();
            restartPosition("r1", Write.THROWS_WITHOUT_WRITING);
            asked.clear();

            // When
            quietTick("q2", Write.RETURNS);
            boolean allowedAfterTheRead = registration.quietTickAllowed();

            // Then q1 is stored but the one slot for a position that threw holds r1. Off is correct, since it only
            // postpones quiet saves until a delivery stores its checkpoint and no quiet save is written that
            // the registration cannot tell covers the delivery. No event is lost by a quiet save that waits
            assertThat(asked).containsExactly("read");
            assertThat(allowedAfterTheRead).as("allowed after the read found q1, which the registration no longer remembers").isFalse();
            assertThat(stored).isEqualTo("q1");
        }

        @Test
        void a_restart_position_stored_after_a_delivery_started_stops_that_delivery_from_saving_and_the_tick_is_allowed_once_it_ends() {
            // Given a delivery of a run that read before the run that restarts
            registration.reading();
            Delivery stale = registration.delivering();
            registration.reading();
            restartPosition("r1", Write.RETURNS);
            boolean whileItWaits = registration.quietTickAllowed();
            asked.clear();

            // When
            save(stale, "e5", Write.RETURNS);
            List<String> askedBySave = new ArrayList<>(asked);
            end(stale, "e5");
            boolean afterItEnded = registration.quietTickAllowed();
            quietTick("q1", Write.RETURNS);

            // Then
            assertThat(askedBySave).as("asked by the save of the delivery started before the restart").isEmpty();
            assertThat(whileItWaits).as("allowed while the delivery waits").isFalse();
            assertThat(afterItEnded).as("allowed once it ended with the restart position stored").isTrue();
            assertThat(asked).containsExactly("save q1");
            assertThat(stored).isEqualTo("q1");
        }

        @Test
        void a_restart_position_stored_after_a_delivery_saved_and_ended_has_the_tick_allowed_with_the_restart_position_stored() {
            // Given
            registration.reading();
            Delivery earlier = registration.delivering();
            save(earlier, "e5", Write.RETURNS);
            end(earlier, "e5");
            registration.reading();

            // When
            restartPosition("r1", Write.RETURNS);
            boolean allowed = registration.quietTickAllowed();
            asked.clear();
            quietTick("q1", Write.RETURNS);

            // Then
            assertThat(allowed).as("allowed with r1 stored over e5").isTrue();
            assertThat(asked).containsExactly("save q1");
            assertThat(stored).isEqualTo("q1");
        }

        @Test
        void a_delivery_that_saved_before_a_restart_position_was_stored_and_ends_after_it_leaves_the_tick_allowed() {
            // Given
            registration.reading();
            Delivery stale = registration.delivering();
            save(stale, "e5", Write.RETURNS);
            registration.reading();
            restartPosition("r1", Write.RETURNS);

            // When
            end(stale, "e5");
            boolean allowed = registration.quietTickAllowed();
            asked.clear();
            quietTick("q1", Write.RETURNS);

            // Then its end says nothing about r1, which is stored
            assertThat(allowed).as("allowed after the delivery ended with r1 stored over its e5").isTrue();
            assertThat(asked).containsExactly("save q1");
            assertThat(stored).isEqualTo("q1");
        }

        @Test
        void a_restart_position_that_threw_stops_a_delivery_started_before_it_from_saving_and_the_read_that_finds_it_has_the_quiet_position_saved() {
            // Given
            registration.reading();
            Delivery stale = registration.delivering();
            registration.reading();
            restartPosition("r1", Write.WRITES_THEN_THROWS);
            asked.clear();

            // When
            save(stale, "e5", Write.RETURNS);
            List<String> askedBySave = new ArrayList<>(asked);
            end(stale, "e5");
            quietTick("q1", Write.RETURNS);

            // Then
            assertThat(askedBySave).as("asked by the save of the delivery started before the restart").isEmpty();
            assertThat(asked).containsExactly("read", "save q1");
            assertThat(stored).isEqualTo("q1");
        }

        @Test
        void a_delivery_started_after_a_restart_position_was_stored_on_the_restarted_read_saves_and_is_current() {
            // Given
            registration.reading();
            Delivery stale = registration.delivering();
            registration.reading();
            restartPosition("r1", Write.RETURNS);
            Delivery restarted = registration.delivering();

            // When
            save(restarted, "e6", Write.RETURNS);
            asked.clear();
            save(stale, "e5", Write.RETURNS);
            List<String> askedByStale = new ArrayList<>(asked);
            end(restarted, "e6");
            end(stale, "e5");
            asked.clear();
            boolean allowed = registration.quietTickAllowed();
            quietTick("q1", Write.RETURNS);

            // Then
            assertThat(stored).isEqualTo("q1");
            assertThat(askedByStale).as("asked by the save of the delivery started before the restart").isEmpty();
            assertThat(allowed).as("allowed with e6 stored by the current delivery").isTrue();
            assertThat(asked).containsExactly("save q1");
        }

        @Test
        void with_no_reads_numbered_a_delivery_started_before_a_restart_position_is_stopped_from_saving_and_one_started_after_it_saves() {
            // Given
            Delivery stale = registration.delivering();
            restartPosition("r1", Write.RETURNS);
            asked.clear();

            // When
            save(stale, "e5", Write.RETURNS);
            List<String> askedByStale = new ArrayList<>(asked);
            end(stale, "e5");
            Delivery restarted = registration.delivering();
            save(restarted, "e6", Write.RETURNS);
            end(restarted, "e6");

            // Then
            assertThat(askedByStale).as("asked by the save of the delivery started before the restart").isEmpty();
            assertThat(asked).containsExactly("save e6");
            assertThat(stored).isEqualTo("e6");
            assertThat(registration.quietTickAllowed()).as("allowed with e6 stored by the delivery started after the restart").isTrue();
        }
    }

    // As the model does, holding the deliveryLock and then the saveLock of the registration, and deciding the quiet
    // ticks once the write has returned or threw
    private void restartPosition(String position, Write write) {
        registration.deliveryLock.lock();
        try {
            registration.saveLock.lock();
            try {
                asked.add("restart " + position);
                write(position, write);
                registration.restartPositionStored(at(position));
            } catch (CheckpointWriteConditionNotFulfilledException refused) {
                registration.storedUnknown();
            } catch (IllegalStateException failed) {
                registration.restartPositionMayBeStored(at(position));
            } finally {
                registration.decideQuietTicks();
                registration.saveLock.unlock();
            }
        } finally {
            registration.deliveryLock.unlock();
        }
    }

    private void deliverStored(String position) {
        Delivery delivery = registration.delivering();
        save(delivery, position, Write.RETURNS);
        end(delivery, position);
    }

    private void save(Delivery delivery, String position, Write write) {
        Runnable save = () -> registration.saveUnlessCancelledRepositionedOrSuperseded(delivery, at(position), () -> {
            asked.add("save " + position);
            write(position, write);
        });
        if (write == Write.RETURNS) {
            save.run();
        } else {
            assertThatThrownBy(save::run).isInstanceOf(IllegalStateException.class);
        }
    }

    private void end(Delivery delivery, @Nullable String position) {
        registration.delivered(delivery, position == null ? null : at(position));
    }

    // Reads first as the model does, so a quiet tick is only started when the registration allows it
    private void quietTick(String quietPosition, Write write) {
        Read read = registration.reading();
        if (!registration.quietTickAllowed()) {
            asked.add("tick off");
            return;
        }
        try {
            registration.saveQuietPositionIfStoredCovers(read.repositions(), at(quietPosition), () -> {
                asked.add("save " + quietPosition);
                try {
                    write(quietPosition, write);
                    return true;
                } catch (CheckpointWriteConditionNotFulfilledException refused) {
                    throw refused;
                } catch (IllegalStateException failed) {
                    return false;
                }
            }, this::readStored);
        } catch (CheckpointWriteConditionNotFulfilledException refused) {
            asked.add("refused");
        }
    }

    // What a resume at a position does under the saveLock, and after it
    private void resumeAt(String position, @Nullable String previous, Write write) {
        registration.saveLock.lock();
        try {
            registration.repositioning();
            try {
                write(position, write);
                registration.repositionStored(at(position), previous == null ? null : at(previous));
            } catch (IllegalStateException failed) {
                registration.repositionMayBeStored(at(position), previous == null ? null : at(previous));
            }
        } finally {
            registration.saveLock.unlock();
        }
        registration.decideQuietTicks();
    }

    // What the put-back of a resume that failed does under both locks. A put-back that threw reads what is stored
    private void putBack(String previous, Write write) {
        registration.deliveryLock.lock();
        try {
            registration.saveLock.lock();
            try {
                registration.puttingBack();
                try {
                    write(previous, write);
                    registration.stored(at(previous));
                } catch (IllegalStateException failed) {
                    registration.storedUnknown();
                    try {
                        asked.add("put-back read");
                        if (readFails) {
                            throw new IllegalStateException("The read failed");
                        }
                        registration.storedRead(stored == null ? null : at(stored));
                    } catch (IllegalStateException unread) {
                        // Still unknown
                    }
                }
                registration.decideQuietTicks();
            } finally {
                registration.saveLock.unlock();
            }
        } finally {
            registration.deliveryLock.unlock();
        }
    }

    private void write(String position, Write write) {
        switch (write) {
            case RETURNS -> stored = position;
            case WRITES_THEN_THROWS -> {
                stored = position;
                throw new IllegalStateException("The reply of the save of " + position + " was lost");
            }
            case THROWS_WITHOUT_WRITING -> throw new IllegalStateException("The save of " + position + " failed");
            case REFUSED -> throw new CheckpointWriteConditionNotFulfilledException("id", OptionalLong.empty(), CheckpointWriteCondition.any());
        }
    }

    private @Nullable Checkpoint readStored() {
        asked.add("read");
        if (readFails) {
            throw new IllegalStateException("The read failed");
        }
        return stored == null ? null : at(stored);
    }

    private static Checkpoint at(String position) {
        return new StringBasedCheckpoint(position);
    }
}
