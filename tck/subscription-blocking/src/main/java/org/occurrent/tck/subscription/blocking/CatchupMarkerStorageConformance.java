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

package org.occurrent.tck.subscription.blocking;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.extension.ExtendWith;
import org.occurrent.subscription.CatchupMarker;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.blocking.CatchupMarkerStorage;
import org.occurrent.tck.FailureNamesTheTestClass;

import java.util.UUID;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * The contract every {@link CatchupMarkerStorage} owes. A marker reads as present only while the generation it was
 * saved with is still the generation of its subscription id, and only a forget changes that generation.
 * <p>
 * An implementation extends this and supplies a {@link CatchupMarkerStorageFixture}:
 * <pre>{@code
 * class PostgresqlCatchupMarkerStorageTest extends CatchupMarkerStorageConformance {
 *     @Override
 *     protected CatchupMarkerStorageFixture createFixture() {
 *         return new PostgresqlCatchupMarkerStorageFixture();
 *     }
 * }
 * }</pre>
 * <p>
 * The suite doesn't assert:
 * <ul>
 *     <li><strong>How a generation changes.</strong> Only that a forget changes it to a value the test hasn't read
 *     for that id. A counter and a random token both qualify.</li>
 *     <li><strong>What a plain {@code save} over a marker reads as.</strong> A storage may keep the generation the
 *     replaced marker was saved with, or drop it.</li>
 * </ul>
 */
@NullMarked
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("the catch-up marker storage contract")
@Timeout(60)
@ExtendWith(FailureNamesTheTestClass.class)
public abstract class CatchupMarkerStorageConformance {

    private static final Checkpoint MARKER = new StringBasedCheckpoint("a-marker");
    private static final Checkpoint OTHER_MARKER = new StringBasedCheckpoint("another-marker");

    private @Nullable CatchupMarkerStorageFixture fixture;

    /**
     * Creates a fixture whose storage holds no checkpoints and no generations. Called before every test method.
     */
    protected abstract CatchupMarkerStorageFixture createFixture();

    @BeforeEach
    final void createFixtureAndCheckIt() {
        CatchupMarkerStorageFixture created = requireNonNull(createFixture(), "createFixture() returned null");
        requireNonNull(created.catchupMarkerStorage(), created.getClass().getName() + " returned null from catchupMarkerStorage()");
        this.fixture = created;
    }

    @AfterEach
    final void closeFixture() {
        CatchupMarkerStorageFixture current = this.fixture;
        this.fixture = null;
        if (current != null) {
            current.close();
        }
    }

    protected final CatchupMarkerStorage storage() {
        CatchupMarkerStorageFixture current = this.fixture;
        if (current == null) {
            throw new IllegalStateException("No fixture. It is created and closed per test method, so it cannot be reached from @BeforeAll or @AfterAll.");
        }
        return current.catchupMarkerStorage();
    }

    private static String subscriptionId() {
        return UUID.randomUUID().toString();
    }

    @Nested
    @DisplayName("an id that was never forgotten")
    class NeverForgotten {

        @Test
        void reads_generation_0_and_no_marker() {
            CatchupMarker marker = storage().readCatchupMarker(subscriptionId());

            assertAll(
                    () -> assertThat(marker.isPresent()).as("present").isFalse(),
                    () -> assertThat(marker.checkpoint()).as("checkpoint").isNull(),
                    () -> assertThat(marker.generation()).as("generation").isZero()
            );
        }

        @Test
        void reads_back_a_marker_saved_with_the_generation_it_read() {
            String id = subscriptionId();
            long generation = storage().readCatchupMarker(id).generation();

            boolean saved = storage().saveCatchupMarker(id, MARKER, generation, CheckpointWriteCondition.any());

            CatchupMarker marker = storage().readCatchupMarker(id);
            assertAll(
                    () -> assertThat(saved).as("saved").isTrue(),
                    () -> assertThat(marker.isPresent()).as("present").isTrue(),
                    () -> assertThat(asString(marker.checkpoint())).as("checkpoint").isEqualTo(MARKER.asString()),
                    () -> assertThat(marker.generation()).as("generation").isEqualTo(generation)
            );
        }
    }

    @Nested
    @DisplayName("forgetting a marker")
    class Forgetting {

        @Test
        void reads_absent_and_changes_the_generation() {
            String id = subscriptionId();
            long before = storage().readCatchupMarker(id).generation();
            storage().saveCatchupMarker(id, MARKER, before, CheckpointWriteCondition.any());

            storage().forgetCatchupMarker(id);

            CatchupMarker marker = storage().readCatchupMarker(id);
            assertAll(
                    () -> assertThat(marker.isPresent()).as("present").isFalse(),
                    () -> assertThat(marker.generation()).as("generation").isNotEqualTo(before),
                    () -> assertThat(storage().read(id)).as("plain read").isNull()
            );
        }

        @Test
        void a_late_save_with_the_generation_read_before_the_forget_returns_true_and_reads_absent() {
            String id = subscriptionId();
            long before = storage().readCatchupMarker(id).generation();
            storage().forgetCatchupMarker(id);

            boolean saved = storage().saveCatchupMarker(id, MARKER, before, CheckpointWriteCondition.any());

            CatchupMarker marker = storage().readCatchupMarker(id);
            Checkpoint plain = storage().read(id);
            assertAll(
                    () -> assertThat(saved).as("saved").isTrue(),
                    () -> assertThat(marker.isPresent()).as("present").isFalse(),
                    () -> assertThat(marker.checkpoint()).as("checkpoint").isNull(),
                    () -> assertThat(asString(plain)).as("plain read").isEqualTo(MARKER.asString())
            );
        }

        @Test
        void a_late_save_under_a_lease_reads_absent_too() {
            String id = subscriptionId();
            long before = storage().readCatchupMarker(id).generation();
            storage().saveCatchupMarker(id, MARKER, before, CheckpointWriteCondition.notOlderThan(3));
            storage().forgetCatchupMarker(id);

            boolean saved = storage().saveCatchupMarker(id, MARKER, before, CheckpointWriteCondition.notOlderThan(3));

            assertAll(
                    () -> assertThat(saved).as("saved").isTrue(),
                    () -> assertThat(storage().readCatchupMarker(id).isPresent()).as("present").isFalse()
            );
        }

        @Test
        void a_save_with_the_generation_read_after_the_forget_reads_present() {
            String id = subscriptionId();
            long before = storage().readCatchupMarker(id).generation();
            storage().saveCatchupMarker(id, MARKER, before, CheckpointWriteCondition.any());
            storage().forgetCatchupMarker(id);
            long after = storage().readCatchupMarker(id).generation();

            boolean saved = storage().saveCatchupMarker(id, OTHER_MARKER, after, CheckpointWriteCondition.any());

            CatchupMarker marker = storage().readCatchupMarker(id);
            assertAll(
                    () -> assertThat(saved).as("saved").isTrue(),
                    () -> assertThat(marker.isPresent()).as("present").isTrue(),
                    () -> assertThat(asString(marker.checkpoint())).as("checkpoint").isEqualTo(OTHER_MARKER.asString()),
                    () -> assertThat(marker.generation()).as("generation").isEqualTo(after)
            );
        }

        @Test
        void twice_in_a_row_gives_three_distinct_generations() {
            String id = subscriptionId();
            long first = storage().readCatchupMarker(id).generation();
            storage().forgetCatchupMarker(id);
            long second = storage().readCatchupMarker(id).generation();
            storage().forgetCatchupMarker(id);
            long third = storage().readCatchupMarker(id).generation();

            assertThat(new long[]{first, second, third}).doesNotHaveDuplicates();
        }

        @Test
        void a_save_with_the_generation_between_two_forgets_reads_absent_after_the_second() {
            String id = subscriptionId();
            storage().forgetCatchupMarker(id);
            long between = storage().readCatchupMarker(id).generation();
            storage().forgetCatchupMarker(id);

            storage().saveCatchupMarker(id, MARKER, between, CheckpointWriteCondition.any());

            assertThat(storage().readCatchupMarker(id).isPresent()).as("present").isFalse();
        }
    }

    @Nested
    @DisplayName("a save the condition refuses")
    class RefusedSave {

        @Test
        void if_absent_over_a_stored_marker_returns_false_and_leaves_the_marker_as_it_was() {
            String id = subscriptionId();
            long generation = storage().readCatchupMarker(id).generation();
            storage().saveCatchupMarker(id, MARKER, generation, CheckpointWriteCondition.any());

            boolean saved = storage().saveCatchupMarker(id, OTHER_MARKER, generation, CheckpointWriteCondition.ifAbsent());

            CatchupMarker marker = storage().readCatchupMarker(id);
            assertAll(
                    () -> assertThat(saved).as("saved").isFalse(),
                    () -> assertThat(marker.isPresent()).as("present").isTrue(),
                    () -> assertThat(asString(marker.checkpoint())).as("checkpoint").isEqualTo(MARKER.asString()),
                    () -> assertThat(marker.generation()).as("generation").isEqualTo(generation)
            );
        }

        @Test
        void not_older_than_a_lower_version_returns_false_and_leaves_the_marker_as_it_was() {
            String id = subscriptionId();
            long generation = storage().readCatchupMarker(id).generation();
            storage().saveCatchupMarker(id, MARKER, generation, CheckpointWriteCondition.notOlderThan(5));

            boolean saved = storage().saveCatchupMarker(id, OTHER_MARKER, generation, CheckpointWriteCondition.notOlderThan(3));

            CatchupMarker marker = storage().readCatchupMarker(id);
            assertAll(
                    () -> assertThat(saved).as("saved").isFalse(),
                    () -> assertThat(marker.isPresent()).as("present").isTrue(),
                    () -> assertThat(asString(marker.checkpoint())).as("checkpoint").isEqualTo(MARKER.asString()),
                    () -> assertThat(marker.generation()).as("generation").isEqualTo(generation)
            );
        }

        @Test
        void over_a_late_saved_marker_returns_false_and_leaves_it_absent() {
            String id = subscriptionId();
            long before = storage().readCatchupMarker(id).generation();
            storage().forgetCatchupMarker(id);
            long after = storage().readCatchupMarker(id).generation();
            storage().saveCatchupMarker(id, MARKER, before, CheckpointWriteCondition.notOlderThan(5));

            boolean saved = storage().saveCatchupMarker(id, OTHER_MARKER, after, CheckpointWriteCondition.notOlderThan(3));

            CatchupMarker marker = storage().readCatchupMarker(id);
            assertAll(
                    () -> assertThat(saved).as("saved").isFalse(),
                    () -> assertThat(marker.isPresent()).as("present").isFalse(),
                    () -> assertThat(marker.generation()).as("generation").isEqualTo(after),
                    () -> assertThat(asString(storage().read(id))).as("plain read").isEqualTo(MARKER.asString())
            );
        }
    }

    @Nested
    @DisplayName("a plain delete")
    class PlainDelete {

        @Test
        void leaves_the_generation_as_it_was() {
            String id = subscriptionId();
            storage().forgetCatchupMarker(id);
            long generation = storage().readCatchupMarker(id).generation();
            storage().saveCatchupMarker(id, MARKER, generation, CheckpointWriteCondition.any());

            storage().delete(id);

            CatchupMarker marker = storage().readCatchupMarker(id);
            assertAll(
                    () -> assertThat(marker.isPresent()).as("present").isFalse(),
                    () -> assertThat(marker.generation()).as("generation").isEqualTo(generation)
            );
        }

        @Test
        void leaves_generation_0_on_an_id_that_was_never_forgotten() {
            String id = subscriptionId();
            storage().saveCatchupMarker(id, MARKER, 0, CheckpointWriteCondition.any());

            storage().delete(id);

            assertThat(storage().readCatchupMarker(id).generation()).as("generation").isZero();
        }
    }

    private static @Nullable String asString(@Nullable Checkpoint checkpoint) {
        return checkpoint == null ? null : checkpoint.asString();
    }
}
