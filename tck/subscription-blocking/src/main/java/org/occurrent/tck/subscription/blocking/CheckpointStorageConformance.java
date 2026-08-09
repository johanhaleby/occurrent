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
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.tck.FailureNamesTheTestClass;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The contract every {@link CheckpointStorage} owes. A checkpoint saved for a subscription id can be read back, is
 * gone once deleted, and reports its own existence honestly in between.
 * <p>
 * An implementation extends this and supplies a {@link CheckpointStorageFixture}:
 * <pre>{@code
 * class PostgresqlCheckpointStorageTest extends CheckpointStorageConformance {
 *     @Override
 *     protected CheckpointStorageFixture createFixture() {
 *         return new PostgresqlCheckpointStorageFixture();
 *     }
 * }
 * }</pre>
 * <p>
 * Not extending this suite is how an implementation declines to be conformance tested. That is a visible, greppable
 * absence rather than a runtime skip, and nothing here calls {@code Assumptions}. Where storages legitimately differ,
 * the fixture declares which way it goes and the suite asserts the documented outcome for that answer, so both branches
 * are checked by somebody.
 * <p>
 * What this suite deliberately does not assert:
 * <ul>
 *     <li><strong>What a stored checkpoint looks like.</strong> A document, a row, a string key, one collection or
 *     several, are all storage's business. The suite only ever reads back through {@code read}.</li>
 *     <li><strong>What happens on a null argument.</strong> The interface is JSpecify-annotated, so null is outside the
 *     contract rather than a case with a defined outcome, and demanding a particular exception would hold every
 *     implementation to this repository's own validation convention.</li>
 *     <li><strong>Concurrency.</strong> Whether two threads saving the same subscription id leave a coherent value is
 *     unspecified today, so the suite does not invent an answer.</li>
 *     <li><strong>Legacy field migration.</strong> Occurrent's MongoDB storages read an older field name and rewrite
 *     it, which the suite cannot describe without naming a field. That stays in their own tests.</li>
 * </ul>
 */
@NullMarked
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("the checkpoint storage contract")
@Timeout(60)
@ExtendWith(FailureNamesTheTestClass.class)
public abstract class CheckpointStorageConformance {

    private @Nullable CheckpointStorageFixture fixture;

    /**
     * Creates a fixture whose storage holds no checkpoints. Called before every test method.
     */
    protected abstract CheckpointStorageFixture createFixture();

    @BeforeEach
    final void createFixtureAndCheckItsDeclaration() {
        CheckpointStorageFixture created = requireNonNull(createFixture(), "createFixture() returned null");
        // Touch the accessor now, so a fixture that has not wired up its storage says so before the first assertion
        // rather than halfway through a test that looks like a storage failure.
        requireNonNull(created.checkpointStorage(),
                created.getClass().getName() + " returned null from checkpointStorage()");
        List<Checkpoint> additional = requireNonNull(created.additionalCheckpoints(),
                created.getClass().getName() + " returned null from additionalCheckpoints()");
        // Not contains(null): an immutable list from List.of(..) throws rather than answering false, and List.of() is
        // exactly what the fixture returns by default.
        if (additional.stream().anyMatch(java.util.Objects::isNull)) {
            throw new IllegalStateException(created.getClass().getName()
                    + " returned a null checkpoint from additionalCheckpoints(). A checkpoint the suite cannot save is "
                    + "not a checkpoint to declare.");
        }
        this.fixture = created;
    }

    @AfterEach
    final void closeFixture() {
        CheckpointStorageFixture current = this.fixture;
        this.fixture = null;
        if (current != null) {
            current.close();
        }
    }

    protected final CheckpointStorageFixture fixture() {
        CheckpointStorageFixture current = this.fixture;
        if (current == null) {
            throw new IllegalStateException("No fixture. It is created and closed per test method, so it cannot be "
                    + "reached from @BeforeAll or @AfterAll. Anything shared across the class, a container or a "
                    + "client, belongs in one of those rather than in the fixture.");
        }
        return current;
    }

    protected final CheckpointStorage checkpointStorage() {
        return fixture().checkpointStorage();
    }

    /**
     * The two checkpoints every storage owes an answer for, plus whatever the fixture added.
     */
    private List<Checkpoint> checkpointsToRoundTrip() {
        List<Checkpoint> checkpoints = new ArrayList<>();
        checkpoints.add(new StringBasedCheckpoint("a-checkpoint-value"));
        checkpoints.add(GlobalCheckpoint.of(42));
        checkpoints.addAll(fixture().additionalCheckpoints());
        return checkpoints;
    }

    private static String subscriptionId() {
        return UUID.randomUUID().toString();
    }

    @Nested
    @DisplayName("round tripping")
    class RoundTripping {

        @Test
        void reads_back_the_value_of_every_checkpoint_it_was_given() {
            // asString() is the whole of what Checkpoint promises, so it is the one thing every storage owes for every
            // checkpoint, whatever it does to the type on the way through.
            for (Checkpoint checkpoint : checkpointsToRoundTrip()) {
                String id = subscriptionId();

                checkpointStorage().save(id, checkpoint);

                Checkpoint read = checkpointStorage().read(id);
                assertThat(read)
                        .as("a saved checkpoint must be readable, for %s", checkpoint.getClass().getSimpleName())
                        .isNotNull();
                assertThat(read.asString())
                        .as("the value must survive the round trip, for %s", checkpoint.getClass().getSimpleName())
                        .isEqualTo(checkpoint.asString());
            }
        }

        @Test
        void keeps_the_checkpoint_type_only_where_the_fixture_declares_it() {
            for (Checkpoint checkpoint : checkpointsToRoundTrip()) {
                String id = subscriptionId();
                checkpointStorage().save(id, checkpoint);

                Checkpoint read = requireNonNull(checkpointStorage().read(id));

                if (fixture().preservesCheckpointType(checkpoint)) {
                    assertThat(read)
                            .as("this storage declares it rebuilds %s, so it must come back as one",
                                    checkpoint.getClass().getSimpleName())
                            .isInstanceOf(checkpoint.getClass());
                } else {
                    // Not a weaker assertion, the same one in the other direction. Without this a fixture could answer
                    // false for everything and never be asked to prove any of it, which is a flag hiding behaviour
                    // rather than a declared difference. The value it still owes is pinned by the test above.
                    assertThat(read)
                            .as("this storage declares it does not rebuild %s, so it must not come back as one",
                                    checkpoint.getClass().getSimpleName())
                            .isNotInstanceOf(checkpoint.getClass());
                }
            }
        }

        @Test
        void reads_nothing_for_a_subscription_it_has_never_seen() {
            assertThat(checkpointStorage().read(subscriptionId()))
                    .as("an unknown subscription id has no checkpoint, which is how a subscription starting for the "
                            + "first time is told to ask the model for a global checkpoint instead")
                    .isNull();
        }

        @Test
        void the_last_saved_checkpoint_is_the_one_that_is_read() {
            String id = subscriptionId();
            checkpointStorage().save(id, new StringBasedCheckpoint("first"));

            checkpointStorage().save(id, new StringBasedCheckpoint("second"));

            assertThat(requireNonNull(checkpointStorage().read(id)).asString())
                    .as("saving again for the same subscription replaces the checkpoint rather than adding one, since "
                            + "a subscription has one position")
                    .isEqualTo("second");
        }

        @Test
        void a_checkpoint_of_one_type_can_be_overwritten_by_a_checkpoint_of_another() {
            for (Checkpoint checkpoint : checkpointsToRoundTrip()) {
                String id = subscriptionId();
                checkpointStorage().save(id, checkpoint);

                checkpointStorage().save(id, new StringBasedCheckpoint("replaced-a-" + checkpoint.getClass().getSimpleName()));

                assertThat(requireNonNull(checkpointStorage().read(id)).asString())
                        .as("saving over a %s must replace it rather than leave part of it behind. A storage that "
                                + "encodes different checkpoint types into different fields has to clear the old field, "
                                + "or a subscription resumes from the position it had two saves ago",
                                checkpoint.getClass().getSimpleName())
                        .isEqualTo("replaced-a-" + checkpoint.getClass().getSimpleName());
            }
        }

        @Test
        void gives_back_the_checkpoint_it_saved_so_a_caller_can_chain() {
            Checkpoint checkpoint = new StringBasedCheckpoint("chained");

            Checkpoint returned = checkpointStorage().save(subscriptionId(), checkpoint);

            // The same instance, not an equal one. A storage that hands back a StringBasedCheckpoint carrying the same
            // value would pass an asString() check while quietly changing what the caller chains into: StartAt on the
            // MongoDB path branches on the checkpoint's type first and only parses the string if it recognises neither
            // of its own two types.
            assertThat(returned)
                    .as("save returns the checkpoint it was given, which is what lets a caller write "
                            + "StartAt.checkpoint(storage.save(id, checkpoint))")
                    .isSameAs(checkpoint);
        }

        @Test
        void keeps_two_subscriptions_apart() {
            String first = subscriptionId();
            String second = subscriptionId();

            checkpointStorage().save(first, new StringBasedCheckpoint("for-first"));
            checkpointStorage().save(second, new StringBasedCheckpoint("for-second"));

            assertThat(requireNonNull(checkpointStorage().read(first)).asString()).isEqualTo("for-first");
            assertThat(requireNonNull(checkpointStorage().read(second)).asString())
                    .as("one subscription's checkpoint must not overwrite another's, since the subscription id is the key")
                    .isEqualTo("for-second");
        }
    }

    @Nested
    @DisplayName("existence")
    class Existence {

        @Test
        void reports_no_checkpoint_before_one_is_saved() {
            assertThat(checkpointStorage().exists(subscriptionId())).isFalse();
        }

        @Test
        void reports_a_checkpoint_once_it_is_saved() {
            String id = subscriptionId();

            checkpointStorage().save(id, GlobalCheckpoint.of(7));

            assertThat(checkpointStorage().exists(id))
                    .as("exists is what ResumeStartPositions asks to decide whether a subscription is resuming or "
                            + "starting fresh, so it must agree with read")
                    .isTrue();
        }

        @Test
        void agrees_with_read_for_every_checkpoint_type() {
            for (Checkpoint checkpoint : checkpointsToRoundTrip()) {
                String id = subscriptionId();
                checkpointStorage().save(id, checkpoint);

                // Both are asserted against true rather than against each other. Comparing the two answers alone would
                // hold for a storage that saves nothing, since a missing checkpoint makes exists false and read null,
                // and those agree.
                assertThat(checkpointStorage().exists(id))
                        .as("a saved %s must exist", checkpoint.getClass().getSimpleName())
                        .isTrue();
                assertThat(checkpointStorage().read(id))
                        .as("exists and read must not disagree, for %s", checkpoint.getClass().getSimpleName())
                        .isNotNull();
            }
        }
    }

    @Nested
    @DisplayName("deleting")
    class Deleting {

        @Test
        void a_deleted_checkpoint_is_gone_from_both_read_and_exists() {
            String id = subscriptionId();
            checkpointStorage().save(id, new StringBasedCheckpoint("doomed"));

            checkpointStorage().delete(id);

            assertThat(checkpointStorage().read(id))
                    .as("a deleted checkpoint must not be readable, since cancelling a subscription discards its "
                            + "position and the next start has to be treated as a first one")
                    .isNull();
            assertThat(checkpointStorage().exists(id)).isFalse();
        }

        @Test
        void deleting_a_subscription_it_has_never_seen_does_nothing() {
            String unknown = subscriptionId();

            checkpointStorage().delete(unknown);

            assertThat(checkpointStorage().exists(unknown)).isFalse();
        }

        @Test
        void deleting_twice_does_nothing_the_second_time() {
            String id = subscriptionId();
            checkpointStorage().save(id, new StringBasedCheckpoint("doomed"));
            checkpointStorage().delete(id);

            checkpointStorage().delete(id);

            assertThat(checkpointStorage().exists(id)).isFalse();
        }

        @Test
        void deleting_one_subscription_leaves_another_alone() {
            String deleted = subscriptionId();
            String kept = subscriptionId();
            checkpointStorage().save(deleted, new StringBasedCheckpoint("doomed"));
            checkpointStorage().save(kept, new StringBasedCheckpoint("kept"));

            checkpointStorage().delete(deleted);

            assertThat(requireNonNull(checkpointStorage().read(kept)).asString()).isEqualTo("kept");
        }

        @Test
        void a_checkpoint_can_be_saved_again_after_being_deleted() {
            String id = subscriptionId();
            checkpointStorage().save(id, new StringBasedCheckpoint("first"));
            checkpointStorage().delete(id);

            checkpointStorage().save(id, new StringBasedCheckpoint("again"));

            assertThat(requireNonNull(checkpointStorage().read(id)).asString())
                    .as("a subscription that was cancelled and registered again must be able to store a position, so "
                            + "delete cannot leave a tombstone that refuses later saves")
                    .isEqualTo("again");
        }
    }

    @Nested
    @DisplayName("write conditions")
    class WriteConditions {

        @Test
        void not_older_than_is_accepted_when_nothing_is_stored_yet() {
            String id = subscriptionId();

            if (!fixture().evaluatesWriteConditions()) {
                assertThatThrownBy(() -> checkpointStorage().save(id, new StringBasedCheckpoint("v"), CheckpointWriteCondition.notOlderThan(0)))
                        .as("a storage that declares it does not evaluate write conditions refuses anything but any()")
                        .isInstanceOf(UnsupportedOperationException.class);
                return;
            }

            checkpointStorage().save(id, new StringBasedCheckpoint("first-ever"), CheckpointWriteCondition.notOlderThan(0));

            assertThat(requireNonNull(checkpointStorage().read(id)).asString())
                    .as("nothing stored means a checkpoint written before this condition existed, so the write must "
                            + "be accepted whatever version it offers")
                    .isEqualTo("first-ever");
            assertThat(checkpointStorage().writeVersion(id))
                    .as("the offered version becomes the stored one")
                    .isEqualTo(OptionalLong.of(0));
        }

        @Test
        void not_older_than_accepts_a_version_not_below_the_one_stored_and_refuses_a_lower_one() {
            String id = subscriptionId();

            if (!fixture().evaluatesWriteConditions()) {
                assertThatThrownBy(() -> checkpointStorage().save(id, new StringBasedCheckpoint("v"), CheckpointWriteCondition.notOlderThan(5)))
                        .as("a storage that declares it does not evaluate write conditions refuses anything but any()")
                        .isInstanceOf(UnsupportedOperationException.class);
                return;
            }

            checkpointStorage().save(id, new StringBasedCheckpoint("at-5"), CheckpointWriteCondition.notOlderThan(5));
            checkpointStorage().save(id, new StringBasedCheckpoint("at-7"), CheckpointWriteCondition.notOlderThan(7));

            assertThat(requireNonNull(checkpointStorage().read(id)).asString())
                    .as("a version not below the stored one is accepted and its checkpoint written")
                    .isEqualTo("at-7");
            assertThat(checkpointStorage().writeVersion(id))
                    .as("the accepted version becomes the stored one")
                    .isEqualTo(OptionalLong.of(7));

            assertThatThrownBy(() -> checkpointStorage().save(id, new StringBasedCheckpoint("stale"), CheckpointWriteCondition.notOlderThan(3)))
                    .as("a version below the stored one is refused, which is what keeps a node whose lease moved on "
                            + "from writing a checkpoint over a newer one")
                    .isInstanceOf(CheckpointWriteConditionNotFulfilledException.class);
            assertThat(requireNonNull(checkpointStorage().read(id)).asString())
                    .as("a refused write must not change the stored checkpoint")
                    .isEqualTo("at-7");
            assertThat(checkpointStorage().writeVersion(id))
                    .as("a refused write must not change the stored version")
                    .isEqualTo(OptionalLong.of(7));
        }

        @Test
        void if_absent_is_accepted_only_when_nothing_is_stored() {
            String id = subscriptionId();

            if (!fixture().evaluatesWriteConditions()) {
                assertThatThrownBy(() -> checkpointStorage().save(id, new StringBasedCheckpoint("v"), CheckpointWriteCondition.ifAbsent()))
                        .as("a storage that declares it does not evaluate write conditions refuses anything but any()")
                        .isInstanceOf(UnsupportedOperationException.class);
                return;
            }

            checkpointStorage().save(id, new StringBasedCheckpoint("pinned"), CheckpointWriteCondition.ifAbsent());

            assertThat(requireNonNull(checkpointStorage().read(id)).asString())
                    .as("nothing was stored for this subscription id, so the pinning write must be accepted")
                    .isEqualTo("pinned");

            assertThatThrownBy(() -> checkpointStorage().save(id, new StringBasedCheckpoint("overwrite"), CheckpointWriteCondition.ifAbsent()))
                    .as("a checkpoint already exists for this subscription id, so a second ifAbsent write must be refused")
                    .isInstanceOf(CheckpointWriteConditionNotFulfilledException.class);
            assertThat(requireNonNull(checkpointStorage().read(id)).asString())
                    .as("a refused write must not change the stored checkpoint")
                    .isEqualTo("pinned");
        }

        @Test
        void any_leaves_the_stored_version_untouched_and_carries_it_forward() {
            String id = subscriptionId();

            if (!fixture().evaluatesWriteConditions()) {
                // any() is the one condition every storage is required to support, whatever it declares.
                checkpointStorage().save(id, new StringBasedCheckpoint("unconditional"), CheckpointWriteCondition.any());
                assertThat(requireNonNull(checkpointStorage().read(id)).asString()).isEqualTo("unconditional");
                return;
            }

            checkpointStorage().save(id, new StringBasedCheckpoint("fenced"), CheckpointWriteCondition.notOlderThan(9));

            checkpointStorage().save(id, new StringBasedCheckpoint("unconditional"), CheckpointWriteCondition.any());

            assertThat(requireNonNull(checkpointStorage().read(id)).asString())
                    .as("any() writes the checkpoint")
                    .isEqualTo("unconditional");
            assertThat(checkpointStorage().writeVersion(id))
                    .as("any() must leave the stored version exactly as it was, carrying it forward rather than "
                            + "clearing it, since an unconditional write from a hand-wired caller or a node mid-deploy "
                            + "must not re-arm the fence at zero")
                    .isEqualTo(OptionalLong.of(9));

            assertThatThrownBy(() -> checkpointStorage().save(id, new StringBasedCheckpoint("stale"), CheckpointWriteCondition.notOlderThan(4)))
                    .as("the version any() carried forward must still be enforced by a later conditional write")
                    .isInstanceOf(CheckpointWriteConditionNotFulfilledException.class);
        }
    }
}
