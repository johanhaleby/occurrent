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

package org.occurrent.tck.subscription.reactor;

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
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.tck.FailureNamesTheTestClass;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The contract every reactive {@link CheckpointStorage} owes, the reactive twin of
 * {@code org.occurrent.tck.subscription.blocking.CheckpointStorageConformance}.
 * <p>
 * Every assertion here bridges through {@link reactor.core.publisher.Mono#block()}, the same way
 * {@code ReactiveSubscriptionModelConformance} does elsewhere in this module, since what this suite is proving is the
 * signal a caller eventually sees, not how the signal travels. That bridging is exactly why this suite exists
 * separately from the blocking one rather than reusing it over some adapter. {@link WriteConditions} tests which
 * signal a refused write produces, {@code Mono.error} versus a silent empty completion, and a bridge that turns both
 * into "block threw" or "block returned null" would erase the very distinction being checked. See ADR 116.
 * <p>
 * An implementation extends this and supplies a {@link CheckpointStorageFixture}:
 * <pre>{@code
 * class ReactorPostgresqlCheckpointStorageTest extends CheckpointStorageConformance {
 *     @Override
 *     protected CheckpointStorageFixture createFixture() {
 *         return new ReactorPostgresqlCheckpointStorageFixture();
 *     }
 * }
 * }</pre>
 * <p>
 * Not extending this suite is how an implementation declines to be conformance tested. That is a visible, greppable
 * absence rather than a runtime skip, and nothing here calls {@code Assumptions}.
 * <p>
 * What this suite deliberately does not assert:
 * <ul>
 *     <li><strong>Existence.</strong> The reactive {@link CheckpointStorage} has no {@code exists} method, unlike its
 *     blocking counterpart, so there is nothing here to mirror the blocking suite's existence nested class with.</li>
 *     <li><strong>What a stored checkpoint looks like, what happens on a null argument, or concurrency.</strong> The
 *     same reasons the blocking suite gives apply unchanged.</li>
 *     <li><strong>Whether a refusal was retried.</strong> That is an implementation's own retry policy, not part of
 *     the contract every storage owes, and it is asserted where a storage's own retry exists to get it wrong, such as
 *     {@code ReactorCheckpointStorageResilienceTest}.</li>
 * </ul>
 */
@NullMarked
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("the reactive checkpoint storage contract")
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

    /**
     * The version currently stored, bridging {@code writeVersion}'s empty-Mono-means-absent signal to a nullable
     * {@code Long} the way {@code read} already does for a checkpoint, rather than introducing a second convention.
     */
    private @Nullable Long storedVersion(String id) {
        return checkpointStorage().writeVersion(id).block();
    }

    @Nested
    @DisplayName("round tripping")
    class RoundTripping {

        @Test
        void reads_back_the_value_of_every_checkpoint_it_was_given() {
            for (Checkpoint checkpoint : checkpointsToRoundTrip()) {
                String id = subscriptionId();

                checkpointStorage().save(id, checkpoint).block();

                Checkpoint read = checkpointStorage().read(id).block();
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
                checkpointStorage().save(id, checkpoint).block();

                Checkpoint read = requireNonNull(checkpointStorage().read(id).block());

                if (fixture().preservesCheckpointType(checkpoint)) {
                    assertThat(read)
                            .as("this storage declares it rebuilds %s, so it must come back as one",
                                    checkpoint.getClass().getSimpleName())
                            .isInstanceOf(checkpoint.getClass());
                } else {
                    assertThat(read)
                            .as("this storage declares it does not rebuild %s, so it must not come back as one",
                                    checkpoint.getClass().getSimpleName())
                            .isNotInstanceOf(checkpoint.getClass());
                }
            }
        }

        @Test
        void reads_nothing_for_a_subscription_it_has_never_seen() {
            assertThat(checkpointStorage().read(subscriptionId()).block())
                    .as("an unknown subscription id has no checkpoint, which is how a subscription starting for the "
                            + "first time is told to ask the model for a global checkpoint instead")
                    .isNull();
        }

        @Test
        void the_last_saved_checkpoint_is_the_one_that_is_read() {
            String id = subscriptionId();
            checkpointStorage().save(id, new StringBasedCheckpoint("first")).block();

            checkpointStorage().save(id, new StringBasedCheckpoint("second")).block();

            assertThat(requireNonNull(checkpointStorage().read(id).block()).asString())
                    .as("saving again for the same subscription replaces the checkpoint rather than adding one, since "
                            + "a subscription has one position")
                    .isEqualTo("second");
        }

        @Test
        void a_checkpoint_of_one_type_can_be_overwritten_by_a_checkpoint_of_another() {
            for (Checkpoint checkpoint : checkpointsToRoundTrip()) {
                String id = subscriptionId();
                checkpointStorage().save(id, checkpoint).block();

                checkpointStorage().save(id, new StringBasedCheckpoint("replaced-a-" + checkpoint.getClass().getSimpleName())).block();

                assertThat(requireNonNull(checkpointStorage().read(id).block()).asString())
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

            Checkpoint returned = requireNonNull(checkpointStorage().save(subscriptionId(), checkpoint).block());

            assertThat(returned)
                    .as("save returns the checkpoint it was given, which is what lets a caller write "
                            + "StartAt.checkpoint(storage.save(id, checkpoint))")
                    .isSameAs(checkpoint);
        }

        @Test
        void keeps_two_subscriptions_apart() {
            String first = subscriptionId();
            String second = subscriptionId();

            checkpointStorage().save(first, new StringBasedCheckpoint("for-first")).block();
            checkpointStorage().save(second, new StringBasedCheckpoint("for-second")).block();

            assertThat(requireNonNull(checkpointStorage().read(first).block()).asString()).isEqualTo("for-first");
            assertThat(requireNonNull(checkpointStorage().read(second).block()).asString())
                    .as("one subscription's checkpoint must not overwrite another's, since the subscription id is the key")
                    .isEqualTo("for-second");
        }
    }

    @Nested
    @DisplayName("deleting")
    class Deleting {

        @Test
        void a_deleted_checkpoint_is_gone_from_read() {
            String id = subscriptionId();
            checkpointStorage().save(id, new StringBasedCheckpoint("doomed")).block();

            checkpointStorage().delete(id).block();

            assertThat(checkpointStorage().read(id).block())
                    .as("a deleted checkpoint must not be readable, since cancelling a subscription discards its "
                            + "position and the next start has to be treated as a first one")
                    .isNull();
        }

        @Test
        void deleting_a_subscription_it_has_never_seen_does_nothing() {
            String unknown = subscriptionId();

            checkpointStorage().delete(unknown).block();

            assertThat(checkpointStorage().read(unknown).block()).isNull();
        }

        @Test
        void deleting_twice_does_nothing_the_second_time() {
            String id = subscriptionId();
            checkpointStorage().save(id, new StringBasedCheckpoint("doomed")).block();
            checkpointStorage().delete(id).block();

            checkpointStorage().delete(id).block();

            assertThat(checkpointStorage().read(id).block()).isNull();
        }

        @Test
        void deleting_one_subscription_leaves_another_alone() {
            String deleted = subscriptionId();
            String kept = subscriptionId();
            checkpointStorage().save(deleted, new StringBasedCheckpoint("doomed")).block();
            checkpointStorage().save(kept, new StringBasedCheckpoint("kept")).block();

            checkpointStorage().delete(deleted).block();

            assertThat(requireNonNull(checkpointStorage().read(kept).block()).asString()).isEqualTo("kept");
        }

        @Test
        void a_checkpoint_can_be_saved_again_after_being_deleted() {
            String id = subscriptionId();
            checkpointStorage().save(id, new StringBasedCheckpoint("first")).block();
            checkpointStorage().delete(id).block();

            checkpointStorage().save(id, new StringBasedCheckpoint("again")).block();

            assertThat(requireNonNull(checkpointStorage().read(id).block()).asString())
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
                assertThatThrownBy(() -> checkpointStorage().save(id, new StringBasedCheckpoint("v"), CheckpointWriteCondition.notOlderThan(0)).block())
                        .as("a storage that declares it does not evaluate write conditions refuses anything but any()")
                        .isInstanceOf(UnsupportedOperationException.class);
                return;
            }

            checkpointStorage().save(id, new StringBasedCheckpoint("first-ever"), CheckpointWriteCondition.notOlderThan(0)).block();

            assertThat(requireNonNull(checkpointStorage().read(id).block()).asString())
                    .as("nothing stored means a checkpoint written before this condition existed, so the write must "
                            + "be accepted whatever version it offers")
                    .isEqualTo("first-ever");
            assertThat(storedVersion(id))
                    .as("the offered version becomes the stored one")
                    .isEqualTo(0L);
        }

        @Test
        void not_older_than_accepts_a_version_not_below_the_one_stored_and_refuses_a_lower_one() {
            String id = subscriptionId();

            if (!fixture().evaluatesWriteConditions()) {
                assertThatThrownBy(() -> checkpointStorage().save(id, new StringBasedCheckpoint("v"), CheckpointWriteCondition.notOlderThan(5)).block())
                        .as("a storage that declares it does not evaluate write conditions refuses anything but any()")
                        .isInstanceOf(UnsupportedOperationException.class);
                return;
            }

            checkpointStorage().save(id, new StringBasedCheckpoint("at-5"), CheckpointWriteCondition.notOlderThan(5)).block();
            checkpointStorage().save(id, new StringBasedCheckpoint("at-7"), CheckpointWriteCondition.notOlderThan(7)).block();

            assertThat(requireNonNull(checkpointStorage().read(id).block()).asString())
                    .as("a version not below the stored one is accepted and its checkpoint written")
                    .isEqualTo("at-7");
            assertThat(storedVersion(id))
                    .as("the accepted version becomes the stored one")
                    .isEqualTo(7L);

            // The refusal must reach the caller as an error signal, never as an empty completion. A .block() on a
            // Mono that completed empty instead of erroring would return null here, and assertThatThrownBy would
            // fail with "Expecting code to raise a throwable" rather than silently pass, which is exactly what turns
            // a refused-write-degrades-to-nothing-happened regression into a failing test rather than a quiet one.
            assertThatThrownBy(() -> checkpointStorage().save(id, new StringBasedCheckpoint("stale"), CheckpointWriteCondition.notOlderThan(3)).block())
                    .as("a version below the stored one is refused, which is what keeps a node whose lease moved on "
                            + "from writing a checkpoint over a newer one")
                    .isInstanceOf(CheckpointWriteConditionNotFulfilledException.class);
            assertThat(requireNonNull(checkpointStorage().read(id).block()).asString())
                    .as("a refused write must not change the stored checkpoint")
                    .isEqualTo("at-7");
            assertThat(storedVersion(id))
                    .as("a refused write must not change the stored version")
                    .isEqualTo(7L);
        }

        @Test
        void if_absent_is_accepted_only_when_nothing_is_stored() {
            String id = subscriptionId();

            if (!fixture().evaluatesWriteConditions()) {
                assertThatThrownBy(() -> checkpointStorage().save(id, new StringBasedCheckpoint("v"), CheckpointWriteCondition.ifAbsent()).block())
                        .as("a storage that declares it does not evaluate write conditions refuses anything but any()")
                        .isInstanceOf(UnsupportedOperationException.class);
                return;
            }

            checkpointStorage().save(id, new StringBasedCheckpoint("pinned"), CheckpointWriteCondition.ifAbsent()).block();

            assertThat(requireNonNull(checkpointStorage().read(id).block()).asString())
                    .as("nothing was stored for this subscription id, so the pinning write must be accepted")
                    .isEqualTo("pinned");

            assertThatThrownBy(() -> checkpointStorage().save(id, new StringBasedCheckpoint("overwrite"), CheckpointWriteCondition.ifAbsent()).block())
                    .as("a checkpoint already exists for this subscription id, so a second ifAbsent write must be refused")
                    .isInstanceOf(CheckpointWriteConditionNotFulfilledException.class);
            assertThat(requireNonNull(checkpointStorage().read(id).block()).asString())
                    .as("a refused write must not change the stored checkpoint")
                    .isEqualTo("pinned");
        }

        @Test
        void any_leaves_the_stored_version_untouched_and_carries_it_forward() {
            String id = subscriptionId();

            if (!fixture().evaluatesWriteConditions()) {
                // any() is the one condition every storage is required to support, whatever it declares.
                checkpointStorage().save(id, new StringBasedCheckpoint("unconditional"), CheckpointWriteCondition.any()).block();
                assertThat(requireNonNull(checkpointStorage().read(id).block()).asString()).isEqualTo("unconditional");
                return;
            }

            checkpointStorage().save(id, new StringBasedCheckpoint("fenced"), CheckpointWriteCondition.notOlderThan(9)).block();

            checkpointStorage().save(id, new StringBasedCheckpoint("unconditional"), CheckpointWriteCondition.any()).block();

            assertThat(requireNonNull(checkpointStorage().read(id).block()).asString())
                    .as("any() writes the checkpoint")
                    .isEqualTo("unconditional");
            assertThat(storedVersion(id))
                    .as("any() must leave the stored version exactly as it was, carrying it forward rather than "
                            + "clearing it, since an unconditional write from a hand-wired caller or a node mid-deploy "
                            + "must not re-arm the fence at zero")
                    .isEqualTo(9L);

            assertThatThrownBy(() -> checkpointStorage().save(id, new StringBasedCheckpoint("stale"), CheckpointWriteCondition.notOlderThan(4)).block())
                    .as("the version any() carried forward must still be enforced by a later conditional write")
                    .isInstanceOf(CheckpointWriteConditionNotFulfilledException.class);
        }

        @Test
        void any_on_a_fresh_key_does_not_create_a_version() {
            String id = subscriptionId();

            if (!fixture().evaluatesWriteConditions()) {
                return;
            }

            checkpointStorage().save(id, new StringBasedCheckpoint("unconditional"), CheckpointWriteCondition.any()).block();

            assertThat(requireNonNull(checkpointStorage().read(id).block()).asString())
                    .as("any() writes the checkpoint even on a key nothing was ever stored for")
                    .isEqualTo("unconditional");
            assertThat(storedVersion(id))
                    .as("an unconditional write on a subscription id with no stored version must not create one, or "
                            + "it arms the fence at version zero for a caller that never asked for a fence at all, "
                            + "refusing the very next notOlderThan(0) write it should have accepted")
                    .isNull();
        }
    }
}
