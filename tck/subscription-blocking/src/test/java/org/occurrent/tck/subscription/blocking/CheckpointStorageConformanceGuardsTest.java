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

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.api.blocking.CheckpointStorage;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The rules that stop a conformance suite from quietly testing nothing have no other coverage, so they get their own
 * tests. Everything here drives {@link CheckpointStorageConformance}'s lifecycle hook directly, which this test can
 * reach because it shares the suite's package.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("the conformance guards")
class CheckpointStorageConformanceGuardsTest {

    @Test
    void reject_a_fixture_that_was_never_created() {
        CheckpointStorageConformance suite = suiteWith(null);

        assertThatThrownBy(suite::createFixtureAndCheckItsDeclaration)
                .isExactlyInstanceOf(NullPointerException.class)
                .hasMessageContaining("createFixture() returned null");
    }

    @Test
    void reject_a_fixture_that_has_not_wired_up_its_storage() {
        CheckpointStorageConformance suite = suiteWith(new StubFixture(null, List.of()));

        assertThatThrownBy(suite::createFixtureAndCheckItsDeclaration)
                .isExactlyInstanceOf(NullPointerException.class)
                .hasMessageContaining("returned null from checkpointStorage()");
    }

    @Test
    void reject_a_fixture_that_declares_a_null_list_of_extra_checkpoints() {
        CheckpointStorageConformance suite = suiteWith(new StubFixture(NoopCheckpointStorage.INSTANCE, null));

        assertThatThrownBy(suite::createFixtureAndCheckItsDeclaration)
                .isExactlyInstanceOf(NullPointerException.class)
                .hasMessageContaining("returned null from additionalCheckpoints()");
    }

    @Test
    void reject_a_fixture_that_declares_a_checkpoint_that_is_null() {
        List<Checkpoint> withNull = new ArrayList<>();
        withNull.add(null);
        CheckpointStorageConformance suite = suiteWith(new StubFixture(NoopCheckpointStorage.INSTANCE, withNull));

        assertThatThrownBy(suite::createFixtureAndCheckItsDeclaration)
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessageContaining("returned a null checkpoint from additionalCheckpoints()");
    }

    @Test
    void report_that_there_is_no_fixture_rather_than_a_null_pointer_when_asked_outside_a_test() {
        CheckpointStorageConformance suite = suiteWith(new StubFixture(NoopCheckpointStorage.INSTANCE, List.of()));

        assertThatThrownBy(suite::fixture)
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No fixture");
    }

    @Test
    void close_the_fixture_after_a_test_even_when_the_test_failed() {
        CountingFixture counting = new CountingFixture();
        CheckpointStorageConformance suite = suiteWith(counting);
        suite.createFixtureAndCheckItsDeclaration();

        suite.closeFixture();

        assertThat(counting.closed)
                .as("a fixture that opened a container or a connection must be closed whatever the test did")
                .isEqualTo(1);
    }

    private static CheckpointStorageConformance suiteWith(@Nullable CheckpointStorageFixture fixture) {
        return new CheckpointStorageConformance() {
            @Override
            @SuppressWarnings("NullAway")
            protected CheckpointStorageFixture createFixture() {
                return fixture;
            }
        };
    }

    private static class StubFixture implements CheckpointStorageFixture {

        private final @Nullable CheckpointStorage storage;
        private final @Nullable List<Checkpoint> additional;

        StubFixture(@Nullable CheckpointStorage storage, @Nullable List<Checkpoint> additional) {
            this.storage = storage;
            this.additional = additional;
        }

        @Override
        @SuppressWarnings("NullAway")
        public CheckpointStorage checkpointStorage() {
            return storage;
        }

        @Override
        @SuppressWarnings("NullAway")
        public List<Checkpoint> additionalCheckpoints() {
            return additional;
        }

        @Override
        public boolean preservesCheckpointType(Checkpoint checkpoint) {
            return true;
        }
    }

    private static final class CountingFixture extends StubFixture {

        private int closed;

        CountingFixture() {
            super(NoopCheckpointStorage.INSTANCE, List.of());
        }

        @Override
        public void close() {
            closed++;
        }
    }
}
