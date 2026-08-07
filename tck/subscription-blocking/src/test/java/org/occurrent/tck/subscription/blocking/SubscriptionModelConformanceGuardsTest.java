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

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.blocking.SubscriptionModel;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The rules that stop a conformance suite from quietly testing nothing have no other coverage, so they get their own
 * tests. Everything here drives {@link SubscriptionModelConformance}'s lifecycle hooks directly, which this test can
 * reach because it shares the suite's package.
 * <p>
 * The failures these pin all name the fixture class and the method it owes. That is the point of them: whoever hits one
 * is writing a fixture for their own subscription model, and a plain {@code NullPointerException} from inside the suite
 * would tell them nothing about which of their methods was wrong.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("the subscription model conformance guards")
class SubscriptionModelConformanceGuardsTest {

    @Test
    void reject_a_fixture_that_was_never_created() {
        SubscriptionModelConformance suite = suiteWith(null);

        assertThatThrownBy(suite::createFixtureAndCheckItsDeclaration)
                .isExactlyInstanceOf(NullPointerException.class)
                .hasMessageContaining("createFixture() returned null");
    }

    @Test
    void reject_a_fixture_that_has_not_wired_up_its_model() {
        SubscriptionModelConformance suite = suiteWith(new StubFixture(null));

        assertThatThrownBy(suite::createFixtureAndCheckItsDeclaration)
                .isExactlyInstanceOf(NullPointerException.class)
                .hasMessageContaining("returned null from subscriptionModel()");
    }

    @Test
    void report_that_there_is_no_fixture_rather_than_a_null_pointer_when_asked_outside_a_test() {
        SubscriptionModelConformance suite = suiteWith(new StubFixture(NoopSubscriptionModel.INSTANCE));

        assertThatThrownBy(suite::fixture)
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No fixture");
    }

    @Test
    void reject_a_fixture_that_declares_no_start_position_at_all() {
        SubscriptionModelConformance suite = suiteWith(new StubFixture(NoopSubscriptionModel.INSTANCE) {
            @Override
            public Set<StartAtVariant> acceptedStartAtVariants() {
                return Set.of();
            }
        });

        assertThatThrownBy(suite::createFixtureAndCheckItsDeclaration)
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessageContaining("accepts no StartAt variant at all");
    }

    @Test
    void reject_a_fixture_with_no_checkpoint_to_start_from() {
        SubscriptionModelConformance suite = suiteWith(new StubFixture(NoopSubscriptionModel.INSTANCE) {
            @Override
            @SuppressWarnings("NullAway")
            public Checkpoint aCheckpointToStartFrom() {
                return null;
            }
        });

        assertThatThrownBy(suite::createFixtureAndCheckItsDeclaration)
                .isExactlyInstanceOf(NullPointerException.class)
                .hasMessageContaining("returned null from aCheckpointToStartFrom()");
    }

    @Test
    void reject_a_fixture_that_declares_no_delivery_timeout_at_all() {
        SubscriptionModelFixture fixture = new StubFixture(NoopSubscriptionModel.INSTANCE) {
            @Override
            @SuppressWarnings("NullAway")
            public Duration deliveryTimeout() {
                return null;
            }
        };
        SubscriptionModelConformance suite = suiteWith(fixture);

        assertThatThrownBy(suite::createFixtureAndCheckItsDeclaration)
                .isExactlyInstanceOf(NullPointerException.class)
                .hasMessageContaining(fixture.getClass().getName())
                .hasMessageContaining("returned null from deliveryTimeout()");
    }

    @Test
    void reject_a_fixture_that_declares_a_zero_delivery_timeout() {
        SubscriptionModelFixture fixture = new StubFixture(NoopSubscriptionModel.INSTANCE) {
            @Override
            public Duration deliveryTimeout() {
                return Duration.ZERO;
            }
        };
        SubscriptionModelConformance suite = suiteWith(fixture);

        assertThatThrownBy(suite::createFixtureAndCheckItsDeclaration)
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(fixture.getClass().getName())
                .hasMessageContaining("declared a deliveryTimeout() of PT0S");
    }

    @Test
    void reject_a_fixture_that_declares_a_negative_delivery_timeout() {
        SubscriptionModelFixture fixture = new StubFixture(NoopSubscriptionModel.INSTANCE) {
            @Override
            public Duration deliveryTimeout() {
                return Duration.ofSeconds(-1);
            }
        };
        SubscriptionModelConformance suite = suiteWith(fixture);

        assertThatThrownBy(suite::createFixtureAndCheckItsDeclaration)
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(fixture.getClass().getName())
                .hasMessageContaining("declared a deliveryTimeout() of PT-1S");
    }

    @Test
    void close_the_fixture_after_a_test_even_when_the_test_failed() {
        CountingFixture counting = new CountingFixture();
        SubscriptionModelConformance suite = suiteWith(counting);
        suite.createFixtureAndCheckItsDeclaration();

        suite.closeFixture();

        assertThat(counting.closed)
                .as("a fixture that opened a container, a client or a thread pool must be closed whatever the test did")
                .isEqualTo(1);
    }

    private static SubscriptionModelConformance suiteWith(@Nullable SubscriptionModelFixture fixture) {
        return new SubscriptionModelConformance() {
            @Override
            @SuppressWarnings("NullAway")
            protected SubscriptionModelFixture createFixture() {
                return fixture;
            }
        };
    }

    private static class StubFixture implements SubscriptionModelFixture {

        private final @Nullable SubscriptionModel model;

        StubFixture(@Nullable SubscriptionModel model) {
            this.model = model;
        }

        @Override
        @SuppressWarnings("NullAway")
        public SubscriptionModel subscriptionModel() {
            return model;
        }

        @Override
        public void publish(List<CloudEvent> events) {
            throw new UnsupportedOperationException("not reached");
        }

        @Override
        public boolean deliversEventsPublishedWhilePaused() {
            return false;
        }

        @Override
        public boolean retriesAFailingHandler() {
            return false;
        }

        @Override
        public Checkpoint aCheckpointToStartFrom() {
            return new StringBasedCheckpoint("a-checkpoint");
        }
    }

    private static final class CountingFixture extends StubFixture {

        private int closed;

        CountingFixture() {
            super(NoopSubscriptionModel.INSTANCE);
        }

        @Override
        public void close() {
            closed++;
        }
    }
}
