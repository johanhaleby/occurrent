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

import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.api.reactor.SubscriptionModel;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The mirror of {@code org.occurrent.tck.subscription.blocking.SubscriptionModelConformanceGuardsTest} for this leaf's
 * own fixture check. {@link ReactiveSubscriptionModelConformance#createTheFixture()} validates
 * {@link ReactiveSubscriptionModelFixture#deliveryTimeout()} the same way the blocking suite validates its own, and
 * that check has no other coverage, so it gets its own tests too. Everything here drives
 * {@code createTheFixture()} directly, which this test can reach because it shares the suite's package.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("the reactive subscription model conformance guards")
class ReactiveSubscriptionModelConformanceGuardsTest {

    @Test
    void reject_a_fixture_that_declares_no_delivery_timeout_at_all() {
        ReactiveSubscriptionModelFixture fixture = new StubFixture() {
            @Override
            @SuppressWarnings("NullAway")
            public Duration deliveryTimeout() {
                return null;
            }
        };
        ReactiveSubscriptionModelConformance suite = suiteWith(fixture);

        assertThatThrownBy(suite::createTheFixture)
                .isExactlyInstanceOf(NullPointerException.class)
                .hasMessageContaining(fixture.getClass().getName())
                .hasMessageContaining("returned null from deliveryTimeout()");
    }

    @Test
    void reject_a_fixture_that_declares_a_zero_delivery_timeout() {
        ReactiveSubscriptionModelFixture fixture = new StubFixture() {
            @Override
            public Duration deliveryTimeout() {
                return Duration.ZERO;
            }
        };
        ReactiveSubscriptionModelConformance suite = suiteWith(fixture);

        assertThatThrownBy(suite::createTheFixture)
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(fixture.getClass().getName())
                .hasMessageContaining("declared a deliveryTimeout() of PT0S");
    }

    @Test
    void reject_a_fixture_that_declares_a_negative_delivery_timeout() {
        ReactiveSubscriptionModelFixture fixture = new StubFixture() {
            @Override
            public Duration deliveryTimeout() {
                return Duration.ofSeconds(-1);
            }
        };
        ReactiveSubscriptionModelConformance suite = suiteWith(fixture);

        assertThatThrownBy(suite::createTheFixture)
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(fixture.getClass().getName())
                .hasMessageContaining("declared a deliveryTimeout() of PT-1S");
    }

    private static ReactiveSubscriptionModelConformance suiteWith(ReactiveSubscriptionModelFixture fixture) {
        return new ReactiveSubscriptionModelConformance() {
            @Override
            protected ReactiveSubscriptionModelFixture createFixture() {
                return fixture;
            }
        };
    }

    private static class StubFixture implements ReactiveSubscriptionModelFixture {

        @Override
        public SubscriptionModel subscriptionModel() {
            return NoopReactiveSubscriptionModel.INSTANCE;
        }

        @Override
        public void publish(List<CloudEvent> events) {
            throw new UnsupportedOperationException("not reached");
        }
    }
}
