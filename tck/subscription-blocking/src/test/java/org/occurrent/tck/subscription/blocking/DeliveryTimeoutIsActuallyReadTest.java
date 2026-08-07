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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.platform.testkit.engine.Events;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.api.blocking.SubscriptionModel;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectNestedMethod;
import static org.junit.platform.testkit.engine.EngineTestKit.engine;

/**
 * {@link SubscriptionModelSuite#checkFixtureCanAnswerThisSuite} rejects a bad {@code deliveryTimeout()} before the
 * first assertion, which {@link SubscriptionModelConformanceGuardsTest} already covers. What that leaves unproven is
 * that a good one is actually read. A suite that quietly used a hardcoded bound instead would pass those guard tests
 * and every other test in this module just as well.
 * <p>
 * This drives one real conformance test through the JUnit Platform Launcher against a model whose delivery is
 * deliberately delayed, once with a fixture budget shorter than the delay and once with one comfortably longer. The
 * same delay has to fail the first and pass the second, and nothing except {@code deliveryTimeout()} differs between
 * them, which is what a hardcoded bound could not do.
 * <p>
 * One test is selected rather than the whole suite. {@link SubscriptionModelConformance} has 24 test methods, and a
 * short budget paid out in full by every one of them on the failing run alone would already be several seconds. One test
 * that waits on a delayed delivery is enough to answer the question this class asks.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("the delivery timeout a fixture declares")
class DeliveryTimeoutIsActuallyReadTest {

    private static final String THE_DELIVERING_TEST = "delivers_every_published_event_to_a_running_subscription_in_order";
    private static final Duration DELIVERY_DELAY = Duration.ofMillis(300);

    @Test
    void a_budget_shorter_than_the_models_delivery_delay_makes_the_suite_fail() {
        Events tests = run(ShortBudgetConformance.class);

        assertThat(tests.started().count())
                .as("the selected test must actually run, or this proves nothing about deliveryTimeout() being read")
                .isEqualTo(1);
        assertThat(tests.failed().count())
                .as("a fixture declaring a budget shorter than the model's delivery delay must make the wait give up "
                        + "before the events arrive. A suite reading a hardcoded bound instead of this fixture's "
                        + "declaration would pass here, which is what this test rules out")
                .isEqualTo(1);
        assertThat(tests.succeeded().count()).isZero();
    }

    @Test
    void a_budget_longer_than_the_models_delivery_delay_makes_the_suite_pass() {
        Events tests = run(LongBudgetConformance.class);

        assertThat(tests.started().count())
                .as("the selected test must actually run, or this proves nothing about deliveryTimeout() being read")
                .isEqualTo(1);
        assertThat(tests.succeeded().count())
                .as("the very same delivery delay must pass once the fixture declares a budget comfortably above it, "
                        + "which is what proves the wait bound comes from this fixture's deliveryTimeout() rather "
                        + "than from a fixed constant that happened to be long enough for the earlier assertion")
                .isEqualTo(1);
        assertThat(tests.failed().count()).isZero();
    }

    private static Events run(Class<?> suite) {
        return engine("junit-jupiter")
                .selectors(selectNestedMethod(List.of(suite), SubscriptionModelConformance.Delivering.class, THE_DELIVERING_TEST))
                .execute()
                .testEvents();
    }

    /**
     * Not named {@code *Test}, so Surefire does not pick either of these two up as a test of its own. They exist only
     * for the selected runs above.
     */
    static class ShortBudgetConformance extends SubscriptionModelConformance {

        @Override
        protected SubscriptionModelFixture createFixture() {
            return new DelayedDeliveryFixture(Duration.ofMillis(100));
        }
    }

    static class LongBudgetConformance extends SubscriptionModelConformance {

        @Override
        protected SubscriptionModelFixture createFixture() {
            return new DelayedDeliveryFixture(Duration.ofSeconds(3));
        }
    }

    private static final class DelayedDeliveryFixture implements SubscriptionModelFixture {

        private final WorkingSubscriptionModel model = new WorkingSubscriptionModel(DELIVERY_DELAY);
        private final Duration deliveryTimeout;

        DelayedDeliveryFixture(Duration deliveryTimeout) {
            this.deliveryTimeout = deliveryTimeout;
        }

        @Override
        public SubscriptionModel subscriptionModel() {
            return model;
        }

        @Override
        public void publish(List<CloudEvent> events) {
            model.feed(events);
        }

        @Override
        public boolean deliversEventsPublishedWhilePaused() {
            return false;
        }

        @Override
        public boolean retriesAFailingHandler() {
            return true;
        }

        @Override
        public Checkpoint aCheckpointToStartFrom() {
            return model.globalCheckpoint();
        }

        @Override
        public Duration deliveryTimeout() {
            return deliveryTimeout;
        }

        @Override
        public void close() {
            model.shutdown();
        }
    }
}
