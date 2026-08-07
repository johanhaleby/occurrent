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

package org.occurrent.tck.subscription.blocking.outofpackage;

import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.tck.subscription.blocking.SubscriptionModelConformance;
import org.occurrent.tck.subscription.blocking.SubscriptionModelFixture;
import org.occurrent.tck.subscription.blocking.WorkingSubscriptionModel;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code deliveryTimeout()}, {@code fixture()} and {@code subscriptionModel()} are {@code protected} members
 * {@link SubscriptionModelConformance} inherits from the package private {@code SubscriptionModelSuite}. The constant
 * they replaced, {@code DELIVERY_TIMEOUT}, was {@code protected static} directly on this public conformance class.
 * That is a strictly weaker access situation than before, reachable only from a subclass and never from a sibling in
 * the declaring package, so this class lives in a different package on purpose and calls all three from its own
 * body.
 * <p>
 * It also extends {@link SubscriptionModelConformance} for real, so the whole suite runs here as an ordinary Surefire
 * test against {@link WorkingSubscriptionModel}. A class that merely compiled would prove the access is legal.
 * Running the whole suite green proves it is usable, so an implementer outside
 * {@code org.occurrent.tck.subscription.blocking} can satisfy the conformance suite using only what this package can
 * see.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("the conformance suite subclassed from another package")
class DeliveryTimeoutIsUsableFromASubclassOutsideThePackageTest extends SubscriptionModelConformance {

    @Override
    protected SubscriptionModelFixture createFixture() {
        return new WorkingFixture();
    }

    @Test
    void deliveryTimeout_fixture_and_subscriptionModel_are_reachable_from_a_subclass_in_another_package() {
        assertThat(deliveryTimeout())
                .as("deliveryTimeout() is protected on the package private SubscriptionModelSuite, so a subclass "
                        + "outside its package must still read the fixture's own declaration through it rather than "
                        + "being unable to reach it at all")
                .isEqualTo(WorkingFixture.DELIVERY_TIMEOUT);
        assertThat(fixture())
                .as("fixture() must hand back the very fixture createFixture() returned, reachable from here the "
                        + "same way it always was")
                .isInstanceOf(WorkingFixture.class);
        assertThat(subscriptionModel())
                .as("subscriptionModel() must be the same model fixture().subscriptionModel() reports")
                .isSameAs(fixture().subscriptionModel());
    }

    private static final class WorkingFixture implements SubscriptionModelFixture {

        static final Duration DELIVERY_TIMEOUT = Duration.ofSeconds(5);

        private final WorkingSubscriptionModel model = new WorkingSubscriptionModel();

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
            return new StringBasedCheckpoint("out-of-package");
        }

        @Override
        public Duration deliveryTimeout() {
            return DELIVERY_TIMEOUT;
        }

        @Override
        public void close() {
            model.shutdown();
        }
    }
}
