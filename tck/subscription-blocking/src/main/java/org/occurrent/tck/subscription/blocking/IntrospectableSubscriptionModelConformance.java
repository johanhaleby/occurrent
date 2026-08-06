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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.subscription.api.blocking.IntrospectableSubscriptionModel;
import org.occurrent.subscription.api.blocking.SubscriptionModel;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The contract a model owes when it can list the subscriptions it knows.
 * <p>
 * A model that cannot list them declines this suite by not extending it, which is the same visible absence as declining
 * any other suite. There is no declaration for it, because {@link IntrospectableSubscriptionModel} is itself the
 * declaration.
 * <p>
 * Worth having rather than assumed. Both of Occurrent's MongoDB subscription models implement {@code subscriptionIds()}
 * and neither had a test for it before this suite.
 */
@NullMarked
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("the introspectable subscription model contract")
@Timeout(60)
public abstract class IntrospectableSubscriptionModelConformance extends SubscriptionModelSuite {

    /**
     * Creates a fixture whose model has no subscriptions and implements {@link IntrospectableSubscriptionModel}.
     * Called before every test method.
     */
    @Override
    protected abstract SubscriptionModelFixture createFixture();

    @Override
    protected void checkFixtureCanAnswerThisSuite(SubscriptionModelFixture fixture) {
        SubscriptionModel model = fixture.subscriptionModel();
        if (!(model instanceof IntrospectableSubscriptionModel)) {
            throw new IllegalStateException(model.getClass().getName() + " does not implement "
                    + IntrospectableSubscriptionModel.class.getSimpleName() + ", so it cannot answer this suite. A model "
                    + "that cannot list its subscriptions declines by not extending it.");
        }
    }

    private IntrospectableSubscriptionModel introspectable() {
        return (IntrospectableSubscriptionModel) fixture().subscriptionModel();
    }

    private static String subscriptionId() {
        return UUID.randomUUID().toString();
    }

    @Test
    void knows_nothing_before_anything_subscribes() {
        assertThat(introspectable().subscriptionIds()).isEmpty();
    }

    @Test
    void knows_a_running_subscription() {
        String id = subscriptionId();

        fixture().subscriptionModel().subscribe(id, new RecordedEvents()).waitUntilStarted(SubscriptionModelConformance.DELIVERY_TIMEOUT);

        assertThat(introspectable().subscriptionIds()).containsExactly(id);
    }

    @Test
    void knows_a_paused_subscription_too() {
        String id = subscriptionId();
        fixture().subscriptionModel().subscribe(id, new RecordedEvents()).waitUntilStarted(SubscriptionModelConformance.DELIVERY_TIMEOUT);

        fixture().subscriptionModel().pauseSubscription(id);

        assertThat(introspectable().subscriptionIds())
                .as("every subscription this model knows, whether running or paused, which is what lets a caller "
                        + "resume everything that is paused without having tracked the ids itself")
                .containsExactly(id);
    }

    @Test
    void forgets_a_cancelled_subscription() {
        String id = subscriptionId();
        fixture().subscriptionModel().subscribe(id, new RecordedEvents()).waitUntilStarted(SubscriptionModelConformance.DELIVERY_TIMEOUT);

        fixture().subscriptionModel().cancelSubscription(id);

        assertThat(introspectable().subscriptionIds())
                .as("cancelling releases the id, so a model still listing it would report a subscription that no "
                        + "longer exists")
                .isEmpty();
    }

    @Test
    void knows_every_subscription_when_it_accepts_several() {
        if (!fixture().acceptsSeveralSubscriptions()) {
            // A model that feeds one subscription has nothing more to say here, and the refusal itself is asserted by
            // SubscriptionModelConformance rather than restated.
            return;
        }
        String first = subscriptionId();
        String second = subscriptionId();

        fixture().subscriptionModel().subscribe(first, new RecordedEvents()).waitUntilStarted(SubscriptionModelConformance.DELIVERY_TIMEOUT);
        fixture().subscriptionModel().subscribe(second, new RecordedEvents()).waitUntilStarted(SubscriptionModelConformance.DELIVERY_TIMEOUT);

        assertThat(introspectable().subscriptionIds()).containsExactlyInAnyOrder(first, second);
    }
}
