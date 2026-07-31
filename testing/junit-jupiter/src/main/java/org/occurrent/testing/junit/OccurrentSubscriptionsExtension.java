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

package org.occurrent.testing.junit;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModelLifeCycle;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Stops every subscription before and after each test, so a test only runs the subscriptions it names with
 * {@link #start(String)}. A test that writes events then cannot race a subscription it never asked for.
 * <p>
 * Stopping after each test matters because a Spring test context is cached across test classes, so a subscription one
 * class started would otherwise still be running for the next.
 */
public final class OccurrentSubscriptionsExtension implements BeforeEachCallback, AfterEachCallback {

    private final SubscriptionModelLifeCycle subscriptionModel;
    private final Set<String> alwaysStartIds = new LinkedHashSet<>();
    private final Set<String> knownIds = new LinkedHashSet<>();

    private OccurrentSubscriptionsExtension(SubscriptionModelLifeCycle subscriptionModel) {
        this.subscriptionModel = Objects.requireNonNull(subscriptionModel, "subscriptionModel must not be null");
    }

    /**
     * An extension over {@code subscriptionModel} where no subscription runs until a test asks for one. Register it
     * with {@code @RegisterExtension}.
     *
     * @param subscriptionModel the subscription model to stop and start, must not be {@code null}
     * @return a new extension
     */
    public static OccurrentSubscriptionsExtension stoppedByDefault(SubscriptionModelLifeCycle subscriptionModel) {
        return new OccurrentSubscriptionsExtension(subscriptionModel);
    }

    /**
     * Start these subscriptions before every test, for a test class where each test needs the same ones.
     *
     * @param subscriptionIds the ids to start before every test, must not be {@code null} and must not contain {@code null}
     * @return this extension, so the call can be chained onto {@link #stoppedByDefault(SubscriptionModelLifeCycle)}
     */
    public OccurrentSubscriptionsExtension alwaysStart(String... subscriptionIds) {
        Objects.requireNonNull(subscriptionIds, "subscriptionIds must not be null");
        for (String subscriptionId : subscriptionIds) {
            Objects.requireNonNull(subscriptionId, "subscriptionIds must not contain null");
            alwaysStartIds.add(subscriptionId);
            knownIds.add(subscriptionId);
        }
        return this;
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        subscriptionModel.stop();
        for (String subscriptionId : alwaysStartIds) {
            resumeAndWait(subscriptionId);
        }
    }

    @Override
    public void afterEach(ExtensionContext context) {
        subscriptionModel.stop();
    }

    /**
     * Start one subscription and block until it is actually listening, so a write that follows cannot outrun it.
     *
     * @param subscriptionId the id of a currently stopped subscription, must not be {@code null}
     * @return the running {@link Subscription}
     * @throws IllegalArgumentException if the model has no stopped subscription with that id
     */
    public Subscription start(String subscriptionId) {
        Objects.requireNonNull(subscriptionId, "subscriptionId must not be null");
        return resumeAndWait(subscriptionId);
    }

    private Subscription resumeAndWait(String subscriptionId) {
        Subscription subscription;
        try {
            subscription = subscriptionModel.resumeSubscription(subscriptionId);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Could not start subscription '" + subscriptionId + "', " + e.getMessage() +
                            ". Ids known to this extension (named via alwaysStart or start): " + describeKnownIds(), e);
        }
        knownIds.add(subscriptionId);
        subscription.waitUntilStarted();
        return subscription;
    }

    private String describeKnownIds() {
        return knownIds.isEmpty() ? "none" : knownIds.toString();
    }
}
