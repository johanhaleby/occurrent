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
import java.util.stream.Collectors;

/**
 * A JUnit 5 extension that keeps a {@link SubscriptionModelLifeCycle} paused by default in tests, so a test that
 * writes events never races a subscription it never asked to run. Every subscription is stopped before each test,
 * and a test opts a subscription back in with {@link #start(String)}, or a whole test class opts a fixed set in
 * with {@link #keepRunning(String...)}.
 * <p>
 * Every subscription is also stopped again after each test. This matters for a Spring test whose application
 * context is cached across test classes, since a subscription one class resumed would otherwise keep running while
 * the next class's tests execute against the same context.
 * <p>
 * {@link SubscriptionModelLifeCycle} has no operation that lists the subscription ids it knows about, so this
 * extension cannot enumerate the subscriptions registered on the model. It only ever acts on ids it has itself been
 * told about, through {@link #keepRunning(String...)} or {@link #start(String)}. That is also why there is no
 * {@code startAll}: it could only resume the ids named here, which is not what the name would promise. A test that
 * needs several subscriptions running names each of them.
 */
public final class OccurrentSubscriptionsExtension implements BeforeEachCallback, AfterEachCallback {

    private final SubscriptionModelLifeCycle subscriptionModel;
    private final Set<String> keepRunningIds = new LinkedHashSet<>();
    private final Set<String> knownIds = new LinkedHashSet<>();

    private OccurrentSubscriptionsExtension(SubscriptionModelLifeCycle subscriptionModel) {
        this.subscriptionModel = Objects.requireNonNull(subscriptionModel, "subscriptionModel must not be null");
    }

    /**
     * Create an extension that stops every subscription on {@code subscriptionModel} before each test and again
     * after each test. Register the result with {@code @RegisterExtension}.
     *
     * @param subscriptionModel the subscription model to pause and resume, must not be {@code null}
     * @return a new extension
     */
    public static OccurrentSubscriptionsExtension stopAllBeforeAndAfterEach(SubscriptionModelLifeCycle subscriptionModel) {
        return new OccurrentSubscriptionsExtension(subscriptionModel);
    }

    /**
     * Name subscriptions that should be resumed automatically in {@code beforeEach}, right after every subscription
     * has been stopped. Use this for a test class where every test needs the same subscriptions running, so
     * individual tests don't each have to call {@link #start(String)}.
     *
     * @param subscriptionIds the ids to resume automatically before each test, must not be {@code null} and must not contain {@code null}
     * @return this extension, so calls can be chained onto {@link #stopAllBeforeAndAfterEach(SubscriptionModelLifeCycle)}
     */
    public OccurrentSubscriptionsExtension keepRunning(String... subscriptionIds) {
        Objects.requireNonNull(subscriptionIds, "subscriptionIds must not be null");
        for (String subscriptionId : subscriptionIds) {
            Objects.requireNonNull(subscriptionId, "subscriptionIds must not contain null");
            keepRunningIds.add(subscriptionId);
            knownIds.add(subscriptionId);
        }
        return this;
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        subscriptionModel.stop();
        for (String subscriptionId : keepRunningIds) {
            resumeAndWait(subscriptionId);
        }
    }

    @Override
    public void afterEach(ExtensionContext context) {
        subscriptionModel.stop();
    }

    /**
     * Resume the subscription named {@code subscriptionId} and block until it has actually started, so the caller
     * cannot forget to wait and write an event before the subscription is listening for it.
     *
     * @param subscriptionId the id of a subscription that is currently paused, must not be {@code null}
     * @return the now-running {@link Subscription}
     * @throws IllegalArgumentException if {@code subscriptionId} is not a paused subscription on the model, the message names every id this extension knows about
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
                            ". Ids known to this extension (named via keepRunning or start): " + describeKnownIds(), e);
        }
        knownIds.add(subscriptionId);
        subscription.waitUntilStarted();
        return subscription;
    }

    private String describeKnownIds() {
        return knownIds.isEmpty() ? "none" : knownIds.stream().collect(Collectors.joining(", ", "[", "]"));
    }
}
