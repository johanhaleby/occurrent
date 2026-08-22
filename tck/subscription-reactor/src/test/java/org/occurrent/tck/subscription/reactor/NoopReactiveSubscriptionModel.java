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
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import reactor.core.publisher.Mono;

import java.util.function.Function;

/**
 * Honours nothing: every method throws. {@link ReactiveSuiteNeverSkipsTest} runs the reactive suite against it to
 * prove the suite has tests and that every one of them fails, rather than skips, when nothing is honoured. Throwing
 * (rather than answering emptily or hanging) also keeps that failing run fast, since each test dies on its first call
 * into the model instead of waiting out a bounded wait.
 */
@NullMarked
final class NoopReactiveSubscriptionModel implements SubscriptionModel {

    static final NoopReactiveSubscriptionModel INSTANCE = new NoopReactiveSubscriptionModel();

    private NoopReactiveSubscriptionModel() {
    }

    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
        throw honoursNothing();
    }

    @Override
    public void pauseSubscription(String subscriptionId) {
        throw honoursNothing();
    }

    @Override
    public Subscription resumeSubscription(String subscriptionId) {
        throw honoursNothing();
    }

    @Override
    public void cancelSubscription(String subscriptionId) {
        throw honoursNothing();
    }

    @Override
    public void stop() {
        throw honoursNothing();
    }

    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
        throw honoursNothing();
    }

    @Override
    public boolean isRunning() {
        throw honoursNothing();
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        throw honoursNothing();
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        throw honoursNothing();
    }

    @Override
    public void shutdown() {
        // The one exception: the suite closes its fixture after every test, and a close that throws would bury the
        // real failure the test is there to make.
    }

    private static UnsupportedOperationException honoursNothing() {
        return new UnsupportedOperationException("honours nothing, on purpose");
    }
}
