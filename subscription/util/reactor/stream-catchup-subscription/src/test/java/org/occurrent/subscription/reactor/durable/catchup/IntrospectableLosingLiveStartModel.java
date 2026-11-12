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

package org.occurrent.subscription.reactor.durable.catchup;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.reactor.IntrospectableSubscriptions;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.reactor.durable.catchup.ReactorStreamCatchupSubscriptionModelHistoryLossTest.LosingLiveStartModel;
import reactor.core.publisher.Mono;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

// A LosingLiveStartModel that lists its ids, as ReactorMongoSubscriptionModel does. Its subscriptionIds() can throw
// once when the catch-up asks from a given method of its own, its next cancel can drop the subscription before it
// throws, and its next subscribe(..) can throw once it registered the subscription.
class IntrospectableLosingLiveStartModel extends LosingLiveStartModel implements IntrospectableSubscriptions {
    // The catch-up methods that ask on behalf of another
    private static final Set<String> ASKING_FOR_ANOTHER = Set.of("holds", "holdsOrCannotTell", "underIdLock");

    private final AtomicReference<@Nullable String> idsThrowFrom = new AtomicReference<>();
    volatile boolean idsThrew;
    volatile boolean nextCancelDropsFirst;
    // Runs once, on the thread of the next subscribe(..) before it does anything
    volatile Runnable beforeTheNextSubscribe = () -> {
    };
    volatile @Nullable RuntimeException nextSubscribeThrows;
    // Whether the next subscribe(..) that throws drops the subscription first, as a model does for a run it ends
    volatile boolean nextSubscribeThatThrowsDropsIt;

    IntrospectableLosingLiveStartModel() {
        super(true);
    }

    void idsThrowOnceFrom(String method) {
        idsThrowFrom.set(method);
    }

    @Override
    public Set<String> subscriptionIds() {
        String method = idsThrowFrom.get();
        if (method != null && askedFrom(method) && idsThrowFrom.compareAndSet(method, null)) {
            idsThrew = true;
            throw new IllegalStateException("subscriptionIds() throws once, asked from " + method);
        }
        return heldIds();
    }

    private static boolean askedFrom(String method) {
        return StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE).walk(frames -> frames
                .filter(frame -> frame.getDeclaringClass() == NamedCatchupSupport.class)
                .filter(frame -> !ASKING_FOR_ANOTHER.contains(frame.getMethodName()))
                .findFirst()
                .map(frame -> frame.getMethodName().equals(method) || frame.getMethodName().startsWith("lambda$" + method + "$"))
                .orElse(false));
    }

    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
        Runnable before = beforeTheNextSubscribe;
        beforeTheNextSubscribe = () -> {
        };
        before.run();
        Subscription subscription = super.subscribe(subscriptionId, filter, startAt, action);
        RuntimeException failure = nextSubscribeThrows;
        if (failure != null) {
            nextSubscribeThrows = null;
            if (nextSubscribeThatThrowsDropsIt) {
                endsTheSubscriptionOnItsOwn(subscriptionId);
            }
            throw failure;
        }
        return subscription;
    }

    @Override
    public Mono<Void> cancelSubscription(String subscriptionId) {
        if (nextCancelDropsFirst) {
            nextCancelDropsFirst = false;
            endsTheSubscriptionOnItsOwn(subscriptionId);
        }
        return super.cancelSubscription(subscriptionId);
    }
}
