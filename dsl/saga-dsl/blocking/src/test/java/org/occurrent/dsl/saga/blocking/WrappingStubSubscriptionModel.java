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

package org.occurrent.dsl.saga.blocking;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.api.blocking.SubscriptionModelWrapper;

import java.util.function.Consumer;

/**
 * A wrapper that subscribes through the model it wraps and says nothing about commit order itself. Events are
 * delivered on the wrapped model, which is where the listener ends up.
 */
class WrappingStubSubscriptionModel extends StubSubscriptionModel implements SubscriptionModelWrapper {

    private final SubscriptionModel wrapped;

    WrappingStubSubscriptionModel(SubscriptionModel wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        return wrapped.subscribe(subscriptionId, filter, startAt, action);
    }

    @Override
    public SubscriptionModel getWrappedSubscriptionModel() {
        return wrapped;
    }
}
