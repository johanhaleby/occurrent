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

import org.occurrent.subscription.api.blocking.CommitOrderedSubscriptions;

/**
 * A {@link StubSubscriptionModel} that implements {@link CommitOrderedSubscriptions} the way its contract has it.
 * An id the model does not know is never delivered in commit order, and a model that declares every event in commit
 * order answers {@code true} for every id it knows. The answer of a model that does not declare it can be flipped
 * between deliveries.
 */
final class CommitOrderStubSubscriptionModel extends StubSubscriptionModel implements CommitOrderedSubscriptions {

    private final boolean declaresEveryEvent;
    private volatile boolean inCommitOrder;

    private CommitOrderStubSubscriptionModel(boolean declaresEveryEvent, boolean inCommitOrder) {
        this.declaresEveryEvent = declaresEveryEvent;
        this.inCommitOrder = inCommitOrder;
    }

    /** Declares through {@link #deliversEveryEventInCommitOrder()} that every subscription is delivered in commit order, always. */
    static CommitOrderStubSubscriptionModel declaringEveryEventInCommitOrder() {
        return new CommitOrderStubSubscriptionModel(true, true);
    }

    /** Answers {@code true} per subscription for now, without declaring it for good. */
    static CommitOrderStubSubscriptionModel inCommitOrderForNow() {
        return new CommitOrderStubSubscriptionModel(false, true);
    }

    /** Answers {@code false} per subscription for now. */
    static CommitOrderStubSubscriptionModel notInCommitOrderForNow() {
        return new CommitOrderStubSubscriptionModel(false, false);
    }

    void answerInCommitOrder(boolean answer) {
        if (declaresEveryEvent && !answer) {
            throw new IllegalStateException("A model that declares every event in commit order cannot answer false for a subscription it knows");
        }
        this.inCommitOrder = answer;
    }

    @Override
    public boolean deliversInCommitOrder(String subscriptionId) {
        return isSubscribed(subscriptionId) && (declaresEveryEvent || inCommitOrder);
    }

    @Override
    public boolean deliversEveryEventInCommitOrder() {
        return declaresEveryEvent;
    }
}
