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

package org.occurrent.subscription.blocking.competingconsumers;

import org.jspecify.annotations.Nullable;

import java.time.Instant;

import static java.util.Objects.requireNonNull;

/**
 * Where a competing subscription is on this node, as {@link CompetingConsumerSubscriptionModel#competingConsumerStatus(String)}
 * reports it.
 *
 * @param subscriptionId The id of the subscription.
 * @param subscriberId   The id this node competes for the lease with.
 * @param state          Where the subscription is on this node.
 * @param failure        What is wrong with the subscription on this node, a refusal by the wrapped model or a call that
 *                       keeps failing, or {@code null} when nothing is.
 */
public record CompetingConsumerStatus(String subscriptionId, String subscriberId, State state, @Nullable Failure failure) {

    public CompetingConsumerStatus {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(subscriberId, "subscriberId cannot be null");
        requireNonNull(state, "state cannot be null");
        if (state == State.GIVEN_UP && failure == null) {
            throw new IllegalArgumentException("A subscription given up has the refusal it was given up for as its failure");
        }
    }

    /**
     * Whether something is wrong with the subscription on this node, which is {@code true} exactly when
     * {@link #failure()} is not {@code null}.
     *
     * @return {@code true} when {@link #failure()} is not {@code null}.
     */
    public boolean isFailing() {
        return failure != null;
    }

    /**
     * Where a competing subscription is on this node.
     */
    public enum State {
        /**
         * This node holds the lease and the wrapped subscription model runs the subscription.
         */
        LEASE_HELD,
        /**
         * This node competes for the lease and runs the subscription once it gets it. Another node may hold the lease,
         * or nobody does yet. It is also the state of a node that lost the lease, which {@link CompetingConsumerSubscriptionModel#isPaused(String)}
         * reports as paused.
         */
        STANDBY,
        /**
         * The subscription is paused on this node and doesn't compete for the lease until it is resumed. That is a pause
         * by the user, and a subscription held paused after the wrapped model refused to start it from its position, which
         * has the refusal as its {@link #failure()}.
         */
        PAUSED,
        /**
         * The model is stopped, and the subscription doesn't compete for the lease until the model is started or the user
         * resumes it.
         */
        STOPPED,
        /**
         * The wrapped model refused to start the subscription after {@code subscribe} returned, so this node gave the lease
         * back and forgot the subscription, and its id is free. It stays reported until the id is subscribed again or
         * cancelled, or the model is shut down. The refusal is the {@link #failure()}.
         */
        GIVEN_UP
    }

    /**
     * What is wrong with a subscription.
     *
     * @param cause The latest failure.
     * @param since When the failures began. A failure stays until a later try succeeds, so a subscription that keeps
     *              failing has one {@code since} and a changing {@code cause}.
     */
    public record Failure(Throwable cause, Instant since) {
        public Failure {
            requireNonNull(cause, "cause cannot be null");
            requireNonNull(since, "since cannot be null");
        }
    }
}
