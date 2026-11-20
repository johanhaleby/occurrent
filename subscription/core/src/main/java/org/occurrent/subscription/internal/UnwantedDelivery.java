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

package org.occurrent.subscription.internal;

import org.jspecify.annotations.Nullable;

import java.util.concurrent.CompletionStage;

/**
 * Internal to Occurrent and not part of its public API, so it can change or go away in any release.
 * <p>
 * Tells the action a subscription model calls that the model no longer wants the event the call delivers. The
 * catch-up subscription model sets a stage around each call to the action that a replay makes. It completes the stage
 * once a cancel or a stop has ended the replay and begins to wait for the action to return, and once the catch-up
 * model begins to shut down. The competing consumer subscription model reads the stage while an event waits for the
 * lease, and withdraws the event once the stage is complete.
 * <p>
 * Read on the thread that calls the action. A call that a model makes on a thread of its own never sees it. The stage
 * names the subscription it was set for, and a reader asks for the stage of its own subscription, so an event that the
 * action delivers on the same thread to a subscription with another id doesn't read it. A model that reads it calls
 * the action it wraps through {@link #without}, so no event delivered from inside that action reads it either.
 */
public final class UnwantedDelivery {

    private static final ThreadLocal<OfTheCall> OF_THE_CALL = new ThreadLocal<>();

    private UnwantedDelivery() {
    }

    /**
     * Runs {@code call} with {@code unwanted} as the stage of the call on this thread for {@code subscriptionId}, and puts
     * back the stage that was there before once {@code call} returns or throws.
     *
     * @param subscriptionId the subscription whose action {@code call} calls
     * @param unwanted       completes once the model that runs {@code call} no longer wants the event it delivers
     * @param call           the call to the action
     */
    public static void run(String subscriptionId, CompletionStage<Void> unwanted, Runnable call) {
        runWith(new OfTheCall(subscriptionId, unwanted), call);
    }

    /**
     * Runs {@code call} with no stage on this thread, and puts back the stage that was there before once {@code call}
     * returns or throws.
     *
     * @param call the call to the action that a model reading the stage wraps
     */
    public static void without(Runnable call) {
        runWith(null, call);
    }

    /**
     * @param subscriptionId the subscription whose action is called on this thread
     * @return the stage of the call to the action of {@code subscriptionId} on this thread, or {@code null} when the
     * model that called the action set none for that subscription
     */
    public static @Nullable CompletionStage<Void> ofTheCallOnThisThread(String subscriptionId) {
        OfTheCall ofTheCall = OF_THE_CALL.get();
        return ofTheCall == null || !ofTheCall.subscriptionId.equals(subscriptionId) ? null : ofTheCall.unwanted;
    }

    private static void runWith(@Nullable OfTheCall ofTheCall, Runnable call) {
        OfTheCall before = OF_THE_CALL.get();
        if (ofTheCall == null) {
            OF_THE_CALL.remove();
        } else {
            OF_THE_CALL.set(ofTheCall);
        }
        try {
            call.run();
        } finally {
            if (before == null) {
                OF_THE_CALL.remove();
            } else {
                OF_THE_CALL.set(before);
            }
        }
    }

    private record OfTheCall(String subscriptionId, CompletionStage<Void> unwanted) {
    }
}
