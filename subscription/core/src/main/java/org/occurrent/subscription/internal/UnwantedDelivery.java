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
 * once a cancel or a stop has ended the replay and begins to wait for the action to return. The competing consumer
 * subscription model reads the stage while an event waits for the lease, and withdraws the event once the stage is
 * complete.
 * <p>
 * Read on the thread that calls the action. A call that a model makes on a thread of its own never sees it.
 */
public final class UnwantedDelivery {

    private static final ThreadLocal<CompletionStage<Void>> OF_THE_CALL = new ThreadLocal<>();

    private UnwantedDelivery() {
    }

    /**
     * Runs {@code call} with {@code unwanted} as the stage of the call on this thread, and puts back the stage that was
     * there before once {@code call} returns or throws.
     *
     * @param unwanted completes once the model that runs {@code call} no longer wants the event it delivers
     * @param call     the call to the action
     */
    public static void run(CompletionStage<Void> unwanted, Runnable call) {
        CompletionStage<Void> before = OF_THE_CALL.get();
        OF_THE_CALL.set(unwanted);
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

    /**
     * @return the stage of the call to the action on this thread, or {@code null} when the model that called the
     * action set none
     */
    public static @Nullable CompletionStage<Void> ofTheCallOnThisThread() {
        return OF_THE_CALL.get();
    }
}
