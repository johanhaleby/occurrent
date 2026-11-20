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

package org.occurrent.springboot.common;

import org.jspecify.annotations.Nullable;

/**
 * Controls which subscription beans Occurrent creates and whether they start automatically, set with
 * {@code occurrent.subscription.mode}.
 */
public enum SubscriptionMode {
    /**
     * No subscription beans at all, so no subscription model, no checkpoint storage and no competing consumer lease.
     * Use this on a node that does not run subscriptions, for example because another node does.
     */
    DISABLED,

    /**
     * Every subscription bean exists and every subscription is registered, but only the synchronous ones run. Start
     * the others yourself with {@code start()} or one at a time with {@code resumeSubscription(id)}. Use this to bring
     * subscriptions up behind a leader election or a health check, or in a test that chooses which subscriptions run.
     * <p>
     * The synchronous subscription model, which runs {@code @SynchronousSubscription} and a {@code @Projection} or
     * {@code @Snapshot} with {@code mode = SYNCHRONOUS}, runs from the start, so every write through the application
     * service runs those handlers, also before the others start. A synchronous handler has no replay, so a write it
     * missed would be lost to it. While that model is stopped, or a synchronous subscription is paused, the
     * application service refuses a write those handlers would miss with {@code WriteRefusedException}. In 0.34.0
     * this mode kept the synchronous handlers stopped too, and such a write succeeded without them.
     */
    MANUAL,

    /**
     * Subscriptions are created and started, which is the default and how Occurrent has always behaved.
     */
    AUTO;

    /**
     * The mode an application asked for, given the new {@code mode} property and the deprecated {@code enabled} one.
     * Either may be {@code null}, meaning it was not set.
     *
     * @param mode    The value of {@code occurrent.subscription.mode}.
     * @param enabled The value of the deprecated {@code occurrent.subscription.enabled}.
     * @return The mode to use, {@link #AUTO} when neither is set.
     * @throws IllegalStateException if both are set and they contradict each other
     */
    public static SubscriptionMode resolve(@Nullable SubscriptionMode mode, @Nullable Boolean enabled) {
        if (mode == null) {
            return enabled == null ? AUTO : fromEnabled(enabled);
        } else if (enabled != null && mode != fromEnabled(enabled)) {
            throw new IllegalStateException(
                    "occurrent.subscription.mode is " + mode.name().toLowerCase() + " but the deprecated occurrent.subscription.enabled is "
                            + enabled + ", which means " + fromEnabled(enabled).name().toLowerCase() + ". Remove occurrent.subscription.enabled, "
                            + "and check for it in environment variables and external configuration as well as your configuration files.");
        }
        return mode;
    }

    private static SubscriptionMode fromEnabled(boolean enabled) {
        return enabled ? AUTO : DISABLED;
    }
}
