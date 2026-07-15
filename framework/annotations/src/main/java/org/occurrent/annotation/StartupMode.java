/*
 *
 *  Copyright 2023 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.annotation;

/**
 * Specifies how a subscription or projection behaves during application startup. Shared by the subscription
 * annotations ({@link Subscription}, {@link StreamSubscription}, {@link DcbSubscription}) and by {@link Projection}.
 */
public enum StartupMode {
    /**
     * Occurrent determines the startup mode from the subscription's other properties (such as its start position and
     * resume behavior). It uses {@link #BACKGROUND} when the subscription needs to replay historic events before
     * subscribing to new ones, otherwise it uses {@link #WAIT_UNTIL_STARTED}.
     */
    DEFAULT,
    /**
     * The subscription waits until it has started up fully before Spring continues starting the rest of the
     * application. This is usually recommended, since otherwise a request could reach the application before the
     * subscription has bootstrapped and the subscription could miss that event. This matters only for a brand new
     * subscription. Once it has received an event recorded in a checkpoint storage, it never misses an event during
     * startup.
     */
    WAIT_UNTIL_STARTED,
    /**
     * The subscription does not wait until it has started up fully. It starts in the background instead. This is mainly
     * useful when the subscription starts from an earlier point (such as the beginning of time) with many events to
     * replay, and you do not want to block application startup while it catches up. It replays historic events in the
     * background and then switches to continuous mode.
     */
    BACKGROUND
}
