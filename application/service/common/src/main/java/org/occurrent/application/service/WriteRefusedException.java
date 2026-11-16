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

package org.occurrent.application.service;

import org.jspecify.annotations.NullMarked;

import java.util.Set;
import java.util.TreeSet;

/**
 * Thrown by an application service before it writes anything, when a registered synchronous handler would never see
 * the events of that write. A synchronous handler has no replay, so a write it misses is lost to it for good, and the
 * application service refuses it instead. Nothing is stored, so the command can be run again once the handler runs.
 */
@NullMarked
public class WriteRefusedException extends IllegalStateException {

    private WriteRefusedException(String message) {
        super(message);
    }

    /**
     * @return The refusal of a write made while the synchronous subscription model is stopped.
     */
    public static WriteRefusedException synchronousSubscriptionsStopped() {
        return new WriteRefusedException("The synchronous subscription model is stopped, so its handlers would never see this write. Start it before writing.");
    }

    /**
     * @param subscriptionIds The paused synchronous subscriptions that the events of the write match
     * @return The refusal of a write that paused synchronous subscriptions would never see.
     */
    public static WriteRefusedException synchronousSubscriptionsPaused(Set<String> subscriptionIds) {
        return new WriteRefusedException(("The synchronous subscriptions %s are paused and match the events of this write, so they would never see it. " +
                "Resume them before writing.").formatted(new TreeSet<>(subscriptionIds)));
    }
}
