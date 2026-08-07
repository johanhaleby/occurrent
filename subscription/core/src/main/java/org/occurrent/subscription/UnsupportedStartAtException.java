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

package org.occurrent.subscription;

/**
 * A start position this model does not accept, so the call was refused. Starting somewhere else instead would deliver
 * from a position the caller did not ask for, and silently losing or repeating history is worse than refusing.
 * <p>
 * Which positions a model accepts is a property of the model rather than a bug. {@code InMemorySubscriptionModel}
 * keeps no history, so it starts from now or from its own default and nothing else, and a catch-up-then-push model
 * always replays from the beginning, so there is no position for a caller to choose.
 */
public final class UnsupportedStartAtException extends SubscriptionRefusedException {

    private final StartAt startAt;

    /**
     * Creates an exception with the standard message. This is the message every Occurrent subscription model
     * produces, so prefer this constructor over supplying your own.
     *
     * @param startAt The start position that was refused
     */
    public UnsupportedStartAtException(StartAt startAt) {
        this(startAt, "Unsupported " + StartAt.class.getSimpleName() + ": " + startAt + ".");
    }

    /**
     * Creates an exception with a message of your own, which is how a model names the start positions it does accept.
     *
     * @param startAt The start position that was refused
     * @param message The message to report
     */
    public UnsupportedStartAtException(StartAt startAt, String message) {
        super(message);
        this.startAt = startAt;
    }

    /**
     * @return The start position that was refused
     */
    public StartAt startAt() {
        return startAt;
    }
}
