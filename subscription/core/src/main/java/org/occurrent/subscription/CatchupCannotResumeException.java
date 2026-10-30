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
 * A catch-up subscription refused to go on, because it has no start it can replay from and then go live from without
 * missing an event. Running the same catch-up again fails the same way until what it starts from changes.
 * <p>
 * This extends {@link IllegalStateException}, which is what a catch-up threw before this type existed when the
 * subscription model lost its live start during or right after each of its replays, so code catching that still
 * catches it. A catch-up also throws it when it is asked to start from a checkpoint that a time-based catch-up stored,
 * but can only replay by position. A blocking catch-up whose replay a resume or a takeover started doesn't run that
 * replay again after this, and holds it until the subscription is resumed.
 */
public class CatchupCannotResumeException extends IllegalStateException {

    /**
     * Creates an exception with the message to report.
     *
     * @param message The message to report
     */
    public CatchupCannotResumeException(String message) {
        super(message);
    }
}
