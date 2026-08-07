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
 * A subscription model refused a call because of what the caller named or passed, rather than because something went
 * wrong. Each subtype is one condition, so a caller can tell them apart by type instead of by reading the message.
 * <p>
 * This extends {@link IllegalArgumentException}, which is what every one of these refusals threw before they were
 * named, so code catching that still catches all of them.
 * <p>
 * <strong>What is deliberately not here.</strong> A refusal in this family means the call named something that does
 * not exist, or something in a state the call does not apply to, and passing different arguments would have worked.
 * Two neighbouring kinds of refusal are therefore {@link IllegalStateException} instead, and stay that way:
 * <ul>
 *     <li>An operation a model cannot serve at all because of how it was built, for example a store handed no
 *     {@code DataFieldReader} refusing a filter on a payload field. Nothing the caller passes fixes that, so it is an
 *     {@link UnsupportedOperationException}, which is the same answer an event store gives for a capability it was
 *     not built with.</li>
 *     <li>A failure that already happened, or contention with another node. A catch-up that failed, a feed with no
 *     consumer registered, and a competing consumer whose lock another node holds are all
 *     {@link IllegalStateException}, because none of them is a mistake in the calling code.</li>
 * </ul>
 * <p>
 * The message is not part of the contract, but every subtype builds the same one from its own constructor, so two
 * models refusing for the same reason word it identically unless one deliberately supplies its own.
 */
public sealed abstract class SubscriptionRefusedException extends IllegalArgumentException
        permits DuplicateSubscriptionIdException, SubscriptionAlreadyRunningException, SubscriptionNotRunningException,
        UnknownSubscriptionException, UnsupportedStartAtException, UnsupportedSubscriptionFilterException {

    SubscriptionRefusedException(String message) {
        super(message);
    }
}
