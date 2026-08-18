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

import org.occurrent.filter.Filter;
import org.occurrent.filtermatching.DataFieldReader;

/**
 * A live CloudEvent match was asked to evaluate a {@link Filter} that references a {@code data} field, against a
 * {@link DataFieldReader} that cannot read one. Thrown the first time a registration's live match runs into this,
 * for example {@code DomainEventFeed.acceptCloudEvent(CloudEvent)}.
 * <p>
 * This is a permanent configuration error for the registration that raised it, not a transient failure. Nothing
 * about a later call changes the answer, since the {@link Filter} and the {@link DataFieldReader} the registration
 * was built with are both fixed, so the same instance of this exception is thrown for every call after the first
 * rather than a fresh one rebuilt each time. A caller that catches it must stop or park that registration rather
 * than retry it, and must never acknowledge and redeliver the event that triggered it expecting a different answer.
 * The fix is a new registration, either with a {@link Filter} that does not reference a {@code data} field or with
 * a {@link DataFieldReader} that can read the one it references.
 * <p>
 * This is an {@link IllegalStateException} rather than the {@link UnsupportedOperationException} a store or a
 * subscription throws for the same underlying reason at subscribe time, because by the time this is thrown the
 * registration itself already succeeded. The condition was only discoverable once a live event actually needed the
 * matcher, which is what forces the failure here instead of there.
 */
public final class UnreadableLiveFilterException extends IllegalStateException {

    public UnreadableLiveFilterException(String message, Throwable cause) {
        super(message, cause);
    }
}
