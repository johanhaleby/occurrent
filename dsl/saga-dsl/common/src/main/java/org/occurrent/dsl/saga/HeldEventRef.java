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

package org.occurrent.dsl.saga;

import org.jspecify.annotations.Nullable;

import java.time.Instant;

import static java.util.Objects.requireNonNull;

/**
 * What an attempt on a saga instance needs to know about one held event, without its body. A body can be close to the
 * store's document limit, so an attempt reads the references first and fetches the bodies it applies in pages.
 *
 * @param key         the event's redelivery key ({@code streamId@streamVersion}), or its CloudEvent id and source for an
 *                    event that carries none. Unique per saga instance
 * @param streamId    the stream the event was written to, or {@code null} when it carries no stream key
 * @param version     the event's stream version, or {@code null} when it carries no stream key
 * @param position    the event's global position, or {@code null} when it carries none
 * @param type        the CloudEvent type
 * @param heldAt      when the event was held
 * @param startEvent   whether the saga would start on the event, answered when it was held. A body that could not be
 *                     converted counts as a start event, so its copy is applied and its failure shows
 * @param failingSince when applying the held event first failed, or {@code null} when it has not. Quarantine measures
 *                     the event's budget from it
 */
public record HeldEventRef(String key,
                           @Nullable String streamId,
                           @Nullable Long version,
                           @Nullable Long position,
                           String type,
                           Instant heldAt,
                           boolean startEvent,
                           @Nullable Instant failingSince) {
    public HeldEventRef {
        requireNonNull(key, "key cannot be null");
        requireNonNull(type, "type cannot be null");
        requireNonNull(heldAt, "heldAt cannot be null");
    }
}
