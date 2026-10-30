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

import io.cloudevents.CloudEvent;

import static java.util.Objects.requireNonNull;

/**
 * One event a saga instance holds, with its body.
 *
 * @param sagaId     the instance the event belongs to
 * @param ref        what an attempt reads about the event without its body
 * @param cloudEvent the event as it was delivered
 */
public record HeldEvent(String sagaId, HeldEventRef ref, CloudEvent cloudEvent) {
    public HeldEvent {
        requireNonNull(sagaId, "sagaId cannot be null");
        requireNonNull(ref, "ref cannot be null");
        requireNonNull(cloudEvent, "cloudEvent cannot be null");
    }

    /** The key the event is held under, the same as {@link HeldEventRef#key()}. */
    public String key() {
        return ref.key();
    }
}
