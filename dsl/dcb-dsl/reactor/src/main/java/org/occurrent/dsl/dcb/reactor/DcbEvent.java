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

package org.occurrent.dsl.dcb.reactor;

import org.jspecify.annotations.NullMarked;

import static java.util.Objects.requireNonNull;

/**
 * A domain event delivered by a DCB subscription together with its DCB metadata.
 *
 * @param metadata the DCB metadata of the delivered event
 * @param event the converted domain event
 * @param <E> the domain event type
 */
@NullMarked
public record DcbEvent<E>(DcbEventMetadata metadata, E event) {

    public DcbEvent {
        requireNonNull(metadata, "metadata cannot be null");
        requireNonNull(event, "event cannot be null");
    }
}
