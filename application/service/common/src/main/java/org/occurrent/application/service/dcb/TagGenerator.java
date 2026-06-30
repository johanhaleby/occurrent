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

package org.occurrent.application.service.dcb;

import org.jspecify.annotations.NullMarked;

import java.util.Set;

/**
 * Derives DCB tags from domain events before they are stored as CloudEvents.
 * <p>
 * Tags describe the Dynamic Consistency Boundary that future reads and append
 * conditions can match. The application service stores them in the {@code dcbtags}
 * CloudEvent extension.
 */
@FunctionalInterface
@NullMarked
public interface TagGenerator<E> {

    /**
     * Returns the DCB tags that should be attached to {@code event}.
     *
     * @param event the domain event about to be written
     * @return the tags that make the event visible to relevant DCB queries
     */
    Set<String> tags(E event);
}
