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

package org.occurrent.dsl.dcb

import org.occurrent.dsl.subscription.EventMetadata

/**
 * The DCB sequence position of an event, or `null` when the event has no DCB position.
 *
 * Events delivered by a DCB subscription are DCB-tagged and therefore have a non-null position.
 */
val EventMetadata.dcbPosition: Long?
    get() {
        val position = DcbEventMetadata.from(this).dcbPosition()
        return if (position.isPresent) position.asLong else null
    }

/**
 * The canonical DCB tags of an event, or an empty set when the event has no DCB tags.
 */
val EventMetadata.dcbTags: Set<String> get() = DcbEventMetadata.from(this).dcbTags()
