/*
 *
 *  Copyright 2022 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.library.hederlig.model

import java.time.Duration
import java.time.Instant
import java.time.ZonedDateTime

/**
 * Data structure that represents Delay's
 */
sealed interface Delay {
    companion object {
        fun ofMinutes(minutes: Long): Delay = RelativeDelay(Duration.ofMinutes(minutes))
        fun until(zonedDateTime: ZonedDateTime): Delay = DelayUntil(zonedDateTime.toInstant())
    }
}

internal class RelativeDelay internal constructor(val duration: Duration) : Delay
internal class DelayUntil internal constructor(val instant: Instant) : Delay