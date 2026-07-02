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

package org.occurrent.example.domain.hotelbooking.features.booking.model

import java.time.LocalDate

/**
 * A half-open date interval `[checkIn, checkOut)`. Modelling check-out as exclusive is what lets a guest check out and
 * another check in on the same day without the two stays counting as overlapping.
 */
data class Stay(val checkIn: LocalDate, val checkOut: LocalDate) {
    init {
        require(checkIn < checkOut) { "Check-in ($checkIn) must be before check-out ($checkOut)" }
    }

    /** Two half-open intervals overlap iff each starts before the other ends. */
    fun overlaps(other: Stay): Boolean = checkIn < other.checkOut && other.checkIn < checkOut
}
