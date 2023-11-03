/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.example.domain.uno


sealed class Digit {
    data object Zero : Digit()
    data object One : Digit()
    data object Two : Digit()
    data object Three : Digit()
    data object Four : Digit()
    data object Five : Digit()
    data object Six : Digit()
    data object Seven : Digit()
    data object Eight : Digit()
    data object Nine : Digit()
}

sealed class Color {
    data object Red : Color()
    data object Green : Color()
    data object Blue : Color()
    data object Yellow : Color()
}

sealed class Card {
    abstract val color: Color

    data class DigitCard(val digit: Digit, override val color: Color) : Card()
    data class KickBack(override val color: Color) : Card()
    data class Skip(override val color: Color) : Card()
}

sealed class Direction {
    data object Clockwise : Direction()
    data object CounterClockwise : Direction()
}
