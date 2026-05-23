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

package org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.dcb

import java.util.UUID

internal object GameDcbTags {
    fun game(gameId: UUID): String = "game:$gameId"
    fun gameplay(gameId: UUID): String = "gameplay:$gameId"
    fun wordHint(gameId: UUID): String = "wordhint:$gameId"
    fun points(gameId: UUID): String = "points:$gameId"
}
