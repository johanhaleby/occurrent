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

package org.occurrent.example.domain.projection.dcbkotlin

import org.occurrent.dsl.projection.DcbProjection
import org.occurrent.dsl.projection.dcbProjection

/** Account events. Top-level data classes so the reflection CloudEvent type mapper resolves each from its simple name. */
sealed interface AccountEvent

data class AccountRegistered(val username: String) : AccountEvent
data class AccountClosed(val username: String) : AccountEvent
data class UsernameChanged(val newUsername: String) : AccountEvent

/**
 * Issue #194 verbatim: a projection that also creates its subscription, parameterized per key. Initial state plus a
 * handler per event type, and a DCB tag filter scoping the read to the events that ever mentioned this one username.
 * The same descriptor answers "is this username claimed?" either eventually (subscription-fed) or strongly
 * (query-folded on demand).
 */
fun isUsernameClaimedProjection(username: String): DcbProjection<Boolean, AccountEvent, String> =
    dcbProjection(initialState = false) {
        tags("username:$username")
        id { username }
        on<AccountRegistered> { _, _ -> true }
        on<AccountClosed> { _, _ -> false }
        on<UsernameChanged> { _, event -> event.newUsername == username }
    }
