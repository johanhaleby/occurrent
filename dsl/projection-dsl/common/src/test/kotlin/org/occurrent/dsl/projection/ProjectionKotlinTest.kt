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

package org.occurrent.dsl.projection

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.occurrent.eventstore.api.dcb.DcbCriteria
import org.occurrent.eventstore.api.dcb.Tag
import org.occurrent.filter.Filter

class ProjectionKotlinTest {

    sealed interface AccountEvent {
        val accountId: String
    }

    data class AccountRegistered(override val accountId: String, val username: String) : AccountEvent
    data class AccountClosed(override val accountId: String) : AccountEvent
    data class UsernameChanged(override val accountId: String, val newUsername: String) : AccountEvent

    @Nested
    inner class ProjectionDsl {

        @Test
        fun `builds a projection with per-event-type handlers`() {
            val projection = projection<Boolean, AccountEvent, String>(initialState = false) {
                id { it.accountId }
                on<AccountRegistered> { _, e -> e.username == "bob" }
                on<UsernameChanged> { _, e -> e.newUsername == "bob" }
            }

            assertThat(projection.eventTypes())
                .containsExactlyInAnyOrder(AccountRegistered::class.java, UsernameChanged::class.java)
            assertThat(projection.view().evolve(false, AccountRegistered("1", "bob"))).isTrue()
            assertThat(projection.view().evolve(false, AccountRegistered("1", "alice"))).isFalse()
            assertThat(projection.id()!!.apply(AccountRegistered("acc-1", "bob"))).isEqualTo("acc-1")
        }

        @Test
        fun `no-ops on an unregistered event type`() {
            val projection = projection<Int, AccountEvent, String>(initialState = 0) {
                id { it.accountId }
                on<AccountRegistered> { state, _ -> state + 1 }
            }

            val state = projection.view().evolve(3, AccountClosed("1"))

            assertThat(state).isEqualTo(3)
        }

        @Test
        fun `keeps an explicit filter`() {
            val projection = projection<Boolean, AccountEvent, String>(initialState = false) {
                id { it.accountId }
                on<AccountRegistered> { _, _ -> true }
                filter(Filter.subject("acc-1"))
            }

            assertThat(projection.filter()).isNotNull()
        }
    }

    @Nested
    inner class MetadataKeyedFlag {

        @Test
        fun `is true after a metadata aware id block`() {
            val projection = projection<Long, AccountEvent, String>(initialState = 0L) {
                id { metadata, _ -> metadata.streamId }
                on<AccountRegistered> { _, metadata, _ -> metadata.position ?: 0L }
            }

            assertThat(projection.metadataKeyed()).isTrue()
        }

        @Test
        fun `is false after an event only id block even though it delegates to a BiFunction internally`() {
            val projection = projection<Boolean, AccountEvent, String>(initialState = false) {
                id { it.accountId }
                on<AccountRegistered> { _, _ -> true }
            }

            assertThat(projection.metadataKeyed()).isFalse()
        }

        @Test
        fun `is false for a singleton projection`() {
            val projection = singletonProjection<Boolean, AccountEvent>(initialState = false) {
                on<AccountRegistered> { _, _ -> true }
            }

            assertThat(projection.metadataKeyed()).isFalse()
        }
    }

    @Nested
    inner class DcbProjectionDsl {

        private fun isUsernameClaimed(username: String) =
            dcbProjection<Boolean, AccountEvent, String>(initialState = false) {
                tags("username:$username")
                id { username }
                on<AccountRegistered> { _, _ -> true }
                on<AccountClosed> { _, _ -> false }
                on<UsernameChanged> { _, e -> e.newUsername == username }
            }

        @Test
        fun `derives a tag-scoped read boundary from tags`() {
            val projection = isUsernameClaimed("bob")

            assertThat(projection.criteria()).isEqualTo(DcbCriteria.tags(Tag.of("username", "bob")))
        }

        @Test
        fun `carries the projection with its handlers and single-instance id`() {
            val projection = isUsernameClaimed("bob")

            assertThat(projection.projection().eventTypes())
                .containsExactlyInAnyOrder(
                    AccountRegistered::class.java, AccountClosed::class.java, UsernameChanged::class.java
                )
            assertThat(projection.projection().id()!!.apply(AccountRegistered("acc-1", "bob"))).isEqualTo("bob")
            // The fold realizes issue #194's IsUsernameClaimedProjection.
            val view = projection.projection().view()
            var state = view.evolve(false, AccountRegistered("acc-1", "bob"))
            assertThat(state).isTrue()
            state = view.evolve(state, UsernameChanged("acc-1", "alice"))
            assertThat(state).isFalse()
        }

        @Test
        fun `defaults the read boundary to all when no tags or criteria are given`() {
            val projection = dcbProjection<Int, AccountEvent, String>(initialState = 0) {
                id { it.accountId }
                on<AccountRegistered> { state, _ -> state + 1 }
            }

            assertThat(projection.criteria()).isEqualTo(DcbCriteria.all())
        }

        @Test
        fun `lets an explicit criteria override tags`() {
            val explicit = DcbCriteria.type("SomethingHappened")
            val projection = dcbProjection<Int, AccountEvent, String>(initialState = 0) {
                tags("username:bob")
                criteria(explicit)
                id { it.accountId }
                on<AccountRegistered> { state, _ -> state + 1 }
            }

            assertThat(projection.criteria()).isEqualTo(explicit)
        }

        @Test
        fun `criteria cannot be set twice`() {
            assertThatThrownBy {
                dcbProjection<Int, AccountEvent, String>(initialState = 0) {
                    criteria(DcbCriteria.all())
                    criteria(DcbCriteria.type("SomethingHappened"))
                    id { it.accountId }
                    on<AccountRegistered> { state, _ -> state + 1 }
                }
            }
                .isInstanceOf(IllegalStateException::class.java)
                .hasMessageContaining("criteria")
        }

        @Test
        fun `dcbSingletonProjection builds a single-instance descriptor with no id function`() {
            val projection = dcbSingletonProjection<Boolean, AccountEvent>(initialState = false) {
                tags("username:bob")
                on<AccountRegistered> { _, _ -> true }
            }

            assertThat(projection.projection().id()).isNull()
            assertThat(projection.projection().view().evolve(false, AccountRegistered("1", "bob"))).isTrue()
        }
    }

    @Nested
    inner class NoArgDsl {

        @Test
        fun `projection with no argument starts from null like initialState null`() {
            val projection = projection<Boolean?, AccountEvent, String> {
                id { it.accountId }
            }

            assertThat(projection.view().initialState()).isNull()
        }

        @Test
        fun `singletonProjection with no argument starts from null like initialState null`() {
            val projection = singletonProjection<Boolean?, AccountEvent> { }

            assertThat(projection.view().initialState()).isNull()
        }
    }
}
