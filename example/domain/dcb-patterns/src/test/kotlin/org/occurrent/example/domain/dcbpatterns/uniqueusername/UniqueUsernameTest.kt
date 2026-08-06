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

package org.occurrent.example.domain.dcbpatterns.uniqueusername

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper
import org.occurrent.application.service.blocking.dcb.DcbApplicationService
import org.occurrent.application.service.blocking.dcb.GenericDcbApplicationService
import org.occurrent.dsl.dcb.blocking.execute
import org.occurrent.eventstore.api.dcb.DcbAppendConditionNotFulfilledException
import org.occurrent.eventstore.api.dcb.DcbCriteria
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import org.occurrent.retry.RetryStrategy
import tools.jackson.module.kotlin.jacksonObjectMapper
import java.net.URI
import java.time.Instant
import java.util.*
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class UniqueUsernameTest {

    private val eventStore = InMemoryEventStore()
    private val converter: CloudEventConverter<UsernameEvent> = JacksonCloudEventConverter.Builder<UsernameEvent>(jacksonObjectMapper(), URI.create("urn:occurrent:example:dcb-patterns"))
        .typeMapper(ReflectionCloudEventTypeMapper.simple(UsernameEvent::class.java))
        .idMapper { it.eventId.toString() }
        .build()
    private val applicationService: DcbApplicationService<UsernameEvent> = GenericDcbApplicationService(eventStore, converter)

    @Test
    fun `register succeeds when the username is free`() {
        val now = Instant.parse("2026-01-01T00:00:00Z")

        applicationService.execute(UsernameCommand.RegisterAccount(UUID.randomUUID(), "johan", now), usernameDcbDecider)

        assertThat(eventStore.read(DcbCriteria.all()).events()).hasSize(1)
    }

    @Test
    fun `re-registering the same username is rejected`() {
        val now = Instant.parse("2026-01-01T00:00:00Z")
        applicationService.execute(UsernameCommand.RegisterAccount(UUID.randomUUID(), "johan", now), usernameDcbDecider)

        assertThatThrownBy {
            applicationService.execute(UsernameCommand.RegisterAccount(UUID.randomUUID(), "johan", now.plusSeconds(1)), usernameDcbDecider)
        }.isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("already taken")
    }

    @Test
    fun `registering again after close and the retention window is allowed`() {
        val accountId = UUID.randomUUID()
        val registeredAt = Instant.parse("2026-01-01T00:00:00Z")
        val closedAt = registeredAt.plusSeconds(60)
        applicationService.execute(UsernameCommand.RegisterAccount(accountId, "johan", registeredAt), usernameDcbDecider)
        applicationService.execute(UsernameCommand.CloseAccount(accountId, "johan", closedAt), usernameDcbDecider)

        val afterRetention = closedAt.plus(UsernamePolicy.RETENTION).plusSeconds(1)
        applicationService.execute(UsernameCommand.RegisterAccount(UUID.randomUUID(), "johan", afterRetention), usernameDcbDecider)

        assertThat(eventStore.read(DcbCriteria.all()).events()).hasSize(3)
    }

    @Test
    fun `registering again within the retention window is rejected`() {
        val accountId = UUID.randomUUID()
        val registeredAt = Instant.parse("2026-01-01T00:00:00Z")
        val closedAt = registeredAt.plusSeconds(60)
        applicationService.execute(UsernameCommand.RegisterAccount(accountId, "johan", registeredAt), usernameDcbDecider)
        applicationService.execute(UsernameCommand.CloseAccount(accountId, "johan", closedAt), usernameDcbDecider)

        val withinRetention = closedAt.plus(UsernamePolicy.RETENTION).minusSeconds(1)
        assertThatThrownBy {
            applicationService.execute(UsernameCommand.RegisterAccount(UUID.randomUUID(), "johan", withinRetention), usernameDcbDecider)
        }.isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("reserved")
    }

    @Test
    fun `concurrent double registration - only one wins`() {
        // A genuine race: both threads are released at the same instant (via the barrier) so both read the "free"
        // state before either has appended. Retries are disabled so a losing thread surfaces the raw append-condition
        // conflict from the store instead of quietly retrying into a business-rule rejection.
        val now = Instant.parse("2026-01-01T00:00:00Z")
        val racingService: DcbApplicationService<UsernameEvent> = GenericDcbApplicationService(eventStore, converter, RetryStrategy.none())
        val barrier = CyclicBarrier(2)
        val executor = Executors.newFixedThreadPool(2)

        val results = try {
            listOf(UUID.randomUUID(), UUID.randomUUID()).map { accountId ->
                executor.submit<Throwable?> {
                    barrier.await(10, TimeUnit.SECONDS)
                    try {
                        racingService.execute(UsernameCommand.RegisterAccount(accountId, "johan", now), usernameDcbDecider)
                        null
                    } catch (e: Throwable) {
                        e
                    }
                }
            }.map { it.get(10, TimeUnit.SECONDS) }
        } finally {
            executor.shutdownNow()
        }

        assertThat(results.count { it == null }).isEqualTo(1)
        val failure = results.single { it != null }!!
        assertThat(failure).isInstanceOfAny(IllegalArgumentException::class.java, DcbAppendConditionNotFulfilledException::class.java)
        assertThat(eventStore.read(DcbCriteria.all()).events()).hasSize(1)
    }
}
