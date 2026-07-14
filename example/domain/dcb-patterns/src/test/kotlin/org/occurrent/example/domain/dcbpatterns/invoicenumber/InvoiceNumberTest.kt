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

package org.occurrent.example.domain.dcbpatterns.invoicenumber

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper
import org.occurrent.eventstore.api.dcb.DcbAppendCondition
import org.occurrent.eventstore.api.dcb.DcbAppendConditionNotFulfilledException
import org.occurrent.eventstore.api.dcb.DcbCloudEvents
import org.occurrent.eventstore.api.dcb.DcbCriteria
import org.occurrent.eventstore.api.dcb.DcbReadOptions
import org.occurrent.eventstore.api.dcb.Tag
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import tools.jackson.module.kotlin.jacksonObjectMapper
import java.net.URI
import java.time.Instant
import java.util.UUID

class InvoiceNumberTest {

    private val eventStore = InMemoryEventStore()
    private val converter: CloudEventConverter<InvoiceCreated> = JacksonCloudEventConverter.Builder<InvoiceCreated>(jacksonObjectMapper(), URI.create("urn:occurrent:example:dcb-patterns"))
        .typeMapper(ReflectionCloudEventTypeMapper.simple(InvoiceCreated::class.java))
        .idMapper { it.eventId.toString() }
        .build()
    private val service = InvoiceService(eventStore, converter)

    @Test
    fun `sequential invoices are gapless`() {
        val now = Instant.parse("2026-01-01T00:00:00Z")

        val numbers = (1..3).map { service.createInvoice(now) }

        assertThat(numbers).containsExactly(1, 2, 3)
    }

    @Test
    fun `a concurrent racer using a stale token is rejected, and a retry still yields a gapless number`() {
        val now = Instant.parse("2026-01-01T00:00:00Z")
        service.createInvoice(now) // invoice 1

        // Both "requests" read the store's last-invoice position before either appends: this is the read InvoiceService
        // does internally in createInvoice, replayed here by hand so we can hold on to the stale token.
        val staleRead = eventStore.read(DcbCriteria.type("InvoiceCreated"), DcbReadOptions.backwardsLimited(1))
        val staleLastNumber = converter.toDomainEvent(staleRead.events().last()).number
        val staleNextNumber = staleLastNumber + 1

        // The legitimate request goes through the service and wins, advancing the sequence to 2.
        val wonNumber = service.createInvoice(now)
        assertThat(wonNumber).isEqualTo(2)

        // The racer tries to append using the token it read before invoice 2 was committed: the append condition still
        // catches it, even though it only asked about the single last position, because the token reflects the whole
        // matching set observed at read time (see the class doc on InvoiceService and ADR 0056).
        val racerEvent = InvoiceCreated(UUID.randomUUID(), now, staleNextNumber)
        val racerCloudEvent = DcbCloudEvents.withTags(converter.toCloudEvent(racerEvent), setOf(Tag.of("invoice", staleNextNumber.toString())))
        assertThatThrownBy {
            eventStore.append(listOf(racerCloudEvent), DcbAppendCondition.failIfEventsMatch(DcbCriteria.type("InvoiceCreated"), staleRead.consistencyToken()))
        }.isInstanceOf(DcbAppendConditionNotFulfilledException::class.java)

        // Retrying (i.e. calling the service again, which re-reads the now-current last number) still produces a
        // gapless sequence: no gap (it's exactly 3, not 4) and no duplicate (it's not 2 again).
        val retriedNumber = service.createInvoice(now)
        assertThat(retriedNumber).isEqualTo(3)

        val allInvoiceNumbers = eventStore.read(DcbCriteria.type("InvoiceCreated")).events().map { converter.toDomainEvent(it).number }
        assertThat(allInvoiceNumbers).containsExactly(1, 2, 3)
    }
}
