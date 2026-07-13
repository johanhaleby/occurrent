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

import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.eventstore.api.dcb.DcbAppendCondition
import org.occurrent.eventstore.api.dcb.DcbCloudEvents
import org.occurrent.eventstore.api.dcb.DcbCriteria
import org.occurrent.eventstore.api.dcb.DcbEventStore
import org.occurrent.eventstore.api.dcb.DcbReadOptions
import org.occurrent.eventstore.api.dcb.Tag
import java.time.Instant
import java.util.UUID

private const val INVOICE_CREATED_TYPE = "InvoiceCreated"

data class InvoiceCreated(val eventId: UUID, val occurredAt: Instant, val number: Int)

/**
 * Pattern: a gapless, monotonically increasing sequence (invoice numbers, in most jurisdictions, must never skip or
 * repeat). This is the one vignette here that deliberately does NOT go through a [org.occurrent.dsl.dcb.DcbDecider]:
 * a decider folds the entire matching event history on every decision, which is O(n) in the number of invoices ever
 * created - fine for a boundary that is always small (one product, one sign-up), wrong for a boundary that grows
 * forever.
 * <p>
 * Instead this talks to the [DcbEventStore] directly:
 * 1. [DcbReadOptions.backwardsLimited] reads only the single highest-position `InvoiceCreated` event in one round
 *    trip, instead of folding every invoice ever created, to learn the last number issued.
 * 2. The append condition is still [DcbCriteria.type] scoped to `InvoiceCreated`, guarded by the read's
 *    [org.occurrent.eventstore.api.dcb.DcbEventStream.consistencyToken]. The token reflects the whole matching set
 *    observed at read time, not just the one returned event, so the append still fails if ANY `InvoiceCreated` -
 *    not just the last one this call happened to see - was committed after the read (see ADR 0056).
 * <p>
 * A conflict throws [org.occurrent.eventstore.api.dcb.DcbAppendConditionNotFulfilledException]; the caller decides
 * whether to retry (a retry re-reads the now-current last number, so retrying still produces a gapless sequence, see
 * the test).
 */
class InvoiceService(
    private val eventStore: DcbEventStore,
    private val converter: CloudEventConverter<InvoiceCreated>
) {
    fun createInvoice(occurredAt: Instant): Int {
        val stream = eventStore.read(DcbCriteria.type(INVOICE_CREATED_TYPE), DcbReadOptions.backwardsLimited(1))
        val lastNumber = stream.events().lastOrNull()?.let { converter.toDomainEvent(it).number } ?: 0
        val nextNumber = lastNumber + 1

        val domainEvent = InvoiceCreated(UUID.randomUUID(), occurredAt, nextNumber)
        val cloudEvent = DcbCloudEvents.withTags(converter.toCloudEvent(domainEvent), setOf(Tag.of("invoice", nextNumber.toString())))

        eventStore.append(listOf(cloudEvent), DcbAppendCondition.failIfEventsMatch(DcbCriteria.type(INVOICE_CREATED_TYPE), stream.consistencyToken()))
        return nextNumber
    }
}
