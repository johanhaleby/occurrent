package org.occurrent.example.domain.hotelbooking.features.guestmanagement.model

import org.occurrent.example.domain.hotelbooking.common.DomainEvent
import org.occurrent.example.domain.hotelbooking.common.GuestId
import java.time.Instant
import java.util.*

sealed interface GuestEvent : DomainEvent

/** A guest exists and may book rooms. Belongs to the guest boundary. */
data class GuestRegistered(
    override val eventId: UUID,
    override val occurredAt: Instant,
    val guestId: GuestId,
    val name: String
) : GuestEvent

/** A guest is no longer registered. Belongs to the guest boundary. */
data class GuestDeregistered(
    override val eventId: UUID,
    override val occurredAt: Instant,
    val guestId: GuestId
) : GuestEvent
