package org.occurrent.example.domain.hotelbooking.features.guestmanagement.model

import org.occurrent.eventstore.api.dcb.Tag
import org.occurrent.example.domain.hotelbooking.common.GuestId

internal object GuestTags {
    fun guest(guestId: GuestId): Tag = Tag.of("guest", guestId.toString())
}
