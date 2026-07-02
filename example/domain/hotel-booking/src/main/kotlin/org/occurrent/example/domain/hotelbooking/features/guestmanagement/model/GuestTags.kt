package org.occurrent.example.domain.hotelbooking.features.guestmanagement.model

import org.occurrent.example.domain.hotelbooking.common.GuestId

internal object GuestTags {
    fun guest(guestId: GuestId): String = "guest:$guestId"
}
