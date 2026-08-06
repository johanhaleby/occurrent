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

package org.occurrent.example.domain.hotelbooking

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator
import org.junit.jupiter.api.Test
import org.occurrent.application.service.reactor.dcb.DcbApplicationService
import org.occurrent.example.domain.hotelbooking.common.DomainEvent
import org.occurrent.example.domain.hotelbooking.features.booking.model.BookingPolicy.MAX_ACTIVE_BOOKINGS_PER_GUEST
import org.occurrent.example.domain.hotelbooking.features.booking.model.Stay
import org.occurrent.example.domain.hotelbooking.features.booking.usecases.bookRoom
import org.occurrent.example.domain.hotelbooking.features.booking.usecases.cancelBooking
import org.occurrent.example.domain.hotelbooking.features.guestmanagement.usecases.deregisterGuest
import org.occurrent.example.domain.hotelbooking.features.guestmanagement.usecases.registerGuest
import org.occurrent.example.domain.hotelbooking.features.roommanagement.usecases.closeRoom
import org.occurrent.example.domain.hotelbooking.features.roommanagement.usecases.defineRoom
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.test.annotation.DirtiesContext
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.mongodb.MongoDBContainer
import reactor.core.publisher.Mono
import java.time.LocalDate
import java.util.*

/**
 * Test harness for the hotel-booking example on the reactive stack. The MongoDB replica set required for DCB
 * transactions is started by Testcontainers and wired in via {@code @ServiceConnection}, so no local setup is needed.
 *
 * The use cases are extension functions on the reactive [DcbApplicationService] returning {@code Mono}, so the test
 * autowires the service and blocks on the results to keep the assertions straightforward.
 */
@SpringBootTest
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@DisplayNameGeneration(DisplayNameGenerator.Simple::class)
class HotelBookingTest {

    companion object {
        @Container
        @ServiceConnection
        @JvmStatic
        val mongoDBContainer: MongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion()
    }

    @Autowired
    lateinit var applicationService: DcbApplicationService<DomainEvent>

    private val hotelId: UUID = UUID.randomUUID()

    private fun stay(from: String, to: String) = Stay(LocalDate.parse(from), LocalDate.parse(to))

    // Blocks on the use case Mono and returns whether it produced an append result (empty Mono = no-op).
    private fun <T : Any> Mono<T>.await(): T? = block()

    @Test
    fun `a room can be booked by a registered guest`() {
        val roomId = UUID.randomUUID()
        val guestId = UUID.randomUUID()
        applicationService.defineRoom(hotelId, roomId, "101").await()
        applicationService.registerGuest(guestId, "Ada Lovelace").await()

        val result = applicationService.bookRoom(hotelId, roomId, guestId, stay("2026-07-01", "2026-07-05")).await()

        assertThat(result).describedAs("a successful booking should produce an append result").isNotNull()
    }

    @Test
    fun `a room cannot be double-booked for an overlapping stay`() {
        val roomId = UUID.randomUUID()
        val first = UUID.randomUUID()
        val second = UUID.randomUUID()
        applicationService.defineRoom(hotelId, roomId, "102").await()
        applicationService.registerGuest(first, "First Guest").await()
        applicationService.registerGuest(second, "Second Guest").await()

        applicationService.bookRoom(hotelId, roomId, first, stay("2026-07-01", "2026-07-05")).await()

        assertThatThrownBy { applicationService.bookRoom(hotelId, roomId, second, stay("2026-07-03", "2026-07-07")).block() }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("overlapping")
    }

    @Test
    fun `non-overlapping stays for the same room both succeed`() {
        val roomId = UUID.randomUUID()
        val guestId = UUID.randomUUID()
        applicationService.defineRoom(hotelId, roomId, "103").await()
        applicationService.registerGuest(guestId, "Grace Hopper").await()

        // Half-open intervals, so checking out on the 5th and checking in on the 5th do not overlap.
        val firstBooking = applicationService.bookRoom(hotelId, roomId, guestId, stay("2026-07-01", "2026-07-05")).await()
        val secondBooking = applicationService.bookRoom(hotelId, roomId, guestId, stay("2026-07-05", "2026-07-09")).await()

        assertThat(firstBooking).isNotNull()
        assertThat(secondBooking).isNotNull()
    }

    @Test
    fun `a guest cannot exceed the per-guest active-bookings limit`() {
        val guestId = UUID.randomUUID()
        applicationService.registerGuest(guestId, "Busy Traveller").await()
        val rooms = List(MAX_ACTIVE_BOOKINGS_PER_GUEST + 1) { UUID.randomUUID() }
        rooms.forEachIndexed { i, roomId -> applicationService.defineRoom(hotelId, roomId, "20$i").await() }

        // Fill the guest up to the limit, one distinct room each.
        rooms.take(MAX_ACTIVE_BOOKINGS_PER_GUEST).forEach { roomId ->
            applicationService.bookRoom(hotelId, roomId, guestId, stay("2026-07-01", "2026-07-05")).await()
        }

        assertThatThrownBy { applicationService.bookRoom(hotelId, rooms.last(), guestId, stay("2026-07-01", "2026-07-05")).block() }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("active bookings")
    }

    @Test
    fun `cancelling a booking frees the slot so the room can be re-booked`() {
        val roomId = UUID.randomUUID()
        val first = UUID.randomUUID()
        val second = UUID.randomUUID()
        applicationService.defineRoom(hotelId, roomId, "104").await()
        applicationService.registerGuest(first, "First Guest").await()
        applicationService.registerGuest(second, "Second Guest").await()

        val theStay = stay("2026-07-01", "2026-07-05")
        applicationService.bookRoom(hotelId, roomId, first, theStay).await()

        // The overlapping second booking is rejected while the first is active.
        assertThatThrownBy { applicationService.bookRoom(hotelId, roomId, second, theStay).block() }
            .isInstanceOf(IllegalArgumentException::class.java)

        // After the first guest cancels, the freed slot can be re-booked by the second guest.
        applicationService.cancelBooking(roomId, first, theStay).await()
        val rebooked = applicationService.bookRoom(hotelId, roomId, second, theStay).await()

        assertThat(rebooked).describedAs("re-booking the freed slot should succeed").isNotNull()
    }

    @Test
    fun `a closed room cannot be booked`() {
        val roomId = UUID.randomUUID()
        val guestId = UUID.randomUUID()
        applicationService.defineRoom(hotelId, roomId, "105").await()
        applicationService.registerGuest(guestId, "Guest").await()
        applicationService.closeRoom(hotelId, roomId).await()

        assertThatThrownBy { applicationService.bookRoom(hotelId, roomId, guestId, stay("2026-07-01", "2026-07-05")).block() }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("closed")
    }

    @Test
    fun `an unregistered guest cannot book a room`() {
        val roomId = UUID.randomUUID()
        val guestId = UUID.randomUUID()
        applicationService.defineRoom(hotelId, roomId, "106").await()

        assertThatThrownBy { applicationService.bookRoom(hotelId, roomId, guestId, stay("2026-07-01", "2026-07-05")).block() }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("not registered")
    }

    @Test
    fun `a deregistered guest cannot book a room`() {
        val roomId = UUID.randomUUID()
        val guestId = UUID.randomUUID()
        applicationService.defineRoom(hotelId, roomId, "107").await()
        applicationService.registerGuest(guestId, "Leaving Guest").await()
        applicationService.deregisterGuest(guestId).await()

        assertThatThrownBy { applicationService.bookRoom(hotelId, roomId, guestId, stay("2026-07-01", "2026-07-05")).block() }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("deregistered")
    }
}
