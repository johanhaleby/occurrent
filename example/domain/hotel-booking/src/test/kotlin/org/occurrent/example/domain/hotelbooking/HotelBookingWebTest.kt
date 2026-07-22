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
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator
import org.junit.jupiter.api.Test
import org.occurrent.application.service.reactor.dcb.DcbApplicationService
import org.junit.jupiter.api.BeforeEach
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.http.MediaType
import org.occurrent.example.domain.hotelbooking.common.DomainEvent
import org.occurrent.example.domain.hotelbooking.features.booking.model.Stay
import org.occurrent.example.domain.hotelbooking.features.booking.usecases.bookRoom
import org.occurrent.example.domain.hotelbooking.features.guestmanagement.usecases.registerGuest
import org.occurrent.example.domain.hotelbooking.features.roommanagement.usecases.defineRoom
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.web.reactive.server.WebTestClient
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer
import org.testcontainers.mongodb.MongoDBContainer
import java.time.LocalDate
import java.util.*
import java.util.concurrent.TimeUnit

/**
 * WebFlux integration tests for the hotel-booking web layer, driven with [WebTestClient] bound to the application
 * context. The MongoDB replica set required for DCB transactions is started by Testcontainers and wired via
 * {@code @ServiceConnection}, mirroring the setup in {@link HotelBookingTest}.
 *
 * The dashboard is eventually consistent (fed by a DCB subscription), so dashboard assertions use Awaitility. The
 * room-detail read model is strongly consistent, so detail assertions are immediate.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@DisplayNameGeneration(DisplayNameGenerator.Simple::class)
class HotelBookingWebTest {

    companion object {
        @Container
        @ServiceConnection
        @JvmStatic
        val mongoDBContainer: MongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion()
    }

    @LocalServerPort
    var port: Int = 0

    lateinit var client: WebTestClient

    @BeforeEach
    fun setUp() {
        // Bind to the real Netty server on its random port, so form POSTs go through the full WebFlux pipeline.
        client = WebTestClient.bindToServer().baseUrl("http://localhost:$port").build()
    }

    @Autowired
    lateinit var applicationService: DcbApplicationService<DomainEvent>

    private val hotelId: UUID = UUID.randomUUID()

    private fun body(uri: String): String =
        client.get().uri(uri).exchange().expectStatus().isOk.expectBody(String::class.java).returnResult().responseBody ?: ""

    @Test
    fun `dashboard eventually reflects a defined room`() {
        val roomId = UUID.randomUUID()
        applicationService.defineRoom(hotelId, roomId, "W-101").block()

        await().atMost(10, TimeUnit.SECONDS).untilAsserted { assertThat(body("/dashboard")).contains("W-101") }
    }

    @Test
    fun `dashboard eventually lists a registered guest`() {
        val guestId = UUID.randomUUID()
        applicationService.registerGuest(guestId, "Grace Hopper").block()

        await().atMost(10, TimeUnit.SECONDS).untilAsserted { assertThat(body("/dashboard")).contains("Grace Hopper") }
    }

    @Test
    fun `GET room detail returns strongly-consistent view with room number`() {
        val roomId = UUID.randomUUID()
        applicationService.defineRoom(hotelId, roomId, "W-202").block()

        assertThat(body("/rooms/$roomId")).contains("W-202")
    }

    @Test
    fun `GET room detail shows a booked guest name after a seeded booking`() {
        val roomId = UUID.randomUUID()
        val guestId = UUID.randomUUID()
        applicationService.defineRoom(hotelId, roomId, "W-203").block()
        applicationService.registerGuest(guestId, "Alice Wonderland").block()
        applicationService.bookRoom(hotelId, roomId, guestId, Stay(LocalDate.parse("2026-08-01"), LocalDate.parse("2026-08-04"))).block()

        assertThat(body("/rooms/$roomId")).contains("Alice Wonderland")
    }

    @Test
    fun `POST booking an overlapping stay returns 200 with an inline error fragment, not 500`() {
        val roomId = UUID.randomUUID()
        val first = UUID.randomUUID()
        val second = UUID.randomUUID()
        applicationService.defineRoom(hotelId, roomId, "W-204").block()
        applicationService.registerGuest(first, "First Guest").block()
        applicationService.registerGuest(second, "Second Guest").block()
        applicationService.bookRoom(hotelId, roomId, first, Stay(LocalDate.parse("2026-08-01"), LocalDate.parse("2026-08-05"))).block()

        val responseBody = postForm("/rooms/$roomId/bookings", "guestId=$second&checkIn=2026-08-03&checkOut=2026-08-07")

        assertThat(responseBody).contains("feedback error").containsIgnoringCase("overlapping")
    }

    @Test
    fun `POST rooms and POST guests return 200 with a feedback fragment`() {
        assertThat(postForm("/rooms", "roomNumber=W-999")).contains("W-999")
        assertThat(postForm("/guests", "name=Carol+Danvers")).contains("Carol Danvers")
    }

    // Posts a urlencoded form body and returns the 200 response text.
    private fun postForm(uri: String, form: String): String =
        client.post().uri(uri)
            .contentType(MediaType.APPLICATION_FORM_URLENCODED)
            .bodyValue(form)
            .exchange().expectStatus().isOk.expectBody(String::class.java).returnResult().responseBody ?: ""
}
