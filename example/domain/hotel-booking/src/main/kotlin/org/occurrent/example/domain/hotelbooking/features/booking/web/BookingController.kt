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

package org.occurrent.example.domain.hotelbooking.features.booking.web

import org.occurrent.application.service.reactor.dcb.DcbApplicationService
import org.occurrent.dsl.dcb.reactor.DcbSubscriptions
import org.occurrent.example.domain.hotelbooking.common.DomainEvent
import org.occurrent.example.domain.hotelbooking.features.booking.model.BookingCancelled
import org.occurrent.example.domain.hotelbooking.features.booking.model.RoomBooked
import org.occurrent.example.domain.hotelbooking.features.booking.model.Stay
import org.occurrent.example.domain.hotelbooking.features.booking.readmodel.RoomDetail
import org.occurrent.example.domain.hotelbooking.features.booking.usecases.bookRoom
import org.occurrent.example.domain.hotelbooking.features.booking.usecases.cancelBooking
import org.occurrent.example.domain.hotelbooking.features.hoteldashboard.readmodel.HotelDashboard
import org.occurrent.example.domain.hotelbooking.infrastructure.dcb.HotelBookingCriteria
import org.springframework.format.annotation.DateTimeFormat
import org.springframework.http.MediaType
import org.springframework.http.codec.ServerSentEvent
import org.springframework.stereotype.Controller
import org.springframework.ui.Model
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.ModelAttribute
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.util.HtmlUtils
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.LocalDate
import java.util.*

/**
 * Form-backed booking command. On WebFlux, @RequestParam reads only query params, so form bodies bind via
 * @ModelAttribute. The dates are ISO (yyyy-MM-dd) from the HTML date inputs.
 */
data class BookingForm(
    var guestId: UUID? = null,
    @field:DateTimeFormat(iso = DateTimeFormat.ISO.DATE) var checkIn: LocalDate? = null,
    @field:DateTimeFormat(iso = DateTimeFormat.ISO.DATE) var checkOut: LocalDate? = null
)

@Controller
class BookingController(
    private val applicationService: DcbApplicationService<DomainEvent>,
    private val roomDetail: RoomDetail,
    private val hotelDashboard: HotelDashboard,
    private val dcbSubscriptions: DcbSubscriptions<DomainEvent>
) {

    // A single hotel is assumed for the demo, so a fixed hotel id keeps the UI free of hotel plumbing.
    private val hotelId: UUID = UUID.fromString("00000000-0000-0000-0000-0000000000a1")

    @GetMapping("/rooms/{id}")
    fun detail(@PathVariable id: UUID, model: Model): Mono<String> =
        roomDetail.of(id)
            .map { view ->
                model.addAttribute("room", view)
                model.addAttribute("guests", hotelDashboard.guests())
                "booking/detail"
            }
            .defaultIfEmpty("redirect:/")

    @PostMapping("/rooms/{id}/bookings")
    fun book(@PathVariable id: UUID, @ModelAttribute form: BookingForm, model: Model): Mono<String> =
        Mono.fromCallable { Stay(form.checkIn!!, form.checkOut!!) }
            .flatMap { stay -> applicationService.bookRoom(hotelId, id, form.guestId!!, stay) }
            .then(Mono.defer { detailFragment(id, model) })
            .onErrorResume { e ->
                model.addAttribute("message", e.message ?: "Could not book")
                model.addAttribute("error", true)
                detailFragment(id, model)
            }

    @PostMapping("/rooms/{id}/cancellations")
    fun cancel(@PathVariable id: UUID, @ModelAttribute form: BookingForm, model: Model): Mono<String> =
        Mono.fromCallable { Stay(form.checkIn!!, form.checkOut!!) }
            .flatMap { stay -> applicationService.cancelBooking(id, form.guestId!!, stay) }
            .then(Mono.defer { detailFragment(id, model) })
            .onErrorResume { e ->
                model.addAttribute("message", e.message ?: "Could not cancel")
                model.addAttribute("error", true)
                detailFragment(id, model)
            }

    // The fragment is built from the strongly-consistent read, so it reflects the booking that just succeeded with no lag.
    private fun detailFragment(id: UUID, model: Model): Mono<String> =
        roomDetail.of(id).map { view ->
            model.addAttribute("room", view)
            model.addAttribute("guests", hotelDashboard.guests())
            "booking/detail :: detail"
        }.defaultIfEmpty("redirect:/")

    /**
     * A live activity feed for a single room, scoped by the room tag. This is where the reactive stack shines: the
     * endpoint returns the subscription's [Flux] directly, mapped into server-sent events. WebFlux cancels the
     * underlying DCB subscription automatically when the client disconnects, so unlike the blocking SseEmitter version
     * there is no manual subscription id, cancel callbacks, or waitUntilStarted bookkeeping to get right.
     */
    @GetMapping("/rooms/{id}/activity", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun activity(@PathVariable id: UUID): Flux<ServerSentEvent<String>> =
        dcbSubscriptions.subscribe(HotelBookingCriteria.roomCriteria(id))
            .mapNotNull { event ->
                // The guest name is user input, so it is HTML-escaped before going into this raw SSE fragment.
                val line = when (event) {
                    is RoomBooked -> "<li>${nameOf(event.guestId)} booked ${event.stay.checkIn} to ${event.stay.checkOut}</li>"
                    is BookingCancelled -> "<li>${nameOf(event.guestId)} cancelled ${event.stay.checkIn} to ${event.stay.checkOut}</li>"
                    else -> null
                }
                line?.let { ServerSentEvent.builder(it).event("activity").build() }
            }

    // Resolve the guest name from the eventually-consistent dashboard, falling back to the id, and HTML-escape it
    // because it is interpolated into a raw SSE fragment.
    private fun nameOf(guestId: UUID): String =
        HtmlUtils.htmlEscape(hotelDashboard.guestName(guestId) ?: guestId.toString())
}
