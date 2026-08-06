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

package org.occurrent.example.domain.hotelbooking.features.roommanagement.web

import org.occurrent.application.service.reactor.dcb.DcbApplicationService
import org.occurrent.example.domain.hotelbooking.common.DomainEvent
import org.occurrent.example.domain.hotelbooking.features.roommanagement.usecases.closeRoom
import org.occurrent.example.domain.hotelbooking.features.roommanagement.usecases.defineRoom
import org.springframework.stereotype.Controller
import org.springframework.ui.Model
import org.springframework.web.bind.annotation.ModelAttribute
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import reactor.core.publisher.Mono
import java.util.*

/** Form-backed command. On WebFlux, @RequestParam reads only query params, so form bodies bind via @ModelAttribute. */
data class DefineRoomForm(var roomNumber: String = "")

@Controller
class RoomManagementController(private val applicationService: DcbApplicationService<DomainEvent>) {

    // A single hotel is assumed for the demo, so a fixed hotel id keeps the UI free of hotel plumbing.
    private val hotelId: UUID = UUID.fromString("00000000-0000-0000-0000-0000000000a1")

    /**
     * Defines a room. The dashboard read model is eventually consistent, so the new room shows up via the dashboard poll
     * rather than in this response. We just report success or the domain rejection here.
     */
    @PostMapping("/rooms")
    fun defineRoom(@ModelAttribute form: DefineRoomForm, model: Model): Mono<String> =
        applicationService.defineRoom(hotelId, UUID.randomUUID(), form.roomNumber)
            .then(Mono.fromCallable {
                model.addAttribute("message", "Defined room \"${form.roomNumber}\"")
                "fragments/feedback :: feedback"
            })
            .onErrorResume { e -> Mono.fromCallable {
                model.addAttribute("message", e.message ?: "Could not define room")
                model.addAttribute("error", true)
                "fragments/feedback :: feedback"
            } }

    @PostMapping("/rooms/{id}/closure")
    fun closeRoom(@PathVariable id: UUID, model: Model): Mono<String> =
        applicationService.closeRoom(hotelId, id)
            .then(Mono.fromCallable {
                model.addAttribute("message", "Closed room")
                "fragments/feedback :: feedback"
            })
            .onErrorResume { e -> Mono.fromCallable {
                model.addAttribute("message", e.message ?: "Could not close room")
                model.addAttribute("error", true)
                "fragments/feedback :: feedback"
            } }
}
