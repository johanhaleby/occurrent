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

package org.occurrent.example.domain.hotelbooking.features.guestmanagement.web

import org.occurrent.application.service.reactor.dcb.DcbApplicationService
import org.occurrent.example.domain.hotelbooking.common.DomainEvent
import org.occurrent.example.domain.hotelbooking.features.guestmanagement.usecases.deregisterGuest
import org.occurrent.example.domain.hotelbooking.features.guestmanagement.usecases.registerGuest
import org.springframework.stereotype.Controller
import org.springframework.ui.Model
import org.springframework.web.bind.annotation.ModelAttribute
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import reactor.core.publisher.Mono
import java.util.UUID

/** Form-backed command. On WebFlux, @RequestParam reads only query params, so form bodies bind via @ModelAttribute. */
data class RegisterGuestForm(var name: String = "")

@Controller
class GuestManagementController(private val applicationService: DcbApplicationService<DomainEvent>) {

    @PostMapping("/guests")
    fun registerGuest(@ModelAttribute form: RegisterGuestForm, model: Model): Mono<String> =
        applicationService.registerGuest(UUID.randomUUID(), form.name)
            .then(Mono.fromCallable {
                model.addAttribute("message", "Registered guest \"${form.name}\"")
                "fragments/feedback :: feedback"
            })
            .onErrorResume { e -> Mono.fromCallable {
                model.addAttribute("message", e.message ?: "Could not register guest")
                model.addAttribute("error", true)
                "fragments/feedback :: feedback"
            } }

    @PostMapping("/guests/{id}/deregistration")
    fun deregisterGuest(@PathVariable id: UUID, model: Model): Mono<String> =
        applicationService.deregisterGuest(id)
            .then(Mono.fromCallable {
                model.addAttribute("message", "Deregistered guest")
                "fragments/feedback :: feedback"
            })
            .onErrorResume { e -> Mono.fromCallable {
                model.addAttribute("message", e.message ?: "Could not deregister guest")
                model.addAttribute("error", true)
                "fragments/feedback :: feedback"
            } }
}
