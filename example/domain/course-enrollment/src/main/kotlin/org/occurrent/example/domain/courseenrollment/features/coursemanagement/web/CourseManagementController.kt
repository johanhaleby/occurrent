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

package org.occurrent.example.domain.courseenrollment.features.coursemanagement.web

import org.occurrent.application.service.blocking.dcb.DcbApplicationService
import org.occurrent.example.domain.courseenrollment.common.DomainEvent
import org.occurrent.example.domain.courseenrollment.features.coursemanagement.usecases.cancelCourse
import org.occurrent.example.domain.courseenrollment.features.coursemanagement.usecases.defineCourse
import org.springframework.stereotype.Controller
import org.springframework.ui.Model
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestParam
import java.util.UUID

@Controller
class CourseManagementController(private val applicationService: DcbApplicationService<DomainEvent>) {

    /**
     * Defines a course. The dashboard read model is eventually consistent, so the new course shows up via the dashboard
     * poll rather than in this response. We just report success or the domain rejection here.
     */
    @PostMapping("/courses")
    fun defineCourse(@RequestParam title: String, @RequestParam capacity: Int, model: Model): String =
        try {
            applicationService.defineCourse(UUID.randomUUID(), title, capacity)
            model.addAttribute("message", "Defined course \"$title\" with $capacity seats")
            "fragments/feedback :: feedback"
        } catch (e: Exception) {
            model.addAttribute("message", e.message ?: "Could not define course")
            model.addAttribute("error", true)
            "fragments/feedback :: feedback"
        }

    @PostMapping("/courses/{id}/cancellation")
    fun cancelCourse(@PathVariable id: UUID, model: Model): String =
        try {
            applicationService.cancelCourse(id)
            model.addAttribute("message", "Cancelled course")
            "fragments/feedback :: feedback"
        } catch (e: Exception) {
            model.addAttribute("message", e.message ?: "Could not cancel course")
            model.addAttribute("error", true)
            "fragments/feedback :: feedback"
        }
}
