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

package org.occurrent.example.domain.courseenrollment.features.enrollment.web

import org.occurrent.application.service.blocking.dcb.DcbApplicationService
import org.occurrent.dsl.dcb.blocking.DcbSubscriptions
import org.occurrent.example.domain.courseenrollment.common.DomainEvent
import org.occurrent.example.domain.courseenrollment.features.coursedashboard.readmodel.CourseDashboard
import org.occurrent.example.domain.courseenrollment.features.enrollment.model.StudentEnrolledInCourse
import org.occurrent.example.domain.courseenrollment.features.enrollment.model.StudentUnenrolledFromCourse
import org.occurrent.example.domain.courseenrollment.features.enrollment.readmodel.CourseDetail
import org.occurrent.example.domain.courseenrollment.features.enrollment.usecases.enrollStudent
import org.occurrent.example.domain.courseenrollment.features.enrollment.usecases.unenrollStudent
import org.occurrent.example.domain.courseenrollment.infrastructure.dcb.CourseEnrollmentDcbQueries
import org.occurrent.subscription.api.blocking.SubscriptionModel
import org.springframework.stereotype.Controller
import org.springframework.ui.Model
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter
import java.util.UUID

@Controller
class EnrollmentController(
    private val applicationService: DcbApplicationService<DomainEvent>,
    private val courseDetail: CourseDetail,
    private val courseDashboard: CourseDashboard,
    private val dcbSubscriptions: DcbSubscriptions<DomainEvent>,
    private val subscriptionModel: SubscriptionModel
) {

    @GetMapping("/courses/{id}")
    fun detail(@PathVariable id: UUID, model: Model): String {
        val view = courseDetail.of(id) ?: return "redirect:/"
        model.addAttribute("course", view)
        model.addAttribute("students", courseDashboard.students())
        return "enrollment/detail"
    }

    @PostMapping("/courses/{id}/enrollments")
    fun enroll(@PathVariable id: UUID, @RequestParam studentId: UUID, model: Model): String {
        runCatching { applicationService.enrollStudent(id, studentId) }
            .onFailure { model.addAttribute("message", it.message ?: "Could not enroll"); model.addAttribute("error", true) }
        return detailFragment(id, model)
    }

    @PostMapping("/courses/{id}/unenrollments")
    fun unenroll(@PathVariable id: UUID, @RequestParam studentId: UUID, model: Model): String {
        runCatching { applicationService.unenrollStudent(id, studentId) }
            .onFailure { model.addAttribute("message", it.message ?: "Could not unenroll"); model.addAttribute("error", true) }
        return detailFragment(id, model)
    }

    // The fragment is built from the strongly-consistent read, so it reflects the enrollment that just succeeded with no lag.
    private fun detailFragment(id: UUID, model: Model): String {
        model.addAttribute("course", courseDetail.of(id))
        model.addAttribute("students", courseDashboard.students())
        return "enrollment/detail :: detail"
    }

    /**
     * A live activity feed for a single course, scoped by the course tag. This is the genuinely tag-scoped DCB
     * subscription: it sees only this course's boundary, including the cross-boundary enrollment events. The
     * per-connection subscription is cancelled when the SSE stream ends so it does not leak.
     */
    @GetMapping("/courses/{id}/activity")
    fun activity(@PathVariable id: UUID): SseEmitter {
        val emitter = SseEmitter(EMITTER_TIMEOUT_MILLIS)
        val subscriptionId = "activity-$id-${UUID.randomUUID()}"
        val subscription = dcbSubscriptions.subscribe(subscriptionId, CourseEnrollmentDcbQueries.courseDecisionContext(id)) { event ->
            // Only a UUID is interpolated into this HTML, so it needs no escaping. Do not interpolate a name here.
            val line = when (event) {
                is StudentEnrolledInCourse -> "<li>Student ${event.studentId} enrolled</li>"
                is StudentUnenrolledFromCourse -> "<li>Student ${event.studentId} unenrolled</li>"
                else -> null
            }
            if (line != null) {
                try {
                    emitter.send(SseEmitter.event().name("activity").data(line))
                } catch (e: Exception) {
                    emitter.completeWithError(e)
                }
            }
        }
        emitter.onCompletion { subscriptionModel.cancelSubscription(subscriptionId) }
        emitter.onTimeout { subscriptionModel.cancelSubscription(subscriptionId) }
        emitter.onError { subscriptionModel.cancelSubscription(subscriptionId) }
        // Wait until the subscription is running before returning, so the feed does not miss the first action after the
        // page opens.
        subscription.waitUntilStarted()
        return emitter
    }

    companion object {
        private const val EMITTER_TIMEOUT_MILLIS = 10L * 60L * 1000L
    }
}
