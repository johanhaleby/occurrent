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

package org.occurrent.example.domain.courseenrollment.common

import org.occurrent.dsl.decider.Decider
import org.occurrent.dsl.decider.compose
import org.occurrent.example.domain.courseenrollment.features.coursemanagement.model.CourseState
import org.occurrent.example.domain.courseenrollment.features.coursemanagement.model.courseDecider
import org.occurrent.example.domain.courseenrollment.features.enrollment.model.EnrollmentState
import org.occurrent.example.domain.courseenrollment.features.enrollment.model.enrollmentDecider
import org.occurrent.example.domain.courseenrollment.features.studentmanagement.model.StudentState
import org.occurrent.example.domain.courseenrollment.features.studentmanagement.model.studentDecider

/**
 * Demonstration of the `compose` combinator from `org.occurrent.dsl.decider`.
 *
 * Each feature has its own decider over its own command and event types. The three decider `compose` overload adapts each
 * one to the shared [DomainCommand] and [DomainEvent] types itself, so the feature deciders are passed in directly, and
 * combines them into a single decider whose state is the [Triple] of the per-feature states. A command is routed to the
 * feature that recognizes it (the others produce no events), and each event evolves only the slice that understands it,
 * so the three feature states stay independent.
 *
 * This shows the decider algebra. It is not yet wired into a single `DcbApplicationService.execute` call, because that
 * needs a layer that carries a per-command DCB consistency boundary so one `execute` can route every command to the right
 * `DcbQuery`. Until then, each use case still executes its own feature decider against its own decision context (see the
 * `defineCourse`, `registerStudent`, and `enrollStudent` use cases). The single-feature use cases also use `adapt()`
 * directly, which is the common case where only the event type is widened.
 */
val courseEnrollmentDecider: Decider<DomainCommand, Triple<CourseState, StudentState, EnrollmentState>, DomainEvent> =
    compose(courseDecider, studentDecider, enrollmentDecider)
