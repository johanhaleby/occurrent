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

package org.occurrent.application.service.dcb.annotation

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.occurrent.annotation.DcbTag
import org.occurrent.eventstore.api.dcb.Tag

data class FieldTaggedEvent(
    @field:DcbTag val courseId: String,
    @field:DcbTag(key = "student") val studentId: String
)

data class GetterTaggedEvent(
    @get:DcbTag val courseId: String,
    @get:DcbTag(key = "student") val studentId: String
)

@Target(AnnotationTarget.FIELD, AnnotationTarget.PROPERTY_GETTER)
@Retention(AnnotationRetention.RUNTIME)
annotation class CustomKotlinTag(val key: String = "")

data class CustomKotlinTaggedEvent(
    @field:CustomKotlinTag val courseId: String,
    @get:CustomKotlinTag(key = "student") val studentId: String
)

data class NoTagsEvent(val id: String)

class AnnotationTagGeneratorKotlinTest {

    private val generator = AnnotationTagGenerator<Any>()

    @Test
    fun `field DcbTag use-site target produces tags`() {
        val tags = generator.tags(FieldTaggedEvent("course-1", "student-1"))

        assertThat(tags).containsExactlyInAnyOrder(
            Tag.of("courseId", "course-1"),
            Tag.of("student", "student-1")
        )
    }

    @Test
    fun `get DcbTag use-site target produces tags`() {
        val tags = generator.tags(GetterTaggedEvent("course-1", "student-1"))

        assertThat(tags).containsExactlyInAnyOrder(
            Tag.of("courseId", "course-1"),
            Tag.of("student", "student-1")
        )
    }

    @Test
    fun `kotlin data class with no DcbTag annotations produces empty set`() {
        val tags = generator.tags(NoTagsEvent("id-1"))

        assertThat(tags).isEmpty()
    }

    @Test
    fun `cache is reused across calls for the same kotlin class`() {
        val tags1 = generator.tags(FieldTaggedEvent("course-1", "student-1"))
        val tags2 = generator.tags(FieldTaggedEvent("course-2", "student-2"))

        assertThat(tags1).containsExactlyInAnyOrder(Tag.of("courseId", "course-1"), Tag.of("student", "student-1"))
        assertThat(tags2).containsExactlyInAnyOrder(Tag.of("courseId", "course-2"), Tag.of("student", "student-2"))
    }

    @Test
    fun `custom annotation field and getter use-site targets produce tags`() {
        val customGenerator = AnnotationTagGenerator<Any>(CustomKotlinTag::class.java)

        val tags = customGenerator.tags(CustomKotlinTaggedEvent("course-1", "student-1"))

        assertThat(tags).containsExactlyInAnyOrder(
            Tag.of("courseId", "course-1"),
            Tag.of("student", "student-1")
        )
    }
}
