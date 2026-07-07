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

package org.occurrent.application.service.dcb.annotation;

import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.dcb.Tag;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Set;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.RECORD_COMPONENT;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AnnotationTagGeneratorTest {

    private final AnnotationTagGenerator<Object> generator = new AnnotationTagGenerator<>();

    @Test
    void extracts_tags_from_record_components_with_explicit_and_default_keys() {
        Set<Tag> tags = generator.tags(new CourseRegistered("course-1", "student-1"));

        assertThat(tags).containsExactlyInAnyOrder(
                Tag.of("courseId", "course-1"),
                Tag.of("studentId", "student-1")
        );
    }

    @Test
    void default_key_uses_the_record_component_name() {
        Set<Tag> tags = generator.tags(new EmailTag("a:b@x"));

        assertThat(tags).containsExactly(Tag.of("email", "a:b@x"));
        assertThat(tags.iterator().next().canonical()).isEqualTo("email:a:b@x");
    }

    @Test
    void null_component_value_is_skipped() {
        Set<Tag> tags = generator.tags(new NullableValue(null));

        assertThat(tags).isEmpty();
    }

    @Test
    void blank_component_value_is_skipped() {
        Set<Tag> tags = generator.tags(new BlankValue("   "));

        assertThat(tags).isEmpty();
    }

    @Test
    void record_with_no_dcb_tag_annotations_produces_empty_set() {
        Set<Tag> tags = generator.tags(new NoTags("id-1"));

        assertThat(tags).isEmpty();
    }

    @Test
    void cache_is_reused_across_multiple_calls_for_the_same_class() {
        CourseRegistered event1 = new CourseRegistered("course-1", "student-1");
        CourseRegistered event2 = new CourseRegistered("course-2", "student-2");

        Set<Tag> tags1 = generator.tags(event1);
        Set<Tag> tags2 = generator.tags(event2);

        assertThat(tags1).containsExactlyInAnyOrder(Tag.of("courseId", "course-1"), Tag.of("studentId", "student-1"));
        assertThat(tags2).containsExactlyInAnyOrder(Tag.of("courseId", "course-2"), Tag.of("studentId", "student-2"));
    }

    @Test
    void multiple_tags_have_deterministic_set_contents() {
        Set<Tag> tags = generator.tags(new CourseRegistered("course-1", "student-1"));

        assertThat(tags).hasSize(2);
        assertThat(tags).allSatisfy(tag -> assertThat(tag.canonical()).contains(":"));
    }

    @Test
    void works_for_a_package_private_event_class() {
        Set<Tag> tags = generator.tags(new PackagePrivateCourseRegistered("course-1", "student-1"));

        assertThat(tags).containsExactlyInAnyOrder(
                Tag.of("courseId", "course-1"),
                Tag.of("studentId", "student-1")
        );
    }

    @Test
    void custom_annotation_without_key_element_uses_member_name_as_tag_key() {
        AnnotationTagGenerator<Object> customGenerator = new AnnotationTagGenerator<>(BoundaryTag.class);

        Set<Tag> tags = customGenerator.tags(new CustomMemberNameTags("course-1", "student-1"));

        assertThat(tags).containsExactlyInAnyOrder(Tag.of("courseId", "course-1"), Tag.of("studentId", "student-1"));
    }

    @Test
    void custom_annotation_with_key_element_uses_explicit_key_and_falls_back_to_member_name_for_blank_key() {
        AnnotationTagGenerator<Object> customGenerator = new AnnotationTagGenerator<>(CustomTag.class);

        Set<Tag> tags = customGenerator.tags(new CustomKeyTags("course-1", "student-1"));

        assertThat(tags).containsExactlyInAnyOrder(Tag.of("course", "course-1"), Tag.of("studentId", "student-1"));
    }

    @Test
    void custom_annotation_can_use_explicit_key_resolver_for_nonstandard_attribute() {
        AnnotationTagGenerator<Object> customGenerator = new AnnotationTagGenerator<>(NamedTag.class, NamedTag::name);

        Set<Tag> tags = customGenerator.tags(new CustomNamedTags("course-1", "student-1"));

        assertThat(tags).containsExactlyInAnyOrder(Tag.of("course", "course-1"), Tag.of("studentId", "student-1"));
    }

    @Test
    void custom_key_resolver_null_result_falls_back_to_member_name() {
        AnnotationTagGenerator<Object> customGenerator = new AnnotationTagGenerator<>(NamedTag.class, __ -> null);

        Set<Tag> tags = customGenerator.tags(new CustomNamedTags("course-1", "student-1"));

        assertThat(tags).containsExactlyInAnyOrder(Tag.of("courseId", "course-1"), Tag.of("studentId", "student-1"));
    }

    @Test
    void constructor_rejects_null_annotation_type() {
        assertThatThrownBy(() -> new AnnotationTagGenerator<>((Class<BoundaryTag>) null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Annotation type cannot be null");
    }

    @Test
    void constructor_rejects_null_key_resolver() {
        assertThatThrownBy(() -> new AnnotationTagGenerator<>(BoundaryTag.class, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Key resolver cannot be null");
    }

    @Target({RECORD_COMPONENT, FIELD, METHOD})
    @Retention(RUNTIME)
    private @interface BoundaryTag {
    }

    @Target({RECORD_COMPONENT, FIELD, METHOD})
    @Retention(RUNTIME)
    private @interface CustomTag {
        String key() default "";
    }

    @Target({RECORD_COMPONENT, FIELD, METHOD})
    @Retention(RUNTIME)
    private @interface NamedTag {
        String name() default "";
    }

    private record CustomMemberNameTags(@BoundaryTag String courseId, @BoundaryTag String studentId) {
    }

    private record CustomKeyTags(@CustomTag(key = "course") String courseId, @CustomTag String studentId) {
    }

    private record CustomNamedTags(@NamedTag(name = "course") String courseId, @NamedTag String studentId) {
    }
}
