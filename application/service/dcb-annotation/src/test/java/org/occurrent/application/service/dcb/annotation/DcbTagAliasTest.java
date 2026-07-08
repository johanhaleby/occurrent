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

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.DcbTag;
import org.occurrent.application.service.dcb.annotation.AnnotationTagGenerator.AnnotationTagGeneratorException;
import org.occurrent.eventstore.api.dcb.Tag;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbTagAliasTest {

    private final AnnotationTagGenerator<Object> generator = new AnnotationTagGenerator<>();

    record ShorthandValue(@DcbTag("clinician") String id) {
    }

    record NamedKey(@DcbTag(key = "clinician") String id) {
    }

    record Conflicting(@DcbTag(value = "a", key = "b") String id) {
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.RECORD_COMPONENT)
    @interface CustomTag {
        String value() default "";

        String key() default "";
    }

    record ValueBlankKeySet(@CustomTag(key = "student") String id) {
    }

    @Test
    void value_shorthand_and_key_alias_produce_the_same_tag() {
        assertThat(generator.tags(new ShorthandValue("1"))).containsExactly(Tag.of("clinician", "1"));
        assertThat(generator.tags(new NamedKey("1"))).containsExactly(Tag.of("clinician", "1"));
    }

    @Test
    void setting_both_value_and_key_to_different_strings_is_rejected() {
        assertThatThrownBy(() -> generator.tags(new Conflicting("1")))
                .isInstanceOf(AnnotationTagGeneratorException.class)
                .hasMessageContaining("conflicting");
    }

    @Test
    void a_custom_annotation_with_both_elements_falls_back_to_key_when_value_is_blank() {
        AnnotationTagGenerator<Object> custom = new AnnotationTagGenerator<>(CustomTag.class);
        assertThat(custom.tags(new ValueBlankKeySet("s1"))).containsExactly(Tag.of("student", "s1"));
    }
}
