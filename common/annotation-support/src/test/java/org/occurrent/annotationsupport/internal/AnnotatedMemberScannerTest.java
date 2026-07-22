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

package org.occurrent.annotationsupport.internal;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.List;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.RECORD_COMPONENT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("AnnotatedMemberScanner")
class AnnotatedMemberScannerTest {

    private final AnnotatedMemberScanner scanner = new AnnotatedMemberScanner(Marker.class);

    @Test
    @DisplayName("finds annotated record components in declaration order and skips unannotated ones")
    void scansRecordComponents() {
        record CourseRegistered(@Marker String courseId, @Marker String studentId, String ignored) {
        }

        List<ScannedMember> members = scanner.scan(CourseRegistered.class);

        assertThat(members).extracting(ScannedMember::propertyName).containsExactly("courseId", "studentId");
        assertThat(read(members.get(0), new CourseRegistered("c1", "s1", "x"))).isEqualTo("c1");
        assertThat(read(members.get(1), new CourseRegistered("c1", "s1", "x"))).isEqualTo("s1");
    }

    @Test
    @DisplayName("finds an annotated getter and derives the property name from it")
    void scansAnnotatedGetter() {
        List<ScannedMember> members = scanner.scan(AnnotatedGetter.class);

        assertThat(members).extracting(ScannedMember::propertyName).containsExactly("name");
        assertThat(read(members.get(0), new AnnotatedGetter("Ada"))).isEqualTo("Ada");
    }

    @Test
    @DisplayName("reads an annotated field through its getter when one exists")
    void prefersGetterForAnnotatedField() {
        List<ScannedMember> members = scanner.scan(AnnotatedFieldWithGetter.class);

        assertThat(members).extracting(ScannedMember::propertyName).containsExactly("value");
        // The getter uppercases, so reading "via getter" proves the getter was bound rather than the raw field.
        assertThat(read(members.get(0), new AnnotatedFieldWithGetter("abc"))).isEqualTo("ABC");
    }

    @Test
    @DisplayName("reads an annotated field directly when it has no getter")
    void fallsBackToFieldWithoutGetter() {
        List<ScannedMember> members = scanner.scan(AnnotatedFieldNoGetter.class);

        assertThat(members).extracting(ScannedMember::propertyName).containsExactly("id");
        assertThat(read(members.get(0), new AnnotatedFieldNoGetter("id-1"))).isEqualTo("id-1");
    }

    @Test
    @DisplayName("collapses a property annotated on both its getter and its field into a single member")
    void collapsesGetterAndFieldOfSameProperty() {
        List<ScannedMember> members = scanner.scan(GetterAndField.class);

        assertThat(members).extracting(ScannedMember::propertyName).containsExactly("id");
        // The getter wins, so its uppercasing is observed.
        assertThat(read(members.get(0), new GetterAndField("abc"))).isEqualTo("ABC");
    }

    @Test
    @DisplayName("finds annotated members declared on a superclass")
    void walksTheClassHierarchy() {
        List<ScannedMember> members = scanner.scan(Sub.class);

        assertThat(members).extracting(ScannedMember::propertyName).containsExactlyInAnyOrder("base", "extra");
    }

    @Test
    @DisplayName("returns no members when nothing is annotated")
    void returnsEmptyWhenAbsent() {
        assertThat(scanner.scan(NoMarkers.class)).isEmpty();
    }

    @Test
    @DisplayName("caches the result per class")
    void cachesPerClass() {
        assertThat(scanner.scan(AnnotatedGetter.class)).isSameAs(scanner.scan(AnnotatedGetter.class));
    }

    @Test
    @DisplayName("exposes the annotation type it scans for")
    void exposesAnnotationType() {
        assertThat(scanner.annotationType()).isEqualTo(Marker.class);
    }

    @Test
    @DisplayName("rejects an annotation that is not retained at runtime")
    void rejectsNonRuntimeAnnotation() {
        assertThatThrownBy(() -> new AnnotatedMemberScanner(NotRuntime.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("@Retention(RUNTIME)");
    }

    @Test
    @DisplayName("rejects a null annotation type")
    void rejectsNullAnnotationType() {
        assertThatThrownBy(() -> new AnnotatedMemberScanner(null))
                .isInstanceOf(NullPointerException.class);
    }

    private static Object read(ScannedMember member, Object target) {
        try {
            return member.accessor().invoke(target);
        } catch (Throwable t) {
            throw new AssertionError("Failed to read member " + member.propertyName(), t);
        }
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target({METHOD, FIELD, RECORD_COMPONENT})
    private @interface Marker {
    }

    @Retention(RetentionPolicy.CLASS)
    @Target({METHOD, FIELD})
    private @interface NotRuntime {
    }

    static final class AnnotatedGetter {
        private final String name;

        AnnotatedGetter(String name) {
            this.name = name;
        }

        @Marker
        String getName() {
            return name;
        }
    }

    static final class AnnotatedFieldWithGetter {
        @Marker
        private final String value;

        AnnotatedFieldWithGetter(String value) {
            this.value = value;
        }

        String getValue() {
            return value.toUpperCase();
        }
    }

    static final class AnnotatedFieldNoGetter {
        @Marker
        private final String id;

        AnnotatedFieldNoGetter(String id) {
            this.id = id;
        }
    }

    static final class GetterAndField {
        @Marker
        private final String id;

        GetterAndField(String id) {
            this.id = id;
        }

        @Marker
        String getId() {
            return id.toUpperCase();
        }
    }

    static class Base {
        @Marker
        private final String base;

        Base(String base) {
            this.base = base;
        }
    }

    static final class Sub extends Base {
        @Marker
        private final String extra;

        Sub(String base, String extra) {
            super(base);
            this.extra = extra;
        }
    }

    static final class NoMarkers {
        @SuppressWarnings("unused")
        private final String plain = "nope";
    }
}
