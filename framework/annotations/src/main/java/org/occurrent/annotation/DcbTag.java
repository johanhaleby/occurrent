/*
 *
 *  Copyright 2026 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.annotation;

import java.lang.annotation.*;

/**
 * Marks a member of a domain event as a Dynamic Consistency Boundary (DCB) tag source.
 * <p>
 * The resulting tag is {@code key:value}, where the key comes from {@link #value()} (or its alias
 * {@link #key()}) and, when both are left empty, the annotated member's name (the record component
 * name, the field name, or the property name derived from a getter method). The tag value is the
 * annotated member's runtime value, converted with {@code toString()}. A null value, or a value
 * whose {@code toString()} is blank, produces no tag for that member and is skipped rather than
 * failing.
 * <p>
 * The key is the annotation's {@code value}, so it can be given in shorthand: {@code @DcbTag("clinician")}.
 * {@code @DcbTag(key = "clinician")} means the same thing.
 * <p>
 * This annotation may be placed on a record component, a field, or a no-arg method. On a Kotlin
 * data class, use the {@code @field} or {@code @get} use-site targets to apply it to the backing
 * field or the generated getter.
 * <p>
 * The resulting key and value follow the constraints of the underlying tag type: the key may not
 * contain {@code :}, and neither the key nor the value may contain a newline. An explicit key or a
 * runtime value that breaks these rules fails when the tag is derived, not when this annotation is
 * applied.
 */
@Target({ElementType.RECORD_COMPONENT, ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface DcbTag {

    /**
     * The tag key. When empty, {@link #key()} is used, and when both are empty the annotated member's
     * name (record component, field, or property derived from a getter method) is used instead.
     */
    String value() default "";

    /**
     * An alias for {@link #value()}, for when naming the element reads better: {@code @DcbTag(key = "clinician")}.
     * Setting both {@code value} and {@code key} to different non-blank strings is an error.
     */
    String key() default "";
}
