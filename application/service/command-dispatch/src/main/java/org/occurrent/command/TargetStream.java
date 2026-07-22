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

package org.occurrent.command;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks the command member (a record component, field, or no-arg getter) whose value is the target stream id, so a
 * reflection-based {@link StreamIdResolver} can route the command without a hand-written {@code command -> streamId}
 * function. Exactly one property of a command must carry this annotation, with a getter and its backing field counting
 * as one. Its runtime value, converted with {@code toString()}, is the stream id (a {@code null} or blank value is an
 * error, since the command cannot be routed).
 * <p>
 * It may be placed on a record component, a field, or a no-arg getter method. On a Kotlin data class, use the
 * {@code @field} or {@code @get} use-site targets to apply it to the backing field or the generated getter.
 * <p>
 * This is the command, write-side counterpart of the event, DCB tag-side {@code @DcbTag}. It is unrelated to the
 * subscription-handler {@code @StreamId} parameter annotation, which binds the delivered event's stream id into a
 * handler parameter on the read side.
 */
@Target({ElementType.RECORD_COMPONENT, ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface TargetStream {
}
