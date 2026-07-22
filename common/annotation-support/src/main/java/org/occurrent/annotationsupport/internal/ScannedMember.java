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

import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandle;

/**
 * An annotated member found by {@link AnnotatedMemberScanner}: the property name (a record component
 * name, a decapitalized getter name, or a field name), the {@link MethodHandle} bound to read it, and
 * the annotation instance found on it. The annotation is carried so a caller that derives an explicit
 * key from an annotation element can do so without re-scanning.
 *
 * @param propertyName the member's property name, used as the default key by callers that resolve one
 * @param accessor     a {@link MethodHandle} that reads the member's value from an instance
 * @param annotation   the annotation instance found on the member
 */
public record ScannedMember(String propertyName, MethodHandle accessor, Annotation annotation) {
}
