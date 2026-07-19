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

package org.occurrent.dsl.dcb

import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.converter.typemapper.CloudEventTypeGetter
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper
import org.occurrent.eventstore.api.dcb.DcbCriteria
import org.occurrent.eventstore.api.dcb.DcbCriterion
import org.occurrent.eventstore.api.dcb.Tag
import java.util.Objects
import kotlin.reflect.KClass

/**
 * Builds [DcbCriteria] from domain event classes rather than raw CloudEvent type strings.
 *
 * A [DcbCriterion] matches on the CloudEvent type string produced at write time by the configured
 * [CloudEventTypeMapper] (or [CloudEventConverter]). This builder resolves each supplied class to that type string
 * through [CloudEventTypeGetter.getCloudEventType], so the criteria match the same string the events were written with.
 *
 * Java callers use the `Class`-based [type] and [types]; Kotlin callers can use the reified [type] / [types] (the base
 * event type is inferred from the builder), or the [KClass] forms. The tag- and combinator-oriented methods ([tags],
 * [tagsAnyOf], [all], [anyOf]) are thin passthroughs to [DcbCriteria].
 *
 * A builder can be seeded with a boundary [DcbCriterion] (via [DcbDomainEventQueries.criteria]/[DcbSubscriptions.criteria]
 * or a constructor). Then [type]/[types]/[tags] refine that boundary (setting their own dimension, keeping the others),
 * which reuses a shared tag boundary and adds query-specific event types. The combinators [all]/[anyOf]/[tagsAnyOf]
 * contradict a single boundary and throw when the builder is seeded.
 *
 * @param E the base domain event type
 */
class DcbCriteriaBuilder<E : Any> private constructor(
    private val typeGetter: CloudEventTypeGetter<E>,
    private val boundary: DcbCriterion?,
) {

    /** Creates a builder backed by a [CloudEventTypeMapper]. */
    constructor(typeMapper: CloudEventTypeMapper<E>) : this(typeMapper as CloudEventTypeGetter<E>, null)

    /** Creates a builder backed by a [CloudEventConverter]. */
    constructor(cloudEventConverter: CloudEventConverter<E>) : this(CloudEventTypeGetter { cloudEventConverter.getCloudEventType(it) }, null)

    /** Creates a builder that refines the given boundary criterion, backed by a [CloudEventTypeMapper]. */
    constructor(typeMapper: CloudEventTypeMapper<E>, boundary: DcbCriterion) : this(typeMapper as CloudEventTypeGetter<E>, boundary)

    /** Creates a builder that refines the given boundary criterion, backed by a [CloudEventConverter]. */
    constructor(cloudEventConverter: CloudEventConverter<E>, boundary: DcbCriterion) : this(CloudEventTypeGetter { cloudEventConverter.getCloudEventType(it) }, boundary)

    /** A criterion matching events whose CloudEvent type is the type string mapped from [type]. */
    fun type(type: Class<out E>): DcbCriterion {
        val mapped = typeGetter.getCloudEventType(type)
        return if (boundary != null) boundary.types(mapped) else DcbCriteria.type(mapped)
    }

    /** A criterion matching events whose CloudEvent type is any of the type strings mapped from the supplied classes (any-of). */
    fun types(first: Class<out E>, vararg rest: Class<out E>): DcbCriterion {
        val mapped = ArrayList<String>(rest.size + 1)
        mapped.add(typeGetter.getCloudEventType(first))
        // A Java caller can still pass a null vararg element despite the non-null Kotlin type, so validate each explicitly.
        for (type in rest) mapped.add(typeGetter.getCloudEventType(Objects.requireNonNull(type, "Type cannot be null")))
        return if (boundary != null) boundary.types(mapped) else DcbCriteria.types(mapped)
    }

    /** A criterion matching events containing all the supplied DCB tags (all-of). */
    fun tags(first: Tag, vararg rest: Tag): DcbCriterion =
        if (boundary != null) boundary.tags(first, *rest) else DcbCriteria.tags(first, *rest)

    /** A criteria matching events that carry any one of the supplied DCB tags. Not valid on a boundary-seeded builder. */
    fun tagsAnyOf(first: Tag, vararg rest: Tag): DcbCriteria {
        requireNoBoundary("tagsAnyOf")
        return DcbCriteria.tagsAnyOf(first, *rest)
    }

    /** A criteria matching every DCB event. Not valid on a boundary-seeded builder. */
    fun all(): DcbCriteria {
        requireNoBoundary("all")
        return DcbCriteria.all()
    }

    /** A criteria matching an event when it matches any of the supplied alternatives. Not valid on a boundary-seeded builder. */
    fun anyOf(first: DcbCriteria, vararg rest: DcbCriteria): DcbCriteria {
        requireNoBoundary("anyOf")
        return DcbCriteria.anyOf(first, *rest)
    }

    /** [KClass] form of [type]. */
    fun type(type: KClass<out E>): DcbCriterion = type(type.java)

    /** [KClass] form of [types]: any-of over the supplied event types, with a required first type. */
    fun types(first: KClass<out E>, vararg rest: KClass<out E>): DcbCriterion =
        types(first.java, *rest.map { it.java }.toTypedArray())

    /** Reified single-type criterion; the base event type is inferred from the builder. */
    inline fun <reified E1 : E> type(): DcbCriterion = type(E1::class.java)

    /**
     * Reified two-type criterion (any-of); the base event type is inferred from the builder.
     *
     * The `@JvmName` only disambiguates the JVM signature from the three-type overload (both erase to `types()`); Kotlin
     * callers still write `types<A, B>()`.
     */
    @JvmName("types2")
    inline fun <reified E1 : E, reified E2 : E> types(): DcbCriterion = types(E1::class.java, E2::class.java)

    /** Reified three-type criterion (any-of); the base event type is inferred from the builder. */
    @JvmName("types3")
    inline fun <reified E1 : E, reified E2 : E, reified E3 : E> types(): DcbCriterion =
        types(E1::class.java, E2::class.java, E3::class.java)

    private fun requireNoBoundary(method: String) {
        check(boundary == null) {
            "$method() cannot refine a boundary criterion. Call criteria() without a boundary instead."
        }
    }
}
