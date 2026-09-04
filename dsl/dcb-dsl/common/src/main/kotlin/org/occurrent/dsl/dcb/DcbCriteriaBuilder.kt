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
import org.occurrent.filter.internal.EventTypeExpansion
import java.util.*
import kotlin.reflect.KClass

/**
 * Builds [DcbCriteria] from domain event classes rather than raw CloudEvent type strings.
 *
 * A [DcbCriterion] matches on the CloudEvent type string produced at write time by the configured
 * [CloudEventTypeMapper] (or [CloudEventConverter]). This builder resolves each supplied class to that type string
 * through [CloudEventTypeGetter.getCloudEventType], so the criteria match the same string the events were written with.
 *
 * Java callers use the `Class`-based [type] and [types]. Kotlin callers can use the reified [type] / [types] (the base
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

    // Do not unify type/types below with a future buildDcbCriteria helper a separate epic is moving into this same
    // module. By that epic's own plan the incoming helper narrows uniformly rather than applying the refuse policy
    // type/types use here. This is not a claim about today's buildDcbCriteria (in SubscriptionAnnotations), which
    // takes pre-resolved CloudEvent type strings rather than classes and expands nothing itself, it is about the
    // version landing here. Two type-derivation helpers that look alike is not a reason to share an implementation.
    // Sharing one picks a single policy for both, and whichever one loses either starts refusing a caller that was
    // fine, or starts silently missing concrete subtypes it used to find.

    /**
     * A criterion matching events whose CloudEvent type is any of the CloudEvent types [type] expands into.
     *
     * [type] is expanded the way every other type-filter derivation in the library expands a declared type, through
     * [EventTypeExpansion]. A sealed type expands to the concrete types it permits, all the way down, and a type
     * whose concrete types cannot all be found is refused rather than turned into a criterion that would miss some
     * of them. The finding is the sealed-permits walk, which starts at the declared type, follows a `permits` clause
     * through `Class.getPermittedSubclasses`, and stops at the first level that is not sealed. It reads no classpath
     * and consults no index of subtypes, so a subclass declared outside a `permits` clause is beyond it.
     *
     * An enum is expanded through its constants rather than through a `permits` clause, so an `enum class` whose
     * constants have bodies works, in Java and in Kotlin alike, and so does a sealed event interface above one.
     * A constant with a body is matched under its own class, `MyEnum$A`, and a constant without one under the enum
     * class itself, so an enum with constant bodies and one without are stored under different CloudEvent types.
     * Decide whether a constant has a body before you have events in the store rather than after.
     */
    fun type(type: Class<out E>): DcbCriterion {
        val mapped = expandedCloudEventTypes(setOf(type))
        return if (boundary != null) boundary.types(mapped) else DcbCriteria.types(mapped)
    }

    /**
     * A criterion matching events whose CloudEvent type is any of the CloudEvent types the supplied classes expand
     * into (any-of). Each declared type is expanded the way [type] expands one.
     */
    fun types(first: Class<out E>, vararg rest: Class<out E>): DcbCriterion {
        val declaredTypes = LinkedHashSet<Class<out E>>(rest.size + 1)
        declaredTypes.add(first)
        // A Java caller can still pass a null vararg element despite the non-null Kotlin type, so validate each explicitly.
        for (type in rest) declaredTypes.add(Objects.requireNonNull(type, "Type cannot be null"))
        val mapped = expandedCloudEventTypes(declaredTypes)
        return if (boundary != null) boundary.types(mapped) else DcbCriteria.types(mapped)
    }

    private fun expandedCloudEventTypes(declaredTypes: Set<Class<out E>>): List<String> =
        EventTypeExpansion.expand(declaredTypes, ::cannotBuildCriterionOn).map { typeGetter.getCloudEventType(it) }

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
     * Reified two-type criterion (any-of). The base event type is inferred from the builder.
     *
     * The `@JvmName` only disambiguates the JVM signature from the three-type overload (both erase to `types()`). Kotlin
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

private fun cannotBuildCriterionOn(eventType: Class<*>): IllegalArgumentException {
    if (eventType.isArray) {
        return IllegalArgumentException(
            "${eventType.typeName} cannot be a declared event type, since this expansion does not support an array. " +
                "An array class is already concrete, so there is no narrower type to name, and it is refused for " +
                "consistency with the other declared shapes rather than because nothing can be an instance of one. " +
                "Build the DcbCriterion yourself with DcbCriteria.type(String)/types(String, ...) if you do mean to " +
                "match an array type."
        )
    }
    if (eventType.isPrimitive) {
        return IllegalArgumentException(
            "${eventType.typeName} cannot be a declared event type, since no event is ever an instance of a primitive type. Declare the concrete event types instead."
        )
    }
    return IllegalArgumentException(
        "the concrete event types dispatch would accept for ${eventType.name} cannot all be enumerated, so a criterion " +
            "derived from it would miss some of them. Declare the concrete event types instead, make ${eventType.simpleName} " +
            "and every level below it final or sealed, or build the DcbCriterion yourself with the raw type string, " +
            "which is the way out when a CloudEventTypeMapper of your own maps the whole hierarchy onto a single " +
            "CloudEvent type string."
    )
}
