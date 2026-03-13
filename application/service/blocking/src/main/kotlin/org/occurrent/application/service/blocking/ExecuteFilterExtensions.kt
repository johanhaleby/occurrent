package org.occurrent.application.service.blocking

import kotlin.jvm.JvmName
import kotlin.reflect.KClass

/**
 * Kotlin namespace for application-service-level typed execute filters.
 *
 * Use this namespace instead of attaching filter construction to
 * [ApplicationService]. Java callers should continue to use the static factory
 * methods on [ExecuteFilter].
 */
object ExecuteFilters {

    /**
     * Create an [ExecuteFilter] that includes events of the reified domain event type [E].
     */
    inline fun <reified E : Any> type(): ExecuteFilter<E> = ExecuteFilter.type(E::class.java)

    /**
     * Create an [ExecuteFilter] that includes events whose domain event type matches any of the supplied [eventTypes].
     */
    fun <E : Any> includeTypes(vararg eventTypes: KClass<out E>): ExecuteFilter<E> {
        require(eventTypes.isNotEmpty()) { "At least one event type must be supplied" }
        val first = eventTypes.first().java
        val remaining = eventTypes.drop(1).map { it.java }.toTypedArray()
        return ExecuteFilter.includeTypes(first, *remaining)
    }

    /**
     * Create an [ExecuteFilter] that excludes events whose domain event type matches any of the supplied [eventTypes].
     */
    fun <E : Any> excludeTypes(vararg eventTypes: KClass<out E>): ExecuteFilter<E> {
        require(eventTypes.isNotEmpty()) { "At least one event type must be supplied" }
        val first = eventTypes.first().java
        val remaining = eventTypes.drop(1).map { it.java }.toTypedArray()
        return ExecuteFilter.excludeTypes(first, *remaining)
    }
}

/**
 * Deprecated top-level alias for [ExecuteFilters.type].
 */
@Deprecated(
    message = "Use ExecuteFilters.type<E>() instead.",
    replaceWith = ReplaceWith("ExecuteFilters.type<E>()", "org.occurrent.application.service.blocking.ExecuteFilters")
)
inline fun <reified E : Any> type(): ExecuteFilter<E> = ExecuteFilters.type()

/**
 * Deprecated top-level alias for [ExecuteFilters.includeTypes].
 */
@Deprecated(
    message = "Use ExecuteFilters.includeTypes(*eventTypes) instead.",
    replaceWith = ReplaceWith("ExecuteFilters.includeTypes(*eventTypes)", "org.occurrent.application.service.blocking.ExecuteFilters")
)
fun <E : Any> includeTypes(vararg eventTypes: KClass<out E>): ExecuteFilter<E> = ExecuteFilters.includeTypes(*eventTypes)

/**
 * Deprecated top-level alias for [ExecuteFilters.excludeTypes].
 */
@Deprecated(
    message = "Use ExecuteFilters.excludeTypes(*eventTypes) instead.",
    replaceWith = ReplaceWith("ExecuteFilters.excludeTypes(*eventTypes)", "org.occurrent.application.service.blocking.ExecuteFilters")
)
fun <E : Any> excludeTypes(vararg eventTypes: KClass<out E>): ExecuteFilter<E> = ExecuteFilters.excludeTypes(*eventTypes)

/**
 * Deprecated weakly typed top-level helper kept for source compatibility.
 */
@Deprecated(
    message = "Use ExecuteFilters.includeTypes(E1::class, E2::class) instead.",
    replaceWith = ReplaceWith("ExecuteFilters.includeTypes(E1::class, E2::class)", "org.occurrent.application.service.blocking.ExecuteFilters")
)
@JvmName("includeTypes2")
inline fun <reified E1 : Any, reified E2 : Any> includeTypes(): ExecuteFilter<Any> =
    ExecuteFilters.includeTypes(E1::class, E2::class)

/**
 * Deprecated weakly typed top-level helper kept for source compatibility.
 */
@Deprecated(
    message = "Use ExecuteFilters.includeTypes(E1::class, E2::class, E3::class) instead.",
    replaceWith = ReplaceWith("ExecuteFilters.includeTypes(E1::class, E2::class, E3::class)", "org.occurrent.application.service.blocking.ExecuteFilters")
)
@JvmName("includeTypes3")
inline fun <reified E1 : Any, reified E2 : Any, reified E3 : Any> includeTypes(): ExecuteFilter<Any> =
    ExecuteFilters.includeTypes(E1::class, E2::class, E3::class)

/**
 * Deprecated weakly typed top-level helper kept for source compatibility.
 */
@Deprecated(
    message = "Use ExecuteFilters.includeTypes(E1::class, E2::class, E3::class, E4::class) instead.",
    replaceWith = ReplaceWith("ExecuteFilters.includeTypes(E1::class, E2::class, E3::class, E4::class)", "org.occurrent.application.service.blocking.ExecuteFilters")
)
@JvmName("includeTypes4")
inline fun <reified E1 : Any, reified E2 : Any, reified E3 : Any, reified E4 : Any> includeTypes(): ExecuteFilter<Any> =
    ExecuteFilters.includeTypes(E1::class, E2::class, E3::class, E4::class)

/**
 * Deprecated weakly typed top-level helper kept for source compatibility.
 */
@Deprecated(
    message = "Use ExecuteFilters.includeTypes(E1::class, E2::class, E3::class, E4::class, E5::class) instead.",
    replaceWith = ReplaceWith("ExecuteFilters.includeTypes(E1::class, E2::class, E3::class, E4::class, E5::class)", "org.occurrent.application.service.blocking.ExecuteFilters")
)
@JvmName("includeTypes5")
inline fun <reified E1 : Any, reified E2 : Any, reified E3 : Any, reified E4 : Any, reified E5 : Any> includeTypes(): ExecuteFilter<Any> =
    ExecuteFilters.includeTypes(E1::class, E2::class, E3::class, E4::class, E5::class)

/**
 * Deprecated weakly typed top-level helper kept for source compatibility.
 */
@Deprecated(
    message = "Use ExecuteFilters.includeTypes(E1::class, E2::class, E3::class, E4::class, E5::class, E6::class) instead.",
    replaceWith = ReplaceWith("ExecuteFilters.includeTypes(E1::class, E2::class, E3::class, E4::class, E5::class, E6::class)", "org.occurrent.application.service.blocking.ExecuteFilters")
)
@JvmName("includeTypes6")
inline fun <reified E1 : Any, reified E2 : Any, reified E3 : Any, reified E4 : Any, reified E5 : Any, reified E6 : Any> includeTypes(): ExecuteFilter<Any> =
    ExecuteFilters.includeTypes(E1::class, E2::class, E3::class, E4::class, E5::class, E6::class)

/**
 * Deprecated weakly typed top-level helper kept for source compatibility.
 */
@Deprecated(
    message = "Use ExecuteFilters.includeTypes(E1::class, E2::class, E3::class, E4::class, E5::class, E6::class, E7::class) instead.",
    replaceWith = ReplaceWith("ExecuteFilters.includeTypes(E1::class, E2::class, E3::class, E4::class, E5::class, E6::class, E7::class)", "org.occurrent.application.service.blocking.ExecuteFilters")
)
@JvmName("includeTypes7")
inline fun <reified E1 : Any, reified E2 : Any, reified E3 : Any, reified E4 : Any, reified E5 : Any, reified E6 : Any, reified E7 : Any> includeTypes(): ExecuteFilter<Any> =
    ExecuteFilters.includeTypes(E1::class, E2::class, E3::class, E4::class, E5::class, E6::class, E7::class)

/**
 * Deprecated weakly typed top-level helper kept for source compatibility.
 */
@Deprecated(
    message = "Use ExecuteFilters.includeTypes(E1::class, E2::class, E3::class, E4::class, E5::class, E6::class, E7::class, E8::class) instead.",
    replaceWith = ReplaceWith("ExecuteFilters.includeTypes(E1::class, E2::class, E3::class, E4::class, E5::class, E6::class, E7::class, E8::class)", "org.occurrent.application.service.blocking.ExecuteFilters")
)
@JvmName("includeTypes8")
inline fun <reified E1 : Any, reified E2 : Any, reified E3 : Any, reified E4 : Any, reified E5 : Any, reified E6 : Any, reified E7 : Any, reified E8 : Any> includeTypes(): ExecuteFilter<Any> =
    ExecuteFilters.includeTypes(E1::class, E2::class, E3::class, E4::class, E5::class, E6::class, E7::class, E8::class)

/**
 * Deprecated weakly typed top-level helper kept for source compatibility.
 */
@Deprecated(
    message = "Use ExecuteFilters.excludeTypes(E1::class, E2::class) instead.",
    replaceWith = ReplaceWith("ExecuteFilters.excludeTypes(E1::class, E2::class)", "org.occurrent.application.service.blocking.ExecuteFilters")
)
@JvmName("excludeTypes2")
inline fun <reified E1 : Any, reified E2 : Any> excludeTypes(): ExecuteFilter<Any> =
    ExecuteFilters.excludeTypes(E1::class, E2::class)

/**
 * Deprecated weakly typed top-level helper kept for source compatibility.
 */
@Deprecated(
    message = "Use ExecuteFilters.excludeTypes(E1::class, E2::class, E3::class) instead.",
    replaceWith = ReplaceWith("ExecuteFilters.excludeTypes(E1::class, E2::class, E3::class)", "org.occurrent.application.service.blocking.ExecuteFilters")
)
@JvmName("excludeTypes3")
inline fun <reified E1 : Any, reified E2 : Any, reified E3 : Any> excludeTypes(): ExecuteFilter<Any> =
    ExecuteFilters.excludeTypes(E1::class, E2::class, E3::class)

/**
 * Deprecated weakly typed top-level helper kept for source compatibility.
 */
@Deprecated(
    message = "Use ExecuteFilters.excludeTypes(E1::class, E2::class, E3::class, E4::class) instead.",
    replaceWith = ReplaceWith("ExecuteFilters.excludeTypes(E1::class, E2::class, E3::class, E4::class)", "org.occurrent.application.service.blocking.ExecuteFilters")
)
@JvmName("excludeTypes4")
inline fun <reified E1 : Any, reified E2 : Any, reified E3 : Any, reified E4 : Any> excludeTypes(): ExecuteFilter<Any> =
    ExecuteFilters.excludeTypes(E1::class, E2::class, E3::class, E4::class)

/**
 * Deprecated weakly typed top-level helper kept for source compatibility.
 */
@Deprecated(
    message = "Use ExecuteFilters.excludeTypes(E1::class, E2::class, E3::class, E4::class, E5::class) instead.",
    replaceWith = ReplaceWith("ExecuteFilters.excludeTypes(E1::class, E2::class, E3::class, E4::class, E5::class)", "org.occurrent.application.service.blocking.ExecuteFilters")
)
@JvmName("excludeTypes5")
inline fun <reified E1 : Any, reified E2 : Any, reified E3 : Any, reified E4 : Any, reified E5 : Any> excludeTypes(): ExecuteFilter<Any> =
    ExecuteFilters.excludeTypes(E1::class, E2::class, E3::class, E4::class, E5::class)

/**
 * Deprecated weakly typed top-level helper kept for source compatibility.
 */
@Deprecated(
    message = "Use ExecuteFilters.excludeTypes(E1::class, E2::class, E3::class, E4::class, E5::class, E6::class) instead.",
    replaceWith = ReplaceWith("ExecuteFilters.excludeTypes(E1::class, E2::class, E3::class, E4::class, E5::class, E6::class)", "org.occurrent.application.service.blocking.ExecuteFilters")
)
@JvmName("excludeTypes6")
inline fun <reified E1 : Any, reified E2 : Any, reified E3 : Any, reified E4 : Any, reified E5 : Any, reified E6 : Any> excludeTypes(): ExecuteFilter<Any> =
    ExecuteFilters.excludeTypes(E1::class, E2::class, E3::class, E4::class, E5::class, E6::class)

/**
 * Deprecated weakly typed top-level helper kept for source compatibility.
 */
@Deprecated(
    message = "Use ExecuteFilters.excludeTypes(E1::class, E2::class, E3::class, E4::class, E5::class, E6::class, E7::class) instead.",
    replaceWith = ReplaceWith("ExecuteFilters.excludeTypes(E1::class, E2::class, E3::class, E4::class, E5::class, E6::class, E7::class)", "org.occurrent.application.service.blocking.ExecuteFilters")
)
@JvmName("excludeTypes7")
inline fun <reified E1 : Any, reified E2 : Any, reified E3 : Any, reified E4 : Any, reified E5 : Any, reified E6 : Any, reified E7 : Any> excludeTypes(): ExecuteFilter<Any> =
    ExecuteFilters.excludeTypes(E1::class, E2::class, E3::class, E4::class, E5::class, E6::class, E7::class)

/**
 * Deprecated weakly typed top-level helper kept for source compatibility.
 */
@Deprecated(
    message = "Use ExecuteFilters.excludeTypes(E1::class, E2::class, E3::class, E4::class, E5::class, E6::class, E7::class, E8::class) instead.",
    replaceWith = ReplaceWith("ExecuteFilters.excludeTypes(E1::class, E2::class, E3::class, E4::class, E5::class, E6::class, E7::class, E8::class)", "org.occurrent.application.service.blocking.ExecuteFilters")
)
@JvmName("excludeTypes8")
inline fun <reified E1 : Any, reified E2 : Any, reified E3 : Any, reified E4 : Any, reified E5 : Any, reified E6 : Any, reified E7 : Any, reified E8 : Any> excludeTypes(): ExecuteFilter<Any> =
    ExecuteFilters.excludeTypes(E1::class, E2::class, E3::class, E4::class, E5::class, E6::class, E7::class, E8::class)
