/*
 *
 *  Copyright 2023 Johan Haleby
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

package org.occurrent.dsl.view

import org.occurrent.cloudevents.EventMetadata
import org.occurrent.dsl.view.internal.MongoBulkViewStateOperations
import org.occurrent.dsl.view.internal.requireMatchingDocumentId
import org.occurrent.retry.Backoff.exponential
import org.occurrent.retry.RetryStrategy
import org.springframework.dao.DuplicateKeyException
import org.springframework.dao.OptimisticLockingFailureException
import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.findById
import org.springframework.data.repository.CrudRepository
import java.lang.reflect.Field
import java.util.*

interface StateConverter<S_VIEW, S_DTO> {
    fun toDTO(viewState: S_VIEW): S_DTO
    fun fromDTO(dto: S_DTO): S_VIEW
}

inline fun <reified S : Any, E : Any, VIEW_ID : Any> View<S, E>.currentState(mongoOperations: MongoOperations, id: VIEW_ID): S? {
    return mongoOperations.findById(id)
}

inline fun <reified S, E : Any, VIEW_ID : Any> View<S, E>.materialized(
    mongoOperations: MongoOperations,
    crossinline deriveViewIdFromEvent: (E) -> VIEW_ID
): MaterializedView<E> = materialized(mongoOperations, SpringMongoViewConfig.config(), deriveViewIdFromEvent)

inline fun <reified S, E : Any, VIEW_ID : Any> View<S, E>.materialized(
    mongoOperations: MongoOperations,
    config: SpringMongoViewConfig = SpringMongoViewConfig.config(),
    crossinline deriveViewIdFromEvent: (E) -> VIEW_ID
): MaterializedView<E> {
    val noopStateConvert = object : StateConverter<S, S & Any> {
        override fun toDTO(viewState: S): S & Any = viewState as (S & Any)
        override fun fromDTO(dto: S & Any): S = dto
    }
    return materialized(mongoOperations, noopStateConvert, config, deriveViewIdFromEvent)
}

inline fun <S_VIEW, reified S_DTO : Any, E : Any, VIEW_ID : Any> View<S_VIEW, E>.materialized(
    mongoOperations: MongoOperations,
    converter: StateConverter<S_VIEW, S_DTO>,
    crossinline deriveViewIdFromEvent: (E) -> VIEW_ID
): MaterializedView<E> = materialized(mongoOperations, converter, SpringMongoViewConfig.config(), deriveViewIdFromEvent)

inline fun <S_VIEW, reified S_DTO : Any, E : Any, VIEW_ID : Any> View<S_VIEW, E>.materialized(
    mongoOperations: MongoOperations,
    converter: StateConverter<S_VIEW, S_DTO>,
    config: SpringMongoViewConfig = SpringMongoViewConfig.config(),
    crossinline deriveViewIdFromEvent: (E) -> VIEW_ID
): MaterializedView<E> {
    val metadataAware = materialized(mongoOperations, converter, config) { _: EventMetadata, e: E -> deriveViewIdFromEvent(e) }
    return object : MaterializedView<E> {
        override fun update(event: E) = metadataAware.update(event)
        override fun update(metadata: EventMetadata, event: E) = metadataAware.update(metadata, event)
    }
}

/**
 * As [materialized], but [deriveViewIdFromEvent] also sees the event's [EventMetadata], so a view instance can be keyed
 * by metadata such as the stream id or stream version. The returned [MaterializedView] folds with the metadata too:
 * [MaterializedView.update] with an event only derives and folds with [EventMetadata.empty], and
 * [MaterializedView.update] with a metadata argument derives and folds with the metadata delivered.
 */
inline fun <reified S, E : Any, VIEW_ID : Any> View<S, E>.materialized(
    mongoOperations: MongoOperations,
    crossinline deriveViewIdFromEvent: (EventMetadata, E) -> VIEW_ID
): MaterializedView<E> = materialized(mongoOperations, SpringMongoViewConfig.config(), deriveViewIdFromEvent)

/**
 * As [materialized], but [deriveViewIdFromEvent] also sees the event's [EventMetadata].
 */
inline fun <reified S, E : Any, VIEW_ID : Any> View<S, E>.materialized(
    mongoOperations: MongoOperations,
    config: SpringMongoViewConfig = SpringMongoViewConfig.config(),
    crossinline deriveViewIdFromEvent: (EventMetadata, E) -> VIEW_ID
): MaterializedView<E> {
    val noopStateConvert = object : StateConverter<S, S & Any> {
        override fun toDTO(viewState: S): S & Any = viewState as (S & Any)
        override fun fromDTO(dto: S & Any): S = dto
    }
    return materialized(mongoOperations, noopStateConvert, config, deriveViewIdFromEvent)
}

/**
 * As [materialized], but [deriveViewIdFromEvent] also sees the event's [EventMetadata].
 */
inline fun <S_VIEW, reified S_DTO : Any, E : Any, VIEW_ID : Any> View<S_VIEW, E>.materialized(
    mongoOperations: MongoOperations,
    converter: StateConverter<S_VIEW, S_DTO>,
    crossinline deriveViewIdFromEvent: (EventMetadata, E) -> VIEW_ID
): MaterializedView<E> = materialized(mongoOperations, converter, SpringMongoViewConfig.config(), deriveViewIdFromEvent)

/**
 * As [materialized], but [deriveViewIdFromEvent] also sees the event's [EventMetadata], so a view instance can be keyed
 * by metadata such as the stream id or stream version rather than only the event. Both [MaterializedView.update]
 * overloads fold with metadata: the metadata-carrying one uses what is delivered, and the event-only one uses
 * [EventMetadata.empty].
 */
inline fun <S_VIEW, reified S_DTO : Any, E : Any, VIEW_ID : Any> View<S_VIEW, E>.materialized(
    mongoOperations: MongoOperations,
    converter: StateConverter<S_VIEW, S_DTO>,
    config: SpringMongoViewConfig = SpringMongoViewConfig.config(),
    crossinline deriveViewIdFromEvent: (EventMetadata, E) -> VIEW_ID
): MaterializedView<E> {
    val (duplicateKeyHandling, optimisticLockingHandling) = config
    val retryStrategy: RetryStrategy = RetryStrategy.retry()
        .let { rs ->
            if (optimisticLockingHandling is OptimisticLockingHandling.Retry) {
                rs.backoff(exponential(optimisticLockingHandling.initial, optimisticLockingHandling.max, optimisticLockingHandling.multiplier))
            } else {
                rs
            }
        }
        .retryIf { e ->
            when (e) {
                is OptimisticLockingFailureException -> optimisticLockingHandling is OptimisticLockingHandling.Retry
                else -> false
            }
        }
        .onError { e ->
            when (e) {
                is DuplicateKeyException -> if (duplicateKeyHandling is DuplicateKeyHandling.Ignore) {
                    duplicateKeyHandling.onDuplicateKeyException(e)
                }

                is OptimisticLockingFailureException -> when (optimisticLockingHandling) {
                    is OptimisticLockingHandling.Ignore -> {
                        optimisticLockingHandling.onOptimisticLockingFailureException(e)
                    }

                    is OptimisticLockingHandling.Retry -> {
                        optimisticLockingHandling.onOptimisticLockingFailureException(e)
                    }

                    else -> {}
                }
            }
        }

    // Built by hand rather than through the viewStateRepository(find, save) factory, whose state type is bounded to Any
    // and so cannot carry a nullable view state.
    val stateRepository = mongoOperationsViewStateRepository<S_VIEW, S_DTO, VIEW_ID>(mongoOperations, converter, S_DTO::class.java)

    val view = this
    return object : MaterializedView<E> {
        override fun update(event: E) = update(EventMetadata.empty(), event)

        override fun update(metadata: EventMetadata, event: E) {
            try {
                updateFromRepository(deriveViewIdFromEvent(metadata, event), metadata, event, view, stateRepository, retryStrategy)
            } catch (e: DuplicateKeyException) {
                if (duplicateKeyHandling is DuplicateKeyHandling.Rethrow) {
                    throw e
                }
            } catch (e: OptimisticLockingFailureException) {
                if (optimisticLockingHandling is OptimisticLockingHandling.Rethrow) {
                    throw e
                }
            }
        }
    }
}

fun <S : Any, E : Any, VIEW_ID : Any> View<S, E>.materialized(
    crudRepository: CrudRepository<S, VIEW_ID>,
    deriveViewIdFromEvent: (E) -> VIEW_ID
): MaterializedView<E> {
    val noopStateConvert = object : StateConverter<S, S> {
        override fun toDTO(viewState: S): S = viewState
        override fun fromDTO(dto: S): S = dto
    }
    return materialized(crudRepository, noopStateConvert, deriveViewIdFromEvent)
}

fun <S_VIEW, S_DTO : Any, E : Any, VIEW_ID : Any> View<S_VIEW, E>.materialized(
    crudRepository: CrudRepository<S_DTO, VIEW_ID>,
    converter: StateConverter<S_VIEW, S_DTO>,
    deriveViewIdFromEvent: (E) -> VIEW_ID
): MaterializedView<E> {
    val viewStateRepository = crudRepositoryViewStateRepository(crudRepository, converter)

    val view = this
    return object : MaterializedView<E> {
        override fun update(event: E) = updateFromRepository(deriveViewIdFromEvent(event), event, view, viewStateRepository)
        override fun update(metadata: EventMetadata, event: E) =
            updateFromRepository(deriveViewIdFromEvent(event), metadata, event, view, viewStateRepository)
    }
}

// Extracted out of the materialized(mongoOperations, ..) overload so a test can obtain a ViewStateRepository
// directly and exercise findAllById/saveAll without going through MaterializedView, which never calls them itself.
fun <S_VIEW, S_DTO : Any, VIEW_ID : Any> mongoOperationsViewStateRepository(
    mongoOperations: MongoOperations,
    converter: StateConverter<S_VIEW, S_DTO>,
    dtoType: Class<S_DTO>
): ViewStateRepository<S_VIEW, VIEW_ID> = object : ViewStateRepository<S_VIEW, VIEW_ID> {
    override fun findById(id: VIEW_ID): Optional<S_VIEW & Any> = Optional.ofNullable(mongoOperations.findById(id, dtoType))
        .map { dto -> converter.fromDTO(dto) }

    override fun save(id: VIEW_ID, state: S_VIEW & Any) {
        val dto = converter.toDTO(state)
        requireMatchingDocumentId(mongoOperations, dtoType, dto, id)
        mongoOperations.save(dto)
    }

    // One "_id in (..)" query instead of ids.size() findById round trips. Reuses the exact machinery
    // MongoOperations.findById(id, ..) relies on for id-type coercion (a hex String resolved against an
    // ObjectId id, for example), so the read is identical to looping findById, just batched.
    override fun findAllById(ids: Collection<VIEW_ID>): Map<VIEW_ID, S_VIEW & Any> {
        val result = LinkedHashMap<VIEW_ID, S_VIEW & Any>()
        MongoBulkViewStateOperations.findAllById(mongoOperations, dtoType, ids).forEach { (id, dto) ->
            val state = converter.fromDTO(dto)
            if (state != null) {
                result[id] = state
            }
        }
        return result
    }

    // requireMatchingDocumentId runs for every entry before any write is issued, so a mismatched id fails the
    // whole batch rather than the entries that would have followed it in a loop. See
    // MongoBulkViewStateOperations for the bulk write and its optimistic-locking and duplicate-key handling.
    override fun saveAll(states: Map<VIEW_ID, S_VIEW & Any>) {
        val dtos = states.map { (id, state) ->
            val dto = converter.toDTO(state)
            requireMatchingDocumentId(mongoOperations, dtoType, dto, id)
            dto
        }
        MongoBulkViewStateOperations.saveAll(mongoOperations, dtoType, dtos)
    }
}

// As mongoOperationsViewStateRepository, extracted out of materialized(crudRepository, ..) for direct testability.
fun <S_VIEW, S_DTO : Any, VIEW_ID : Any> crudRepositoryViewStateRepository(
    crudRepository: CrudRepository<S_DTO, VIEW_ID>,
    converter: StateConverter<S_VIEW, S_DTO>
): ViewStateRepository<S_VIEW, VIEW_ID> = object : ViewStateRepository<S_VIEW, VIEW_ID> {
    override fun findById(id: VIEW_ID): Optional<S_VIEW & Any> = crudRepository.findById(id).map { dto ->
        converter.fromDTO(dto as S_DTO)
    }

    override fun save(id: VIEW_ID, state: S_VIEW & Any) {
        val dto = converter.toDTO(state)
        crudRepository.save(dto)
    }

    // Delegates to Spring Data's own findAllById, a single "id in (..)" query for the common Mongo/JPA
    // implementations. CrudRepository has no generic way to say which id a returned entity belongs to, so
    // pairing results back to ids falls back to the @Id-annotated field every CrudRepository entity must carry.
    override fun findAllById(ids: Collection<VIEW_ID>): Map<VIEW_ID, S_VIEW & Any> {
        val result = LinkedHashMap<VIEW_ID, S_VIEW & Any>()
        if (ids.isEmpty()) {
            return result
        }
        val found = crudRepository.findAllById(ids).toList()
        if (found.isEmpty()) {
            return result
        }
        val idField = requiredIdField(found.first().javaClass)
        @Suppress("UNCHECKED_CAST")
        val dtosById = found.associateBy { dto -> idField.get(dto) as VIEW_ID }
        for (id in ids) {
            val dto = dtosById[id] ?: continue
            val state = converter.fromDTO(dto)
            if (state == null) {
                continue
            }
            result[id] = state
        }
        return result
    }

    // Delegates to Spring Data's own saveAll. For SimpleMongoRepository this is a real bulk insert only when
    // every entry is new; a batch mixing new and existing entries falls back to CrudRepository.save per entry,
    // which is exactly what the looping ViewStateRepository.saveAll default already does, so this is never
    // worse and is strictly better for the all-new case.
    override fun saveAll(states: Map<VIEW_ID, S_VIEW & Any>) {
        if (states.isEmpty()) {
            return
        }
        crudRepository.saveAll(states.values.map { state -> converter.toDTO(state) })
    }
}

// Walks up to find the @Id field, the same annotation Spring Data's own repository implementations require an
// entity to carry for findById/save/etc. to work in the first place.
private fun requiredIdField(dtoType: Class<*>): Field {
    var current: Class<*>? = dtoType
    while (current != null && current != Any::class.java) {
        current.declaredFields.firstOrNull { it.isAnnotationPresent(Id::class.java) }?.let { field ->
            field.isAccessible = true
            return field
        }
        current = current.superclass
    }
    throw IllegalStateException("No @${Id::class.java.name} field found on ${dtoType.name}; findAllById cannot pair its bulk result back to the ids it was queried with")
}
