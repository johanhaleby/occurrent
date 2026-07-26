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
import org.occurrent.dsl.view.internal.requireMatchingDocumentId
import org.occurrent.retry.Backoff.exponential
import org.occurrent.retry.RetryStrategy
import org.springframework.dao.DuplicateKeyException
import org.springframework.dao.OptimisticLockingFailureException
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.findById
import org.springframework.data.repository.CrudRepository
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
    val stateRepository = object : ViewStateRepository<S_VIEW, VIEW_ID> {
        override fun findById(id: VIEW_ID): Optional<S_VIEW & Any> = Optional.ofNullable(mongoOperations.findById(id, S_DTO::class.java))
            .map { dto -> converter.fromDTO(dto) }

        override fun save(id: VIEW_ID, state: S_VIEW & Any) {
            val dto = converter.toDTO(state)
            requireMatchingDocumentId(mongoOperations, S_DTO::class.java, dto, id)
            mongoOperations.save(dto)
        }
    }

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
    val viewStateRepository = object : ViewStateRepository<S_VIEW, VIEW_ID> {
        override fun findById(id: VIEW_ID): Optional<S_VIEW & Any> = crudRepository.findById(id).map { dto ->
            converter.fromDTO(dto as S_DTO)
        }

        override fun save(id: VIEW_ID, state: S_VIEW & Any) {
            val dto = converter.toDTO(state)
            crudRepository.save(dto)
        }
    }

    val view = this
    return object : MaterializedView<E> {
        override fun update(event: E) = updateFromRepository(deriveViewIdFromEvent(event), event, view, viewStateRepository)
        override fun update(metadata: EventMetadata, event: E) =
            updateFromRepository(deriveViewIdFromEvent(event), metadata, event, view, viewStateRepository)
    }
}
