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

import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.findById
import org.springframework.data.repository.CrudRepository
import java.util.*

interface StateConverter<S_VIEW, S_DTO> {
    fun toDTO(viewState: S_VIEW): S_DTO
    fun fromDTO(dto: S_DTO): S_VIEW
}

inline fun <reified S, E, VIEW_ID : Any> View<S, E>.currentState(mongoOperations: MongoOperations, id: VIEW_ID): S? {
    return mongoOperations.findById(id)
}

inline fun <reified S, E, VIEW_ID : Any> View<S, E>.materialized(
    mongoOperations: MongoOperations,
    crossinline deriveViewIdFromEvent: (E) -> VIEW_ID
): MaterializedView<E> {
    val noopStateConvert = object : StateConverter<S, S & Any> {
        override fun toDTO(viewState: S): S & Any = viewState as (S & Any)
        override fun fromDTO(dto: S & Any): S = dto
    }
    return materialized(mongoOperations, noopStateConvert, deriveViewIdFromEvent)
}

inline fun <S_VIEW, reified S_DTO : Any, E, VIEW_ID : Any> View<S_VIEW, E>.materialized(
    mongoOperations: MongoOperations,
    converter: StateConverter<S_VIEW, S_DTO>,
    crossinline deriveViewIdFromEvent: (E) -> VIEW_ID
): MaterializedView<E> {
    val viewStateRepository = object : ViewStateRepository<S_VIEW, VIEW_ID> {
        override fun findById(id: VIEW_ID): Optional<S_VIEW & Any> = Optional.ofNullable(mongoOperations.findById(id, S_DTO::class.java))
            .map { dto -> converter.fromDTO(dto) }

        override fun save(id: VIEW_ID, state: S_VIEW & Any) {
            val dto = converter.toDTO(state)
            mongoOperations.save(dto)
        }
    }

    val view = this
    return object : MaterializedView<E> {
        override fun update(event: E) {
            updateFromRepository(deriveViewIdFromEvent(event), event, view, viewStateRepository)
        }
    }
}

fun <S : Any, E, VIEW_ID : Any> View<S, E>.materialized(
    crudRepository: CrudRepository<S, VIEW_ID>,
    deriveViewIdFromEvent: (E) -> VIEW_ID
): MaterializedView<E> {
    val noopStateConvert = object : StateConverter<S, S> {
        override fun toDTO(viewState: S): S = viewState
        override fun fromDTO(dto: S): S = dto
    }
    return materialized(crudRepository, noopStateConvert, deriveViewIdFromEvent)
}

fun <S_VIEW, S_DTO : Any, E, VIEW_ID : Any> View<S_VIEW, E>.materialized(
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
    }
}