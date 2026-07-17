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

package org.occurrent.dsl.snapshot.reactor

import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.service.reactor.ApplicationService
import org.occurrent.application.service.reactor.dcb.DcbApplicationService
import org.occurrent.dsl.dcb.DcbDecider
import org.occurrent.dsl.decider.Decider
import org.occurrent.dsl.snapshot.DcbSnapshotKeys
import org.occurrent.dsl.snapshot.SnapshotOptions
import org.occurrent.dsl.snapshot.SnapshotPolicy
import org.occurrent.dsl.snapshot.SnapshotView
import org.occurrent.eventstore.api.WriteResult
import org.occurrent.eventstore.api.reactor.EventStore
import org.occurrent.eventstore.api.dcb.DcbAppendResult
import org.occurrent.eventstore.api.dcb.DcbCriteria
import reactor.core.publisher.Mono

/**
 * Run [decider] against [streamId] but resume from the snapshot in [store] instead of replaying the whole stream. See
 * [ReactiveSnapshotDeciderApplicationService].
 */
fun <C : Any, S, E : Any> ApplicationService<E>.execute(streamId: String, command: C, decider: Decider<C, S, E>, store: ReactiveSnapshotStore<S>, options: SnapshotOptions<S, E>): Mono<WriteResult> =
    ReactiveSnapshotDeciderApplicationService(this).execute(streamId, command, decider, store, options)

/**
 * Run [decider] with [commands] against [streamId], resuming from the snapshot in [store].
 */
fun <C : Any, S, E : Any> ApplicationService<E>.execute(streamId: String, commands: List<C>, decider: Decider<C, S, E>, store: ReactiveSnapshotStore<S>, options: SnapshotOptions<S, E>): Mono<WriteResult> =
    ReactiveSnapshotDeciderApplicationService(this).execute(streamId, commands, decider, store, options)

/**
 * Run [dcbDecider] against its DCB boundary but resume from the snapshot in [store]. See
 * [ReactiveSnapshotDcbDeciderApplicationService].
 */
fun <C : Any, S, E : Any> DcbApplicationService<E>.execute(command: C, dcbDecider: DcbDecider<C, S, E>, store: ReactiveSnapshotStore<S>, options: SnapshotOptions<S, E>): Mono<DcbAppendResult> =
    ReactiveSnapshotDcbDeciderApplicationService(this).execute(command, dcbDecider, store, options)

/**
 * Run [dcbDecider] with [commands], resuming from the snapshot in [store], keyed by the resolved criteria.
 */
fun <C : Any, S, E : Any> DcbApplicationService<E>.execute(commands: List<C>, dcbDecider: DcbDecider<C, S, E>, store: ReactiveSnapshotStore<S>, options: SnapshotOptions<S, E>, keyFunction: (DcbCriteria) -> String = DcbSnapshotKeys::canonicalKey): Mono<DcbAppendResult> =
    ReactiveSnapshotDcbDeciderApplicationService(this).execute(commands, dcbDecider, store, options, keyFunction)

/**
 * Read the current state of [snapshotView] for [streamId] on demand, folding only the events after the stored snapshot.
 * See [ReactiveSnapshotViews].
 */
fun <S : Any, E : Any> EventStore.readSnapshotState(converter: CloudEventConverter<E>, streamId: String, snapshotView: SnapshotView<S, E>, store: ReactiveSnapshotStore<S>, policy: SnapshotPolicy<S, E>): Mono<S> =
    ReactiveSnapshotViews.readState(this, converter, streamId, snapshotView, store, policy)
