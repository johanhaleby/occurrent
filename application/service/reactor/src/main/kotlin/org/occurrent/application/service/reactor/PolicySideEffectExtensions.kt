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

package org.occurrent.application.service.reactor

import reactor.core.publisher.Mono
import java.util.function.Function
import java.util.stream.Stream

/**
 * Build a reactive side-effect that runs [policy] for every produced event of the reified type [E]. The result can be
 * passed to [ExecuteOptions.sideEffect].
 */
inline fun <T : Any, reified E : T> executePolicy(noinline policy: (E) -> Mono<Void>): Function<Stream<T>, Mono<Void>> =
    PolicySideEffect.executePolicy(E::class.java, Function { event: E -> policy(event) })
