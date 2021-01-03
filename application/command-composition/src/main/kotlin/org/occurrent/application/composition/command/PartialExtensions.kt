/*
 * Copyright 2021 Johan Haleby
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

package org.occurrent.application.composition.command

fun <EVENT, A> ((Sequence<EVENT>, A) -> Sequence<EVENT>).partial(a: A): (Sequence<EVENT>) -> Sequence<EVENT> = { events ->
    this(events, a)
}

fun <EVENT, A, B> ((Sequence<EVENT>, A, B) -> Sequence<EVENT>).partial(a: A, b: B): (Sequence<EVENT>) -> Sequence<EVENT> = { events ->
    this(events, a, b)
}

fun <EVENT, A, B, C> ((Sequence<EVENT>, A, B, C) -> Sequence<EVENT>).partial(a: A, b: B, c: C): (Sequence<EVENT>) -> Sequence<EVENT> = { events ->
    this(events, a, b, c)
}

fun <EVENT, A, B, C, D> ((Sequence<EVENT>, A, B, C, D) -> Sequence<EVENT>).partial(a: A, b: B, c: C, d: D): (Sequence<EVENT>) -> Sequence<EVENT> = { events ->
    this(events, a, b, c, d)
}

fun <EVENT, A, B, C, D, E> ((Sequence<EVENT>, A, B, C, D, E) -> Sequence<EVENT>).partial(a: A, b: B, c: C, d: D, e: E): (Sequence<EVENT>) -> Sequence<EVENT> = { events ->
    this(events, a, b, c, d, e)
}

fun <EVENT, A, B, C, D, E, F> ((Sequence<EVENT>, A, B, C, D, E, F) -> Sequence<EVENT>).partial(a: A, b: B, c: C, d: D, e: E, f: F): (Sequence<EVENT>) -> Sequence<EVENT> = { events ->
    this(events, a, b, c, d, e, f)
}


fun <EVENT, A, B, C, D, E, F, G> ((Sequence<EVENT>, A, B, C, D, E, F, G) -> Sequence<EVENT>).partial(a: A, b: B, c: C, d: D, e: E, f: F, g: G): (Sequence<EVENT>) -> Sequence<EVENT> = { events ->
    this(events, a, b, c, d, e, f, g)
}


fun <EVENT, A, B, C, D, E, F, G, H> ((Sequence<EVENT>, A, B, C, D, E, F, G, H) -> Sequence<EVENT>).partial(a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H): (Sequence<EVENT>) -> Sequence<EVENT> = { events ->
    this(events, a, b, c, d, e, f, g, h)
}

fun <EVENT, A, B, C, D, E, F, G, H, I> ((Sequence<EVENT>, A, B, C, D, E, F, G, H, I) -> Sequence<EVENT>).partial(a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I): (Sequence<EVENT>) -> Sequence<EVENT> = { events ->
    this(events, a, b, c, d, e, f, g, h, i)
}

fun <EVENT, A, B, C, D, E, F, G, H, I, J> ((Sequence<EVENT>, A, B, C, D, E, F, G, H, I, J) -> Sequence<EVENT>).partial(a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J): (Sequence<EVENT>) -> Sequence<EVENT> = { events ->
    this(events, a, b, c, d, e, f, g, h, i, j)
}

fun <EVENT, A, B, C, D, E, F, G, H, I, J, K> ((Sequence<EVENT>, A, B, C, D, E, F, G, H, I, J, K) -> Sequence<EVENT>).partial(a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K): (Sequence<EVENT>) -> Sequence<EVENT> = { events ->
    this(events, a, b, c, d, e, f, g, h, i, j, k)
}

fun <EVENT, A, B, C, D, E, F, G, H, I, J, K, L> ((Sequence<EVENT>, A, B, C, D, E, F, G, H, I, J, K, L) -> Sequence<EVENT>).partial(a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L): (Sequence<EVENT>) -> Sequence<EVENT> = { events ->
    this(events, a, b, c, d, e, f, g, h, i, j, k, l)
}

fun <EVENT, A, B, C, D, E, F, G, H, I, J, K, L, M> ((Sequence<EVENT>, A, B, C, D, E, F, G, H, I, J, K, L, M) -> Sequence<EVENT>).partial(a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M): (Sequence<EVENT>) -> Sequence<EVENT> = { events ->
    this(events, a, b, c, d, e, f, g, h, i, j, k, l, m)
}

fun <EVENT, A, B, C, D, E, F, G, H, I, J, K, L, M, N> ((Sequence<EVENT>, A, B, C, D, E, F, G, H, I, J, K, L, M, N) -> Sequence<EVENT>).partial(a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N): (Sequence<EVENT>) -> Sequence<EVENT> = { events ->
    this(events, a, b, c, d, e, f, g, h, i, j, k, l, m, n)
}


@JvmName("partialList")
fun <EVENT, A> ((List<EVENT>, A) -> List<EVENT>).partial(a: A): (List<EVENT>) -> List<EVENT> = { events ->
    this(events, a)
}

@JvmName("partialList")
fun <EVENT, A, B> ((List<EVENT>, A, B) -> List<EVENT>).partial(a: A, b: B): (List<EVENT>) -> List<EVENT> = { events ->
    this(events, a, b)
}

@JvmName("partialList")
fun <EVENT, A, B, C> ((List<EVENT>, A, B, C) -> List<EVENT>).partial(a: A, b: B, c: C): (List<EVENT>) -> List<EVENT> = { events ->
    this(events, a, b, c)
}

@JvmName("partialList")
fun <EVENT, A, B, C, D> ((List<EVENT>, A, B, C, D) -> List<EVENT>).partial(a: A, b: B, c: C, d: D): (List<EVENT>) -> List<EVENT> = { events ->
    this(events, a, b, c, d)
}

@JvmName("partialList")
fun <EVENT, A, B, C, D, E> ((List<EVENT>, A, B, C, D, E) -> List<EVENT>).partial(a: A, b: B, c: C, d: D, e: E): (List<EVENT>) -> List<EVENT> = { events ->
    this(events, a, b, c, d, e)
}

@JvmName("partialList")
fun <EVENT, A, B, C, D, E, F> ((List<EVENT>, A, B, C, D, E, F) -> List<EVENT>).partial(a: A, b: B, c: C, d: D, e: E, f: F): (List<EVENT>) -> List<EVENT> = { events ->
    this(events, a, b, c, d, e, f)
}

@JvmName("partialList")
fun <EVENT, A, B, C, D, E, F, G> ((List<EVENT>, A, B, C, D, E, F, G) -> List<EVENT>).partial(a: A, b: B, c: C, d: D, e: E, f: F, g: G): (List<EVENT>) -> List<EVENT> = { events ->
    this(events, a, b, c, d, e, f, g)
}

@JvmName("partialList")
fun <EVENT, A, B, C, D, E, F, G, H> ((List<EVENT>, A, B, C, D, E, F, G, H) -> List<EVENT>).partial(a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H): (List<EVENT>) -> List<EVENT> = { events ->
    this(events, a, b, c, d, e, f, g, h)
}
@JvmName("partialList")
fun <EVENT, A, B, C, D, E, F, G, H, I> ((List<EVENT>, A, B, C, D, E, F, G, H, I) -> List<EVENT>).partial(a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I): (List<EVENT>) -> List<EVENT> = { events ->
    this(events, a, b, c, d, e, f, g, h, i)
}

@JvmName("partialList")
fun <EVENT, A, B, C, D, E, F, G, H, I, J> ((List<EVENT>, A, B, C, D, E, F, G, H, I, J) -> List<EVENT>).partial(a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J): (List<EVENT>) -> List<EVENT> = { events ->
    this(events, a, b, c, d, e, f, g, h, i, j)
}

@JvmName("partialList")
fun <EVENT, A, B, C, D, E, F, G, H, I, J, K> ((List<EVENT>, A, B, C, D, E, F, G, H, I, J, K) -> List<EVENT>).partial(a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K): (List<EVENT>) -> List<EVENT> = { events ->
    this(events, a, b, c, d, e, f, g, h, i, j, k)
}

@JvmName("partialList")
fun <EVENT, A, B, C, D, E, F, G, H, I, J, K, L> ((List<EVENT>, A, B, C, D, E, F, G, H, I, J, K, L) -> List<EVENT>).partial(a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L): (List<EVENT>) -> List<EVENT> = { events ->
    this(events, a, b, c, d, e, f, g, h, i, j, k, l)
}

@JvmName("partialList")
fun <EVENT, A, B, C, D, E, F, G, H, I, J, K, L, M> ((List<EVENT>, A, B, C, D, E, F, G, H, I, J, K, L, M) -> List<EVENT>).partial(a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M): (List<EVENT>) -> List<EVENT> = { events ->
    this(events, a, b, c, d, e, f, g, h, i, j, k, l, m)
}

@JvmName("partialList")
fun <EVENT, A, B, C, D, E, F, G, H, I, J, K, L, M, N> ((List<EVENT>, A, B, C, D, E, F, G, H, I, J, K, L, M, N) -> List<EVENT>).partial(a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K, l: L, m: M, n: N): (List<EVENT>) -> List<EVENT> = { events ->
    this(events, a, b, c, d, e, f, g, h, i, j, k, l, m, n)
}