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

fun <A, B, R> ((A, B) -> R).partial(b: B): (A) -> R = { a ->
    this(a, b)
}

fun <A, B, C, R> ((A, B, C) -> R).partial(b: B, c: C): (A) -> R = { a ->
    this(a, b, c)
}

fun <A, B, C, D, R> ((A, B, C, D) -> R).partial(b: B, c: C, d: D): (A) -> R = { a ->
    this(a, b, c, d)
}

fun <A, B, C, D, E, R> ((A, B, C, D, E) -> R).partial(b: B, c: C, d: D, e: E): (A) -> R = { a ->
    this(a, b, c, d, e)
}

fun <A, B, C, D, E, F, R> ((A, B, C, D, E, F) -> R).partial(b: B, c: C, d: D, e: E, f: F): (A) -> R = { a ->
    this(a, b, c, d, e, f)
}

fun <A, B, C, D, E, F, G, R> ((A, B, C, D, E, F, G) -> R).partial(b: B, c: C, d: D, e: E, f: F, g: G): (A) -> R = { a ->
    this(a, b, c, d, e, f, g)
}

fun <A, B, C, D, E, F, G, H, R> ((A, B, C, D, E, F, G, H) -> R).partial(
    b: B,
    c: C,
    d: D,
    e: E,
    f: F,
    g: G,
    h: H
): (A) -> R = { a ->
    this(a, b, c, d, e, f, g, h)
}

fun <A, B, C, D, E, F, G, H, I, R> ((A, B, C, D, E, F, G, H, I) -> R).partial(
    b: B,
    c: C,
    d: D,
    e: E,
    f: F,
    g: G,
    h: H,
    i: I,
): (A) -> R = { a ->
    this(a, b, c, d, e, f, g, h, i)
}

fun <A, B, C, D, E, F, G, H, I, J, R> ((A, B, C, D, E, F, G, H, I, J) -> R).partial(
    b: B,
    c: C,
    d: D,
    e: E,
    f: F,
    g: G,
    h: H,
    i: I,
    j: J,
): (A) -> R = { a ->
    this(a, b, c, d, e, f, g, h, i, j)
}

fun <A, B, C, D, E, F, G, H, I, J, K, R> ((A, B, C, D, E, F, G, H, I, J, K) -> R).partial(
    b: B,
    c: C,
    d: D,
    e: E,
    f: F,
    g: G,
    h: H,
    i: I,
    j: J,
    k: K,
): (A) -> R = { a ->
    this(a, b, c, d, e, f, g, h, i, j, k)
}

fun <A, B, C, D, E, F, G, H, I, J, K, L, R> ((A, B, C, D, E, F, G, H, I, J, K, L) -> R).partial(
    b: B,
    c: C,
    d: D,
    e: E,
    f: F,
    g: G,
    h: H,
    i: I,
    j: J,
    k: K,
    l: L,
): (A) -> R = { a ->
    this(a, b, c, d, e, f, g, h, i, j, k, l)
}

fun <A, B, C, D, E, F, G, H, I, J, K, L, M, R> ((A, B, C, D, E, F, G, H, I, J, K, L, M) -> R).partial(
    b: B,
    c: C,
    d: D,
    e: E,
    f: F,
    g: G,
    h: H,
    i: I,
    j: J,
    k: K,
    l: L,
    m: M,
): (A) -> R = { a ->
    this(a, b, c, d, e, f, g, h, i, j, k, l, m)
}

fun <A, B, C, D, E, F, G, H, I, J, K, L, M, N, R> ((A, B, C, D, E, F, G, H, I, J, K, L, M, N) -> R).partial(
    b: B,
    c: C,
    d: D,
    e: E,
    f: F,
    g: G,
    h: H,
    i: I,
    j: J,
    k: K,
    l: L,
    m: M,
    n: N,
): (A) -> R = { a ->
    this(a, b, c, d, e, f, g, h, i, j, k, l, m, n)
}

fun <A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, R> ((A, B, C, D, E, F, G, H, I, J, K, L, M, N, O) -> R).partial(
    b: B,
    c: C,
    d: D,
    e: E,
    f: F,
    g: G,
    h: H,
    i: I,
    j: J,
    k: K,
    l: L,
    m: M,
    n: N,
    o: O,
): (A) -> R = { a ->
    this(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o)
}