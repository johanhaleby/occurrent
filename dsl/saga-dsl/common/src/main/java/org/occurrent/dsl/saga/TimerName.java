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

package org.occurrent.dsl.saga;

import static java.util.Objects.requireNonNull;

/**
 * The name of a saga timer. A {@link Simple} name stands on its own, {@code "payment"}. A {@link Qualified} name
 * belongs to a namespace and writes itself out with a colon between the two, {@code "step:awaiting-players"}.
 * <p>
 * {@link #parse(String)} reads a name out of a string and {@link #of(String, String)} builds one from a namespace and a
 * name you already have separately. Every string-taking timer method in the saga DSL calls {@code parse}, so
 * {@code startTimeout("a:b", ..)} and {@code startTimeout(TimerName.of("a", "b"), ..)} arm the same timer, and a handler
 * registered either way matches it.
 * <p>
 * {@link #encode()} writes a name back out as the string a store holds, and {@code toString()} returns the same thing on
 * both shapes, so a timer name passed to a logger prints as {@code step:awaiting-players} rather than as a record's
 * component list.
 * <p>
 * See <a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0121-a-saga-timers-name-carries-its-namespace.md">ADR 121</a>.
 */
public sealed interface TimerName permits TimerName.Simple, TimerName.Qualified {

    /**
     * A timer name with no namespace, what {@code startTimeout("payment", ofMinutes(30))} arms. The name cannot contain
     * a colon, since {@link #parse(String)} reads a string with one back as a {@link Qualified} name. Use
     * {@link Qualified} for a name that belongs to a namespace.
     *
     * @param name the timer's name
     * @throws IllegalArgumentException if {@code name} contains a colon
     */
    record Simple(String name) implements TimerName {
        public Simple {
            requireNonNull(name, "name cannot be null");
            requireNoSeparator(name, "name");
        }

        @Override
        public String encode() {
            return name;
        }

        @Override
        public String toString() {
            return encode();
        }
    }

    /**
     * A timer name inside a namespace, written out as {@code namespace + ":" + name}. The flow DSL arms its step timers
     * under the namespace {@code step}. The namespace cannot contain a colon, since {@link #parse(String)} splits at the
     * first one. The name may contain as many as you like, so {@code new Qualified("a", "b:c")} writes itself out as
     * {@code "a:b:c"} and reads back the same way.
     *
     * @param namespace the namespace the name belongs to
     * @param name      the timer's name within that namespace
     * @throws IllegalArgumentException if {@code namespace} contains a colon
     */
    record Qualified(String namespace, String name) implements TimerName {
        public Qualified {
            requireNonNull(namespace, "namespace cannot be null");
            requireNonNull(name, "name cannot be null");
            requireNoSeparator(namespace, "namespace");
        }

        @Override
        public String encode() {
            return namespace + ':' + name;
        }

        @Override
        public String toString() {
            return encode();
        }
    }

    /**
     * Reads a timer name out of {@code name}, splitting at the first colon. {@code "payment"} gives
     * {@link Simple}, {@code "step:awaiting-players"} gives {@link Qualified}, and {@code "a:b:c"} gives
     * {@code Qualified("a", "b:c")}.
     * <p>
     * Every string is a name, including the awkward ones. {@code ""} gives an empty {@link Simple} name, {@code ":x"}
     * gives an empty namespace, and {@code "x:"} gives an empty name. This never throws for anything other than
     * {@code null}, so a name read back out of a store always parses.
     *
     * @param name the string to read
     * @return the name it holds
     */
    static TimerName parse(String name) {
        requireNonNull(name, "name cannot be null");
        int separator = name.indexOf(':');
        return separator < 0
                ? new Simple(name)
                : new Qualified(name.substring(0, separator), name.substring(separator + 1));
    }

    /**
     * Builds the name {@code name} inside {@code namespace}, the same value {@link #parse(String)} reads out of
     * {@code namespace + ":" + name}.
     *
     * @param namespace the namespace the name belongs to
     * @param name      the timer's name within that namespace
     * @return the qualified name
     * @throws IllegalArgumentException if {@code namespace} contains a colon
     */
    static TimerName of(String namespace, String name) {
        return new Qualified(namespace, name);
    }

    /**
     * This name as the single string a store holds, {@code "payment"} or {@code "step:awaiting-players"}. Reading it
     * back with {@link #parse(String)} gives this same value.
     */
    String encode();

    private static void requireNoSeparator(String value, String what) {
        if (value.indexOf(':') >= 0) {
            throw new IllegalArgumentException(what + " cannot contain ':', got '" + value + "'");
        }
    }
}
