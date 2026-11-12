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

package org.occurrent.subscription.reactor.durable;

import java.io.IOException;

/**
 * What a wrapped model throws from {@code globalCheckpointAsOfNow()} instead of returning a read. That is a
 * RuntimeException, an Error, or a checked exception, which Kotlin code throws without declaring it.
 */
final class ThrownAtTheCall {

    private ThrownAtTheCall() {
    }

    /**
     * A "runtime exception", an "error" or a "checked exception" with {@code message}
     */
    static Throwable of(String kind, String message) {
        return switch (kind) {
            case "runtime exception" -> new IllegalStateException(message);
            case "error" -> new AssertionError(message);
            case "checked exception" -> new IOException(message);
            default -> throw new IllegalArgumentException("No throwable of the kind " + kind);
        };
    }

    /**
     * Throws {@code throwable} without declaring it, as Kotlin code does. Declared to return, so a lambda can write
     * {@code throw undeclared(throwable)}.
     */
    @SuppressWarnings("unchecked")
    static <T extends Throwable> RuntimeException undeclared(Throwable throwable) throws T {
        throw (T) throwable;
    }
}
