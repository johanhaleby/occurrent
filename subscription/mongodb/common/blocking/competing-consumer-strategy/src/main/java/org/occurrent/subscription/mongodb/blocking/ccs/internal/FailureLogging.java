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

package org.occurrent.subscription.mongodb.blocking.ccs.internal;

import org.slf4j.Logger;
import org.slf4j.event.Level;

/**
 * Logs a failure inside a loop that must go on after it. Logging can throw too, such as a second
 * {@link OutOfMemoryError} or a {@code getMessage()} that throws, and that would leave the loop early. So each method
 * catches and ignores whatever logging throws. Each one takes its arguments one by one, so calling it allocates
 * nothing before that catch. The last two placeholders of {@code format} get the class name and the message of
 * {@code failure}.
 */
final class FailureLogging {

    private FailureLogging() {
    }

    static void logIgnoringWhatLoggingThrows(Logger log, Level level, String format, Throwable failure) {
        try {
            log.atLevel(level).setCause(failure).log(format, failure.getClass().getName(), failure.getMessage());
        } catch (Throwable whileLogging) {
            // Ignored, see the class comment
        }
    }

    static void logIgnoringWhatLoggingThrows(Logger log, Level level, String format, Object first, Throwable failure) {
        try {
            log.atLevel(level).setCause(failure).log(format, first, failure.getClass().getName(), failure.getMessage());
        } catch (Throwable whileLogging) {
            // Ignored, see the class comment
        }
    }

    static void logIgnoringWhatLoggingThrows(Logger log, Level level, String format, Object first, Object second, Object third, Object fourth, Throwable failure) {
        try {
            log.atLevel(level).setCause(failure).log(format, first, second, third, fourth, failure.getClass().getName(), failure.getMessage());
        } catch (Throwable whileLogging) {
            // Ignored, see the class comment
        }
    }
}
