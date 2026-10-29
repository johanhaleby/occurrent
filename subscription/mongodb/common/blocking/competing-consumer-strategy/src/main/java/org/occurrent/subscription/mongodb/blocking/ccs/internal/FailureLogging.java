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

/**
 * Logs a failure inside a loop that must go on after it. Logging can throw too, such as a second
 * {@link OutOfMemoryError} or a {@code getMessage()} that throws, and that would leave the loop early. So each method
 * catches and ignores whatever logging throws. Each one takes its arguments one by one, so the class name, the message
 * and the array the logger gets are all built inside the try. The last two placeholders of {@code format} get the
 * class name and the message of {@code failure}.
 * <p>
 * Each class that logs this way creates its instance in a static field, so this class is loaded and initialized along
 * with that class rather than on the first failure, when there may be no memory left for it.
 */
final class FailureLogging {
    private final Logger log;

    FailureLogging(Logger log) {
        this.log = log;
    }

    void error(String format, Throwable failure) {
        try {
            log.error(format, failure.getClass().getName(), failure.getMessage(), failure);
        } catch (Throwable whileLogging) {
            // Ignored, see the class comment
        }
    }

    void error(String format, Object first, Throwable failure) {
        try {
            log.error(format, first, failure.getClass().getName(), failure.getMessage(), failure);
        } catch (Throwable whileLogging) {
            // Ignored, see the class comment
        }
    }

    void error(String format, Object first, Object second, Object third, Object fourth, Throwable failure) {
        try {
            log.error(format, first, second, third, fourth, failure.getClass().getName(), failure.getMessage(), failure);
        } catch (Throwable whileLogging) {
            // Ignored, see the class comment
        }
    }

    void warn(String format, Object first, Object second, Object third, Object fourth, Throwable failure) {
        try {
            log.warn(format, first, second, third, fourth, failure.getClass().getName(), failure.getMessage(), failure);
        } catch (Throwable whileLogging) {
            // Ignored, see the class comment
        }
    }
}
