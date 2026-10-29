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
 * and the array the logger gets are all built inside the try. The last two placeholders of the format get the class
 * name and the message of {@code failure}.
 * <p>
 * Each instance logs one message, and each class that logs this way creates its instances in static fields. So this
 * class and every format string are loaded along with that class, rather than on the first failure, when there may be
 * no memory left for them. A string literal passed at the call can be created as late as the first time that call
 * runs, which is when HotSpot creates it.
 */
final class FailureLogging {
    private final Logger log;
    private final String format;

    FailureLogging(Logger log, String format) {
        this.log = log;
        this.format = format;
    }

    void error(Throwable failure) {
        try {
            log.error(format, failure.getClass().getName(), failure.getMessage(), failure);
        } catch (Throwable whileLogging) {
            // Ignored, see the class comment
        }
    }

    void error(Object first, Throwable failure) {
        try {
            log.error(format, first, failure.getClass().getName(), failure.getMessage(), failure);
        } catch (Throwable whileLogging) {
            // Ignored, see the class comment
        }
    }

    void error(Object first, Object second, Object third, Object fourth, Throwable failure) {
        try {
            log.error(format, first, second, third, fourth, failure.getClass().getName(), failure.getMessage(), failure);
        } catch (Throwable whileLogging) {
            // Ignored, see the class comment
        }
    }

    void warn(Object first, Object second, Object third, Object fourth, Throwable failure) {
        try {
            log.warn(format, first, second, third, fourth, failure.getClass().getName(), failure.getMessage(), failure);
        } catch (Throwable whileLogging) {
            // Ignored, see the class comment
        }
    }
}
