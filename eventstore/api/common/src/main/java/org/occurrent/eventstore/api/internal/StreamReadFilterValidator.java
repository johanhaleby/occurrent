/*
 *
 *  Copyright 2026 Johan Haleby
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

package org.occurrent.eventstore.api.internal;

import org.jspecify.annotations.NullMarked;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.eventstore.api.StreamReadFilter;
import org.occurrent.eventstore.api.StreamReadFilter.AttributeFilter;
import org.occurrent.eventstore.api.StreamReadFilter.ExtensionFilter;

import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Validates {@link StreamReadFilter} for stream read contexts.
 * <p>
 * Ensures that reserved Occurrent extensions for stream identity and stream concurrency are not constrained
 * via attribute(...) or extension(...).
 */
@NullMarked
public final class StreamReadFilterValidator {

    private StreamReadFilterValidator() {
    }

    /**
     * Verify that the provided {@link StreamReadFilter} does not constrain reserved Occurrent fields.
     *
     * @throws IllegalArgumentException if invalid
     */
    public static void validate(StreamReadFilter filter) {
        requireNonNull(filter, "StreamReadFilter cannot be null");
        validateInternal(filter);
    }

    private static void validateInternal(StreamReadFilter filter) {
        if (filter instanceof AttributeFilter<?> af) {
            verifyForbiddenName(normalize(af.attributeName()), "attribute");
            return;
        }

        if (filter instanceof ExtensionFilter<?> ef) {
            verifyForbiddenName(normalize(ef.extensionName()), "extension");
            return;
        }

        if (filter instanceof StreamReadFilter.DataFilter) {
            // Allowed
            return;
        }

        if (filter instanceof StreamReadFilter.CompositionFilter cf) {
            List<StreamReadFilter> filters = cf.filters();
            for (StreamReadFilter f : filters) {
                requireNonNull(f, "StreamReadFilter composition cannot contain null");
                validateInternal(f);
            }
            return;
        }

        throw new IllegalArgumentException("Unknown StreamReadFilter implementation: " + filter.getClass().getName());
    }

    private static void verifyForbiddenName(String normalizedName, String kind) {
        String streamIdName = normalize(OccurrentCloudEventExtension.STREAM_ID);
        String streamVersionName = normalize(OccurrentCloudEventExtension.STREAM_VERSION);

        if (Objects.equals(normalizedName, streamIdName) || Objects.equals(normalizedName, streamVersionName)) {
            throw new IllegalArgumentException(
                    "StreamReadFilter must not constrain " + kind + " '" + normalizedName + "'. " +
                            "streamId is provided by the stream read API, and streamVersion is derived from stream history."
            );
        }
    }

    private static String normalize(String name) {
        requireNonNull(name, "name cannot be null");
        return name.trim().toLowerCase(Locale.ROOT);
    }
}