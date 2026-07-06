/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.subscription;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * A simple {@link Checkpoint} that is backed by a fixed String
 */
@NullMarked
public class StringBasedCheckpoint implements Checkpoint {
    private final String value;

    public StringBasedCheckpoint(String value) {
        Objects.requireNonNull(value, "Stream position value cannot be null");
        this.value = value;
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (!(o instanceof StringBasedCheckpoint that)) return false;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return "StringBasedStreamPosition{" +
                "value='" + value + '\'' +
                '}';
    }

    @Override
    public String asString() {
        return value;
    }
}
