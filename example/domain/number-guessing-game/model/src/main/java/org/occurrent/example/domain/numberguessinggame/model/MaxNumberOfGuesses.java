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

package org.occurrent.example.domain.numberguessinggame.model;

import java.util.Objects;

public class MaxNumberOfGuesses {
    public final int value;

    public MaxNumberOfGuesses(int value) {
        if (value > 100 || value < 1) {
            throw new IllegalArgumentException("Max number of guesses must be between 1 and 100");
        }
        this.value = value;
    }

    public static MaxNumberOfGuesses of(int value) {
        return new MaxNumberOfGuesses(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MaxNumberOfGuesses)) return false;
        MaxNumberOfGuesses that = (MaxNumberOfGuesses) o;
        return value == that.value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return "MaxNumberOfGuesses{" +
                "value=" + value +
                '}';
    }
}
