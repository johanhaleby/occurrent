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

package org.occurrent.application.service.blocking.generic.support;

import org.junit.jupiter.api.Disabled;
import org.occurrent.domain.NameDefined;

import java.util.concurrent.atomic.AtomicReference;

@Disabled
public class WhenNameDefinedThenCountAverageSizeOfNamePolicy {

    AtomicReference<State> averageSizeOfName = new AtomicReference<>(new State());

    public void whenNameDefinedThenCountAverageSizeOfName(NameDefined nameDefined) {
        averageSizeOfName.getAndUpdate(state -> state.addName(nameDefined.name().length()));
    }

    public int getAverageSizeOfName() {
        return averageSizeOfName.get().averagesSizeOfName();
    }

    private static class State {
        private int numberOfNamesDefined;
        private int accumulatedSizeOfNames;

        State() {
        }

        State(int numberOfNamesDefined, int accumulatedSizeOfNames) {
            this.numberOfNamesDefined = numberOfNamesDefined;
            this.accumulatedSizeOfNames = accumulatedSizeOfNames;
        }

        State addName(int sizeOfName) {
            return new State(this.numberOfNamesDefined + 1, accumulatedSizeOfNames + sizeOfName);
        }

        int averagesSizeOfName() {
            return numberOfNamesDefined == 0 ? 0 : accumulatedSizeOfNames / numberOfNamesDefined;
        }
    }
}
