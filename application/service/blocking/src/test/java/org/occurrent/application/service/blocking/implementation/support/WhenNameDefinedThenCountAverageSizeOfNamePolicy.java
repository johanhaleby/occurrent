package org.occurrent.application.service.blocking.implementation.support;

import org.junit.jupiter.api.Disabled;
import org.occurrent.domain.NameDefined;

import java.util.concurrent.atomic.AtomicReference;

@Disabled
public class WhenNameDefinedThenCountAverageSizeOfNamePolicy {

    AtomicReference<State> averageSizeOfName = new AtomicReference<>(new State());

    public void whenNameDefinedThenCountAverageSizeOfName(NameDefined nameDefined) {
        averageSizeOfName.getAndUpdate(state -> state.addName(nameDefined.getName().length()));
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
