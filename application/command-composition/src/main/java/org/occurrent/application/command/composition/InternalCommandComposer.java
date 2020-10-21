package org.occurrent.application.command.composition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

class InternalCommandComposer<T> {

    private final List<Function<List<T>, List<T>>> commands;

    InternalCommandComposer(List<Function<List<T>, List<T>>> commands) {
        this.commands = commands;
    }

    Function<List<T>, List<T>> compose() {
        return initialEvents -> {
            State<T> resultingState = commands.stream().collect(() -> new State<>(initialEvents),
                    (state, fn) -> {
                        List<T> newEvents = fn.apply(state.events);
                        state.addAll(newEvents);
                    }, (state1, state2) -> {
                    });
            return resultingState.events;
        };
    }

    private static class State<T> {
        private final List<T> events;

        private State(List<T> events) {
            this.events = new ArrayList<>(events);
        }

        private void addAll(List<T> events) {
            this.events.addAll(events);
        }
    }

    @SuppressWarnings("unchecked")
    static <T> List<T> createList(T firstCondition, T secondCondition, T[] additionalConditions) {
        requireNonNull(firstCondition, "First condition cannot be null");
        requireNonNull(secondCondition, "Second condition cannot be null");
        T[] additionalConditionsToUse = additionalConditions == null ? (T[]) Collections.emptyList().toArray() : additionalConditions;
        List<T> conditions = new ArrayList<>(2 + additionalConditionsToUse.length);
        conditions.add(firstCondition);
        conditions.add(secondCondition);
        Collections.addAll(conditions, additionalConditionsToUse);
        return conditions;
    }
}