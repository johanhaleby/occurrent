package org.occurrent.application.composition.command.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class SequentialFunctionComposer<T> {

    private final List<Function<List<T>, List<T>>> functions;

    public SequentialFunctionComposer(List<Function<List<T>, List<T>>> functions) {
        this.functions = functions;
    }

    public Function<List<T>, List<T>> compose() {
        return initialEvents -> {
            SequentialFunctionComposer.State<T> resultingState = functions.stream().collect(() -> new State<>(initialEvents),
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
}