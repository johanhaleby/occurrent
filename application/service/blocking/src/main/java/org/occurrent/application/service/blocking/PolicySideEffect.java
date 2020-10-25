package org.occurrent.application.service.blocking;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface PolicySideEffect<T> extends Consumer<Stream<T>> {

    static <T, E extends T> PolicySideEffect<T> executePolicy(Class<E> eventType, Consumer<E> policy) {
        Objects.requireNonNull(eventType, "Event type cannot be null");
        Objects.requireNonNull(policy, "Policy cannot be null");
        return stream -> stream
                .filter(e -> eventType.isAssignableFrom(e.getClass()))
                .map(eventType::cast)
                .forEach(policy);
    }

    default <E extends T> PolicySideEffect<T> andThenExecuteAnotherPolicy(Class<E> eventType, Consumer<E> policy) {
        return stream -> {
            List<T> list = stream.collect(Collectors.toList());
            accept(list.stream());
            PolicySideEffect<T> secondPolicy = executePolicy(eventType, policy);
            secondPolicy.accept(list.stream());
        };
    }
}