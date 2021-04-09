package org.occurrent.application.service.blocking;

import io.cloudevents.CloudEvent;
import org.occurrent.eventstore.api.WriteResult;

import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * An application service interface that should be good enough for most scenarios.
 * Combine it with command composition ({@code org.occurrent:command-composition:<version>}) to solve
 * an even more wide-range of use cases. See <a href="https://occurrent.org/documentation#commands">command documentation</a> for more information.
 *
 * @param <T> The type of the event to store. Normally this would be your custom "DomainEvent" class but it could also be {@link CloudEvent}.
 */
public interface ApplicationService<T> {

    /**
     * Execute a function that loads the events from the event store and apply them to the {@code functionThatCallsDomainModel} and
     * also execute side-effects that are executed synchronously <i>after</i> the events have been written to the event store.
     * If the side-effects write data to the same datastore as the event store you can make use of transactions to write events
     * and side-effects atomically.
     * <br>
     * <br>
     * <p>
     * Note that if you domain model works with {@code java.util.List} as input and output, then depend on the
     * command composition ({@code org.occurrent:command-composition:<version>}) library to convert {@code functionThatCallsDomainModel}
     * from {@code Function<Stream<T>, Stream<T>>} to {@code Function<List<T>, List<T>>} by using
     * {@code org.occurrent.application.command.composition.toListCommand(functionThatCallsDomainModel)}.
     * </p>
     *
     * @param streamId                     The id of the stream to load events from and also write the events returned from {@code functionThatCallsDomainModel} to.
     * @param functionThatCallsDomainModel A <i>pure</i> function that calls the domain model. Use partial application ({@code org.occurrent:command-composition:<version>})
     *                                     if required.
     * @param sideEffect                   Side-effects that are executed <i>after</i> the events have been written to the event store.
     */
    WriteResult execute(String streamId, Function<Stream<T>, Stream<T>> functionThatCallsDomainModel, Consumer<Stream<T>> sideEffect);


    /**
     * Convenience function that lets you specify {@code streamId} as a {@code UUID} instead of a {@code String}. Simply delegates to {@link #execute(String, Function, Consumer)}.
     *
     * @param streamId                     The id of the stream to load events from and also write the events returned from {@code functionThatCallsDomainModel} to.
     * @param functionThatCallsDomainModel A <i>pure</i> function that calls the domain model. Use partial application ({@code org.occurrent:command-composition:<version>})
     *                                     if required.
     * @param sideEffect                   Side-effects that are executed <i>after</i> the events have been written to the event store.
     * @see #execute(String, Function, Consumer)
     */
    default WriteResult execute(UUID streamId, Function<Stream<T>, Stream<T>> functionThatCallsDomainModel, Consumer<Stream<T>> sideEffect) {
        Objects.requireNonNull(streamId, "Stream id cannot be null");
        return execute(streamId.toString(), functionThatCallsDomainModel, sideEffect);
    }

    /**
     * Execute a function that loads the events from the event store and apply them to the {@code functionThatCallsDomainModel}.
     * <br>
     * <br>
     * <p>
     * Note that if you domain model works with {@code java.util.List} as input and output, then depend on the
     * command composition ({@code org.occurrent:command-composition:<version>}) library to convert {@code functionThatCallsDomainModel}
     * from {@code Function<Stream<T>, Stream<T>>} to {@code Function<List<T>, List<T>>} by using
     * {@code org.occurrent.application.command.composition.toListCommand(functionThatCallsDomainModel)}.
     * </p>
     *
     * @param streamId                     The id of the stream to load events from and also write the events returned from {@code functionThatCallsDomainModel} to.
     * @param functionThatCallsDomainModel A <i>pure</i> function that calls the domain model. Use partial application ({@code org.occurrent:command-composition:<version>})
     *                                     if required.
     */
    default WriteResult execute(String streamId, Function<Stream<T>, Stream<T>> functionThatCallsDomainModel) {
        return execute(streamId, functionThatCallsDomainModel, null);
    }

    /**
     * Convenience function that lets you specify {@code streamId} as a {@code UUID} instead of a {@code String}. Simply delegates to {@link #execute(String, Function)}.
     *
     * @param streamId                     The id of the stream to load events from and also write the events returned from {@code functionThatCallsDomainModel} to.
     * @param functionThatCallsDomainModel A <i>pure</i> function that calls the domain model. Use partial application ({@code org.occurrent:command-composition:<version>})
     *                                     if required.
     * @see #execute(String, Function)
     */
    default WriteResult execute(UUID streamId, Function<Stream<T>, Stream<T>> functionThatCallsDomainModel) {
        Objects.requireNonNull(streamId, "Stream id cannot be null");
        return execute(streamId.toString(), functionThatCallsDomainModel);
    }
}
