package org.occurrent.application.service.blocking;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.eventstore.api.StreamReadFilter;
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
 * @param <E> The type of the event to store. Normally this would be your custom "DomainEvent" class but it could also be {@link CloudEvent}.
 */
@NullMarked
public interface ApplicationService<E> {

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
     * @deprecated Use {@link #execute(String, ExecuteOptions, Function)}.
     */
    @Deprecated(forRemoval = true)
    default WriteResult execute(String streamId, Function<Stream<E>, Stream<E>> functionThatCallsDomainModel, @Nullable Consumer<Stream<E>> sideEffect) {
        Objects.requireNonNull(streamId, "Stream id cannot be null");
        Objects.requireNonNull(functionThatCallsDomainModel, "functionThatCallsDomainModel cannot be null");

        final ExecuteOptions<E> options = ExecuteOptions.options().sideEffect(sideEffect);
        return execute(streamId, options, functionThatCallsDomainModel);
    }

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
     * @param filter                       A filter to apply when reading events from the event store.
     * @param functionThatCallsDomainModel A <i>pure</i> function that calls the domain model. Use partial application ({@code org.occurrent:command-composition:<version>})
     *                                     if required.
     * @param sideEffect                   Side-effects that are executed <i>after</i> the events have been written to the event store.
     * @deprecated Use {@link #execute(String, ExecuteOptions, Function)}.
     */
    @Deprecated(forRemoval = true)
    default WriteResult execute(String streamId, @Nullable StreamReadFilter filter, Function<Stream<E>, Stream<E>> functionThatCallsDomainModel, @Nullable Consumer<Stream<E>> sideEffect) {
        Objects.requireNonNull(streamId, "Stream id cannot be null");
        Objects.requireNonNull(functionThatCallsDomainModel, "functionThatCallsDomainModel cannot be null");

        // Delegate to the single method that implementors must provide.
        final ExecuteOptions<E> options = ExecuteOptions.options().filter(filter).sideEffect(sideEffect);
        return execute(streamId, options, functionThatCallsDomainModel);
    }

    /**
     * Execute a function that loads events from the event store and applies them to the domain model using {@link ExecuteOptions}.
     *
     * @param streamId                     The id of the stream to load events from and also write the events returned from {@code functionThatCallsDomainModel} to.
     * @param executeOptions               Options that control stream read filtering and optional side-effects.
     * @param functionThatCallsDomainModel A <i>pure</i> function that calls the domain model.
     */
    WriteResult execute(String streamId, ExecuteOptions<E> executeOptions, Function<Stream<E>, Stream<E>> functionThatCallsDomainModel);

    /**
     * Convenience function that lets you specify {@code streamId} as a {@code UUID} instead of a {@code String}.
     */
    default WriteResult execute(UUID streamId, ExecuteOptions<E> executeOptions, Function<Stream<E>, Stream<E>> functionThatCallsDomainModel) {
        Objects.requireNonNull(streamId, "Stream id cannot be null");
        return execute(streamId.toString(), executeOptions, functionThatCallsDomainModel);
    }

    /**
     * Convenience function that lets you specify {@code streamId} as a {@code UUID} instead of a {@code String}. Simply delegates to {@link #execute(String, Function, Consumer)}.
     *
     * @param streamId                     The id of the stream to load events from and also write the events returned from {@code functionThatCallsDomainModel} to.
     * @param functionThatCallsDomainModel A <i>pure</i> function that calls the domain model. Use partial application ({@code org.occurrent:command-composition:<version>})
     *                                     if required.
     * @param sideEffect                   Side-effects that are executed <i>after</i> the events have been written to the event store.
     * @see #execute(String, Function, Consumer)
     * @deprecated Use {@link #execute(UUID, ExecuteOptions, Function)}.
     */
    @Deprecated(forRemoval = true)
    default WriteResult execute(UUID streamId, Function<Stream<E>, Stream<E>> functionThatCallsDomainModel, Consumer<Stream<E>> sideEffect) {
        Objects.requireNonNull(streamId, "Stream id cannot be null");
        return execute(streamId.toString(), functionThatCallsDomainModel, sideEffect);
    }

    /**
     * Convenience function that lets you specify {@code streamId} as a {@code UUID} instead of a {@code String}. Simply delegates to {@link #execute(String, Function, Consumer)}.
     *
     * @param streamId                     The id of the stream to load events from and also write the events returned from {@code functionThatCallsDomainModel} to.
     * @param filter                       A filter to apply when reading events from the event store.
     * @param functionThatCallsDomainModel A <i>pure</i> function that calls the domain model. Use partial application ({@code org.occurrent:command-composition:<version>})
     *                                     if required.
     * @param sideEffect                   Side-effects that are executed <i>after</i> the events have been written to the event store.
     * @see #execute(String, Function, Consumer)
     * @deprecated Use {@link #execute(UUID, ExecuteOptions, Function)}.
     */
    @Deprecated(forRemoval = true)
    default WriteResult execute(UUID streamId, StreamReadFilter filter, Function<Stream<E>, Stream<E>> functionThatCallsDomainModel, Consumer<Stream<E>> sideEffect) {
        Objects.requireNonNull(streamId, "Stream id cannot be null");
        return execute(streamId.toString(), filter, functionThatCallsDomainModel, sideEffect);
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
    default WriteResult execute(String streamId, Function<Stream<E>, Stream<E>> functionThatCallsDomainModel) {
        return execute(streamId, ExecuteOptions.empty(), functionThatCallsDomainModel);
    }

    /**
     * Convenience function that lets you specify {@code streamId} as a {@code UUID} instead of a {@code String}. Simply delegates to {@link #execute(String, Function)}.
     *
     * @param streamId                     The id of the stream to load events from and also write the events returned from {@code functionThatCallsDomainModel} to.
     * @param functionThatCallsDomainModel A <i>pure</i> function that calls the domain model. Use partial application ({@code org.occurrent:command-composition:<version>})
     *                                     if required.
     * @see #execute(String, Function)
     */
    default WriteResult execute(UUID streamId, Function<Stream<E>, Stream<E>> functionThatCallsDomainModel) {
        Objects.requireNonNull(streamId, "Stream id cannot be null");
        return execute(streamId.toString(), functionThatCallsDomainModel);
    }
}