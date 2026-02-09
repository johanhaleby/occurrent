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
 * @param <T> The type of the event to store. Normally this would be your custom "DomainEvent" class but it could also be {@link CloudEvent}.
 */
@NullMarked
public interface ApplicationService<T> {

    /**
     * Options used when executing a command in {@link ApplicationService}.
     * <p>
     * Use this record to configure:
     * <ul>
     *     <li>a {@link StreamReadFilter} that limits which events are read before command execution</li>
     *     <li>an optional side-effect that is invoked after events have been written</li>
     * </ul>
     * <p>
     * A typical usage pattern is:
     * <pre>{@code
     * applicationService.execute(streamId,
     *         ApplicationService.filter(myFilter).sideEffect(mySideEffect),
     *         functionThatCallsDomainModel);
     * }</pre>
     *
     * @param <T> The event type consumed by the side-effect stream.
     */
    record ExecuteOptions<T>(@Nullable StreamReadFilter filter, @Nullable Consumer<Stream<T>> sideEffect) {
        /**
         * Create empty options, i.e. no read filter and no side-effect.
         */
        public static <T> ExecuteOptions<T> empty() {
            return new ExecuteOptions<>(null, null);
        }

        /**
         * Set stream read filter.
         * <p>
         * Note that the generic type parameter may change when chaining since the side-effect type is established
         * once {@link #sideEffect(Consumer)} is provided.
         *
         * @param filter The filter to use when reading the stream.
         * @param <U>    The side-effect event type for the returned options.
         * @return New options with filter applied.
         */
        public <U> ExecuteOptions<U> filter(StreamReadFilter filter) {
            return new ExecuteOptions<>(Objects.requireNonNull(filter, "filter cannot be null"), null);
        }

        /**
         * Set side-effect to invoke after successful writes.
         *
         * @param sideEffect Side-effect that receives the newly produced domain events.
         * @param <U>        The side-effect event type for the returned options.
         * @return New options with side-effect applied.
         */
        public <U> ExecuteOptions<U> sideEffect(Consumer<Stream<U>> sideEffect) {
            return new ExecuteOptions<>(filter, Objects.requireNonNull(sideEffect, "sideEffect cannot be null"));
        }
    }

    /**
     * Create {@link ExecuteOptions} with a stream read filter.
     *
     * @param filter The filter to use when reading the stream.
     * @param <T>    The side-effect event type to be used if {@link ExecuteOptions#sideEffect(Consumer)} is chained.
     * @return Filtered execute options.
     */
    static <T> ExecuteOptions<T> filter(StreamReadFilter filter) {
        return ExecuteOptions.<T>empty().filter(filter);
    }

    /**
     * Create {@link ExecuteOptions} with a side-effect.
     *
     * @param sideEffect Side-effect that receives the newly produced domain events.
     * @param <T>        The side-effect event type.
     * @return Execute options configured with side-effect.
     */
    static <T> ExecuteOptions<T> sideEffect(Consumer<Stream<T>> sideEffect) {
        return ExecuteOptions.<T>empty().sideEffect(sideEffect);
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
     * @param functionThatCallsDomainModel A <i>pure</i> function that calls the domain model. Use partial application ({@code org.occurrent:command-composition:<version>})
     *                                     if required.
     * @param sideEffect                   Side-effects that are executed <i>after</i> the events have been written to the event store.
     * @deprecated Use {@link #execute(String, ExecuteOptions, Function)}.
     */
    @Deprecated(forRemoval = true)
    WriteResult execute(String streamId, Function<Stream<T>, Stream<T>> functionThatCallsDomainModel, @Nullable Consumer<Stream<T>> sideEffect);

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
    default WriteResult execute(String streamId, @Nullable StreamReadFilter filter, Function<Stream<T>, Stream<T>> functionThatCallsDomainModel, @Nullable Consumer<Stream<T>> sideEffect) {
        if (filter == null) {
            return execute(streamId, functionThatCallsDomainModel, sideEffect);
        }
        throw new UnsupportedOperationException("This ApplicationService implementation does not support StreamReadFilter. Override execute(streamId, filter, functionThatCallsDomainModel, sideEffect) to add support.");
    }

    /**
     * Execute a function that loads events from the event store and applies them to the domain model using {@link ExecuteOptions}.
     *
     * @param streamId                     The id of the stream to load events from and also write the events returned from {@code functionThatCallsDomainModel} to.
     * @param executeOptions               Options that control stream read filtering and optional side-effects.
     * @param functionThatCallsDomainModel A <i>pure</i> function that calls the domain model.
     */
    default WriteResult execute(String streamId, ExecuteOptions<T> executeOptions, Function<Stream<T>, Stream<T>> functionThatCallsDomainModel) {
        Objects.requireNonNull(executeOptions, "Execute options cannot be null");
        return execute(streamId, executeOptions.filter(), functionThatCallsDomainModel, executeOptions.sideEffect());
    }

    /**
     * Convenience function that lets you specify {@code streamId} as a {@code UUID} instead of a {@code String}.
     */
    default WriteResult execute(UUID streamId, ExecuteOptions<T> executeOptions, Function<Stream<T>, Stream<T>> functionThatCallsDomainModel) {
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
    default WriteResult execute(UUID streamId, Function<Stream<T>, Stream<T>> functionThatCallsDomainModel, Consumer<Stream<T>> sideEffect) {
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
    default WriteResult execute(UUID streamId, StreamReadFilter filter, Function<Stream<T>, Stream<T>> functionThatCallsDomainModel, Consumer<Stream<T>> sideEffect) {
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
