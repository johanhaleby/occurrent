package org.occurrent.application.service.blocking;

import org.jspecify.annotations.NullMarked;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * A utility that makes it easier to execute policies (a.k.a triggers) as side-effects after events are written to
 * the event store in a synchronous fashion (if you want async policies then use a subscription instead).
 * A policy is expected to a take one domain event of a specific type ({@code E}) and return void (i.e. a `Consumer<E>`).
 *
 * @param <T> The type of your domain event
 */
@NullMarked
public interface PolicySideEffect<T> extends Consumer<Stream<T>> {

    /**
     * Execute a single policy, for example let's say you have this policy:
     *
     * <pre>
     * public class ExamplePolicy1 {
     *
     *     public static void logWhenGameStarted(GameWasStarted e) {
     *          System.out.printf("Game %s was started\n", e.getId());
     *     }
     * }
     * </pre>
     *
     * <pre>
     * String streamId = ..
     * Function&lt;Stream&lt;DomainEvent&gt;,Stream&lt;DomainEvent&gt;&gt; domainFunction = ..
     *
     * applicationService.execute(streamId, domainFunction, executePolicy(ExamplePolicy1::logWhenGameStarted)
     * </pre>
     * <p>
     * This will call the {@code logWhenGameStarted} function in {@code ExamplePolicy1} after all events returned from {@code domainFunction}
     * has been written to the event store and pass it a {@code GameWasStarted} event instance <i>if</i> the {@code domainFunction}
     * returns such an event. If {@code domainFunction} doesn't return such an event the policy (the {@code logWhenGameStarted} function in {@code ExamplePolicy1})
     * will <i>not</i> be called.
     *
     * @param eventType The type of the domain event
     * @param policy    The policy
     * @param <T>       The type of your domain events
     * @param <E>       The specific event type that the policy is interested in
     * @return A {@link PolicySideEffect}, which is a {@code Consumer<Stream<T>>} that allows composing policies.
     */
    static <T, E extends T> PolicySideEffect<T> executePolicy(Class<E> eventType, Consumer<E> policy) {
        Objects.requireNonNull(eventType, "Event type cannot be null");
        Objects.requireNonNull(policy, "Policy cannot be null");
        return stream -> stream
                .filter(e -> eventType.isAssignableFrom(e.getClass()))
                .map(eventType::cast)
                .forEach(policy);
    }

    /**
     * Compose two policies, for example let's say you have these policies:
     *
     * <pre>
     * public class ExamplePolicy1 {
     *
     *     public static void logWhenGameStarted(GameWasStarted e) {
     *          System.out.printf("Game %s was started\n", e.getId());
     *     }
     * }
     * </pre>
     *
     * <pre>
     * public class ExamplePolicy2 {
     *
     *      public static void logWhenGameEnded(GameWasEnded e) {
     *          System.out.printf("Game %s was ended\n", e.getId());
     *      }
     * }
     * </pre>
     *
     * <pre>
     * String streamId = ..
     * Function&lt;Stream&lt;DomainEvent&gt;,Stream&lt;DomainEvent&gt;&gt; domainFunction = ..
     *
     * applicationService.execute(streamId, domainFunction, executePolicy(ExamplePolicy1::logWhenGameStarted).andThenExecuteAnotherPolicy(ExamplePolicy2::logWhenGameEnded))
     * </pre>
     * <p>
     * This will call the {@code logWhenGameStarted} function in {@code ExamplePolicy1} <i>and</i> {@code logWhenGameEnded} in {@code ExamplePolicy2} after all events returned from {@code domainFunction}
     * has been written to the event store and pass it a {@code GameWasStarted} event instance <i>if</i> the {@code domainFunction}
     * returns such events. If {@code domainFunction} doesn't return any events that a policy in is interested in then <i>no</i> policy will be called.
     *
     * @param eventType The type of the domain event
     * @param policy    The policy
     * @param <E>       The specific event type that the policy is interested in
     * @return A {@link PolicySideEffect}, which is a {@code Consumer<Stream<T>>} that allows composing policies.
     */
    default <E extends T> PolicySideEffect<T> andThenExecuteAnotherPolicy(Class<E> eventType, Consumer<E> policy) {
        return stream -> {
            List<T> list = stream.toList();
            accept(list.stream());
            PolicySideEffect<T> secondPolicy = executePolicy(eventType, policy);
            secondPolicy.accept(list.stream());
        };
    }
}