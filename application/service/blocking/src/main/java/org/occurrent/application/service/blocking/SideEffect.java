package org.occurrent.application.service.blocking;

import org.jspecify.annotations.NullMarked;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * A utility that makes it easier to run side-effects (a.k.a triggers/policies) after events are written to
 * the event store in a synchronous fashion (if you want an async side-effect then use a subscription instead).
 * A side-effect is expected to take one domain event of a specific type ({@code E}) and return void (i.e. a `Consumer<E>`).
 *
 * @param <E> The type of your domain event
 */
@NullMarked
public interface SideEffect<E> extends Consumer<List<E>> {

    /**
     * Execute a single side-effect, for example let's say you have this side-effect:
     *
     * <pre>
     * public class ExampleSideEffect1 {
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
     * applicationService.execute(streamId, domainFunction, executeSideEffect(ExampleSideEffect1::logWhenGameStarted)
     * </pre>
     * <p>
     * This will call the {@code logWhenGameStarted} function in {@code ExampleSideEffect1} after all events returned from {@code domainFunction}
     * has been written to the event store and pass it a {@code GameWasStarted} event instance <i>if</i> the {@code domainFunction}
     * returns such an event. If {@code domainFunction} doesn't return such an event the side-effect (the {@code logWhenGameStarted} function in {@code ExampleSideEffect1})
     * will <i>not</i> be called.
     *
     * @param eventType  The type of the domain event
     * @param sideEffect The side-effect
     * @param <E>        The type of your domain events
     * @param <E_SPECIFIC>       The specific event type that the side-effect is interested in
     * @return A {@link SideEffect}, which is a {@code Consumer<List<T>>} that allows composing side-effects.
     */
    static <E, E_SPECIFIC extends E> SideEffect<E> executeSideEffect(Class<E_SPECIFIC> eventType, Consumer<E_SPECIFIC> sideEffect) {
        Objects.requireNonNull(eventType, "Event type cannot be null");
        Objects.requireNonNull(sideEffect, "Side-effect cannot be null");
        return events -> events.stream()
                .filter(e -> eventType.isAssignableFrom(e.getClass()))
                .map(eventType::cast)
                .forEach(sideEffect);
    }

    /**
     * Compose two side-effects, for example let's say you have these side-effects:
     *
     * <pre>
     * public class ExampleSideEffect1 {
     *
     *     public static void logWhenGameStarted(GameWasStarted e) {
     *          System.out.printf("Game %s was started\n", e.getId());
     *     }
     * }
     * </pre>
     *
     * <pre>
     * public class ExampleSideEffect2 {
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
     * applicationService.execute(streamId, domainFunction, executeSideEffect(ExampleSideEffect1::logWhenGameStarted).andThenExecuteAnotherSideEffect(ExampleSideEffect2::logWhenGameEnded))
     * </pre>
     * <p>
     * This will call the {@code logWhenGameStarted} function in {@code ExampleSideEffect1} <i>and</i> {@code logWhenGameEnded} in {@code ExampleSideEffect2} after all events returned from {@code domainFunction}
     * has been written to the event store and pass it a {@code GameWasStarted} event instance <i>if</i> the {@code domainFunction}
     * returns such events. If {@code domainFunction} doesn't return any events that a side-effect is interested in then <i>no</i> side-effect will be called.
     *
     * @param eventType  The type of the domain event
     * @param sideEffect The side-effect
     * @param <E_SPECIFIC>       The specific event type that the side-effect is interested in
     * @return A {@link SideEffect}, which is a {@code Consumer<List<T>>} that allows composing side-effects.
     */
    default <E_SPECIFIC extends E> SideEffect<E> andThenExecuteAnotherSideEffect(Class<E_SPECIFIC> eventType, Consumer<E_SPECIFIC> sideEffect) {
        return events -> {
            accept(events);
            SideEffect<E> secondSideEffect = executeSideEffect(eventType, sideEffect);
            secondSideEffect.accept(events);
        };
    }
}
