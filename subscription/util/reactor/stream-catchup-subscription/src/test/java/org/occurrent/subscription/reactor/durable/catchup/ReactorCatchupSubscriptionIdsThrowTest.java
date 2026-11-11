package org.occurrent.subscription.reactor.durable.catchup;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.reactor.durable.catchup.ReactorStreamCatchupSubscriptionModelHistoryLossTest.ScriptedReader;
import reactor.core.publisher.Mono;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

// What a cancel and a subscribe leave when the wrapped model throws as the catch-up asks which ids it holds
class ReactorCatchupSubscriptionIdsThrowTest {
    private static final String ID = "sub";

    private final ScriptedReader reader = new ScriptedReader();
    private final CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();

    @Test
    void a_cancel_whose_wrapped_cancel_throws_ends_even_when_putting_the_handover_back_throws() {
        // Given a subscription handed over, whose wrapped cancel throws once
        IntrospectableLosingLiveStartModel wrapped = new IntrospectableLosingLiveStartModel();
        ReactorStreamCatchupSubscriptionModel catchup = catchupOver(wrapped);
        try {
            subscribeAndHandOver(catchup);
            IllegalStateException wrappedFailure = new IllegalStateException("the wrapped cancel throws");
            wrapped.nextCancelThrows = wrappedFailure;
            PutIfAbsentHook.installIn(catchup, "handedOver").afterThePutIfAbsentIn("putBackTheHandover", () -> {
                throw new IllegalStateException("putting the handover back throws");
            });

            // When that cancel throws, and a second cancel ends
            Throwable first = catchThrowable(() -> catchup.cancelSubscription(ID));
            Throwable second = catchThrowable(() -> catchup.cancelSubscription(ID).block(Duration.ofSeconds(5)));
            Set<String> heldAfterTheCancels = wrapped.heldIds();

            // Then a subscribe of the id goes through
            Throwable subscribe = catchThrowable(() -> catchup.subscribe(ID, StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(GlobalCheckpoint.of(0)),
                    cloudEvent -> Mono.empty()));
            assertAll(
                    () -> assertThat(first).as("what the first cancel throws").isSameAs(wrappedFailure),
                    () -> assertThat(first.getSuppressed()).as("what the first cancel's exception suppresses")
                            .anySatisfy(suppressed -> assertThat(suppressed).hasMessage("putting the handover back throws")),
                    () -> assertThat(second).as("what the second cancel throws").isNull(),
                    () -> assertThat(heldAfterTheCancels).as("the ids the wrapped model holds after the second cancel").doesNotContain(ID),
                    () -> assertThat(subscribe).as("what a subscribe of the id throws once every cancel of it ended").isNull()
            );
        } finally {
            catchup.shutdown();
        }
    }

    @Test
    void a_cancel_whose_wrapped_cancel_fails_puts_the_handover_back_when_asking_for_the_ids_throws_so_a_later_loss_of_the_live_start_is_taken_over() {
        // Given a subscription handed over, whose wrapped cancel fails, and whose wrapped model throws once when asked
        // for its ids as the handover is put back
        IntrospectableLosingLiveStartModel wrapped = new IntrospectableLosingLiveStartModel();
        ReactorStreamCatchupSubscriptionModel catchup = catchupOver(wrapped);
        try {
            subscribeAndHandOver(catchup);
            Checkpoint liveStart = wrapped.positionOf(ID);
            reader.head = 4;
            IllegalStateException wrappedFailure = new IllegalStateException("the wrapped cancel fails");
            wrapped.cancelCompletesWhen = Mono.error(wrappedFailure);
            wrapped.idsThrowOnceFrom("putBackTheHandover");

            // When the cancel fails, and the wrapped model, which still runs the subscription, then loses the history
            // from its live start
            Throwable cancel = catchThrowable(() -> catchup.cancelSubscription(ID).block(Duration.ofSeconds(5)));
            wrapped.cancelCompletesWhen = Mono.empty();
            wrapped.losesTheHistoryFrom(ID, liveStart);
            awaitNoReplayInFlight(catchup);

            // Then the catch-up takes that loss over
            assertAll(
                    () -> assertThat(wrapped.idsThrew).as("whether asking for the ids threw as the handover was put back").isTrue(),
                    () -> assertThat(cancel).as("what the cancel fails with").isSameAs(wrappedFailure),
                    () -> assertThat(cancel.getSuppressed()).as("what the cancel's failure suppresses")
                            .anySatisfy(suppressed -> assertThat(suppressed).hasMessageContaining("subscriptionIds() throws once")),
                    () -> assertThat(received).as("what the action received once the wrapped model lost the live start").contains("e4"),
                    () -> assertThat(catchup.isRunning(ID)).as("whether the subscription runs").isTrue()
            );
        } finally {
            catchup.shutdown();
        }
    }

    @Test
    void a_cancel_whose_wrapped_cancel_drops_the_subscription_and_throws_throws_that_exception_when_asking_for_the_ids_throws_as_it_settles() {
        // Given a subscription handed over, whose wrapped cancel drops it and then throws, and whose wrapped model throws
        // once when asked for its ids as the refused cancel settles
        IntrospectableLosingLiveStartModel wrapped = new IntrospectableLosingLiveStartModel();
        ReactorStreamCatchupSubscriptionModel catchup = catchupOver(wrapped);
        try {
            subscribeAndHandOver(catchup);
            IllegalStateException wrappedFailure = new IllegalStateException("the wrapped cancel throws");
            wrapped.nextCancelDropsFirst = true;
            wrapped.nextCancelThrows = wrappedFailure;
            wrapped.idsThrowOnceFrom("cancelRefused");

            // When the cancel throws
            Throwable cancel = catchThrowable(() -> catchup.cancelSubscription(ID));

            // Then it throws what the wrapped model threw, and a subscribe of the id goes through
            Throwable subscribe = catchThrowable(() -> catchup.subscribe(ID, StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(GlobalCheckpoint.of(0)),
                    cloudEvent -> Mono.empty()));
            assertAll(
                    () -> assertThat(wrapped.idsThrew).as("whether asking for the ids threw as the refused cancel settled").isTrue(),
                    () -> assertThat(cancel).as("what the cancel throws").isSameAs(wrappedFailure),
                    () -> assertThat(cancel.getSuppressed()).as("what the cancel's exception suppresses")
                            .anySatisfy(suppressed -> assertThat(suppressed).hasMessageContaining("subscriptionIds() throws once")),
                    () -> assertThat(subscribe).as("what a subscribe of the id throws once the wrapped model dropped it").isNull()
            );
        } finally {
            catchup.shutdown();
        }
    }

    @Test
    void a_subscribe_straight_to_live_cancelled_inside_the_wrapped_subscribe_has_what_it_left_cancelled_when_asking_for_the_ids_throws_as_it_settles() {
        // Given a subscribe straight to live, cancelled while it is inside the wrapped subscribe(..) before the wrapped
        // model registers the subscription, and a wrapped model that throws once when asked for its ids as that
        // subscribe settles
        IntrospectableLosingLiveStartModel wrapped = new IntrospectableLosingLiveStartModel();
        ReactorStreamCatchupSubscriptionModel catchup = catchupOver(wrapped);
        AtomicReference<@Nullable Throwable> cancel = new AtomicReference<>();
        AtomicBoolean once = new AtomicBoolean();
        try {
            wrapped.beforeSubscribeRegisters = () -> {
                if (once.compareAndSet(false, true)) {
                    cancel.set(catchThrowable(() -> catchup.cancelSubscription(ID).block(Duration.ofSeconds(5))));
                    wrapped.idsThrowOnceFrom("settleHoldingIdLock");
                }
            };

            // When that subscribe returns
            Throwable subscribe = catchThrowable(() -> catchup.subscribe(ID, StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(new StringBasedCheckpoint("straight")),
                    cloudEvent -> Mono.empty()));
            Set<String> heldOnceItReturned = wrapped.heldIds();

            // Then the wrapped model cancelled what it registered, and a subscribe of the id goes through
            Throwable again = catchThrowable(() -> catchup.subscribe(ID, StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(new StringBasedCheckpoint("straight")),
                    cloudEvent -> Mono.empty()));
            assertAll(
                    () -> assertThat(wrapped.idsThrew).as("whether asking for the ids threw as the subscribe settled").isTrue(),
                    () -> assertThat(cancel.get()).as("what the cancel throws").isNull(),
                    () -> assertThat(subscribe).as("what the cancelled subscribe throws").isNull(),
                    () -> assertThat(heldOnceItReturned).as("the ids the wrapped model holds once the cancelled subscribe returned").doesNotContain(ID),
                    () -> assertThat(again).as("what a later subscribe of the id throws").isNull()
            );
        } finally {
            catchup.shutdown();
        }
    }

    private ReactorStreamCatchupSubscriptionModel catchupOver(IntrospectableLosingLiveStartModel wrapped) {
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, reader);
        catchup.listenForCatchup(ID, new CatchupListener() {
            @Override
            public void catchupStarted(Object episode) {
            }

            @Override
            public void historyRead(Object episode) {
            }
        });
        return catchup;
    }

    private void subscribeAndHandOver(ReactorStreamCatchupSubscriptionModel catchup) {
        Subscription subscription = catchup.subscribe(ID, StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(GlobalCheckpoint.of(0)),
                cloudEvent -> Mono.fromRunnable(() -> received.add(cloudEvent.getId())));
        subscription.waitUntilStarted().block(Duration.ofSeconds(5));
        assertThat(received).containsExactly("e1", "e2", "e3");
    }

    static void awaitNoReplayInFlight(ReactorStreamCatchupSubscriptionModel catchup) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (catchup.isCatchingUp(ID)) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("A replay of " + ID + " still in flight after 10 seconds");
            }
            Thread.onSpinWait();
        }
    }

    // Takes the place of a map of the catch-up and runs a hook once, right after the first putIfAbsent on it that a
    // given method of the catch-up makes itself
    @SuppressWarnings({"rawtypes", "unchecked"})
    static final class PutIfAbsentHook extends ConcurrentHashMap {
        private final AtomicReference<@Nullable String> putIfAbsentIn = new AtomicReference<>();
        private final AtomicReference<@Nullable Runnable> afterPutIfAbsent = new AtomicReference<>();

        static PutIfAbsentHook installIn(ReactorStreamCatchupSubscriptionModel model, String fieldName) {
            PutIfAbsentHook hook = new PutIfAbsentHook();
            try {
                Field field = NamedCatchupSupport.class.getDeclaredField(fieldName);
                field.setAccessible(true);
                hook.putAll((Map) field.get(model.namedSubscriptions));
                field.set(model.namedSubscriptions, hook);
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException(e);
            }
            return hook;
        }

        void afterThePutIfAbsentIn(String method, Runnable hook) {
            putIfAbsentIn.set(method);
            afterPutIfAbsent.set(hook);
        }

        @Override
        public Object putIfAbsent(Object key, Object value) {
            Object existing = super.putIfAbsent(key, value);
            String method = putIfAbsentIn.get();
            if (method != null && calledFrom(method)) {
                Runnable run = afterPutIfAbsent.getAndSet(null);
                if (run != null) {
                    putIfAbsentIn.set(null);
                    run.run();
                }
            }
            return existing;
        }

        private static boolean calledFrom(String method) {
            return StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE).walk(frames -> frames
                    .filter(frame -> frame.getDeclaringClass() == NamedCatchupSupport.class)
                    .findFirst()
                    .map(frame -> frame.getMethodName().equals(method) || frame.getMethodName().startsWith("lambda$" + method + "$"))
                    .orElse(false));
        }
    }
}
