/*
 * Copyright 2026 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.occurrent.broker.rabbitmq.blocking;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.ShutdownSignalException;
import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.broker.api.blocking.DestinationResolver;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.RoutingOutcome;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.push.blocking.CatchupThenPushSubscriptionModel;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Deque;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongConsumer;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

/**
 * Bridges a RabbitMQ queue into a {@link PushSubscriptionModel}, the CloudEvent-level consume side ADR 133 decision 1
 * describes. Rebuilds each message as a {@link CloudEvent} through {@link RabbitMqCloudEventMapper}, hands it to
 * {@link PushSubscriptionModel#acceptRedeliverable(CloudEvent)}, and acknowledges only once the {@link RoutingOutcome}
 * that reported through a shared {@link RoutingOutcomeChannel} says the event was actually consumed.
 * <p>
 * <strong>Holds a {@link PushSubscriptionModel}, never a {@link CatchupThenPushSubscriptionModel}.</strong> ADR 133
 * decision 1 is explicit that a bridge feeds the live model, not the catch-up wrapper in front of it, since a
 * {@code CatchupThenPushSubscriptionModel} is not itself a push target: it composes a {@link PushSubscriptionModel}
 * as a constructor argument and replays history through it before handing over. An application that wants catch-up
 * builds one from the same {@link PushSubscriptionModel} this bridge is given, in front of it, not instead of it.
 * <p>
 * <strong>Acknowledgement.</strong> {@code acceptRedeliverable(...)} throwing (a handler exception, or a
 * subscription filter that failed to evaluate) never acknowledges. A normal return with
 * {@link RoutingOutcome#DELIVERED} or {@link RoutingOutcome#FILTERED} acknowledges. A normal return with
 * {@link RoutingOutcome#UNAVAILABLE} never acknowledges either, and is held and paced rather than sent through a
 * failure policy, see below. A normal return with {@link RoutingOutcome#NOT_DELIVERABLE} cannot happen, since that
 * outcome always comes with an exception, the filter's own or a transient action refusal's. A
 * {@link RoutingOutcome#REFUSED} stops this bridge for
 * good, also below. For every other failure this bridge's configured {@link DeliveryFailurePolicy} applies, {@link DeliveryFailurePolicy#REDELIVER} (the default) negatively
 * acknowledges with requeue, {@link DeliveryFailurePolicy#PARK} republishes to a parking destination and only then
 * acknowledges the original. A normal return with {@link RoutingOutcome#DEFERRED}, a
 * {@link CatchupThenPushSubscriptionModel} wrapping {@code model} still replaying or draining, say, also never
 * acknowledges, but always negatively acknowledges with requeue, bypassing {@link DeliveryFailurePolicy} entirely:
 * nothing here is broken, only not ready yet, and {@code PARK} exists for failures, not for pacing.
 * <p>
 * <strong>Topology.</strong> By default this bridge declares its own queue (durable, not exclusive, not
 * auto-delete) and binds it to {@link Builder#bindings(Set)} if given, or else to
 * {@link DestinationResolver#destinationsFor(SubscriptionFilter)} for {@link Builder#bindingFilter(SubscriptionFilter)}
 * if given (falling back to {@link DestinationResolver#catchAllDestination()} when the resolver cannot narrow it),
 * or else to {@link DestinationResolver#catchAllDestination()} outright. {@link Builder#declareTopology(boolean)
 * declareTopology(false)} skips all of this for a deployment whose platform team owns the queue and its bindings
 * itself, per #415. A binding only narrows what arrives. A {@link SubscriptionFilter} on anything other than the
 * event type is invisible to it, and {@code acceptRedeliverable(...)} still applies the subscription's own filter
 * regardless of what was bound.
 * <p>
 * <strong>{@link Builder#build()} retries a broker briefly unreachable, per #867, on the {@code Connection} it was
 * given.</strong> Opening the channel, declaring the queue and its bindings, and setting QoS all happen inside
 * {@link Builder#retryStrategy(RetryStrategy)}, exponential backoff from 100 ms up to 2 seconds by default, ten
 * attempts in total, every one of them against that same supplied {@code Connection}. {@code build()} never
 * creates or reconnects the {@code Connection} itself, so this survives only a broker briefly unreachable while
 * that {@code Connection}'s own automatic recovery is what is reopening it; a {@code Connection} with automatic
 * recovery disabled, or a broker still unreachable when the {@code Connection} was created in the first place,
 * stays dead for every attempt. See that method's own javadoc for exactly what is retried and what is refused
 * immediately.
 * <p>
 * <strong>Coarse lifecycle.</strong> A background poll, {@link Builder#pollInterval(Duration)} apart (one second by
 * default), reads {@link PushSubscriptionModel#subscriptionIds()} and {@link PushSubscriptionModel#isRunning(String)}
 * and starts or cancels this bridge's own AMQP consumer to match: consuming while the model has a running
 * subscription, not consuming otherwise. This is deliberately coarse, a small delay either way is harmless, and it
 * exists so this bridge never feeds a stopped or paused model, which per ADR 85 and ADR 104 drops the event rather
 * than holding it. Never used to decide a single message, that decision comes from the {@link RoutingOutcome} above.
 * The same poll also reads {@link Builder#readinessSource(Predicate)} for the subscription id, {@code true} by
 * default, so this bridge pulls fewer messages off the queue while a {@link CatchupThenPushSubscriptionModel}
 * wrapping {@code model} is still replaying or draining into it, cutting down on {@link RoutingOutcome#DEFERRED}
 * redeliveries during a replay. Pacing only: {@link RoutingOutcome#DEFERRED} is what keeps this bridge correct with
 * no {@code readinessSource} configured at all, just noisier, and even that noise is bounded on its own. A
 * {@code DEFERRED} delivery is held unacked rather than nacked immediately, so with {@link Builder#prefetchCount(int)}
 * left at its default of one, the broker sends nothing further on this consumer until the next poll releases it,
 * which is what bounds a replay with no {@code readinessSource} configured to at most one redelivery per
 * {@link Builder#pollInterval(Duration)} (or {@code prefetchCount} many, configured above the default) rather than
 * refusing and requeuing continuously for the whole replay. See {@link Builder#readinessSource(Predicate)} for how
 * to wire it.
 * <p>
 * <strong>{@link RoutingOutcome#UNAVAILABLE} is paced exactly like {@link RoutingOutcome#DEFERRED}, never sent
 * through {@link DeliveryFailurePolicy}.</strong> That outcome says nothing was in a position to be asked, the sole
 * subscription paused, the model not running, or nothing registered at all, and it never comes with an exception.
 * Earlier revisions of this bridge re-read the model's own running state at this point to cancel the consumer
 * immediately. That re-read raced this bridge's own {@link #stopPermanently()} for the same delivery tag, since
 * both a lifecycle check and a permanent stop can be deciding the same tag's fate at once. Held and released the
 * same way and on the same schedule as {@code DEFERRED} instead, bypassing {@link DeliveryFailurePolicy} entirely.
 * Nothing here is broken, only not deliverable right now, and {@code PARK} exists for failures, not for pacing. See
 * {@code heldDeferredDeliveryTags}'s own javadoc for the pacing mechanism itself.
 * <p>
 * <strong>A permanently failed catch-up stops this bridge, it does not park or redeliver into it.</strong> A
 * {@link CatchupThenPushSubscriptionModel} wrapping {@code model} whose replay has permanently failed refuses
 * every later live event before attempting any dispatch, and promises that refusing is permanent, which
 * {@code RegisteringSubscribable.routeReportingMatch} reports as {@link RoutingOutcome#REFUSED}. That outcome is
 * reported for nothing else, so this bridge decides on it alone rather than on the type of whatever exception came
 * with it. A handler that reached into some other permanently failed engine reports
 * {@link RoutingOutcome#DELIVERED} instead and goes through {@link DeliveryFailurePolicy} like any other handler
 * failure.
 * <p>
 * On {@link RoutingOutcome#REFUSED} this bridge logs at error once and stops consuming for good.
 * {@link #stopPermanently()} cancels the consumer, releases every tag this bridge is still holding
 * (negatively acknowledged with requeue), and closes the consume channel, all under the same
 * lock, in that order. Closing the channel also requeues the triggering delivery itself, along with anything else
 * still unacknowledged on it, RabbitMQ's own guarantee for a closed channel, so this bridge never has to
 * acknowledge that delivery tag by hand once a permanent stop has decided to close the channel out from under it.
 * Bypasses {@link DeliveryFailurePolicy} entirely the same way {@link RoutingOutcome#DEFERRED} already does, so
 * every message this permanent stop touches stays visibly on the source queue rather than being parked or
 * committed into the same permanent refusal.
 * <p>
 * <strong>A delivery tag from before an automatic connection recovery is acted on like any other.</strong> The
 * RabbitMQ client already refuses to act on one for you. Every channel a {@code Recoverable} connection hands out
 * offsets its own delivery tags past everything the channel it replaced ever issued, so {@code basicAck} and
 * {@code basicNack} send nothing at all for a tag from the dead channel. A connection that never recovers
 * automatically cannot produce such a tag either, since its channel stays dead and no further delivery arrives on
 * it. So this bridge acknowledges or negatively acknowledges every delivery it is handed and never abandons one
 * unacknowledged, whatever happened to the connection underneath it.
 * <p>
 * Under {@link DeliveryFailurePolicy#PARK} that costs one duplicate. A delivery that fails while the connection is
 * recovering is published to the parking destination, and the acknowledgement that normally follows the park does
 * nothing, so RabbitMQ requeues the message as well. You end up with a parked copy and a copy still on the source
 * queue, which is the same at-least-once delivery this bridge gives you everywhere else.
 */
public final class RabbitMqCloudEventBridge implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RabbitMqCloudEventBridge.class);

    private final PushSubscriptionModel model;
    private final RoutingOutcomeChannel outcomeChannel;
    private final Channel consumeChannel;
    private final String queue;
    private final int prefetchCount;
    private final Duration pollInterval;
    private final RabbitMqDeliveryFailureAction failureAction;
    private final Predicate<String> readinessSource;
    private final ScheduledExecutorService scheduler;

    private final Lock consumeLock = new ReentrantLock();
    private @Nullable String consumerTag;

    // Appended to (never under consumeLock) by handleDelivery the instant a delivery reports DEFERRED or a
    // UNAVAILABLE, in place of nacking it there and then. With prefetchCount == 1 (the default) the
    // broker sends nothing further on this consumer once a delivery is left unacked, so the churn stops at that
    // instant with no cancel involved at all. Configured above 1, up to that many can be held between releases,
    // since that many can be outstanding at once. reconcileConsumption, on its own poll thread, releases at most a
    // snapshot of what is here under its own lock, once per pollInterval, which is what bounds this to at most one
    // redelivery per pollInterval per unit of prefetchCount, by construction, rather than racing a scheduled cancel
    // against however fast the broker itself round-trips a nack. A deque rather than a single held tag, so a bridge
    // configured with prefetchCount above 1 never drops an earlier held tag under a later one, and a failed release
    // can push a tag back to the front rather than only ever appending to the back. See handleDelivery and
    // releaseHeldDeferredDelivery.
    private final Deque<Long> heldDeferredDeliveryTags = new ConcurrentLinkedDeque<>();
    // A REDELIVER-policy failure (a handler or filter that fails on every attempt, say) is held here instead of
    // nacked on the spot, released the same way heldDeferredDeliveryTags is: a snapshot per pollInterval under
    // consumeLock, via the same releaseHeldDeferredDelivery(Deque, LongConsumer) helper. Without this a
    // poison message under REDELIVER (the default policy) would nack-and-redeliver as fast as the broker
    // round-trips it, pinning the AMQP dispatch thread at prefetchCount(1) instead of being paced like DEFERRED
    // already is. PARK is never held here: parking exists to move a failed delivery out of the retry loop, not to
    // pace it, so it still applies immediately, through failureAction, from the point of failure.
    private final Deque<Long> heldFailedDeliveryTags = new ConcurrentLinkedDeque<>();
    private volatile boolean permanentlyStopped;

    private RabbitMqCloudEventBridge(PushSubscriptionModel model, RoutingOutcomeChannel outcomeChannel, Channel consumeChannel,
                                      String queue, int prefetchCount, Duration pollInterval, RabbitMqDeliveryFailureAction failureAction,
                                      Predicate<String> readinessSource) {
        this.model = model;
        this.outcomeChannel = outcomeChannel;
        this.consumeChannel = consumeChannel;
        this.queue = queue;
        this.prefetchCount = prefetchCount;
        this.pollInterval = pollInterval;
        this.failureAction = failureAction;
        this.readinessSource = readinessSource;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "rabbitmq-cloudevent-bridge-" + queue);
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * @param connection     The connection this bridge creates its own {@link Channel} on for consuming, declaring
     *                       topology, and acknowledging.
     * @param model          The live model this bridge feeds. Never a {@link CatchupThenPushSubscriptionModel}, see
     *                       the class javadoc.
     * @param outcomeChannel Shared with {@code model}'s own constructor, see {@link RoutingOutcomeChannel}.
     * @param queue          The queue this bridge consumes from, and declares unless {@link Builder#declareTopology(boolean)}
     *                       is set to {@code false}.
     */
    public static Builder builder(Connection connection, PushSubscriptionModel model, RoutingOutcomeChannel outcomeChannel, String queue) {
        return new Builder(connection, model, outcomeChannel, queue);
    }

    private void start(Builder builder, Set<RabbitMqDestination> destinations) {
        try {
            if (builder.declareTopology) {
                consumeChannel.queueDeclare(queue, true, false, false, null);
                for (RabbitMqDestination destination : destinations) {
                    consumeChannel.queueBind(queue, destination.exchange(), destination.routingKey());
                }
            }
            consumeChannel.basicQos(prefetchCount);
        } catch (IOException e) {
            throw new RabbitMqBridgeException("Failed to declare topology for queue \"" + queue + "\"", e);
        }
        scheduler.scheduleWithFixedDelay(this::reconcileConsumption, 0, pollInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    // Coarse lifecycle control, per the class javadoc: consumes while the model has a running subscription and
    // readinessSource agrees, not otherwise, checked on a fixed poll rather than on every message. subscriptionIds()
    // has at most one element by ADR 90 (PushSubscriptionModel declares Consumers.ONE), so there is never an
    // ambiguity about which id to ask isRunning(...) or readinessSource about.
    //
    // Also releases any DEFERRED delivery handleDelivery is holding unacked, every pass, regardless of
    // shouldConsume: a subscription that stops or pauses right after a delivery was held must not leave that
    // delivery sitting unacked on this channel until this bridge eventually closes, so this runs whether or not
    // this poll also starts or cancels the consumer.
    private void reconcileConsumption() {
        try {
            Set<String> subscriptionIds = model.subscriptionIds();
            String subscriptionId = subscriptionIds.isEmpty() ? null : subscriptionIds.iterator().next();
            boolean shouldConsume = !permanentlyStopped && subscriptionId != null && model.isRunning(subscriptionId) && readinessSource.test(subscriptionId);
            consumeLock.lock();
            try {
                if (permanentlyStopped) {
                    return;
                }
                if (shouldConsume && consumerTag == null) {
                    consumerTag = consumeChannel.basicConsume(queue, false, this::handleDelivery, this::handleCancel);
                } else if (!shouldConsume && consumerTag != null) {
                    consumeChannel.basicCancel(consumerTag);
                    consumerTag = null;
                }
                releaseHeldDeferredDelivery();
                releaseHeldFailedDelivery();
            } finally {
                consumeLock.unlock();
            }
        } catch (IOException | RuntimeException e) {
            log.warn("Failed to reconcile consumption for queue \"{}\" against the subscription model's running state. " +
                    "Retrying on the next poll.", queue, e);
        }
    }

    // Nacks (with requeue) a snapshot of what handleDelivery left held, if anything, bypassing DeliveryFailurePolicy
    // exactly as the DEFERRED branch there always has. Always called under consumeLock, since it issues a
    // blocking Channel call per held tag. With prefetchCount == 1 (the default), the broker sends nothing further
    // on this consumer between a delivery being held and this releasing it, so calling this once per pollInterval
    // is what bounds a DEFERRED delivery to at most one redelivery per poll interval by construction, not a race
    // against however fast the broker itself round-trips a nack.
    //
    // The snapshot size is read once, up front, rather than draining until empty: a nack this loop issues can
    // cause an immediate redelivery whose DEFERRED handleDelivery call appends a fresh tag to this same deque
    // while this loop is still running, and draining until empty would nack that fresh tag too, in the same
    // pass, collapsing the at-most-one-per-poll-interval bound this exists to keep. A tag appended mid-loop waits
    // for the next poll instead.
    //
    // A failed nack (an IOException surfacing as RabbitMqBridgeException from basicNack) never drops the tag it
    // was for: it goes back to the front, ahead of whatever this pass has not gotten to yet, and this pass stops
    // rather than trying the next slot, since a channel failing to nack once is not going to succeed on a
    // different tag straight after. The next poll retries it. A channel that has actually died requeues
    // everything still unacked on it by itself regardless, so this is only for one that survives the failure.
    private void releaseHeldDeferredDelivery() {
        releaseHeldDeferredDelivery(heldDeferredDeliveryTags, failureAction::redeliver);
    }

    // Releases a REDELIVER-policy failure paced behind heldFailedDeliveryTags, the same mechanism and the same
    // once-per-poll bound as releaseHeldDeferredDelivery() above, see that field's own javadoc.
    // failureAction.redeliverFailure, not the plain redeliver() heldDeferredDeliveryTags releases through: this
    // deque holds a genuine failure, and redeliverFailure logs the one warn line that failure needs, where
    // heldDeferredDeliveryTags' own pacing releases (DEFERRED and UNAVAILABLE) are not failures
    // and stay silent.
    private void releaseHeldFailedDelivery() {
        releaseHeldDeferredDelivery(heldFailedDeliveryTags, failureAction::redeliverFailure);
    }

    // Package-private and static, parameterized on the deque and the release call, so this bridge's redelivery
    // bookkeeping is directly testable with a stub that throws on demand, no real Channel or broker required.
    // Always called under consumeLock.
    static void releaseHeldDeferredDelivery(Deque<Long> heldDeferredDeliveryTags, LongConsumer redeliver) {
        int snapshotCount = heldDeferredDeliveryTags.size();
        for (int i = 0; i < snapshotCount; i++) {
            Long heldDeliveryTag = heldDeferredDeliveryTags.pollFirst();
            if (heldDeliveryTag == null) {
                return;
            }
            try {
                redeliver.accept(heldDeliveryTag);
            } catch (RuntimeException e) {
                heldDeferredDeliveryTags.offerFirst(heldDeliveryTag);
                throw e;
            }
        }
    }

    private void handleCancel(String cancelledConsumerTag) {
        consumeLock.lock();
        try {
            if (cancelledConsumerTag.equals(consumerTag)) {
                consumerTag = null;
            }
        } finally {
            consumeLock.unlock();
        }
    }

    private void handleDelivery(String deliveryConsumerTag, Delivery delivery) {
        long deliveryTag = delivery.getEnvelope().getDeliveryTag();
        CloudEvent cloudEvent;
        try {
            cloudEvent = RabbitMqCloudEventMapper.toCloudEvent(delivery.getProperties(), delivery.getBody());
        } catch (RuntimeException e) {
            log.debug("Failed to rebuild a CloudEvent from a message on queue \"{}\", delivery tag {}.", queue, deliveryTag, e);
            routeFailure(deliveryTag, delivery.getProperties(), delivery.getBody());
            return;
        }
        try {
            model.acceptRedeliverable(cloudEvent);
        } catch (RuntimeException | AssertionError e) {
            // Catches AssertionError too, since a filter or the handler can throw one, and an uncaught Error here
            // would leave the delivery unacked and stall the consumer at prefetch one. Any other Error still
            // propagates. Which of the two things went wrong is read off the reported outcome rather than off the
            // exception type. REFUSED is reported only when this bridge's own model refused before attempting
            // dispatch and promised that refusing is permanent, so a handler that reached into some other
            // permanently failed engine reports DELIVERED and lands in the failure policy below where it belongs.
            RoutingOutcome refusedOutcome = outcomeChannel.takeLastOutcome();
            if (refusedOutcome != RoutingOutcome.REFUSED) {
                log.debug("A filter or handler failed for a message on queue \"{}\", delivery tag {}.", queue, deliveryTag, e);
                routeFailure(deliveryTag, delivery.getProperties(), delivery.getBody());
                return;
            }
            // A CatchupThenPushSubscriptionModel wrapping this bridge's model has a permanently failed catch-up.
            // Permanent, exactly like an unreadable live filter, so stop rather than park or redeliver into the
            // same refusal forever. See the class javadoc.
            log.error("A catch-up wrapping this bridge's model has permanently failed for queue \"{}\". Stopping "
                    + "this bridge rather than parking or committing into the same refusal. Delivery tag {}, and "
                    + "every other tag this bridge is still holding, is requeued by the channel this permanent stop "
                    + "closes, so it stays visible on the queue until the wrapper's catch-up is fixed and restarted.",
                    queue, deliveryTag, e);
            stopPermanently();
            return;
        }
        RoutingOutcome outcome = outcomeChannel.takeLastOutcome();
        if (outcome == null) {
            // Only reachable when model was constructed with a different RoutingOutcomeChannel than the one this
            // bridge reads, a wiring defect ADR 133 decision 1 requires against, not an ordinary delivery failure.
            // Named explicitly rather than falling into the generic "not deliverable" branch below, which would
            // say nothing about the actual cause. Logged at error, distinct from and in addition to whatever
            // routeFailure itself logs for the delivery: this line diagnoses the wiring defect, not the delivery.
            log.error("No RoutingOutcome was captured for a message on queue \"{}\", delivery tag {}. This model " +
                    "was very likely constructed with a different RoutingOutcomeChannel than the one this bridge " +
                    "reads; both must be the exact same instance, per RoutingOutcomeChannel's own javadoc.",
                    queue, deliveryTag);
            routeFailure(deliveryTag, delivery.getProperties(), delivery.getBody());
            return;
        }
        if (outcome == RoutingOutcome.DELIVERED || outcome == RoutingOutcome.FILTERED) {
            ackNow(deliveryTag);
        } else if (outcome == RoutingOutcome.DEFERRED || outcome == RoutingOutcome.UNAVAILABLE) {
            // DEFERRED (a catch-up-then-live wrapper still replaying or draining) and UNAVAILABLE
            // (the sole subscription paused, or the model not running at all) are paced identically: held unacked
            // rather than nacked here, so with prefetchCount == 1 (the default) the broker sends nothing further on
            // this consumer until reconcileConsumption's own poll releases it, at most once per pollInterval. An
            // earlier revision of this bridge cancelled its own consumer and redelivered a lifecycle
            // lifecycle state immediately instead, to keep it visible sooner than a full pollInterval away, and that
            // immediate cancel-and-redeliver raced this bridge's own stopPermanently() for the same delivery tag,
            // since both a lifecycle check and a permanent stop can be deciding the same tag's fate at once, so it
            // is gone. Never takes consumeLock here: this callback can run concurrently with reconcileConsumption,
            // which already holds it across a blocking AMQP call, so this only ever hands the tag off for that poll
            // thread to act on instead.
            heldDeferredDeliveryTags.add(deliveryTag);
        } else {
            // NOT_DELIVERABLE, whether the filter itself failed to answer or a transient refusal reported it. It
            // always arrives with an exception, which the catch above already routed, so reaching here means a
            // future outcome this bridge has not been taught yet. Routed as a failure either way.
            log.debug("A message on queue \"{}\", delivery tag {}, reported an outcome this bridge does not " +
                    "recognize. Routing it as a failure.", queue, deliveryTag);
            routeFailure(deliveryTag, delivery.getProperties(), delivery.getBody());
        }
    }

    // Under consumeLock, the lock reconcileConsumption also holds across its own blocking Channel calls, so two
    // threads never talk to consumeChannel at once.
    private void ackNow(long deliveryTag) {
        consumeLock.lock();
        try {
            failureAction.ack(deliveryTag);
        } finally {
            consumeLock.unlock();
        }
    }

    // Routes a genuine failure (UNAVAILABLE and a permanent catch-up refusal are both handled
    // separately above, never reaching here) to this bridge's configured DeliveryFailurePolicy. PARK applies
    // immediately, through failureAction, since parking exists to move a failed delivery out of the retry loop, not
    // to pace it. REDELIVER is paced instead, held and released once per poll exactly like a DEFERRED delivery, via
    // a second deque, so a message that fails on every attempt is bounded to one redelivery per pollInterval rather
    // than nacking as fast as the broker round-trips it. See heldFailedDeliveryTags's own javadoc.
    private void routeFailure(long deliveryTag, BasicProperties properties, byte[] body) {
        if (failureAction.policy() == DeliveryFailurePolicy.REDELIVER) {
            heldFailedDeliveryTags.add(deliveryTag);
        } else {
            consumeLock.lock();
            try {
                failureAction.apply(deliveryTag, properties, body);
            } finally {
                consumeLock.unlock();
            }
        }
    }

    // Cancels this bridge's own consumer, releases every tag this bridge is still holding, and closes the channel,
    // in that order, all under the same lock, then stops the coarse poll for good. Closing the channel here, rather
    // than leaving that to close() as an earlier revision did, is what keeps a permanent stop from leaving an
    // already-held tag stuck: the poll this method also shuts down was the only thing that would otherwise ever
    // release it, and with prefetchCount above the default of one an earlier held delivery could sit invisible on
    // this consumer indefinitely if nothing forced it back onto the queue. Closing an already-closing or
    // already-closed channel is tolerated the same as close() already tolerates it, since an application may still
    // call close() afterward.
    private void stopPermanently() {
        consumeLock.lock();
        try {
            // Set under the same lock reconcileConsumption() now reads it under, so the two can never interleave:
            // whichever of this method and a concurrent poll acquires the lock first fully decides the consumer's
            // fate before the other even reads the flag.
            permanentlyStopped = true;
            if (consumerTag != null) {
                try {
                    consumeChannel.basicCancel(consumerTag);
                } catch (IOException | RuntimeException e) {
                    log.warn("Failed to cancel the consumer on queue \"{}\" while stopping permanently.", queue, e);
                }
                consumerTag = null;
            }
            try {
                releaseHeldDeferredDelivery();
            } catch (RuntimeException ignored) {
                // Best effort: the channel closes right after regardless, which requeues whatever this could not.
            }
            try {
                releaseHeldFailedDelivery();
            } catch (RuntimeException ignored) {
                // Best effort, same reasoning as releaseHeldDeferredDelivery() above.
            }
            try {
                consumeChannel.close();
            } catch (IOException | TimeoutException | RuntimeException ignored) {
                // Best effort, mirroring close()'s own channel teardown: the channel is going away either way, and
                // an already-failed or already-closed channel has nothing further this bridge can do about it.
                // RuntimeException also catches ShutdownSignalException, which extends it.
            }
        } finally {
            consumeLock.unlock();
        }
        scheduler.shutdown();
    }

    /**
     * Stops the background poll, cancels this bridge's consumer if it has one, releases any {@code DEFERRED}
     * delivery still held unacked, and closes the {@link Channel} (and, with {@link DeliveryFailurePolicy#PARK},
     * the parking sink) this bridge created. Does not close the {@link Connection} it was built from.
     * <p>
     * Releasing a held delivery here is belt and braces rather than load bearing: closing a channel with an
     * unacked delivery on it already requeues that delivery at the broker on its own, so skipping this line would
     * still leave nothing stuck. Doing it explicitly means a delivery this bridge is holding is redelivered the
     * instant this method runs rather than whenever the broker notices the channel is gone.
     */
    @Override
    public void close() {
        scheduler.shutdownNow();
        consumeLock.lock();
        try {
            if (consumerTag != null) {
                try {
                    consumeChannel.basicCancel(consumerTag);
                } catch (IOException ignored) {
                    // Best effort: the channel is about to be closed either way.
                }
                consumerTag = null;
            }
            try {
                releaseHeldDeferredDelivery();
            } catch (RuntimeException ignored) {
                // Best effort, matching basicCancel above: the channel is about to be closed either way, and
                // closing it requeues whatever is left held regardless.
            }
            try {
                releaseHeldFailedDelivery();
            } catch (RuntimeException ignored) {
                // Best effort, same reasoning as releaseHeldDeferredDelivery() above.
            }
        } finally {
            consumeLock.unlock();
        }
        try {
            consumeChannel.close();
        } catch (IOException | ShutdownSignalException | TimeoutException ignored) {
            // Best effort, mirroring RabbitMqCloudEventSink#close's own channel teardown.
        }
        failureAction.close();
    }

    public static final class Builder {
        private final Connection connection;
        private final PushSubscriptionModel model;
        private final RoutingOutcomeChannel outcomeChannel;
        private final String queue;
        private @Nullable DestinationResolver<RabbitMqDestination> resolver;
        private @Nullable SubscriptionFilter bindingFilter;
        private @Nullable Set<RabbitMqDestination> bindings;
        private boolean declareTopology = true;
        private DeliveryFailurePolicy deliveryFailurePolicy = DeliveryFailurePolicy.REDELIVER;
        private @Nullable RabbitMqDestination parkingDestination;
        private Duration pollInterval = Duration.ofSeconds(1);
        private int prefetchCount = 1;
        private Predicate<String> readinessSource = subscriptionId -> true;
        private RetryStrategy retryStrategy;

        private Builder(Connection connection, PushSubscriptionModel model, RoutingOutcomeChannel outcomeChannel, String queue) {
            this.connection = requireNonNull(connection, "connection cannot be null");
            this.model = requireNonNull(model, PushSubscriptionModel.class.getSimpleName() + " cannot be null");
            this.outcomeChannel = requireNonNull(outcomeChannel, RoutingOutcomeChannel.class.getSimpleName() + " cannot be null");
            this.queue = requireNonNull(queue, "queue cannot be null");
            this.retryStrategy = defaultRetryStrategy(queue);
        }

        /**
         * Derives the queue's bindings from {@link #bindingFilter(SubscriptionFilter)} or, absent one, from
         * {@link DestinationResolver#catchAllDestination()}. Required unless {@link #bindings(Set)} is given or
         * {@link #declareTopology(boolean)} is set to {@code false}.
         */
        public Builder resolver(DestinationResolver<RabbitMqDestination> resolver) {
            this.resolver = requireNonNull(resolver, DestinationResolver.class.getSimpleName() + " cannot be null");
            return this;
        }

        /**
         * Narrows the declared bindings to {@link DestinationResolver#destinationsFor(SubscriptionFilter)} for this
         * filter, falling back to {@link DestinationResolver#catchAllDestination()} when the resolver cannot derive
         * one. Requires {@link #resolver(DestinationResolver)}. Per ADR 133 decision 5, this filter narrows what
         * arrives. It must be at least as inclusive as the subscription's own filter, or events the subscription
         * would have accepted never arrive at all.
         */
        public Builder bindingFilter(SubscriptionFilter bindingFilter) {
            this.bindingFilter = requireNonNull(bindingFilter, "bindingFilter cannot be null");
            return this;
        }

        /**
         * Declares exactly these bindings instead of deriving any from a resolver, the explicit escape hatch for a
         * binding scheme a resolver cannot express.
         */
        public Builder bindings(Set<RabbitMqDestination> bindings) {
            this.bindings = Set.copyOf(requireNonNull(bindings, "bindings cannot be null"));
            return this;
        }

        /**
         * Whether this bridge declares its queue and bindings at all. {@code true} by default. Set to {@code false}
         * for a deployment whose platform team owns the queue and its bindings itself, per #415. This bridge then
         * only consumes from {@code queue} and never calls {@code queueDeclare} or {@code queueBind}.
         */
        public Builder declareTopology(boolean declareTopology) {
            this.declareTopology = declareTopology;
            return this;
        }

        /**
         * What this bridge does with a delivery it will not acknowledge. {@link DeliveryFailurePolicy#REDELIVER} by
         * default.
         */
        public Builder onDeliveryFailure(DeliveryFailurePolicy deliveryFailurePolicy) {
            this.deliveryFailurePolicy = requireNonNull(deliveryFailurePolicy, DeliveryFailurePolicy.class.getSimpleName() + " cannot be null");
            return this;
        }

        /**
         * The destination {@link DeliveryFailurePolicy#PARK} publishes a failed delivery to. Required when
         * {@link #onDeliveryFailure(DeliveryFailurePolicy)} is {@code PARK} ({@link #build()} refuses otherwise).
         * Given without {@code PARK}, this is accepted but unused, not refused, the same choice
         * {@link RabbitMqDeliveryFailureAction#create(Connection, Channel, DeliveryFailurePolicy, RabbitMqDestination, Logger)}
         * makes and documents, so switching {@link #onDeliveryFailure} back to {@code REDELIVER} in application
         * config never has to strip this call out along with it.
         */
        public Builder parkingDestination(RabbitMqDestination parkingDestination) {
            this.parkingDestination = requireNonNull(parkingDestination, "parkingDestination cannot be null");
            return this;
        }

        /**
         * How often the coarse lifecycle poll checks the model's running state. One second by default. A small
         * delay is harmless, see the class javadoc, so this rarely needs changing.
         */
        public Builder pollInterval(Duration pollInterval) {
            requireNonNull(pollInterval, "pollInterval cannot be null");
            if (pollInterval.toMillis() <= 0) {
                throw new IllegalArgumentException("pollInterval must be at least 1 millisecond, was " + pollInterval);
            }
            this.pollInterval = pollInterval;
            return this;
        }

        /**
         * How many unacknowledged deliveries this bridge allows itself at once. One by default, which is what keeps
         * deliveries processed one at a time, matching {@code PushSubscriptionModel}'s single-evaluation contract
         * and this bridge's one-delivery-tag-outstanding acknowledgement model. Raising it is only safe if the
         * application's own handler is safe to run concurrently with itself.
         */
        public Builder prefetchCount(int prefetchCount) {
            if (prefetchCount < 1) {
                throw new IllegalArgumentException("prefetchCount must be at least 1, was " + prefetchCount);
            }
            this.prefetchCount = prefetchCount;
            return this;
        }

        /**
         * Asked, alongside {@code model}'s own running state, on every coarse lifecycle poll (see the class
         * javadoc). {@code true} for every subscription id by default, which is exactly right for {@code model} fed
         * directly with no catch-up in front of it, or for {@code catchup = NONE}, since no wrapper exists there to
         * pace against.
         * <p>
         * A pacing hint only, never a correctness dependency: {@link RoutingOutcome#DEFERRED} is what keeps this
         * bridge correct against a {@link CatchupThenPushSubscriptionModel} wrapping {@code model} with no
         * {@code readinessSource} configured at all, refusing rather than acknowledging a message the wrapper is
         * still replaying or draining into, and redelivering it. Configuring this only cuts down on how often that
         * refuse-and-redeliver round trip happens during a replay. Wrap {@code model} in a
         * {@link CatchupThenPushSubscriptionModel} and pass {@code catchupThenPush::isReadyForLiveDelivery} here, so
         * this bridge stops pulling from the queue for as long as that wrapper's replay is still running or
         * draining, and resumes once it reaches live. Built by an {@code @Projection(source = PUSH)} or
         * {@code @Saga(source = PUSH)} bean instead of by hand, the wrapper is published as a Spring bean named
         * {@code "catchupThenPushSubscriptionModel-" + id}, so {@code applicationContext.getBean(name,
         * CatchupThenPushSubscriptionModel.class)::isReadyForLiveDelivery} reaches the same object, or, through the
         * RabbitMQ Spring Boot starter, wired automatically with no configuration at all.
         */
        public Builder readinessSource(Predicate<String> readinessSource) {
            this.readinessSource = requireNonNull(readinessSource, "readinessSource cannot be null");
            return this;
        }

        /**
         * How a broker briefly unreachable while {@link #build()} runs is retried before it throws. This retries
         * only opening a channel and declaring topology on the {@code Connection} this builder was given; it
         * neither creates nor reconnects that {@code Connection}, so surviving anything past the very first
         * attempt needs that {@code Connection}'s own automatic recovery already enabled. Exponential backoff from
         * 100 ms up to 2 seconds by default, ten attempts in total, matching the shape
         * {@code AGENTS.md} sets for every component that talks to an external store, capped at that count because a
         * {@link #build()} that never gives up turns a broker that is permanently misconfigured, a rejected
         * credential or a nonexistent vhost, into an application that hangs at startup with no diagnosis, which is
         * worse than the failure this retry exists to absorb. By default {@link #build()} logs each retried
         * attempt at {@code WARN} so a retrying startup is never mistaken for a hung one. A caller-supplied
         * {@link RetryStrategy} replaces that logging along with everything else this default configures. See
         * {@link RabbitMqBuildFailureClassifier} for exactly what is retried and what is refused immediately,
         * including under {@link org.occurrent.broker.api.blocking.DeliveryFailurePolicy#PARK}, where the parking
         * publisher's own channel can fail too. Never retries the {@link IllegalStateException} a missing
         * {@code resolver} or {@code parkingDestination} throws above, since that failure is identical on every
         * attempt regardless of the broker's state, and never any other {@link RuntimeException}, since that is a
         * bug this retry cannot fix by trying again. Passing a {@link RetryStrategy} here replaces that
         * classification too, so a caller that wants a different bound or a wider retry configures its own.
         */
        public Builder retryStrategy(RetryStrategy retryStrategy) {
            this.retryStrategy = requireNonNull(retryStrategy, RetryStrategy.class.getSimpleName() + " cannot be null");
            return this;
        }

        public RabbitMqCloudEventBridge build() {
            if (declareTopology && bindings == null && resolver == null) {
                throw new IllegalStateException("A resolver(...), or explicit bindings(...), is required unless declareTopology(false) is set");
            }
            if (deliveryFailurePolicy == DeliveryFailurePolicy.PARK && parkingDestination == null) {
                throw new IllegalStateException("A parkingDestination is required when onDeliveryFailure(PARK) is set");
            }
            Set<RabbitMqDestination> destinations = declareTopology
                    ? RabbitMqTopology.destinationsToBind(resolver, bindingFilter, bindings)
                    : Set.of();
            // retryStrategy wraps only this call, not the validation above: a failed validation throws the same
            // way on every attempt regardless of the broker's state, so retrying it would spend the whole backoff
            // window on a failure a retry can never fix. A failed attempt closes the local resources it opened,
            // the channel, the failure action, the scheduler, before rethrowing, but not a durable queue
            // declaration or binding it already completed on the broker. Redeclaring those on the next attempt
            // is a safe no-op, since queueDeclare and queueBind succeed again, unchanged, against a queue or
            // binding that already exists exactly as declared.
            return retryStrategy.execute(() -> buildOnce(destinations));
        }

        // Validated above, before opening anything: a failure past this point has a channel (and, under PARK, a
        // parking sink) already open, so every later failure path in this method closes what it opened rather
        // than leaking it.
        private RabbitMqCloudEventBridge buildOnce(Set<RabbitMqDestination> destinations) {
            Channel channel = openChannel(connection);
            RabbitMqDeliveryFailureAction failureAction = null;
            RabbitMqCloudEventBridge bridge = null;
            try {
                failureAction = RabbitMqDeliveryFailureAction.create(connection, channel, deliveryFailurePolicy, parkingDestination, log);
                bridge = new RabbitMqCloudEventBridge(model, outcomeChannel, channel, queue, prefetchCount,
                        pollInterval, failureAction, readinessSource);
                bridge.start(this, destinations);
                return bridge;
            } catch (RuntimeException e) {
                closeQuietly(channel);
                if (failureAction != null) {
                    failureAction.close();
                }
                if (bridge != null) {
                    // start() can fail after the bridge, and its scheduler, already exist. The scheduler has no
                    // thread yet here, since scheduleWithFixedDelay(...) is the last thing start() does, but this
                    // stays defensive against that ordering changing later, and symmetric with the domain bridge's
                    // own build() failure path.
                    bridge.scheduler.shutdownNow();
                }
                throw e;
            }
        }

        /**
         * See {@link RabbitMqBuildFailureClassifier} for the classification and {@link #retryStrategy(RetryStrategy)}
         * for the rest of this default. Takes {@code queue} explicitly rather than reading the field: this runs
         * from the constructor, before the field assignment it would otherwise read completes.
         */
        private static RetryStrategy defaultRetryStrategy(String queue) {
            return RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0)
                    .maxAttempts(10)
                    .retryIf(RabbitMqBuildFailureClassifier::isTransient)
                    .onRetryableError((info, throwable) -> log.warn(
                            "Attempt {} of {} to build the RabbitMQ bridge for queue \"{}\" failed. Retrying in {}.",
                            info.getAttemptNumber(), info.getMaxAttempts(), queue, info.getBackoffBeforeNextRetryAttempt(), throwable));
        }

        private static Channel openChannel(Connection connection) {
            try {
                return connection.openChannel()
                        .orElseThrow(() -> new RabbitMqBridgeException("No RabbitMQ channel number was available to create the bridge's channel on"));
            } catch (IOException e) {
                throw new RabbitMqBridgeException("Failed to create the bridge's RabbitMQ channel", e);
            }
        }

        // Best effort: build() is already failing for another reason by the time this runs, so a failure here is
        // logged rather than allowed to replace the failure the caller actually asked about.
        private static void closeQuietly(Channel channel) {
            try {
                channel.close();
            } catch (IOException | ShutdownSignalException | TimeoutException e) {
                log.warn("Failed to close the bridge's channel while unwinding a failed build().", e);
            }
        }
    }
}
