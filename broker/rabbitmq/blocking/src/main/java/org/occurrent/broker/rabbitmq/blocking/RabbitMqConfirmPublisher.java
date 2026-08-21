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
import com.rabbitmq.client.ShutdownSignalException;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The confirmed, mandatory-routed, retiring-on-timeout publish {@link RabbitMqCloudEventSink} needs, factored out of
 * it so a caller with an already-built {@link BasicProperties} and body, the consume-side parking path in
 * {@link RabbitMqDeliveryFailureAction}, publishes through the exact same machinery instead of a second, weaker copy
 * of it. That second copy is what round after round of review found new resource-lifecycle defects in, since it
 * never had this class's channel retirement, its confirmSelect-failure cleanup, or its own token-based return
 * tracking, and each fix only patched the one symptom found that round rather than closing the gap with the sink's
 * own proven implementation. There is exactly one implementation of a confirmed publish now, used by both.
 * <p>
 * {@link #publish(String, String, BasicProperties, byte[])} does not return until the broker has both confirmed the
 * message and routed it. A confirm alone is not enough, since RabbitMQ confirms a publish to an exchange with no
 * matching binding and then discards it, so every publish also sets {@code mandatory}, and a {@code basic.return}
 * for it fails the publish with {@link RabbitMqUnroutableEventException} even though the confirm that follows it
 * would otherwise look like success. A publish that times out waiting for the confirm, or is abandoned to an
 * interrupted wait, is left outstanding on the broker's side of that channel, so this publisher retires the channel
 * and opens a fresh one underneath it, and a later publish is never kept waiting on an abandoned one or blamed for
 * its eventual nack. Each publish carries its own random token in a dedicated header ({@link #RETURN_TOKEN_HEADER}),
 * so a {@code basic.return} is matched to the publish it belongs to and never to a different one. That token is
 * deliberately not the AMQP {@code correlationId} property. The parking path in {@link RabbitMqDeliveryFailureAction}
 * republishes a delivery's own original properties through this same publisher, and a caller-supplied
 * {@code correlationId} in those properties survives untouched instead of being overwritten with this class's own
 * internal, disposable one.
 * <p>
 * Publishes on one instance are serialized on its channel, and {@link #close()} shares that same serialization,
 * so a close racing a timed-out publish's channel retirement can never close the channel this publisher is about
 * to replace while missing the replacement itself. Call {@link #close()} once no longer needed. It closes the
 * {@link Channel} this publisher created, not the {@link Connection} it was given, since the connection may be
 * shared with other channels the caller still owns.
 */
final class RabbitMqConfirmPublisher implements AutoCloseable {

    /**
     * Caps {@link #returnedTokens}. A publish that never removes its own token, one whose acknowledgement timed out
     * and was later returned anyway, would otherwise grow that set forever under repeated timeouts. Publishes are
     * serialized on this publisher's channel, so at most one token is genuinely in flight at a time, well inside
     * this cap, and the oldest entries are evicted first when it is exceeded.
     */
    private static final int MAX_TRACKED_RETURNED_TOKENS = 10_000;

    /**
     * The header this publisher's own internal per-publish token travels in, so it never collides with or overwrites
     * a caller-supplied AMQP {@code correlationId}. Namespaced under {@code x-occurrent-} since {@code x-} is the
     * established AMQP convention for a broker or client extension header, never an application's own.
     */
    private static final String RETURN_TOKEN_HEADER = "x-occurrent-rabbitmq-confirm-publisher-return-token";

    private final Connection connection;
    private volatile Channel channel;
    private final Duration acknowledgementTimeout;
    private final Lock publishLock = new ReentrantLock();
    private final Set<String> returnedTokens = Collections.synchronizedSet(Collections.newSetFromMap(
            new LinkedHashMap<String, Boolean>() {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                    return size() > MAX_TRACKED_RETURNED_TOKENS;
                }
            }));

    RabbitMqConfirmPublisher(Connection connection, Duration acknowledgementTimeout) {
        this.connection = connection;
        this.acknowledgementTimeout = acknowledgementTimeout;
        this.channel = openConfirmChannel(connection);
        installReturnListener(channel);
    }

    private void installReturnListener(Channel channel) {
        channel.addReturnListener(returned -> {
            Map<String, Object> headers = returned.getProperties().getHeaders();
            Object token = headers == null ? null : headers.get(RETURN_TOKEN_HEADER);
            // toString() rather than a cast. A header value read back off the wire is a
            // com.rabbitmq.client.LongString for a string-valued header, not a java.lang.String (the same gotcha
            // RabbitMqCloudEventMapper.toCloudEvent(BasicProperties, byte[]) already documents and works around).
            if (token != null) {
                returnedTokens.add(token.toString());
            }
        });
    }

    private static Channel openConfirmChannel(Connection connection) {
        // Tracked outside the try so a failure in confirmSelect(), after openChannel() already succeeded, can still
        // close the channel it opened before rethrowing, rather than leaking it. openChannel() itself returning
        // empty throws directly from orElseThrow, before this variable is ever assigned, so there is nothing to
        // close in that case.
        Channel channel = null;
        try {
            channel = connection.openChannel()
                    .orElseThrow(() -> new RabbitMqPublishException("No RabbitMQ channel number was available to create a confirm-mode channel on"));
            channel.confirmSelect();
            return channel;
        } catch (IOException e) {
            closeQuietly(channel);
            throw new RabbitMqPublishException("Failed to create a confirm-mode RabbitMQ channel", e);
        } catch (ShutdownSignalException e) {
            // openChannel() throws this unchecked, not as an IOException, when the connection is itself already
            // closed, in which case channel is still null here and closeQuietly is a no-op.
            closeQuietly(channel);
            throw new RabbitMqPublishException("Failed to create a confirm-mode RabbitMQ channel because the connection is shut down", e);
        }
    }

    /**
     * Publishes {@code body} with {@code properties} to {@code exchange} with routing key {@code routingKey}, not
     * returning until the broker has both confirmed and routed it. {@code properties} is copied with a fresh
     * internal token added to its headers under {@link #RETURN_TOKEN_HEADER}, generated for this call, so a
     * {@code basic.return} for it is never confused with one for a different publish. Every other property,
     * including a caller-supplied {@code correlationId}, passes through unchanged.
     */
    void publish(String exchange, String routingKey, BasicProperties properties, byte[] body) {
        // basic.return carries no delivery tag, so this internal token is the only way to tell which publish a
        // return belongs to. Kept out of the correlationId property so a caller-supplied one, republished unchanged
        // by the parking path in RabbitMqDeliveryFailureAction, is never overwritten by it.
        String token = UUID.randomUUID().toString();
        Map<String, Object> headers = properties.getHeaders() == null ? new HashMap<>() : new HashMap<>(properties.getHeaders());
        headers.put(RETURN_TOKEN_HEADER, token);
        BasicProperties correlated = properties.builder().headers(headers).build();

        publishLock.lock();
        try {
            channel.basicPublish(exchange, routingKey, true, correlated, body);
            // waitForConfirms, not waitForConfirmsOrDie: the latter closes this publisher's own long-lived channel
            // on a nack or a timeout, which would fail every later publish on it too. A nack is reported through
            // the boolean return instead.
            boolean acknowledged = channel.waitForConfirms(acknowledgementTimeout.toMillis());
            if (!acknowledged) {
                throw new RabbitMqPublishException("Broker sent a negative acknowledgement for a publish to exchange \"" +
                        exchange + "\" with routing key \"" + routingKey + "\"");
            }
            if (returnedTokens.remove(token)) {
                throw new RabbitMqUnroutableEventException(exchange, routingKey);
            }
        } catch (IOException e) {
            throw new RabbitMqPublishException("Failed to publish to exchange \"" + exchange +
                    "\" with routing key \"" + routingKey + "\"", e);
        } catch (ShutdownSignalException e) {
            // A dropped connection or channel surfaces here as an unchecked ShutdownSignalException, not as an
            // IOException, so it needs its own catch to reach the caller at all. The RabbitMQ client leaves this
            // channel unusable once it has shut down, so it is retired and replaced here, symmetric with the
            // timeout and interrupted paths below, rather than left in place to fail every later publish on this
            // sink forever with connection auto-recovery off. That recovery only reaches a channel-level shutdown
            // (e.isHardError() false), where the connection itself and every other channel on it stay usable. A
            // hard, connection-level shutdown retires this channel the same way, but the replacement open then
            // fails too, since the connection it would open on is itself gone, and that failure is reported rather
            // than hidden. Recovering from a hard shutdown needs a Connection this sink can itself reconnect to,
            // which is the caller's own connection setup to provide, RabbitMQ's client supports it directly as
            // automatic connection recovery.
            RabbitMqPublishException shutdownException = new RabbitMqPublishException("Channel or connection shut down while publishing to exchange \"" +
                    exchange + "\" with routing key \"" + routingKey + "\"", e);
            retireChannelPreserving(shutdownException);
            throw shutdownException;
        } catch (TimeoutException e) {
            RabbitMqPublishTimeoutException timeoutException = new RabbitMqPublishTimeoutException(acknowledgementTimeout, e);
            retireChannelPreserving(timeoutException);
            throw timeoutException;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // The unconfirmed publish is abandoned here too, exactly as it is on a confirm-wait timeout, so a later
            // publish on this channel must not be left to wait on it or be failed by its eventual nack.
            RabbitMqPublishException interruptedException = new RabbitMqPublishException("Interrupted while waiting for a RabbitMQ publisher confirm", e);
            retireChannelPreserving(interruptedException);
            throw interruptedException;
        } finally {
            publishLock.unlock();
        }
    }

    // Called under publishLock, after waitForConfirms has ended without confirming or denying the publish, by
    // timeout or by interruption. The RabbitMQ client leaves that publish's delivery tag outstanding on the channel
    // forever, so a later publish on the same channel would wait on it too and could inherit its eventual nack.
    // Retiring the channel and opening a fresh one keeps that abandoned publish's outcome from ever being
    // attributed to a later, unrelated one.
    //
    // The old channel's close runs on its own thread rather than this one. Channel#close() sends the close
    // handshake and then blocks, waiting up to the RabbitMQ client's own fixed ten second RPC timeout for the
    // reply, a wait acknowledgementTimeout does not govern, so waiting for it here would let a slow broker stall
    // this publish call by far more than the timeout it was configured with. The replacement is opened without
    // waiting for that close to finish, which can occasionally race a connection with no spare channel number to
    // hand out until the old one's number is actually freed, a case openConfirmChannel already turns into a clear
    // failure rather than a hang.
    private void retireChannel() {
        Channel retiring = channel;
        Thread.ofVirtual().start(() -> closeQuietly(retiring));
        Channel replacement = openConfirmChannel(connection);
        installReturnListener(replacement);
        channel = replacement;
    }

    // A failure to reopen the channel here is attached to primaryFailure as suppressed rather than thrown in its
    // place, so the caller of this publish still sees the failure it actually asked about (a timeout or an
    // interruption), not this secondary one. If reopening fails, the channel field is left pointing at the channel
    // this call just retired, whose close is still running on its own thread, so a publish after this one either
    // fails fast on it or waits on the same outstanding confirm it already would have without this retirement.
    private void retireChannelPreserving(Throwable primaryFailure) {
        try {
            retireChannel();
        } catch (RabbitMqPublishException retireFailure) {
            primaryFailure.addSuppressed(retireFailure);
        }
    }

    private static void closeQuietly(@Nullable Channel channel) {
        if (channel == null) {
            return;
        }
        try {
            channel.close();
        } catch (IOException | TimeoutException | RuntimeException ignored) {
            // Best effort. This channel already failed in some way, so any error closing it is discarded along
            // with the channel itself. RuntimeException also covers ShutdownSignalException, which Channel#close()
            // can throw unchecked.
        }
    }

    /**
     * Closes the {@link Channel} this publisher created. Does not close the {@link Connection} it was built from.
     */
    @Override
    public void close() throws IOException, TimeoutException {
        // Shares publishLock with publish()/retireChannel() rather than reading the channel field on its own,
        // since retireChannel() reassigns that field in more than one step (retire the old one, open a new one,
        // assign it), and a close landing in the middle of that would close whichever channel it read and then
        // never see the one retireChannel() goes on to assign.
        publishLock.lock();
        try {
            channel.close();
        } finally {
            publishLock.unlock();
        }
    }
}
