package org.occurrent.dsl.saga.blocking;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.api.blocking.Subscribable;
import org.occurrent.subscription.push.blocking.CatchupThenPushSubscriptionModel;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A push saga with a catch-up buffers every live version that arrives during the replay. A live version that arrives
 * while the drain delivers those must wait behind them, otherwise it is applied first and every buffered version more
 * than the reorder window behind it is skipped (#1316).
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaCatchUpDrainOrderTest {

    private static final String ORDER = "o1";
    private static final String SAGA = "orders";

    sealed interface OrderEvent permits OrderPlaced, ItemAdded {
        String eventId();

        String orderId();
    }

    record OrderPlaced(String eventId, String orderId) implements OrderEvent {
    }

    record ItemAdded(String eventId, String orderId, String item) implements OrderEvent {
    }

    record Pack(String orderId, String item) {
    }

    record Order(String orderId, List<String> items) {
        Order with(String item) {
            List<String> added = new ArrayList<>(items);
            added.add(item);
            return new Order(orderId, List.copyOf(added));
        }
    }

    private final CloudEventConverter<OrderEvent> converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:orders"))
            .idMapper(OrderEvent::eventId).build();
    private final SagaStateStore<Order> store = SagaStateStore.inMemory();
    private final List<Pack> packed = new CopyOnWriteArrayList<>();
    private final List<SagaSubscription> subscriptionsToClose = new ArrayList<>();
    private final CountDownLatch releaseReplay = new CountDownLatch(1);
    private final CountDownLatch releaseDrain = new CountDownLatch(1);

    @AfterEach
    void shutdown() {
        releaseReplay.countDown();
        releaseDrain.countDown();
        subscriptionsToClose.forEach(SagaSubscription::close);
    }

    private static Saga<OrderEvent, Order, Pack> saga() {
        return Saga.<OrderEvent, Order, Pack>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> new Order(e.orderId(), List.of()))
                .evolve(ItemAdded.class, (state, e) -> state.with(e.item()))
                .react(ItemAdded.class, (state, e) -> List.of(SagaEffect.issue(new Pack(e.orderId(), e.item()))))
                .build();
    }

    @Test
    void a_live_version_arriving_while_the_drain_delivers_more_versions_than_the_reorder_window_is_applied_after_every_buffered_one() throws Exception {
        // Given
        int buffered = SagaExecutionSupport.DEFAULT_REORDER_WINDOW + 44;
        PushSubscriptionModel liveFeed = new PushSubscriptionModel();
        InMemoryEventStore eventStore = new InMemoryEventStore(liveFeed::accept);
        eventStore.write(ORDER, converter.toCloudEvents(List.of(new OrderPlaced("placed", ORDER))));
        CountDownLatch replaying = new CountDownLatch(1);
        PositionOrderedReader gatedReplay = new PositionOrderedReader() {
            @Override
            public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return eventStore.readInPositionOrder(filter, range).peek(event -> {
                    replaying.countDown();
                    awaitLatch(releaseReplay);
                });
            }

            @Override
            public long currentPosition() {
                return eventStore.currentPosition();
            }

            @Override
            public boolean writesPosition() {
                return eventStore.writesPosition();
            }
        };
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(gatedReplay, liveFeed, null, new CatchupThenLiveOptions(1_000, 1_000));
        CountDownLatch drainingFirstBufferedVersion = new CountDownLatch(1);
        Subscribable heldAtTheFirstBufferedVersion = (subscriptionId, filter, startAt, action) -> model.subscribe(subscriptionId, filter, startAt, event -> {
            if (event.getId().equals("i1")) {
                drainingFirstBufferedVersion.countDown();
                awaitLatch(releaseDrain);
            }
            action.accept(event);
        });
        // run(..) waits until the catch-up is done, and the catch-up waits for releaseReplay, so it runs on a thread of its own
        FutureTask<SagaSubscription> running = new FutureTask<>(() -> SagaRunner.<OrderEvent, Pack>agnostic(heldAtTheFirstBufferedVersion, converter)
                .run(SAGA, saga(), store, packed::add, null, SagaRunnerConfig.defaults()));
        Thread runner = new Thread(running, "saga-runner-start");
        runner.setDaemon(true);
        runner.start();
        assertThat(replaying.await(5, TimeUnit.SECONDS)).as("the replay is reading the history").isTrue();
        List<String> expected = new ArrayList<>();
        for (int i = 1; i <= buffered; i++) {
            eventStore.write(ORDER, converter.toCloudEvents(List.of(new ItemAdded("i" + i, ORDER, "i" + i))));
            expected.add("i" + i);
        }

        // When
        releaseReplay.countDown();
        assertThat(drainingFirstBufferedVersion.await(5, TimeUnit.SECONDS)).as("the drain is delivering the first buffered version").isTrue();
        eventStore.write(ORDER, converter.toCloudEvents(List.of(new ItemAdded("live", ORDER, "live"))));
        expected.add("live");
        releaseDrain.countDown();
        subscriptionsToClose.add(running.get(10, TimeUnit.SECONDS));

        // Then
        await().atMost(Duration.ofSeconds(10)).until(() -> model.isRunning(SAGA) && !model.isCatchingUp(SAGA));
        SagaEnvelope<Order> instance = SagaEnvelopes.of(store, null).find(ORDER).orElseThrow();
        assertThat(instance.state().items()).as("every buffered version, then the live one, none skipped as behind the reorder window").containsExactlyElementsOf(expected);
        assertThat(packed).extracting(Pack::item).containsExactlyElementsOf(expected);
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
