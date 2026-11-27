package org.occurrent.subscription.push.blocking;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class CatchupThenPushSubscriptionModelSustainedWriterTest {

    /**
     * A writer on another thread that writes one more event for every event the catch-up delivers can keep the buffer
     * from ever emptying, so the drain after the replay would never go live. The write that finds the drain running
     * waits for it instead, which lets the drain reach an empty buffer and go live, and the waiting write is then
     * delivered live. The writer is paced by the handler, one write per delivered event, so that how fast the machine is
     * does not decide whether the buffer can empty: with the wait, the writer is stuck in its first write during the
     * drain, and without it every delivery is followed by a write that is buffered at once.
     */
    @Test
    void a_catch_up_whose_events_a_sustained_writer_keeps_matching_goes_live_because_the_writes_wait_for_the_drain() throws Exception {
        // Given
        PushSubscriptionModel feed = new PushSubscriptionModel();
        InMemoryEventStore store = new InMemoryEventStore(feed::accept);
        store.write("stream", List.of(event("h1"), event("h2"), event("h3")));
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, feed, new InMemoryCheckpointStorage(), new CatchupThenLiveOptions(10, 1_000_000));
        List<String> handled = new CopyOnWriteArrayList<>();
        Semaphore permits = new Semaphore(0);
        Semaphore written = new Semaphore(0);
        AtomicBoolean stopWriter = new AtomicBoolean();
        AtomicInteger writes = new AtomicInteger();
        Thread writer = Thread.ofPlatform().name("sustained-writer").unstarted(() -> {
            while (!stopWriter.get()) {
                try {
                    permits.acquire();
                    if (stopWriter.get()) {
                        return;
                    }
                    store.write("stream", List.of(event("w" + writes.getAndIncrement())));
                    written.release();
                } catch (InterruptedException e) {
                    return;
                } catch (RuntimeException e) {
                    return;
                }
            }
        });
        writer.start();
        try {
            var subscription = model.subscribe("proj", null, StartAt.subscriptionModelDefault(), e -> {
                handled.add(e.getId());
                if (!stopWriter.get()) {
                    // The next write, then a bounded wait for it: it returns at once when nothing holds it back, and it
                    // is the wait the engine puts on a write that arrives during the drain that makes this time out
                    permits.release();
                    try {
                        written.tryAcquire(50, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    }
                }
            });

            // When
            boolean live = subscription.waitUntilStarted(Duration.ofSeconds(10));
            stopWriter.set(true);
            permits.release(1_000);
            writer.join(5_000);
            List<String> seen = List.copyOf(handled);

            // Then
            assertThat(live).as("the catch-up went live within 10 seconds although a writer wrote one more event for every event it delivered, so its drain never reached an empty buffer").isTrue();
            assertThat(seen.subList(0, 3)).as("the history comes first").containsExactly("h1", "h2", "h3");
            List<String> writtenSoFar = seen.stream().filter(id -> id.startsWith("w")).toList();
            assertThat(writtenSoFar).as("the writer's events were delivered once, in the order they were written, with none skipped")
                    .isEqualTo(IntStream.range(0, writtenSoFar.size()).mapToObj(i -> "w" + i).toList());
        } finally {
            stopWriter.set(true);
            permits.release(1_000);
            writer.join(5_000);
            model.shutdown();
        }
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:occurrent:test")).withType("Created").build();
    }
}
