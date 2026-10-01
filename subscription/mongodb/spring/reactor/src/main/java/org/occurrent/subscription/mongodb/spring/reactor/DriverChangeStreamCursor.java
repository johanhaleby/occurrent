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

package org.occurrent.subscription.mongodb.spring.reactor;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.internal.async.AsyncAggregateResponseBatchCursor;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.internal.BatchCursor;
import com.mongodb.reactivestreams.client.internal.BatchCursorPublisher;
import org.bson.BsonDocument;
import org.bson.Document;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Mono;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The change stream cursor of the MongoDB driver, with the resume token MongoDB sent with its last batch. The driver's
 * reactive API doesn't hand that token out, so this reads the driver's own cursor from a private field of the reactive
 * wrapper. It only reads the field, and reads and closes the cursor through the wrapper's public methods.
 * <p>
 * Every class and member used here is internal to the driver, so a driver release can remove or change them. When
 * {@link #unavailableBecause()} isn't {@code null}, or {@link #open(ChangeStreamPublisher)} fails with
 * {@link Unavailable}, the caller reads its change streams without the token.
 */
@NullMarked
final class DriverChangeStreamCursor {
    // Reaches past BatchCursor, so all of it is looked up once, when the class loads, and a failure is kept as the reason
    private static final @Nullable MethodHandle WRAPPED_CURSOR;
    private static final @Nullable String UNAVAILABLE_BECAUSE;

    static {
        MethodHandle wrappedCursor = null;
        String unavailableBecause = null;
        try {
            BatchCursorPublisher.class.getMethod("batchCursor", int.class);
            AsyncAggregateResponseBatchCursor.class.getMethod("getPostBatchResumeToken");
            // A getter, so the field can be read and never written
            wrappedCursor = MethodHandles.privateLookupIn(BatchCursor.class, MethodHandles.lookup())
                    .findGetter(BatchCursor.class, "wrapped", AsyncBatchCursor.class);
        } catch (ReflectiveOperationException | RuntimeException | LinkageError e) {
            unavailableBecause = e.toString();
        }
        WRAPPED_CURSOR = wrappedCursor;
        UNAVAILABLE_BECAUSE = unavailableBecause;
    }

    private final BatchCursor<ChangeStreamDocument<Document>> cursor;
    private final AsyncAggregateResponseBatchCursor<?> driverCursor;

    private DriverChangeStreamCursor(BatchCursor<ChangeStreamDocument<Document>> cursor, AsyncAggregateResponseBatchCursor<?> driverCursor) {
        this.cursor = cursor;
        this.driverCursor = driverCursor;
    }

    /**
     * @return Why this driver's cursor can't be read, or {@code null} when it can.
     */
    static @Nullable String unavailableBecause() {
        return UNAVAILABLE_BECAUSE;
    }

    /**
     * Opens the change stream. A cursor that MongoDB opens after the returned {@code Mono} was cancelled is closed.
     *
     * @return The cursor, or an {@link Unavailable} error, after closing the cursor, when the driver built one this
     * class can't read the token from.
     */
    static Mono<DriverChangeStreamCursor> open(ChangeStreamPublisher<Document> changeStream) {
        if (UNAVAILABLE_BECAUSE != null) {
            return Mono.error(new Unavailable(UNAVAILABLE_BECAUSE));
        }
        if (!(changeStream instanceof BatchCursorPublisher<?>)) {
            return Mono.error(new Unavailable("the change stream publisher is a " + changeStream.getClass().getName()));
        }
        @SuppressWarnings("unchecked")
        BatchCursorPublisher<ChangeStreamDocument<Document>> publisher = (BatchCursorPublisher<ChangeStreamDocument<Document>>) changeStream;
        return Mono.create(sink -> {
            AtomicBoolean cancelled = new AtomicBoolean();
            sink.onCancel(() -> cancelled.set(true));
            // A batch size of 0 makes the aggregate return no events, so every event comes from a getMore
            publisher.batchCursor(0).subscribe(batchCursor -> {
                if (cancelled.get()) {
                    batchCursor.close();
                    return;
                }
                DriverChangeStreamCursor cursor;
                try {
                    cursor = new DriverChangeStreamCursor(batchCursor, driverCursorOf(batchCursor));
                } catch (Unavailable e) {
                    batchCursor.close();
                    sink.error(e);
                    return;
                }
                sink.success(cursor);
                // Closing twice does nothing, and a cancel between the check above and success drops the cursor
                if (cancelled.get()) {
                    cursor.close();
                }
            }, sink::error);
        });
    }

    private static AsyncAggregateResponseBatchCursor<?> driverCursorOf(BatchCursor<ChangeStreamDocument<Document>> batchCursor) {
        Object driverCursor;
        try {
            driverCursor = requireAvailable().invoke(batchCursor);
        } catch (Throwable t) {
            throw new Unavailable(t.toString());
        }
        if (!(driverCursor instanceof AsyncAggregateResponseBatchCursor<?> aggregateResponseCursor)) {
            throw new Unavailable("the driver's change stream cursor is a " + (driverCursor == null ? "null" : driverCursor.getClass().getName()));
        }
        return aggregateResponseCursor;
    }

    private static MethodHandle requireAvailable() {
        if (WRAPPED_CURSOR == null) {
            throw new Unavailable(String.valueOf(UNAVAILABLE_BECAUSE));
        }
        return WRAPPED_CURSOR;
    }

    /**
     * The next batch with at least one event. The driver asks MongoDB again by itself while a batch comes back empty,
     * and hands back an empty batch only once the cursor is closed. After a resumable error it opens the change stream
     * again by itself.
     */
    Mono<List<ChangeStreamDocument<Document>>> next() {
        return Mono.from(cursor.next());
    }

    /**
     * Read from another thread while {@link #next()} waits, so the token can belong to a batch that {@code next()} is
     * about to hand back.
     *
     * @return The resume token MongoDB sent with the last batch, or {@code null} when there is none to read, such as
     * while the driver opens the change stream again after an error.
     */
    @Nullable BsonDocument postBatchResumeToken() {
        try {
            return driverCursor.getPostBatchResumeToken();
        } catch (RuntimeException | AssertionError e) {
            return null;
        }
    }

    boolean isClosed() {
        return cursor.isClosed();
    }

    void close() {
        cursor.close();
    }

    /**
     * This driver's change stream cursor can't be read.
     */
    static final class Unavailable extends RuntimeException {
        Unavailable(String reason) {
            super(reason, null, false, false);
        }
    }
}
