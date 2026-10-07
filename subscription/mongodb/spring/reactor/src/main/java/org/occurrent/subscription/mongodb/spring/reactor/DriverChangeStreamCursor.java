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
import com.mongodb.internal.operation.CommandCursorResult;
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
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

/**
 * The change stream cursor of the MongoDB driver, with the resume token MongoDB sent with its last batch. The driver's
 * reactive API doesn't hand that token out, so this reads the driver's own cursor from a private field of the reactive
 * wrapper. It only reads private fields, and reads and closes the cursor through the wrapper's public methods.
 * <p>
 * Every class and member used here is internal to the driver, so a driver release can remove or change them. When
 * {@link #unavailableBecause()} isn't {@code null}, or {@link #open(ChangeStreamPublisher)} or
 * {@link #postBatchResumeToken()} fails with {@link Unavailable}, the caller reads its change streams without the token.
 * <p>
 * A token is read on another thread than the one the driver writes it on. Every field on the way to it is final or
 * volatile in the driver, so a read sees a token at least as new as the one an earlier read saw. A driver where one
 * of them is neither is {@link Unavailable}.
 */
@NullMarked
final class DriverChangeStreamCursor {
    private static final String CHANGE_STREAM_CURSOR_CLASS_NAME = "com.mongodb.internal.operation.AsyncChangeStreamBatchCursor";

    // Reaches past BatchCursor, so all of it is looked up once, when the class loads, and a failure is kept as the reason
    private static final @Nullable MethodHandle WRAPPED_CURSOR;
    private static final @Nullable Class<?> CHANGE_STREAM_CURSOR_CLASS;
    private static final @Nullable MethodHandle CURSOR_OF_CHANGE_STREAM;
    private static final @Nullable String UNAVAILABLE_BECAUSE;

    static {
        MethodHandle wrappedCursor = null;
        Class<?> changeStreamCursorClass = null;
        MethodHandle cursorOfChangeStream = null;
        String unavailableBecause;
        try {
            BatchCursorPublisher.class.getMethod("batchCursor", int.class);
            AsyncAggregateResponseBatchCursor.class.getMethod("getPostBatchResumeToken");
            // Getters, so the fields can be read and never written
            wrappedCursor = MethodHandles.privateLookupIn(BatchCursor.class, MethodHandles.lookup())
                    .findGetter(BatchCursor.class, "wrapped", AsyncBatchCursor.class);
            changeStreamCursorClass = MethodHandles.privateLookupIn(CommandCursorResult.class, MethodHandles.lookup()).findClass(CHANGE_STREAM_CURSOR_CLASS_NAME);
            cursorOfChangeStream = MethodHandles.privateLookupIn(changeStreamCursorClass, MethodHandles.lookup())
                    .findGetter(changeStreamCursorClass, "wrapped", AtomicReference.class);
            unavailableBecause = firstNonNull(
                    unlessDeclaredAs(BatchCursor.class, "wrapped", AsyncBatchCursor.class, Modifier.FINAL),
                    unlessDeclaredAs(changeStreamCursorClass, "wrapped", AtomicReference.class, Modifier.FINAL),
                    unlessDeclaredAs(CommandCursorResult.class, "postBatchResumeToken", BsonDocument.class, Modifier.FINAL));
        } catch (ReflectiveOperationException | RuntimeException | LinkageError e) {
            unavailableBecause = e.toString();
        }
        WRAPPED_CURSOR = wrappedCursor;
        CHANGE_STREAM_CURSOR_CLASS = changeStreamCursorClass;
        CURSOR_OF_CHANGE_STREAM = cursorOfChangeStream;
        UNAVAILABLE_BECAUSE = unavailableBecause;
    }

    private final BatchCursor<ChangeStreamDocument<Document>> cursor;
    private final AsyncAggregateResponseBatchCursor<?> driverCursor;
    // The cursor the driver's change stream cursor reads with, which the driver empties while it opens the change stream again
    private final AtomicReference<?> cursorOfChangeStream;

    private DriverChangeStreamCursor(BatchCursor<ChangeStreamDocument<Document>> cursor, AsyncAggregateResponseBatchCursor<?> driverCursor, AtomicReference<?> cursorOfChangeStream) {
        this.cursor = cursor;
        this.driverCursor = driverCursor;
        this.cursorOfChangeStream = cursorOfChangeStream;
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
                    cursor = readable(batchCursor);
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

    private static DriverChangeStreamCursor readable(BatchCursor<ChangeStreamDocument<Document>> batchCursor) {
        Object driverCursor = read(WRAPPED_CURSOR, batchCursor);
        if (driverCursor == null || driverCursor.getClass() != CHANGE_STREAM_CURSOR_CLASS) {
            throw new Unavailable("the driver's change stream cursor is a " + (driverCursor == null ? "null" : driverCursor.getClass().getName()));
        }
        AtomicReference<?> cursorOfChangeStream = (AtomicReference<?>) requireNonNull(read(CURSOR_OF_CHANGE_STREAM, driverCursor));
        // The driver builds a cursor it opens again the same way, so the class seen here is the class every read goes through
        Object commandCursor = cursorOfChangeStream.get();
        String unreadableBecause = commandCursor == null ? "the driver's change stream cursor has no cursor to read with" : unlessTokenIsReadSafelyThrough(commandCursor.getClass());
        if (unreadableBecause != null) {
            throw new Unavailable(unreadableBecause);
        }
        return new DriverChangeStreamCursor(batchCursor, (AsyncAggregateResponseBatchCursor<?>) driverCursor, cursorOfChangeStream);
    }

    private static @Nullable Object read(@Nullable MethodHandle getter, Object target) {
        if (getter == null) {
            throw new Unavailable(String.valueOf(UNAVAILABLE_BECAUSE));
        }
        try {
            return getter.invoke(target);
        } catch (Throwable t) {
            throw new Unavailable(t.toString());
        }
    }

    /**
     * @return Why a token read through a cursor of {@code commandCursorClass} can be older than one read before it, or
     * {@code null} when it can't.
     */
    static @Nullable String unlessTokenIsReadSafelyThrough(Class<?> commandCursorClass) {
        return unlessDeclaredAs(commandCursorClass, "commandCursorResult", CommandCursorResult.class, Modifier.VOLATILE);
    }

    private static @Nullable String unlessDeclaredAs(Class<?> declaringClass, String fieldName, Class<?> type, int modifier) {
        Field field;
        try {
            field = declaringClass.getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            return e.toString();
        }
        if ((field.getModifiers() & modifier) == 0) {
            return field + " isn't " + Modifier.toString(modifier);
        }
        if (field.getType() != type) {
            return field + " isn't a " + type.getName();
        }
        return null;
    }

    private static @Nullable String firstNonNull(@Nullable String... reasons) {
        for (String reason : reasons) {
            if (reason != null) {
                return reason;
            }
        }
        return null;
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
     * @throws Unavailable When the driver fails to hand over the token for any other reason.
     */
    @Nullable BsonDocument postBatchResumeToken() {
        Object commandCursor = cursorOfChangeStream.get();
        if (commandCursor == null) {
            return null;
        }
        try {
            return driverCursor.getPostBatchResumeToken();
        } catch (RuntimeException | AssertionError e) {
            // The driver replaces the cursor it reads with when it opens the change stream again, and never puts one back
            if (cursorOfChangeStream.get() != commandCursor) {
                return null;
            }
            throw new Unavailable(e.toString());
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
