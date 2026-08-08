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

package org.occurrent.testing.springboot;

import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * Put this on a test class to get an {@link org.occurrent.testing.junit.blocking.OccurrentSubscriptionsExtension} bean wired to
 * the application context's subscription model, then autowire it:
 *
 * <pre>{@code
 * @SpringBootTest
 * @EnableOccurrentTesting
 * class OrderProjectionTest {
 *
 *     @Autowired
 *     @RegisterExtension
 *     OccurrentSubscriptionsExtension subscriptions;
 *
 *     @Test
 *     void order_projection_is_updated_when_an_order_is_placed() {
 *         subscriptions.start("order-projection");
 *         ...
 *     }
 * }
 * }</pre>
 *
 * The extension bean is all this adds. Your event store, subscription model and everything else in the context are
 * left exactly as the application wires them, so a test still runs against the real store rather than an in-memory
 * substitute. That is the point, since a subscription is only worth testing against the change streams, checkpoints
 * and catch-up it actually uses.
 * <p>
 * Everything this adds is available without Spring by constructing the extension directly, see
 * {@link org.occurrent.testing.junit.blocking.OccurrentSubscriptionsExtension#stoppedByDefault} or, on the reactive
 * stack, {@link org.occurrent.testing.junit.reactor.OccurrentSubscriptionsExtension#stoppedByDefault}.
 * <p>
 * A reactive application gets an {@code org.occurrent.testing.junit.reactor.OccurrentSubscriptionsExtension} bean
 * instead, which is autowired the same way, once {@code occurrent-testing-junit-jupiter-reactor} is a test dependency.
 * Adding that artifact rather than the blocking one, or both for an application that runs both stacks, is the opt-in.
 * {@link OccurrentTestingImportSelector} decides which configuration this annotation imports by checking the
 * classpath. Whichever context is present, every {@code SubscriptionModelLifeCycle} bean in it is stopped, because a
 * Spring context can hold more than one life-cycle bearing model, for example a durable model and a
 * {@code SynchronousSubscriptionModel}, and deny-by-default means stopping every one of them.
 * <p>
 * The extension bean also clears state it can reach on its own. Exactly one {@code CheckpointStorage} bean in the
 * context is applied with {@code clearingCheckpoints(..)} without {@link #clearState()}, since deleting a checkpoint
 * a test never wrote a document for is harmless and the ambiguous case of more than one such bean is left to a test
 * naming the one it means with {@code clearingCheckpointsFor(..)}. Flushing the event store itself is not harmless
 * the same way, since a wrong flush is a silently passing test rather than a failing one, so it is behind
 * {@link #clearState()} instead.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@Import(OccurrentTestingImportSelector.class)
public @interface EnableOccurrentTesting {

    /**
     * Flush the store between tests as well as stopping subscriptions and clearing checkpoints, for a test that
     * writes events and needs the next test to start from an empty store rather than accumulating fixtures across
     * a cached Spring test context.
     * <p>
     * Requires a store integration on the test classpath that knows how to flush. {@code occurrent-testing-mongodb}
     * plus a {@code MongoTemplate} bean is the one this module wires today, with
     * {@code OccurrentMongoFlush.everyCollectionIn(..)} against that template's database. Set to {@code true} with
     * neither on the classpath and {@link OccurrentTestingImportSelector} fails context refresh rather than leaving
     * the flush silently unwired.
     *
     * @return {@code false} by default, since flushing is store-specific and destructive where a checkpoint delete is
     * neither
     */
    boolean clearState() default false;
}
