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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

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
 * instead, autowired the same way, once {@code occurrent-testing-junit-jupiter-reactor} is a test dependency. Adding
 * that artifact rather than the blocking one, or both for an application that runs both stacks, is the opt-in;
 * {@link OccurrentTestingImportSelector} decides which configuration this annotation imports by checking the
 * classpath. Whichever context is present, every {@code SubscriptionModelLifeCycle} bean in it is stopped, because a
 * Spring context can hold more than one life-cycle bearing model, for example a durable model and a
 * {@code SynchronousSubscriptionModel}, and deny-by-default means stopping every one of them.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@Import(OccurrentTestingImportSelector.class)
public @interface EnableOccurrentTesting {
}
