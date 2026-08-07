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

/**
 * Conformance suites for the blocking subscription contracts, and the fixtures an implementation hands them.
 *
 * <h2>How a suite is run</h2>
 *
 * Every suite is an abstract JUnit 5 class. You extend it and return a fixture from {@code createFixture()}, which is
 * called before every test method and closed after every one, including a failing one. A fresh fixture per test is the
 * contract rather than an implementation detail. The model it hands back must have no subscriptions on it, and the
 * storage behind a strategy or a checkpoint storage must hold nothing.
 *
 * <table border="1">
 *   <caption>Which suite covers what</caption>
 *   <tr><th>Suite</th><th>Fixture</th><th>Extend it if</th></tr>
 *   <tr><td>{@link org.occurrent.tck.subscription.blocking.SubscriptionModelConformance}</td>
 *       <td>{@link org.occurrent.tck.subscription.blocking.SubscriptionModelFixture}</td>
 *       <td>you have a {@code SubscriptionModel} at all</td></tr>
 *   <tr><td>{@link org.occurrent.tck.subscription.blocking.IntrospectableSubscriptionModelConformance}</td>
 *       <td>{@code SubscriptionModelFixture}</td>
 *       <td>it implements {@code IntrospectableSubscriptionModel}</td></tr>
 *   <tr><td>{@link org.occurrent.tck.subscription.blocking.CheckpointAwareSubscriptionModelConformance}</td>
 *       <td>{@code SubscriptionModelFixture}</td>
 *       <td>it implements {@code CheckpointAwareSubscriptionModel}</td></tr>
 *   <tr><td>{@link org.occurrent.tck.subscription.blocking.InProcessDeliveryConformance}</td>
 *       <td>{@code SubscriptionModelFixture}</td>
 *       <td>it calls the handler on the thread that published the event</td></tr>
 *   <tr><td>{@link org.occurrent.tck.subscription.blocking.RestartConformance}</td>
 *       <td>{@link org.occurrent.tck.subscription.blocking.RestartableSubscriptionModelFixture}</td>
 *       <td>it can be rebuilt over the durable state it left behind</td></tr>
 *   <tr><td>{@link org.occurrent.tck.subscription.blocking.CheckpointStorageConformance}</td>
 *       <td>{@link org.occurrent.tck.subscription.blocking.CheckpointStorageFixture}</td>
 *       <td>you have a {@code CheckpointStorage}</td></tr>
 *   <tr><td>{@link org.occurrent.tck.subscription.blocking.CompetingConsumerStrategyConformance}</td>
 *       <td>{@link org.occurrent.tck.subscription.blocking.CompetingConsumerStrategyFixture}</td>
 *       <td>you have a {@code CompetingConsumerStrategy}</td></tr>
 * </table>
 *
 * <strong>Not extending a suite is how you decline it.</strong> There is no runtime skip and no flag, so an
 * implementation that opts out of a contract does so by an absence anyone can grep for. Nothing here calls
 * {@code Assumptions}, and a build guard fails on a reference to one.
 *
 * <h2>The three kinds of member a fixture holds</h2>
 *
 * Telling them apart is most of what makes a fixture easy to write correctly.
 *
 * <h3>An accessor hands over the thing under test</h3>
 *
 * {@code subscriptionModel()}, {@code publish(..)}, {@code checkpointStorage()},
 * {@code competingConsumerStrategy()}, {@code newCompetingConsumerStrategy()}, {@code restart()},
 * {@code aCheckpointToStartFrom()} and {@code additionalCheckpoints()}. It owes nothing beyond being wired up, and it
 * is where the two mistakes that cost the most live. {@code close()} must shut the model down without dropping a
 * collection or a database a live change stream is watching, since dropping either leaves the next test watching a
 * stream that will never deliver, and nothing may publish the same event id twice, because a store-backed model
 * refuses a duplicate through a unique index while an in-process model delivers it twice.
 *
 * <h3>A declaration is a difference nothing on the API reports</h3>
 *
 * <table border="1">
 *   <caption>Every declaration, and what each answer obliges</caption>
 *   <tr><th>Declaration</th><th>{@code true} owes</th><th>{@code false} owes</th><th>Asserted by</th></tr>
 *   <tr><td>{@code deliversEventsPublishedWhilePaused()}</td>
 *       <td>the event arrives after {@code resumeSubscription}</td>
 *       <td>it never arrives at that handler</td>
 *       <td>{@code SubscriptionModelConformance}</td></tr>
 *   <tr><td>{@code retriesAFailingHandler()}</td>
 *       <td>a later call to the handler</td>
 *       <td>the exception out of {@code publish(..)}</td>
 *       <td>{@code SubscriptionModelConformance}</td></tr>
 *   <tr><td>{@code acceptsSeveralSubscriptions()}</td>
 *       <td>two subscriptions receiving independently</td>
 *       <td>a refusal of the second {@code subscribe}</td>
 *       <td>{@code SubscriptionModelConformance}</td></tr>
 *   <tr><td>{@code acceptedStartAtVariants()}</td>
 *       <td>each accepted variant delivers what follows it</td>
 *       <td>each omitted variant is refused by {@code subscribe}</td>
 *       <td>{@code SubscriptionModelConformance}</td></tr>
 *   <tr><td>{@code replaysHistoryToANewSubscription()}</td>
 *       <td>a new subscription receives what predates it</td>
 *       <td>it receives nothing that predates it</td>
 *       <td>{@code SubscriptionModelConformance}</td></tr>
 *   <tr><td>{@code resumesAfterARestart()}</td>
 *       <td>an event published while nothing ran still arrives</td>
 *       <td>that event is gone and the rebuilt subscription starts at the present</td>
 *       <td>{@code RestartConformance}</td></tr>
 *   <tr><td>{@code preservesCheckpointType(..)}</td>
 *       <td>the type comes back out of {@code read}</td>
 *       <td>the type must <em>not</em> come back, and {@code asString()} must still round-trip</td>
 *       <td>{@code CheckpointStorageConformance}</td></tr>
 * </table>
 *
 * Three rules govern them, and each one was learned by getting it wrong. Reasoning in
 * <a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0094-the-subscription-tck-declares-three-differences-and-waits-deterministically.md">ADR 94</a>.
 *
 * <ol>
 *   <li><strong>Declare what cannot be asked, and ask everything else.</strong> A declaration exists because no method
 *   reports the answer. {@code globalCheckpoint()} is asked rather than declared, because a declaration can go stale
 *   while a call cannot.</li>
 *   <li><strong>Both branches are asserted, so neither answer is free.</strong> A declaration that costs nothing on one
 *   branch is a switch for turning off the only test of a property, and it sits in the file belonging to whoever
 *   changed the model. That is why a {@code deliversSynchronously()} flag was rejected and
 *   {@code InProcessDeliveryConformance} exists instead.</li>
 *   <li><strong>A declaration must not park a bug.</strong> If a suite fails, the first question is whether the model
 *   is wrong, not which way to declare it. The second way to get this wrong is reaching for the word bug before
 *   measuring what the fix costs.</li>
 * </ol>
 *
 * <h3>A budget bounds a wait on a schedule the interface does not publish</h3>
 *
 * {@link org.occurrent.tck.subscription.blocking.SubscriptionModelFixture#deliveryTimeout()} and
 * {@link org.occurrent.tck.subscription.blocking.CompetingConsumerStrategyFixture#timeToConverge()}. Neither is a
 * difference between implementations and neither is asserted both ways. Both are bounds rather than delays. A wait
 * stops as soon as its condition holds, so a generous budget costs a passing run nothing and is paid in full only by a
 * test that was going to fail. Declare comfortably above your worst case rather than tightly, since a bound too small
 * turns a loaded machine into a red build while a bound too large slows down only failures.
 *
 * <h2>What a version of this artifact promises</h2>
 *
 * Reasoning in
 * <a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0107-what-a-tck-version-promises.md">ADR 107</a>.
 *
 * <ul>
 *   <li><strong>A minor bump may add suites and tighten assertions</strong>, so bumping can turn a green build red.
 *   That is this artifact working rather than regressing. The two supported responses are to fix the implementation,
 *   or to stay on the Occurrent version you were on. Holding the TCK back on its own is not a third one, because each
 *   leaf declares its runtime dependencies at its own version and its suites are compiled against that API.</li>
 *   <li><strong>A fixture never stops compiling on a minor bump.</strong> A new member arrives as a {@code default},
 *   and where a returned value would be a lie it arrives as a {@code default} that throws, naming itself and the suite
 *   that reached it. Removing a member, changing a signature or removing a suite is a major bump.</li>
 *   <li><strong>There is no way to disable one test group</strong>, and that is the answer rather than a gap.</li>
 *   <li>A patch bump may loosen an assertion that was wrong. It may not tighten one.</li>
 * </ul>
 */
@NullMarked
package org.occurrent.tck.subscription.blocking;

import org.jspecify.annotations.NullMarked;
