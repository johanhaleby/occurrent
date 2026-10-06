package org.occurrent.subscription.blocking.competingconsumers;

import io.cloudevents.CloudEvent;
import jakarta.annotation.PreDestroy;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionNotRunningException;
import org.occurrent.subscription.SubscriptionRefusedException;
import org.occurrent.subscription.UnknownSubscriptionException;
import org.occurrent.subscription.api.blocking.*;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy.CompetingConsumerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * A competing consumer subscription model wraps another subscription model to allow several subscribers to subscribe to the same subscription. One of the subscribes will get a lock of the subscription
 * and receive events from it. If a subscriber looses its lock, another subscriber will take over automatically. To achieve distributed locking, the subscription model uses a {@link CompetingConsumerStrategy} to
 * support different algorithms. You can write custom algorithms by implementing this interface yourself. Here's an example of how to create and use the {@link CompetingConsumerSubscriptionModel}. This example
 * uses the {@code NativeMongoLeaseCompetingConsumerStrategy} from module {@code org.occurrent:subscription-mongodb-native-blocking-competing-consumer-strategy}.
 * It also wraps the <a href="https://occurrent.org/documentation#durable-subscriptions-blocking">DurableSubscriptionModel</a> which in turn wraps the
 * <a href="https://occurrent.org/documentation#blocking-subscription-using-the-native-java-mongodb-driver">Native MongoDB</a> subscription model.
 * <br>
 * <br>
 * <pre>
 * MongoDatabase mongoDatabase = mongoClient.getDatabase("some-database");
 * CheckpointStorage positionStorage = NativeMongoCheckpointStorage(mongoDatabase, "position-storage");
 * SubscriptionModel wrappedSubscriptionModel = new DurableSubscriptionModel(new NativeMongoSubscriptionModel(mongoDatabase, "events", TimeRepresentation.DATE), positionStorage);
 *
 * // Create the CompetingConsumerSubscriptionModel
 * NativeMongoLeaseCompetingConsumerStrategy competingConsumerStrategy = NativeMongoLeaseCompetingConsumerStrategy.withDefaults(mongoDatabase);
 * CompetingConsumerSubscriptionModel competingConsumerSubscriptionModel = new CompetingConsumerSubscriptionModel(wrappedSubscriptionModel, competingConsumerStrategy);
 *
 * // Now subscribe!
 * competingConsumerSubscriptionModel.subscribe("subscriptionId", type("SomeEvent"));
 * </pre>
 * <p>
 * If the above code is executed on multiple nodes/processes, then only <i>one</i> subscriber at a time receives events,
 * apart from an event this model lets through without the lease, see below, which can reach two nodes.
 * <br>
 * <br>
 * That is also the scope of the usual "a subscription id identifies one subscription" rule here: it holds within one
 * {@link CompetingConsumerSubscriptionModel} instance, which refuses a subscription id it already has, and says nothing
 * about the other instances, which are meant to use the very same id.
 * <br>
 * <br>
 * {@link #pauseSubscription(String)} works on a node that has not won the lock, not only on the one delivering events.
 * Pausing such a node records the pause and stops it from competing until it is explicitly resumed, so cluster-wide
 * pause is calling {@link #pauseSubscription(String)} on every node
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0112-a-competing-consumer-can-be-paused-while-still-waiting-for-the-lock.md">ADR 112</a>).
 * <br>
 * <br>
 * After {@link #stop()}, and until the next {@link #start(boolean)}, this node holds no lease and delivers nothing for a
 * competing subscription, also one made while it is stopped, with three exceptions. {@code stop()} pauses a
 * subscription in the wrapped model when that model still runs it, which it can after throwing from its own
 * {@code stop()}. The first exception is a subscription the wrapped model cannot pause, which keeps its lease and stays
 * running, since it still delivers, and {@code stop()} throws. The second is a subscription the user resumes with
 * {@link #resumeSubscription(String)}. It competes for its lease and runs once this node wins it, whether that happens
 * straight away or on a later grant. The third is described below. A {@code subscribe(..)} on another thread whose
 * registration is under way when {@code stop()} runs, or whose subscription the wrapped model is making or already
 * runs, can still hold a lease after {@code stop()} has returned. It tries to give the lease up at its next step, as
 * {@code stop()} would. A subscription whose lock another call holds when {@code stop()} gets to it, such as a try
 * waiting for the lease strategy, can too. That call, or at the latest the try of the subscription once it gets the
 * lock, tries to unregister it. The subscription delivers nothing meanwhile, since {@code stop()} stops the wrapped
 * model. When the try of a subscription fails to unregister it, the try releases the lease instead, if the lease
 * strategy still reports it held. When the MongoDB lease strategies fail to remove a lease, they report it as not held,
 * stop refreshing it and remove it again on their next refresh. Another node can take the subscription over once that
 * removal succeeds, or at the latest once the lease has expired.
 * <br>
 * <br>
 * While this model is started, a competing subscription that is neither cancelled nor paused by the user is registered
 * with the lease strategy, and runs in the wrapped model only while this node holds its lease. A call to the lease
 * strategy or the wrapped model that throws delays that, and never needs a call from the user to recover. The
 * subscription it failed for is tried again on a thread of its own, with a backoff, until it is where it belongs, and
 * every fifth try that fails again is logged as a warning. {@link #start(boolean)} and
 * {@link #resumeSubscription(String)} log such a failure as a warning and return, and so does {@code subscribe(..)} of a
 * competing subscription once the wrapped model has made it. {@link #stop()}, {@link #pauseSubscription(String)} and
 * {@link #cancelSubscription(String)} throw it, and a cancel after which the wrapped model no longer holds the
 * subscription forgets it instead of trying it again. A call to the wrapped model can take effect before it throws, so a
 * subscription recorded as running that the wrapped model no longer runs, or cannot say whether it runs, is recorded as
 * paused before it is tried again, and each try decides from what the wrapped model does. While this model is stopped,
 * the same tries give up the registration and pause the subscription in the wrapped model, apart from the exceptions
 * above. A wrapped model that returns from {@code pauseSubscription} normally but keeps running the subscription
 * holds each event of it while this node does not hold the lease, until this model lets the event through, see below.
 * <br>
 * <br>
 * This model asks {@link CompetingConsumerStrategy#hasLock} before it runs the action of a competing subscription for
 * an event. The action runs once that reports the lease held and no {@code stop()} has begun since a resume or
 * subscribe of the subscription that it does not wait for, see below. Until then the event waits on the thread the
 * wrapped model delivers it on. A lease strategy whose {@code hasLock} throws an exception counts as reporting the lease
 * not held.
 * <br>
 * <br>
 * A waiting event also runs without {@code hasLock} reporting the lease held, as it would without the lease strategy.
 * It is never dropped, but it can run on this node while another node holds the lease and runs it as well, so it can
 * be delivered twice. That happens in these cases:
 * <ul>
 * <li>This model calls the wrapped model for a step of its own on the subscription, such as a pause on the loss of
 * the lease, a cancel, or asking whether the wrapped model runs it or holds it paused.</li>
 * <li>This model calls the wrapped model as a whole, to start, stop or shut it down or to ask whether it runs, which
 * lets the waiting events of every competing subscription through. It asks whether the wrapped model runs before every
 * call that may start it, so a grant, resume or subscribe that runs another competing subscription in the wrapped
 * model lets the event through, and so do {@code start(..)} and {@code shutdown()}. {@code stop()} asks the same
 * before it stops the wrapped model.</li>
 * <li>The wrapped model delivers the event on a thread that is inside a call this model makes into it, or hands it
 * over once a cancel of the subscription has returned or a subscribe of it has thrown. That includes a subscription
 * the wrapped model still holds, which it can after a cancel that throws in the wrapped model while the subscription
 * is being made.</li>
 * <li>An interrupt of its thread ends a wait between two looks at the lease, or the interrupt flag is already set as
 * such a wait begins.</li>
 * <li>{@code shutdown()} has begun.</li>
 * </ul>
 * So a wrapped model that waits in such a call for the thread that delivers the event, as one does that waits for a
 * running action in {@code pauseSubscription} or that delivers while holding a lock the call takes, never holds the
 * call up, on a platform thread or a virtual one.
 * <br>
 * <br>
 * When an interrupt ended the wait and the interrupt flag was clear as the event came, the action runs with the flag
 * clear, and the flag is set again once the action returns or throws. In every other case this model does not change
 * the flag, so it can be set while the action runs, for instance when the interrupt comes while this model asks the
 * lease strategy and the strategy then reports the lease held. A lease strategy whose {@code hasLock} clears the flag
 * takes the interrupt instead, also one already set when the event came, and the event then waits for the lease and
 * runs with the flag clear. The MongoDB lease strategies do not change the flag.
 * <br>
 * <br>
 * So a node stops delivering for a subscription once its lease strategy stops reporting the lease held, also while a
 * call for another subscription waits, apart from the events this model lets through in the cases above. The MongoDB
 * lease strategies stop reporting a lease held three quarters of the lease time after the request that last set it
 * was sent, also when every refresh since has failed. An action already running for an event runs to its end.
 * <br>
 * <br>
 * A wrapped model of your own should not hold a lock while it hands an event to the action. If it does, an event that
 * waits for the lease keeps the lock, and everything that needs the lock waits with it until this node holds the lease
 * again or this model lets the event through in one of the cases above, as it does when it pauses the subscription
 * once the lease is lost. With the MongoDB lease strategies that is the next refresh that gets through, which they try
 * every 10 seconds with the default lease time of 20 seconds, so the wait goes on for as long as MongoDB can't be
 * reached. The calls that wait are {@link #isRunning(String)}, {@link #isPaused(String)} for a subscription this model
 * has not paused and
 * {@link #subscriptionIds()}, which this model passes on to the wrapped model as they are. A
 * pause of another subscription waits too, and so does the loss of the lease of another subscription, since this model
 * passes both on to the wrapped model, and so does a thread of your own that waits for the lock. The subscription
 * models that Occurrent ships hand events over without holding such a lock, so nothing waits with them.
 * <br>
 * <br>
 * This model's monitor is held only to read and write what several subscriptions share, and never while a call waits
 * for the lease strategy or the wrapped model. A pause, resume or cancel, a grant or loss of the lease, and a try hold
 * a lock kept for their subscription instead, and a subscribe registers and subscribes holding neither. So a call that
 * waits for the database through an outage holds up calls for its own subscription and none for any other, unless a
 * {@code stop()} or {@code shutdown()} waits for that call to return. While a {@code stop()} waits, a call for another
 * subscription that may run while this model is stopped, such as a resume the user asks for, waits for it too. While
 * {@code shutdown()} waits, a loss of the lease of another subscription is not acted on, so that subscription goes on
 * delivering until {@code shutdown()} has shut the wrapped model down. A grant or loss of the lease that finds the lock
 * taken, also by a subscribe that is making the subscription, is left to a try. The try takes the lock once the call
 * holding it has returned, and decides from what holds then, so no grant or loss goes unanswered.
 * <br>
 * <br>
 * {@link #start(boolean)} and {@link #stop()} take every subscription at the same time, each on a thread of its own, so
 * a call for one subscription that waits for the lease strategy or the wrapped model holds up no other. They return
 * once each subscription whose lock was free has been taken care of. One whose lock another call holds is handed to a
 * thread of its own. Once it has the lock, that thread applies each {@code start(..)} and {@code stop()} not yet
 * applied to the subscription, oldest first, without letting go of the lock in between. The subscription then ends
 * where it would have with its lock free, also in whether {@code start(false)} keeps it paused afterwards and whether
 * the wrapped model runs. A {@code start(..)} or {@code stop()} is recorded as not applied yet for each subscription it
 * takes in the step that begins it. Any call that takes the lock of one of them after that, a pause, resume, cancel,
 * lease callback or try as much as a later {@code start(..)} or {@code stop()}, applies it before its own, also when
 * the thread of that {@code start(..)} or {@code stop()} has not got to the subscription yet. So a call that begins
 * once a {@code start(..)} or {@code stop()} has begun comes after it. The exception is a {@code stop()} that a
 * {@code start(..)} waiting behind it may still take back, described below. Only a pause, resume or cancel that began
 * after that {@code stop()} applies it first. A lease callback or a try that takes the lock meanwhile does not apply
 * it, and the thread of the {@code stop()} applies it once it is not taken back, so that callback or try comes before
 * it. A pause, resume or cancel also comes before each {@code start(..)} and {@code stop()} that began after it, also
 * when that one began while the pause, resume or cancel waited for the lock. The pause, resume and cancel calls waiting
 * for one lock go on in the order they began, and a lease callback lets each of them that began before the callback
 * came go first. The one exception is a call made on a thread that already holds the subscription's lock, from inside a
 * call this model makes to the lease strategy or the wrapped model. It waits for nothing and applies nothing recorded
 * as not applied yet, so it comes before each {@code start(..)} or {@code stop()} not yet applied to that subscription,
 * also one that began before it. When one of them fails, a competing subscription is tried again like after any other
 * failed call. Any other subscription is tried again by the thread it was handed to, together with each call after it,
 * until that succeeds or this model is shut down. A pause, resume or cancel that finds one of them still failing gives
 * it up when it began before the pause, resume or cancel, so the thread it was handed to does not try it again, applies
 * the ones after it in turn, and is then made all the same. The failure is logged as a warning, and the
 * {@code start(..)} or {@code stop()} does not throw it, also when the pause, resume or cancel applied it before the
 * thread of the {@code start(..)} or {@code stop()} got to the subscription. So a pause, resume or cancel throws what
 * fails in its own call, unless the one it gave up threw an {@link Error}, which it throws once its own call is made.
 * Giving up a {@code start(..)} does not give up starting the wrapped model. A thread of its own tries that until it
 * succeeds, or until a later {@code stop()} or {@code shutdown()}. A {@code start(..)} or {@code stop()} takes every
 * subscription this model knows in the same step that makes it visible to the other calls, so a subscription that a
 * {@code subscribe(..)} records meanwhile is either taken or reads it at its next step. A {@code start(..)} or
 * {@code stop()} that begins while another one runs waits for it to return, in the order they began, with one exception
 * described below.
 * <br>
 * <br>
 * Once {@link #shutdown()} has begun, no call starts the wrapped model or runs a subscription there, and a lease
 * callback that comes after that does nothing. {@code shutdown()} waits for each such call under way to return before
 * it shuts the wrapped model down, for as long as the call takes, apart from a {@code subscribe(..)} and a resume or
 * subscribe of a competing subscription in the wrapped model, see below. It makes one attempt to give up each lease,
 * all at once, and waits at most five seconds for them. It does not wait for a registration with the lease strategy
 * under way, and one that returns once {@code shutdown()} has begun makes one attempt to give up the lease it took. A
 * lease not given up expires after the lease time. When the wrapped model throws from its own {@code shutdown()}, no
 * lease is given up, since that model may still deliver.
 * <br>
 * <br>
 * A competing subscription made while this model is stopped goes to the wrapped model straight away, through
 * {@link SubscriptionModel#subscribePaused}, which holds it paused whether or not the wrapped model runs. It starts from
 * the position the wrapped model gives a subscription made while that model is stopped. For a
 * {@code DurableSubscriptionModel} with no stored position, that is the position when {@code subscribe(..)} was called,
 * so an event written before {@link #start(boolean)} is delivered. The subscription competes for its lease once this
 * model is started, with or without resuming subscriptions automatically, and winning the lease resumes it, or
 * subscribes it in the wrapped model when that model does not have it.
 * <br>
 * <br>
 * A competing subscription made while this model runs goes to the wrapped model through {@code subscribePaused} too,
 * once this node has won its lease, and is resumed there only when this node still holds the lease after that. So one
 * whose lease goes to another node while the wrapped model makes it delivers nothing, and waits for a grant.
 * <br>
 * <br>
 * A wrapped model that refuses {@code subscribePaused} with {@link UnsupportedOperationException}, as its default
 * implementation does, makes the third exception. A subscription made while this model is stopped competes for its
 * lease straight away and, when this node wins it, is subscribed in that wrapped model without starting it. When the
 * wrapped model runs it, it delivers events before {@link #start(boolean)}. Waiting for {@code start()} instead would
 * subscribe it where that model starts a subscription at that moment, past every event written in between, and losing
 * no event ranks above what {@code stop()} promises. Its events still wait for the lease as described above, and
 * {@code start()} finds it started. Once it has won the lease it competes as one the user resumed does, so losing the
 * lease pauses it and a later grant resumes it. One that loses the lease when it registers, or that the wrapped model
 * holds paused, waits for {@code start()} and competes from there, so a stopped node competes for nothing it does not
 * deliver.
 * <br>
 * <br>
 * The wrapped model is started, without resuming what it holds paused, before a subscription whose lease this node
 * holds is subscribed or resumed there, since a stopped model holds a new subscription paused. Every {@code start(..)}
 * starts it the same way when it is not running, so the wrapped model runs while this model is started, unless {@code
 * stop()} was called on the wrapped model itself. When a call of this model that can start the wrapped model finds it
 * not running, such as after {@code stop()} on the wrapped model itself, each competing subscription this model records
 * as running and whose lease this node holds runs there again once the wrapped model is started, also when starting it
 * took effect and then threw. Among the calls that do this are {@code start(..)}, a lease granted for another
 * subscription, a {@code subscribe(..)} that wins its lease and a resume of any subscription. A {@code start(..)}
 * returns without waiting for such a subscription to run, see {@link #start(boolean)}. The other calls leave it to a
 * try.
 * <br>
 * <br>
 * Pause a competing subscription through {@link #pauseSubscription(String)} on this model, not on the wrapped model.
 * This model decides whether a competing subscription runs. One it records as running and whose lease this node holds
 * runs again on the next {@code start(..)}, with or without resuming subscriptions automatically, on a grant of its
 * lease while this model is started and on a resume of it, also when the wrapped model holds it paused while it runs.
 * A {@code start(..)} returns without waiting for it to run. So a pause called directly on the wrapped model lasts only until one of those calls, and so does a {@code stop()} and
 * {@code start(..)} called on the wrapped model itself. A subscription the user paused on the wrapped model before
 * calling {@code stop()} there runs again on any of the calls above that start the wrapped model. One whose pause
 * through this model took effect in the wrapped model and then threw counts as paused through this model, and stays
 * paused through {@code start(false)}.
 * <br>
 * <br>
 * A model built over a wrapped model that is not running is stopped until its first {@code start(..)}, as
 * if {@code stop()} had been called on it. Until a {@code start(..)} has returned without throwing, each one resumes
 * every subscription that is paused, with either flag, also one {@code stop()} paused, except one the user paused and
 * has not resumed since, which {@code start(false)} keeps paused. Once a {@code start(true)} has resumed such a
 * subscription, {@code start(false)} resumes it too, also when that {@code start(true)} threw for another subscription.
 * Calling {@code start()} on the wrapped model instead runs the
 * subscriptions that do not compete but never registers a competing one for its lease, so every event of a competing
 * one waits. A warning is logged the first time that happens for each such subscription. Calling it on both models, in
 * either order, runs a competing subscription once this node holds its lease. Cancelling a competing subscription
 * directly on the wrapped model is not supported. This model may then take a later subscription with that id in the
 * wrapped model for its own, and pause, resume or cancel it there. Calling {@code stop()} directly on the wrapped model
 * while a call through this model is under way is not supported either. It can leave a competing subscription whose
 * lease this node holds paused in the wrapped model. A call checks that this model is not stopped under the lock {@code stop()} takes to record that it is,
 * and {@code stop()} stops the wrapped model only once every call let through before that has returned. So a {@code
 * stop()} that has returned is never followed by a start of the wrapped model for a subscription it overtook, or by a
 * resume there of one that does not compete. {@code stop()} waits for such a call for as long as the call takes. For a
 * {@code DurableSubscriptionModel} that includes reading the stored position, which the MongoDB checkpoint storages
 * retry by default for as long as the database cannot be reached. A resume or subscribe of a competing subscription in
 * the wrapped model is waited for only while it starts the wrapped model. A {@code stop()} or {@code shutdown()} that
 * begins after that returns without waiting for the call. Once a {@code stop()} has begun, an event the call lets the
 * wrapped model deliver waits, as described above. Once the call returns, the subscription is paused in the wrapped
 * model, which lets such an event run, and a wrapped model that the call started is stopped again, unless a {@code
 * start(..)} has come since. A {@code subscribe(..)} is an exception too. Neither {@code stop()} nor {@code shutdown()}
 * waits for the wrapped model to make the subscription, and the subscribe decides from what holds once it has. A
 * competing subscription is then paused in the wrapped model when this model is stopped, as at any other step, and
 * competes for its lease once this model is started. A wrapped model that started itself to make it is stopped again,
 * unless a {@code start(..)} or a call allowed while stopped has come since. One that does not compete stays in the
 * wrapped model as one made once {@code stop()} has returned does. When this model is shut down by then, the subscribe
 * throws, and pauses what it made when the wrapped model still runs it.
 * <br>
 * <br>
 * A call {@code stop()} refuses is refused at once, and only a call allowed while stopped, such as a resume that began
 * after {@code stop()} did, waits until the wrapped model is stopped and then runs. A resume
 * of a subscription that does not compete is allowed while stopped too. {@code stop()} waits for one under way before
 * it stops the wrapped model, and one that comes once {@code stop()} has stopped it does what the wrapped model does
 * with a resume while it is stopped. Some wrapped models, {@code InMemorySubscriptionModel} among them, start again
 * then and deliver. A resume of a competing subscription that began before {@code stop()} did is refused
 * instead, and {@code stop()} pauses the subscription after it, as it would have had the resume run first. That resume
 * starts nothing, so when the wrapped model does not run, {@code stop()} finds nothing to stop there, and a subscription
 * that does not compete and was resumed in the meantime stays resumed in the stopped wrapped model, where the resume
 * running first would have had {@code stop()} stop it. {@code stop()} waits for a call in the wrapped model only while
 * no {@code start(..)} is waiting behind it. Once one is, also one that was waiting before {@code stop()} got to the call,
 * {@code stop()} returns without stopping the wrapped model or a subscription, and the {@code start(..)} decides for
 * every subscription. Until then a pause, resume or cancel that began after {@code stop()} stops its subscription
 * before making its own call, as it would have had {@code stop()} got there first, and that subscription stays stopped
 * unless its own call or the {@code start(..)} changes that. {@code stop()} pauses a subscription in the wrapped model
 * only once it is done stopping that model, and only one that this model records as running and the wrapped model runs
 * when {@code stop()} gets to it, unless a resume that began after {@code stop()} has let it run. A failure to pause a
 * competing consumer in the wrapped model that such a pause, resume or cancel meets before that leaves the consumer
 * registered. {@code stop()} throws it when, once {@code stop()} gets to that subscription, the wrapped model runs it or
 * cannot say whether it does, and no such resume has let it run. Otherwise {@code stop()} logs it as a warning. Unless
 * such a resume has let the subscription run, {@code stop()} then stops it as it stops any other and throws what fails
 * there, such as the lease strategy failing to unregister it for {@code stop()}. If another call holds the lock of the
 * subscription by then, that call, or at the latest the try of the subscription once it gets the lock, tries to
 * unregister it instead, again unless such a resume has let it run. When the unregister of the try fails, the try
 * releases the lease only if the lease strategy still reports it held. Any other failure such a pause, resume or
 * cancel meets stopping a competing consumer, such as the lease strategy failing to unregister it for that call,
 * {@code stop()} throws, as it throws what it meets itself when it gets to that subscription first. A second
 * {@code stop()} waiting behind it waits for it to return instead.
 */
@NullMarked
public class CompetingConsumerSubscriptionModel implements SubscriptionModelWrapper, SubscriptionModel, SubscriptionModelLifeCycle, IntrospectableSubscriptions, CompetingConsumerListener {
    private static final Logger log = LoggerFactory.getLogger(CompetingConsumerSubscriptionModel.class);

    private final SubscriptionModel delegate;
    private final CompetingConsumerStrategy competingConsumerStrategy;

    private final AtomicBoolean stoppedByUser = new AtomicBoolean(false);
    // Consumers the user resumed since stop(), and ones made since that a wrapped model refusing subscribePaused runs,
    // which compete and run although this model is stopped
    private final Set<SubscriptionIdAndSubscriberId> mayRunWhileStopped = ConcurrentHashMap.newKeySet();

    private final ConcurrentMap<SubscriptionIdAndSubscriberId, CompetingConsumer> competingConsumers = new ConcurrentHashMap<>();
    // Subscriptions whose StartAt position indicated they should not use the competing consumer model
    private final Set<String> nonCompetingConsumersSubscriptions = Collections.newSetFromMap(new ConcurrentHashMap<>());
    // Ids a subscribe is making right now, read and written under the monitor only. A second subscribe for one of them
    // is refused before the first records it.
    private final Map<String, BeingMade> subscriptionsBeingMade = new HashMap<>();
    // Set before the lease strategy is shut down, which happens outside the monitor
    private volatile boolean shutDown;
    // What the strategy knows of each consumer, as far as this model can tell. TRUE once a register has returned, FALSE
    // while a register or an unregister is under way or after one threw, and no entry once an unregister has returned.
    private final ConcurrentMap<SubscriptionIdAndSubscriberId, Boolean> registrations = new ConcurrentHashMap<>();
    // The try under way for each consumer a call failed for, or a lease callback handed over, which a thread of its own
    // brings to where it belongs. Added and removed under the monitor only, and removed on every path that ends the
    // thread, so the next failure for the consumer starts a new try.
    private final Map<SubscriptionIdAndSubscriberId, Try> tries = new HashMap<>();
    // Consumers recorded as running while they register, which the caller resumes once the register returns, read and
    // written under the consumer's lock. A grant during the register finds them recorded as running before the wrapped
    // model runs them.
    private final Set<SubscriptionIdAndSubscriberId> resumedOnceRegistered = ConcurrentHashMap.newKeySet();
    // Waited for after a call failed, and doubled after each failure that follows, up to the most. A lease callback
    // handed to a try is acted on without waiting, since nothing failed.
    private static final Duration RECONCILE_FIRST_BACKOFF = Duration.ofMillis(100);
    // How often a thread waiting for a subscription's lock checks whether this model is shut down
    private static final Duration SHUTDOWN_CHECK_INTERVAL = Duration.ofMillis(100);
    // How long a thread that took a subscription's lock ahead of a call that comes first waits before it looks again.
    // That is a thread the subscription was handed to ahead of a pause, resume or cancel, or a pause, resume or cancel
    // ahead of a try for a lease callback that came before it.
    private static final Duration LEFT_TO_A_CALL_WAITING = Duration.ofMillis(10);
    // What a try lets go first when no lease callback is handed to it, which is every pause, resume or cancel waiting
    private static final long NO_LEASE_CALLBACK = Long.MAX_VALUE;
    private static final Duration RECONCILE_MAX_BACKOFF = Duration.ofSeconds(2);
    private static final String WRAPPED_MODEL_START_THREAD = "occurrent-competing-consumer-wrapped-model-start";
    // A consumer that keeps failing warns on every fifth try, which is every ten seconds once the backoff has reached two
    private static final int RECONCILE_TRIES_BETWEEN_WARNINGS = 5;
    // A try makes at most two calls when nothing changes while it runs, and four when a stop() comes in between. More
    // means the lease, stop() and start() keep changing what it has to do, and the try then fails, so the next one
    // waits for the backoff.
    private static final int CALLS_PER_RECONCILE_TRY = 8;
    // How long shutdown() waits for the one attempt it makes to give up each lease. A lease it does not give up expires
    // on its own after the lease time.
    private static final Duration LEASE_RELEASE_TIMEOUT_ON_SHUTDOWN = Duration.ofSeconds(5);
    // One per subscription id while a thread holds or waits for it, always taken before the monitor. Every call for a
    // subscription holds it while it calls the lease strategy or the wrapped model, and none of them holds the monitor
    // then. A pause, resume or cancel of that subscription waits for it. A lease callback, start() and stop() take it
    // only when it is free, and hand the subscription over otherwise, so none of them waits for a call that waits for
    // the database. Whatever takes it first applies each start() and stop() handed over and not applied yet before its
    // own call. The entry goes once no thread holds or waits for it, so ids cancelled long ago keep no lock.
    private final ConcurrentMap<String, SubscriptionLock> subscriptionLocks = new ConcurrentHashMap<>();
    // Held for the whole of start(..) and stop(), so a second one waits for the first to return instead of taking over
    // from it part way, in the order they came. Taken before any other lock. The one exception is a start(..) waiting
    // behind a stop() that has calls under way in the wrapped model to wait for, which that stop() returns for, see
    // startsWaiting.
    private final ReentrantLock lifecycleLock = new ReentrantLock(true);
    // The start(..) calls waiting for lifecycleLock, read and written under wrappedModelStart only. A stop() with calls
    // under way in the wrapped model to wait for returns without stopping anything while one waits, since the start(..)
    // it lets in decides for every subscription this model knows. A pause, resume or cancel that began after that stop()
    // still stops its subscription first, see stopThatMayBeTakenBack. Anything else waiting for the lock waits for the
    // stop().
    private int startsWaiting;
    // Held while stop() records that this model is stopped, and while a call checks that it is not before it starts
    // the wrapped model or runs a subscription there, which it counts in runsInTheWrappedModel
    private final Object wrappedModelStart = new Object();
    // Calls under way that start the wrapped model or run a subscription there, admitted while this model was not
    // stopped, read and written under wrappedModelStart only. stop() waits for them before it stops the wrapped model,
    // so nothing they start is left running after it.
    private int runsInTheWrappedModel;
    private final ThreadLocal<int[]> runsInTheWrappedModelOnThisThread = ThreadLocal.withInitial(() -> new int[1]);
    // Per competing subscription id, what its action asks before it runs an event, see awaitTheLease
    private final ConcurrentMap<String, Delivery> deliveries = new ConcurrentHashMap<>();
    private final ThreadLocal<int[]> callsIntoTheWrappedModelOnThisThread = ThreadLocal.withInitial(() -> new int[1]);
    // How long an event waits before its action asks again whether it may run it, doubled up to the most
    private static final long DELIVERY_FIRST_WAIT_MILLIS = 10;
    private static final long DELIVERY_MAX_WAIT_MILLIS = 200;
    // The stop() that is waiting for those calls and stopping the wrapped model, or 0, read and written under
    // wrappedModelStart only. A call that would start the wrapped model or run a subscription there waits for it.
    private long stoppingTheWrappedModel;
    // The stop() in effect, or 0 while this model is started, read and written under wrappedModelStart only. A consumer
    // is let run while stopped only under the stop() it was decided under, so a later stop() that cleared
    // mayRunWhileStopped is not undone by a decision made before it.
    private long stopInEffect;
    // The last stop() that began, or 0, read and written under wrappedModelStart only. A start(..) applied after a later
    // stop() began runs nothing in the wrapped model, also once a start(..) after that stop() has begun.
    private long lastStopBegun;
    // The last stop() that let a call allowed while stopped into the wrapped model, read and written under
    // wrappedModelStart only. That call may have started the wrapped model, which only a later stop() stops again.
    private long stopThatLetACallRun;
    // Set when the wrapped model was not running as this model was built, until the first start(..) begins, and written
    // under the monitor only. A held event of a competing subscription warns that no start(..) made it compete.
    private volatile boolean notStartedSinceBuilt;
    // Set when the wrapped model was not running as this model was built, until a start(..) returns without throwing,
    // read and written under the monitor only. Until then a subscription is held paused because this model never ran,
    // or because a stop() paused what a start(..) that threw had begun, so a start(..) resumes it with either flag,
    // unless the user paused it.
    private boolean noStartReturnedSinceBuilt;
    // The subscriptions the user paused while noStartReturnedSinceBuilt was set and has not resumed since, added under
    // the monitor while it is set and removed under the subscription's lock. A start(false) keeps them paused, and a
    // start(true) that resumes one removes it. Read only by a start(..) that began while noStartReturnedSinceBuilt was
    // set, which a thread it was handed to can apply after it returned.
    private final Set<String> pausedByTheUserBeforeAStartReturned = ConcurrentHashMap.newKeySet();
    // The competing consumers recorded as running here when this model found the wrapped model not running, which
    // paused them there. Each is resumed there once this node holds its lease and the wrapped model runs, and is removed
    // from the set once it is resumed or found running there. One paused there while the wrapped model kept running stays
    // out of it, and runs again on a start(..), a grant of its lease or a resume of it.
    private final Set<SubscriptionIdAndSubscriberId> pausedByAStopOfTheWrappedModel = ConcurrentHashMap.newKeySet();
    // Set once a start(..) has thrown, until a later one returns without throwing, so isRunning() returns false and a
    // caller that starts this model only when it is not running tries again
    private volatile boolean lastStartThrew;
    // The start(..) applied on this thread, set while it is applied to one subscription
    private final ThreadLocal<@Nullable Lifecycle> lifecycleAppliedOnThisThread = new ThreadLocal<>();
    // True on a thread that a start(..) or stop() waits for, while that thread holds the lock of one subscription and
    // applies to it the calls handed over for it and then that start(..) or stop(). Such a thread hands to the try of
    // the subscription the resume of one the wrapped model holds paused, and of one stop() paused that a start(false)
    // resumes.
    private final ThreadLocal<Boolean> aLifecycleCallWaitsForThisThread = ThreadLocal.withInitial(() -> false);
    // The consumer a try is working on, set on the thread of that try. A lease callback out of the try's own call to
    // the strategy is left to the try, which decides again once that call returns.
    private final ThreadLocal<@Nullable SubscriptionIdAndSubscriberId> triedOnThisThread = new ThreadLocal<>();
    // Counts every start(..) and stop(), read and written under the monitor only
    private long lifecycleCalls;
    // Numbers each pause, resume or cancel that waits for the lock of its subscription, in the order they began, read
    // and written under the monitor only
    private long callsWaited;
    // The start(..) and stop() calls not applied yet to each subscription id, read and written under the monitor only.
    // A start(..) or stop() is handed over to each subscription it applies to in the block that begins it, so any call
    // that takes the lock of one of them after that applies it first. One whose own thread finds the lock taken gets a
    // thread of its own, which takes the lock and applies each one handed over, oldest first. An entry stays while a
    // start(..) or stop() is being applied, so its own thread finds itself applied already instead of applying itself
    // again or starting another thread.
    private final Map<String, HandedOver> handedOver = new HashMap<>();
    // Runs on such a thread once it has failed and let go of the lock, before it waits out its backoff. Exists so a
    // test can stand there, which nothing outside this model can.
    private volatile Runnable beforeAHandedOverThreadBacksOff = () -> {
    };
    // Runs on the thread of a try before it waits for its backoff, which a try that is to act at once does not. Exists
    // so a test can tell one from the other without measuring time.
    private volatile Runnable beforeATryWaitsForItsBackoff = () -> {
    };
    // Runs on the thread of a pause, resume or cancel once it counts as waiting for the lock of its subscription, before
    // it waits. Exists so a test can stand there, which nothing outside this model can.
    private volatile Runnable beforeACallWaitsForTheLock = () -> {
    };
    // Runs on the thread of a start(..) or stop() once it has begun, before it gets to any subscription. Exists so a
    // test can stand there, which nothing outside this model can.
    private volatile Runnable onceAStartOrStopHasBegun = () -> {
    };
    // Runs on the thread of a cancel once its subscription id is free for a new subscribe, before the cancel has
    // returned. Exists so a test can stand there, which nothing outside this model can.
    private volatile Runnable onceACancelHasFreedTheId = () -> {
    };
    // The start(..) or stop() being applied to every subscription, or 0, read and written under the monitor only
    private long lifecycleBeingApplied;
    // The stop() that a start(..) waiting behind it may still take back, or 0, read and written under the monitor only.
    // Only a pause, resume or cancel that began after it applies it to a subscription before its own thread does, since
    // that call decides where the subscription ends either way. Any other call leaves it to the thread of the stop(),
    // which applies it only once it is not taken back.
    private long stopThatMayBeTakenBack;
    // The latest start(..) whose start of the wrapped model a pause, resume or cancel gave up along with the rest of
    // what it handed over for one subscription, or null, read and written under the monitor only. While it is set a
    // thread of its own starts the wrapped model for it.
    private @Nullable Lifecycle wrappedModelStartGivenUp;

    public CompetingConsumerSubscriptionModel(SubscriptionModel subscriptionModel, CompetingConsumerStrategy strategy) {
        requireNonNull(subscriptionModel, "Subscription model cannot be null");
        requireNonNull(strategy, CompetingConsumerStrategy.class.getSimpleName() + " cannot be null");
        this.delegate = subscriptionModel;
        this.competingConsumerStrategy = strategy;
        // Built stopped, the same as a model with no subscription after stop(), so nothing runs until start(..). A
        // failing isRunning() is thrown from here. Built as started, the model would leave every subscription that does not
        // compete paused with nothing to resume it. Built as stopped, it would wait for a start(..) that a caller
        // reading isRunning() as the wrapped model's answer may never make.
        if (!subscriptionModel.isRunning()) {
            Lifecycle stop = beginLifecycle(false, false);
            stoppedByUser.set(true);
            stopInEffect = stop.id();
            lastStopBegun = stop.id();
            notStartedSinceBuilt = true;
            noStartReturnedSinceBuilt = true;
        }
        this.competingConsumerStrategy.addListener(this);
    }

    /**
     * Start listening to cloud events persisted to the event store using the supplied start position and <code>filter</code>.
     *
     * @param subscriberId   The unique if of the subscriber
     * @param subscriptionId The id of the subscription, must be unique in this subscription model instance! Other
     *                       instances are expected to use the same subscription id, since that is what makes them
     *                       compete for it.
     * @param filter         The filter to use to limit which events that are of interest from the EventStore.
     * @param startAt        The position to start the subscription from
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore.
     * @throws DuplicateSubscriptionIdException If this subscription model instance already has a subscription with this id.
     * @throws IllegalStateException            If this model is shut down, or is shut down or has the subscription
     *                                          cancelled while it is being made.
     *                                          <p>
     *                                          A failure of the wrapped model or the lease strategy before the wrapped
     *                                          model has made the subscription is thrown too. Nothing is then recorded,
     *                                          and the registration is given up. For a competing subscription, a failure
     *                                          once the wrapped model has made it is logged as a warning instead, the
     *                                          subscription is recorded, and {@code subscribe} returns. What failed is
     *                                          then tried again on a thread of its own, until the subscription is
     *                                          registered with the lease strategy while this model is started, and runs
     *                                          in the wrapped model only while this node holds its lease. A subscription
     *                                          that does not compete is not tried again. When resuming it for a
     *                                          {@code start(true)} that came while the wrapped model made it fails,
     *                                          {@code subscribe} throws that failure. The subscription is recorded by
     *                                          then, and unless that resume took effect before it threw, it stays paused
     *                                          in the wrapped model, where
     *                                          {@link #resumeSubscription(String)} or a later {@code start(true)}
     *                                          resumes it and {@link #cancelSubscription(String)} cancels it.
     */
    // Registers with the lease strategy and subscribes in the wrapped model holding neither the monitor nor the
    // subscription's lock, since a registration retries through a whole database outage and a wrapped subscribe can
    // take as long as opening a change stream. Every other step runs under the subscription's lock and decides from
    // what holds at that moment.
    public Subscription subscribe(String subscriberId, String subscriptionId, SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        Objects.requireNonNull(subscriberId, "SubscriberId cannot be null");
        Objects.requireNonNull(subscriptionId, "SubscriptionId cannot be null");
        BeingMade beingMade = reserveSubscriptionId(SubscriptionIdAndSubscriberId.from(subscriptionId, subscriberId));
        try {
            boolean competes = startAt.get(new SubscriptionModelContext(CompetingConsumerSubscriptionModel.class)) != null;
            if (!competes) {
                // Not allowed to start the competing consumer subscription, delegate to parent instead. One case: a
                // non-durable in-memory subscription started on multiple nodes, where every node should receive every
                // event, so competing consumption is not wanted.
                // Neither stop() nor shutdown() waits for this, since over a durable model it reads the stored position,
                // which retries for as long as the database cannot be reached. What a shutdown() that began meanwhile
                // makes of it is decided once it returns.
                beingMade.subscription = getWrappedSubscriptionModel().subscribe(subscriptionId, filter, startAt, action);
                recordNonCompetingSubscription(beingMade);
                return beingMade.subscription;
            }
            return makeCompetingConsumerSubscription(beingMade, filter, startAt, action);
        } finally {
            releaseSubscriptionId(beingMade);
        }
    }

    private synchronized BeingMade reserveSubscriptionId(SubscriptionIdAndSubscriberId key) {
        if (shutDown) {
            throw new IllegalStateException("Subscription " + key.subscriptionId() + " was not made, since " + CompetingConsumerSubscriptionModel.class.getSimpleName() + " is shut down");
        }
        if (isSubscriptionIdInUse(key.subscriptionId()) || subscriptionsBeingMade.containsKey(key.subscriptionId())) {
            throw new DuplicateSubscriptionIdException(key.subscriptionId());
        }
        BeingMade beingMade = new BeingMade(key);
        subscriptionsBeingMade.put(key.subscriptionId(), beingMade);
        return beingMade;
    }

    // A call that failed for the subscription while it was being made left the rest to be tried again from here
    private synchronized void releaseSubscriptionId(BeingMade beingMade) {
        subscriptionsBeingMade.remove(beingMade.key.subscriptionId());
        if (beingMade.triedAgainOnceMade) {
            reconcileLater(beingMade.key, beingMade.triedAgainAtOnce, beingMade.callsWaitedBeforeALeaseCallback);
        }
    }

    // Recorded only once the delegate has accepted it. Recording first would leave the id occupied by a subscription
    // that was refused, and the check in subscribe would then refuse it for good. A start(true) since the delegate got
    // it resumed only what it knew, so the subscription is resumed here.
    private void recordNonCompetingSubscription(BeingMade beingMade) {
        String subscriptionId = beingMade.key.subscriptionId();
        SubscriptionLock lock = lockSubscription(subscriptionId);
        try {
            boolean made;
            synchronized (this) {
                made = !shutDown && !beingMade.cancelled;
                if (made) {
                    nonCompetingConsumersSubscriptions.add(subscriptionId);
                }
            }
            if (!made) {
                throw notMade(beingMade);
            }
            // Recorded by now, so a failure is thrown with the subscription paused in the wrapped model, where a resume
            // or a cancel finds it
            try {
                if (beingMade.resumedMeanwhile && !stoppedByUser.get() && delegate.isPaused(subscriptionId)) {
                    runInTheWrappedModel(null, true, () -> delegate.resumeSubscription(subscriptionId));
                }
            } catch (StoppedMeanwhile e) {
                logDebug("Not resuming subscription, since this model was stopped meanwhile (subscriptionId={})", subscriptionId);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * @see SubscriptionModel#subscribe(String, SubscriptionFilter, StartAt, Consumer)
     */
    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, @Nullable StartAt startAt, Consumer<CloudEvent> action) {
        return subscribe(UUID.randomUUID().toString(), subscriptionId, filter, startAt, action);
    }

    /**
     * @see SubscriptionModelLifeCycle#cancelSubscription(String)
     */
    @Override
    public void cancelSubscription(String subscriptionId) {
        actOn(subscriptionId, () -> {
            cancel(subscriptionId);
            return null;
        });
    }

    private void cancel(String subscriptionId) {
        logDebug("Cancelling CompetingConsumer subscription (subscriptionId={})", subscriptionId);
        // A subscribe making this id right now takes back what it made and throws once it finds this
        @Nullable BeingMade beingMade;
        synchronized (this) {
            beingMade = subscriptionsBeingMade.get(subscriptionId);
            if (beingMade != null) {
                beingMade.cancelled = true;
            }
        }
        try {
            cancelInTheWrappedModel(subscriptionId);
        } catch (Throwable e) {
            // The cancel can take effect before it throws. What the delegate still holds stays recorded as what it does
            // there, so the cancel can be asked for again, and anything it no longer holds is forgotten here too.
            if (heldByTheWrappedModel(subscriptionId, e)) {
                findFirstCompetingConsumerMatching(cc -> cc.hasSubscriptionId(subscriptionId)).ifPresent(cc -> {
                    recordAsPausedUnlessItRuns(cc.subscriptionIdAndSubscriberId, true, e);
                    reconcileLater(cc.subscriptionIdAndSubscriberId);
                });
            } else {
                try {
                    forgetCancelled(subscriptionId, beingMade);
                } catch (RuntimeException forgetFailure) {
                    e.addSuppressed(forgetFailure);
                }
            }
            throw e;
        }
        forgetCancelled(subscriptionId, beingMade);
    }

    private boolean heldByTheWrappedModel(String subscriptionId, Throwable failure) {
        try {
            return isRunningInTheWrappedModel(subscriptionId) || isPausedInTheWrappedModel(subscriptionId);
        } catch (RuntimeException e) {
            failure.addSuppressed(e);
            return true;
        }
    }

    private void forgetCancelled(String subscriptionId, @Nullable BeingMade beingMade) {
        @Nullable Delivery beingMadeDelivery = beingMade == null ? null : beingMade.delivery;
        if (beingMadeDelivery != null) {
            // That subscribe forgets it too, but only once it next takes the lock, which can be after a long wrapped
            // call. Its own rather than whatever is under the id, since a subscribe that does not compete, or whose
            // startAt throws, has none and frees the id without the lock, so another subscribe of the id can put one
            // meanwhile.
            deliveries.remove(subscriptionId, beingMadeDelivery);
            beingMadeDelivery.forget();
        }
        // Forgotten here too, not only in the competing consumer map, so the id is free for a new subscription
        // afterwards. Remembering a cancelled one also made start() resume a subscription the delegate no longer has.
        nonCompetingConsumersSubscriptions.remove(subscriptionId);
        pausedByTheUserBeforeAStartReturned.remove(subscriptionId);
        pausedByAStopOfTheWrappedModel.removeIf(key -> key.subscriptionId().equals(subscriptionId));
        findFirstCompetingConsumerMatching(cc -> cc.hasSubscriptionId(subscriptionId))
                .ifPresent(cc -> unregisterCompetingConsumer(cc, __ -> {
                    // First, while the id is in use, since a subscribe of the same id puts its own delivery once it
                    // is free and that one must not be forgotten
                    forgetTheDeliveriesOf(subscriptionId);
                    competingConsumers.remove(cc.subscriptionIdAndSubscriberId);
                    onceACancelHasFreedTheId.run();
                    mayRunWhileStopped.remove(cc.subscriptionIdAndSubscriberId);
                }));
    }

    /**
     * Waits for a {@code start(..)} under way to return first, also while it waits for the wrapped model to start. It
     * does not wait for a resume of a competing subscription that a {@code start(..)} handed to a try, also when that
     * resume blocks in the wrapped model.
     *
     * @see SubscriptionModelLifeCycle#stop()
     */
    @Override
    public void stop() {
        lifecycleLock.lock();
        try {
            stopHoldingTheLifecycleLock();
        } finally {
            lifecycleLock.unlock();
        }
    }

    private void stopHoldingTheLifecycleLock() {
        Lifecycle stop;
        Set<String> subscriptionIds;
        synchronized (this) {
            logDebug("Stopping CompetingConsumer subscription model");
            stop = beginLifecycle(false, false);
            subscriptionsBeingMade.values().forEach(beingMade -> beingMade.resumedMeanwhile = false);
            // Whether the wrapped model runs says nothing about whether this model has anything left to stop. After a
            // start() that failed part way, or a stop() called on the wrapped model itself, the wrapped model can be
            // stopped while consumers are registered, and a grant would resume one after a stop() that had returned
            // without doing anything.
            synchronized (wrappedModelStart) {
                stoppedByUser.set(true);
                mayRunWhileStopped.clear();
                stopInEffect = stop.id();
                lastStopBegun = stop.id();
                stoppingTheWrappedModel = stop.id();
                wrappedModelStart.notifyAll();
            }
            subscriptionIds = subscriptionIdsTakenBy(stop);
            handOverToEach(stop, subscriptionIds);
            stopThatMayBeTakenBack = stop.id();
        }
        boolean takenBack = false;
        try {
            onceAStartOrStopHasBegun.run();
            takenBack = !stopEverySubscription(stop, subscriptionIds);
        } finally {
            doneWith(stop, takenBack);
        }
    }

    // False when a start(..) waiting behind this stop() decides for every subscription instead, and this thread applied
    // the stop() to none of them
    private boolean stopEverySubscription(Lifecycle stop, Set<String> subscriptionIds) {
        try {
            if (!stopTheWrappedModelOnceNothingRunsThere()) {
                logDebug("A start(..) came while stop() waited for calls under way in the wrapped model, and decides instead");
                return false;
            }
        } catch (RuntimeException e) {
            notTakenBack();
            // The consumers are stopped also when stopping the wrapped model throws, or this node would keep leases
            // for subscriptions it no longer means to serve
            List<String> paused = new ArrayList<>();
            RuntimeException consumerFailure = applyToEverySubscription(stop, subscriptionIds, paused, null);
            String outcome = paused.isEmpty()
                    ? "it had no running subscription to pause."
                    : "subscriptions " + paused + " are paused in the wrapped model and gave up their lease. start(true) resumes them, while start(false) keeps them paused until each one is resumed.";
            IllegalStateException failure = new IllegalStateException("Stopping the wrapped subscription model failed. This model is stopped anyway, and " + outcome, e);
            if (consumerFailure != null) {
                failure.addSuppressed(consumerFailure);
            }
            throw failure;
        } catch (Throwable e) {
            notTakenBack();
            RuntimeException consumerFailure = applyToEverySubscription(stop, subscriptionIds, new ArrayList<>(), null);
            if (consumerFailure != null) {
                e.addSuppressed(consumerFailure);
            }
            throw e;
        }
        notTakenBack();
        RuntimeException consumerFailure = applyToEverySubscription(stop, subscriptionIds, new ArrayList<>(), null);
        if (consumerFailure != null) {
            throw consumerFailure;
        }
        return true;
    }

    // From here on any call that takes the lock of a subscription applies the stop() first
    private synchronized void notTakenBack() {
        stopThatMayBeTakenBack = 0;
    }

    // Called under the monitor
    private Lifecycle beginLifecycle(boolean started, boolean resumeSubscriptionsAutomatically) {
        boolean resumesWhatIsPaused = started && (resumeSubscriptionsAutomatically || noStartReturnedSinceBuilt);
        if (started) {
            notStartedSinceBuilt = false;
        }
        return new Lifecycle(++lifecycleCalls, started, resumeSubscriptionsAutomatically, resumesWhatIsPaused);
    }

    // Called under the subscription's lock. Whether the start(..) resumes the subscription when it is paused.
    private boolean resumes(Lifecycle start, String subscriptionId) {
        return start.resumeSubscriptionsAutomatically()
                || start.resumesWhatIsPaused() && !pausedByTheUserBeforeAStartReturned.contains(subscriptionId);
    }

    /**
     * Hands a {@code start(..)} or {@code stop()} over to each subscription it applies to, and to each one handed
     * something over earlier, in the block under the monitor that begins it. A call that takes the lock of one of them
     * from then on applies it first, unless a pause, resume or cancel that began before it is waiting for the lock. So
     * a pause, resume or cancel, a grant or a try that takes the lock before the {@code start(..)} or {@code stop()} has
     * got to that subscription still comes after it. The exception is a {@code stop()} that a {@code start(..)} waiting
     * behind it may still take back, which only a pause, resume or cancel that began after it applies first. A grant, a
     * try or the thread a subscription was handed to leaves it to the thread of that {@code stop()}, which applies it
     * once it is not taken back, see {@link #stopThatMayBeTakenBack}.
     */
    private void handOverToEach(Lifecycle applied, Set<String> subscriptionIds) {
        lifecycleBeingApplied = applied.id();
        handedOver.values().forEach(handed -> handed.notApplied.addLast(applied));
        for (String subscriptionId : subscriptionIds) {
            if (!handedOver.containsKey(subscriptionId)) {
                HandedOver handed = new HandedOver();
                handed.notApplied.addLast(applied);
                handedOver.put(subscriptionId, handed);
            }
        }
    }

    /**
     * Ends handing over a {@code start(..)} or {@code stop()} once its own threads are done, and forgets each
     * subscription no thread of its own applies anything for. With {@code takenBack}, a {@code start(..)} waiting behind
     * a {@code stop()} decides for every subscription instead, and that {@code stop()} is taken back from each
     * subscription it has not been applied to yet.
     */
    private synchronized void doneWith(Lifecycle applied, boolean takenBack) {
        lifecycleBeingApplied = 0;
        stopThatMayBeTakenBack = 0;
        if (takenBack) {
            handedOver.values().forEach(handed -> handed.notApplied.remove(applied));
        }
        handedOver.values().removeIf(handed -> !handed.threadApplies);
    }

    // A start(..) or a stop(), numbered in the order they began
    private record Lifecycle(long id, boolean started, boolean resumeSubscriptionsAutomatically, boolean resumesWhatIsPaused) {
    }

    /**
     * The subscriptions a {@code start(..)} or {@code stop()} applies to, taken under the monitor in the same block
     * that makes it visible. A subscribe records its consumer before it releases its id, which it does under the
     * monitor, so each id this model knows is here, recorded or still being made. A subscribe that reserves its id
     * after this block reads the new {@code start(..)} or {@code stop()} at its next step, which it takes under the
     * subscription's lock, and decides from it.
     */
    private Set<String> subscriptionIdsTakenBy(Lifecycle applied) {
        Set<String> subscriptionIds = new LinkedHashSet<>();
        if (applied.started()) {
            subscriptionIds.addAll(nonCompetingConsumersSubscriptions);
        }
        competingConsumers.keySet().forEach(key -> subscriptionIds.add(key.subscriptionId()));
        subscriptionIds.addAll(subscriptionsBeingMade.keySet());
        return subscriptionIds;
    }

    /**
     * Stops the wrapped model once every call that started it, or ran a subscription there, before this {@code stop()}
     * began has returned, so none of them starts it or runs a subscription there after this {@code stop()} has
     * returned. A call that would do so since waits for the wrapped model to be stopped, and is then refused, unless it
     * resumes a consumer the user resumed after this {@code stop()} began. A {@code stop()} that begins meanwhile waits
     * for this {@code stop()} to return. While a {@code start(..)} is waiting behind this {@code stop()}, also one that
     * was waiting before this got to those calls, this does not wait for them and returns false without stopping
     * anything, since the {@code start(..)} decides for every subscription this model knows once this {@code stop()}
     * has returned.
     * <p>
     * A call for any subscription that waits inside the wrapped model holds this up for as long as it waits. Stopping
     * the wrapped model before it returns would let a late resume start that model again after this {@code stop()}
     * has returned.
     */
    private boolean stopTheWrappedModelOnceNothingRunsThere() {
        int runsOnThisThread = runsInTheWrappedModelOnThisThread.get()[0];
        boolean interrupted = false;
        try {
            synchronized (wrappedModelStart) {
                while (runsInTheWrappedModel > runsOnThisThread && startsWaiting == 0) {
                    try {
                        wrappedModelStart.wait();
                    } catch (InterruptedException e) {
                        // Keeps waiting, since stopping the wrapped model now would let a call still under way run a
                        // subscription after this stop() returned. The interrupt is restored before returning.
                        interrupted = true;
                    }
                }
                if (runsInTheWrappedModel > runsOnThisThread) {
                    return false;
                }
            }
            if (wrappedModelIsRunning()) {
                stopTheWrappedModel();
            }
            return true;
        } finally {
            synchronized (wrappedModelStart) {
                stoppingTheWrappedModel = 0;
                wrappedModelStart.notifyAll();
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Applies a {@code start(..)} or {@code stop()} to each of the given subscriptions, each on a thread of its own and
     * under its own lock, all at once, so a call that waits for the lease strategy or the wrapped model for one
     * subscription holds up no other. Returns once each subscription whose lock was free has been taken care of, with
     * the first failure and any later one attached as suppressed. A subscription whose lock another call holds, which
     * can be a try or a call waiting for the database through an outage, is handed over instead, see
     * {@link #handOver(String, Lifecycle)}, and neither waited for nor reported here.
     * <p>
     * For a {@code stop()}, each running consumer is paused and each consumer unregistered, so none of them competes for
     * the lock. A consumer left registered keeps competing through the strategy's refresh thread, so a stopped model can
     * take a lock it then refuses to act on, and start() sees no status change and never starts it. A waiting one stays
     * waiting, so start() registers it again. A running consumer that the wrapped model still runs is paused there
     * first, since a wrapped model that threw from stop(), or one that keeps its subscriptions running across a stop,
     * still delivers them. One that cannot be paused there keeps its lease and stays recorded as running, since it still
     * delivers, and the failure is returned. The id of each consumer this pauses is added to {@code paused}. Any other
     * call for a consumer that throws, asking the wrapped model whether it runs the subscription included, is returned
     * the same way, and the consumer is tried again on a thread of its own. A consumer recorded as running is recorded
     * as paused then, unless the wrapped model says it still runs it. A consumer whose resume by the user began
     * after this {@code stop()} did is left to run, as one resumed after {@code stop()} has returned is. A subscribe on another
     * thread whose registration has returned gives it up too, unless the wrapped model already runs its subscription or
     * cannot say whether it does. That one, and one whose registration is under way, try to give it up at their next
     * step, and an unregister that throws there has the consumer tried again on a thread of its own.
     * <p>
     * The threads are not pooled, since a pool would queue the subscriptions behind one whose call waits for the
     * database, which is what taking them one at a time did. They are joined before this returns. The wrapped MongoDB
     * models already ask for a thread for each subscription they run, so this adds as many threads again, for as long
     * as the {@code start(..)} or {@code stop()} runs.
     */
    private @Nullable RuntimeException applyToEverySubscription(Lifecycle applied, Set<String> subscriptionIds, List<String> paused, @Nullable RuntimeException firstFailure) {
        List<String> pausedMeanwhile = Collections.synchronizedList(new ArrayList<>());
        List<CompletableFuture<@Nullable RuntimeException>> applications = new ArrayList<>();
        for (String subscriptionId : subscriptionIds) {
            CompletableFuture<@Nullable RuntimeException> application = new CompletableFuture<>();
            Thread.ofPlatform().daemon().name("occurrent-competing-consumer-lifecycle-" + subscriptionId)
                    .start(() -> applyIfTheLockIsFree(subscriptionId, applied, pausedMeanwhile, application));
            applications.add(application);
        }
        @Nullable Error error = null;
        for (CompletableFuture<@Nullable RuntimeException> application : applications) {
            try {
                RuntimeException failure = application.join();
                if (failure != null) {
                    firstFailure = withSuppressed(firstFailure, failure);
                }
            } catch (CompletionException e) {
                if (e.getCause() instanceof RuntimeException failure) {
                    firstFailure = withSuppressed(firstFailure, failure);
                } else if (e.getCause() instanceof Error failure && error == null) {
                    error = failure;
                }
            }
        }
        paused.addAll(pausedMeanwhile);
        if (error != null) {
            throw error;
        }
        return firstFailure;
    }

    private void applyIfTheLockIsFree(String subscriptionId, Lifecycle applied, List<String> paused, CompletableFuture<@Nullable RuntimeException> application) {
        try {
            @Nullable SubscriptionLock lock = tryLockSubscription(subscriptionId);
            if (lock != null && comesAfterACallWaiting(lock, applied)) {
                lock.unlock();
                lock = null;
            }
            if (lock == null) {
                // Handed over before completing, so the next start(..) or stop() finds this call still to be applied,
                // and is applied after it
                handOver(subscriptionId, applied);
                application.complete(failedWhenAppliedFirst(subscriptionId, applied));
                return;
            }
            @Nullable RuntimeException failure;
            aLifecycleCallWaitsForThisThread.set(true);
            try {
                failure = applyInTurn(subscriptionId, applied, paused);
            } finally {
                aLifecycleCallWaitsForThisThread.remove();
                lock.unlock();
            }
            // Completed only once the lock is free, so a call made right after start(..) or stop() returns finds it free
            application.complete(failure);
        } catch (Throwable e) {
            application.completeExceptionally(e);
        }
    }

    // Whether a pause, resume or cancel waiting for the lock began before the call, which then waits for it
    private synchronized boolean comesAfterACallWaiting(SubscriptionLock lock, Lifecycle applied) {
        return applied.id() > firstCallWaiting(lock);
    }

    // Called under the subscription's lock. First applies each start(..) and stop() before the given one that is not
    // applied to the subscription yet, oldest first, and does nothing when a call that took the lock first has applied
    // the given one already. One of those that fails for a subscription no try covers is left to the thread it was
    // handed to, together with the given call, and neither is reported here, since that thread tries it again until it
    // is applied.
    private @Nullable RuntimeException applyInTurn(String subscriptionId, Lifecycle applied, List<String> paused) {
        @Nullable FailedWhenAppliedFirst failedFirst;
        while (true) {
            @Nullable HandedOver handed;
            @Nullable Lifecycle next;
            synchronized (this) {
                handed = handedOver.get(subscriptionId);
                if (handed == null) {
                    next = applied;
                } else if (handed.appliedThrough >= applied.id()) {
                    failedFirst = handed.failures.remove(applied.id());
                    break;
                } else {
                    next = handed.notApplied.peekFirst();
                }
            }
            if (next == null) {
                return null;
            }
            if (next.id() == applied.id()) {
                @Nullable RuntimeException failure = applyLifecycle(subscriptionId, applied, paused, new ArrayList<>(), null);
                if (handed != null) {
                    synchronized (this) {
                        handed.applied(applied);
                    }
                }
                return failure;
            }
            try {
                applyHandedOver(subscriptionId, handed, next);
            } catch (RuntimeException e) {
                logDebug("A start(..) or stop() handed over for subscription {} failed, so the thread it was handed to tries it again, and applies this call after it", subscriptionId);
                return null;
            }
        }
        // Outside the monitor, since it can ask the wrapped model
        @Nullable RuntimeException failure = asMetByItsOwnThread(failedFirst);
        if (failure == null && failedFirst != null && failedFirst.beforeTheWrappedModelWasStopped() != null) {
            // The call that applied this stop() first failed to pause the consumer in the wrapped model, and so left it
            // registered. This thread holds the lock, so it stops the consumer as it stops any other.
            return applyLifecycle(subscriptionId, applied, paused, new ArrayList<>(), null);
        }
        return failure;
    }

    /**
     * Applies each {@code start(..)} and {@code stop()} handed over for a subscription, oldest first, once the call
     * holding its lock has returned, on a thread of its own, one per subscription id at a time. Any other call that
     * takes the lock before this thread does applies them before its own, so this thread may find none left. A
     * competing consumer that this fails for is tried again by its try, as with any other call. For any other
     * subscription this thread tries the same call again itself, with the backoff a try uses, until it has been applied
     * or this model is shut down. It is the only thread that tries it again, also when a later call found it still to
     * be applied and failed to apply it.
     */
    private void handOver(String subscriptionId, Lifecycle applied) {
        @Nullable HandedOver handed;
        synchronized (this) {
            handed = handedOver.get(subscriptionId);
            // A thread already there applies this start(..) or stop(), or a call that took the lock has applied it
            if (shutDown || (handed != null && (handed.threadApplies || handed.appliedThrough >= applied.id()))) {
                return;
            }
            if (handed == null) {
                // Gone already, as once the thread that was there is interrupted
                handed = new HandedOver();
                handed.notApplied.addLast(applied);
                handedOver.put(subscriptionId, handed);
            }
            handed.threadApplies = true;
        }
        try {
            startApplyingWhatWasHandedOver(subscriptionId, handed);
        } catch (Throwable e) {
            // No thread applies it then, so the next start(..) or stop() that finds the lock taken starts one anew
            synchronized (this) {
                handed.threadApplies = false;
            }
            throw e;
        }
    }

    private void startApplyingWhatWasHandedOver(String subscriptionId, HandedOver handed) {
        Thread.ofPlatform().daemon().name("occurrent-competing-consumer-lifecycle-" + subscriptionId).start(() -> {
            Duration backoff = RECONCILE_FIRST_BACKOFF;
            int failures = 0;
            while (true) {
                try {
                    if (applyEverythingHandedOver(subscriptionId, handed)) {
                        return;
                    }
                    // Nothing failed, so this thread waits only for that call to take the lock
                    Thread.sleep(LEFT_TO_A_CALL_WAITING);
                    continue;
                } catch (InterruptedException e) {
                    synchronized (this) {
                        handedOver.remove(subscriptionId, handed);
                    }
                    return;
                } catch (Throwable e) {
                    if (failures++ % RECONCILE_TRIES_BETWEEN_WARNINGS == 0) {
                        log.warn("Could not start or stop subscription {} once the call that held it had returned, so it is tried again", subscriptionId, e);
                    }
                }
                beforeAHandedOverThreadBacksOff.run();
                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException e) {
                    synchronized (this) {
                        handedOver.remove(subscriptionId, handed);
                    }
                    return;
                }
                Duration doubled = backoff.multipliedBy(2);
                backoff = doubled.compareTo(RECONCILE_MAX_BACKOFF) > 0 ? RECONCILE_MAX_BACKOFF : doubled;
            }
        });
    }

    // Package-private for the test that stands where the field describes. Not public, and not part of this model's
    // contract.
    void runBeforeAHandedOverThreadBacksOff(Runnable hook) {
        this.beforeAHandedOverThreadBacksOff = requireNonNull(hook, "hook cannot be null");
    }

    // Package-private for the test that stands where the field describes. Not public, and not part of this model's
    // contract.
    void runBeforeATryWaitsForItsBackoff(Runnable hook) {
        this.beforeATryWaitsForItsBackoff = requireNonNull(hook, "hook cannot be null");
    }

    // Package-private for the test that stands where the field describes. Not public, and not part of this model's
    // contract.
    void runBeforeACallWaitsForTheLock(Runnable hook) {
        this.beforeACallWaitsForTheLock = requireNonNull(hook, "hook cannot be null");
    }

    // Package-private for the test that stands where the field describes. Not public, and not part of this model's
    // contract.
    void runOnceAStartOrStopHasBegun(Runnable hook) {
        this.onceAStartOrStopHasBegun = requireNonNull(hook, "hook cannot be null");
    }

    // Package-private for the test that stands where the field describes. Not public, and not part of this model's
    // contract.
    void runOnceACancelHasFreedTheId(Runnable hook) {
        this.onceACancelHasFreedTheId = requireNonNull(hook, "hook cannot be null");
    }

    // Applies every start(..) and stop() handed over for the subscription, oldest first, under one hold of its lock, so
    // no other call for it comes in between. That includes one handed over while this runs. Returns true once none is
    // left, or this model is shut down, and false when the next one began after a pause, resume or cancel waiting for
    // the lock, which comes first. Throws what failed for a subscription no try covers, and keeps that call and every
    // one after it to be applied again.
    private boolean applyEverythingHandedOver(String subscriptionId, HandedOver handed) throws InterruptedException {
        @Nullable SubscriptionLock lock = lockSubscriptionUnlessShutDown(subscriptionId);
        if (lock == null) {
            synchronized (this) {
                handedOver.remove(subscriptionId, handed);
            }
            return true;
        }
        try {
            while (true) {
                @Nullable Lifecycle next;
                synchronized (this) {
                    if (shutDown) {
                        handedOver.remove(subscriptionId, handed);
                        return true;
                    }
                    next = handed.notApplied.peekFirst();
                    if (next != null && mayBeTakenBack(next)) {
                        // Left to the thread of that stop(), or taken back
                        next = null;
                    }
                    if (next == null) {
                        if (lifecycleBeingApplied == 0) {
                            handedOver.remove(subscriptionId, handed);
                        } else {
                            handed.threadApplies = false;
                        }
                        return true;
                    }
                    if (next.id() > firstCallWaiting(lock)) {
                        return false;
                    }
                }
                applyHandedOver(subscriptionId, handed, next);
            }
        } finally {
            lock.unlock();
        }
    }

    // Applies one start(..) or stop() handed over for the subscription, under its lock, and records it as applied.
    // What fails for a competing consumer is left to its try, as with any other call, and recorded, so the thread of
    // that start(..) or stop() throws it when it would have met it applying the call itself, see asMetByItsOwnThread.
    // What fails for any other subscription is thrown instead, and the call is not recorded as applied, so the thread it
    // was handed to tries it again.
    private void applyHandedOver(String subscriptionId, HandedOver handed, Lifecycle next) {
        @Nullable CompetingConsumer cc = findFirstCompetingConsumerMatching(c -> c.hasSubscriptionId(subscriptionId)).orElse(null);
        List<RuntimeException> failedToPause = new ArrayList<>();
        RuntimeException failure = applyLifecycle(subscriptionId, next, new ArrayList<>(), failedToPause, null);
        if (failure != null && cc == null && nonCompetingConsumersSubscriptions.contains(subscriptionId)) {
            throw failure;
        }
        if (failure != null && cc != null) {
            // Mostly handed to the try already, which this only asks to decide once more
            reconcileLater(cc.subscriptionIdAndSubscriberId, false);
        }
        // Read once pausing the consumer in the wrapped model has failed, so the stop() had not stopped the wrapped model
        // before that call returned. The thread of the stop() unregisters the consumer whatever the wrapped model runs,
        // so a failure to unregister it is thrown as it is.
        @Nullable SubscriptionIdAndSubscriberId beforeTheWrappedModelWasStopped =
                failure != null && cc != null && !failedToPause.isEmpty() && stillStoppingTheWrappedModel(next) ? cc.subscriptionIdAndSubscriberId : null;
        synchronized (this) {
            handed.applied(next);
            if (failure != null) {
                handed.failures.put(next.id(), new FailedWhenAppliedFirst(failure, beforeTheWrappedModelWasStopped));
            }
        }
    }

    // Whether the given stop() is still waiting for calls under way in the wrapped model, or stopping that model
    private boolean stillStoppingTheWrappedModel(Lifecycle stop) {
        synchronized (wrappedModelStart) {
            return stoppingTheWrappedModel == stop.id();
        }
    }

    // What failed when another call applied the start(..) or stop() to the subscription before its own thread got there,
    // or null, see asMetByItsOwnThread. Read once, by that thread.
    private @Nullable RuntimeException failedWhenAppliedFirst(String subscriptionId, Lifecycle applied) {
        @Nullable FailedWhenAppliedFirst failedFirst;
        synchronized (this) {
            @Nullable HandedOver handed = handedOver.get(subscriptionId);
            failedFirst = handed == null ? null : handed.failures.remove(applied.id());
        }
        return asMetByItsOwnThread(failedFirst);
    }

    /**
     * The failure another call met applying a {@code start(..)} or {@code stop()}, for the thread of that call to throw,
     * unless that thread would not have met it. A pause, resume or cancel that began after a {@code stop()} can apply
     * it before that {@code stop()} is done stopping the wrapped model. The thread of the {@code stop()} stops each
     * consumer only after that, and when it gets to one, it pauses it in the wrapped model only when this model records
     * it as running, the wrapped model runs it, and no resume that began after the {@code stop()} has let it run while
     * this model is stopped. A failure to pause a consumer in the wrapped model, met before then, leaves the consumer
     * registered. It is thrown when, once the thread of the {@code stop()} gets here, the wrapped model runs the consumer
     * or cannot say whether it does, and no such resume has let it run. Otherwise it is logged, and the try of the
     * consumer has it already, as with any other failed call. Unless such a resume has let the consumer run, a thread
     * of the {@code stop()} that holds the consumer's lock then stops the consumer as it stops any other, see
     * applyInTurn. One that finds the lock held by another call does not. That call, or at the latest the try of the
     * consumer once it gets the lock, then tries to unregister the consumer, again unless such a resume has let it run.
     * When the unregister of the try fails, the try releases the lease only if the lease strategy still reports it
     * held, see unregisterOrAtLeastGiveUpTheLease.
     * Any other failure is thrown as it is. Called outside the monitor.
     */
    private @Nullable RuntimeException asMetByItsOwnThread(@Nullable FailedWhenAppliedFirst failedFirst) {
        if (failedFirst == null) {
            return null;
        }
        @Nullable SubscriptionIdAndSubscriberId stoppedEarly = failedFirst.beforeTheWrappedModelWasStopped();
        // Asked in this order, since a resume lets a consumer run while stopped before it runs it in the wrapped model
        if (stoppedEarly == null || runsInTheWrappedModel(stoppedEarly, failedFirst.failure()) && !mayRunWhileStopped.contains(stoppedEarly)) {
            return failedFirst.failure();
        }
        log.warn("A pause, resume or cancel failed to pause subscription {} in the wrapped model for a stop() that had not stopped the wrapped model yet. The wrapped model no longer runs it, or a resume that began after the stop() has let it run, so stop() does not throw the failure",
                stoppedEarly.subscriptionId(), failedFirst.failure());
        return null;
    }

    // What failed for a competing consumer when another call than its own thread applied a start(..) or stop(). The
    // consumer is set when a stop() failed to pause it in the wrapped model before it had stopped the wrapped model.
    private record FailedWhenAppliedFirst(RuntimeException failure, @Nullable SubscriptionIdAndSubscriberId beforeTheWrappedModelWasStopped) {
    }

    // The start(..) and stop() calls handed over for one subscription id, read and written under the monitor only
    private static final class HandedOver {
        // Oldest first
        private final ArrayDeque<Lifecycle> notApplied = new ArrayDeque<>();
        // The id of the last one applied, or 0
        private long appliedThrough;
        // A thread of its own applies them
        private boolean threadApplies;
        // What failed for each one applied by another call than its own thread, until that thread has read it
        private final Map<Long, FailedWhenAppliedFirst> failures = new HashMap<>();

        private void applied(Lifecycle lifecycle) {
            notApplied.remove(lifecycle);
            appliedThrough = lifecycle.id();
        }
    }

    // Applies a start(..) or stop() to one subscription, under that subscription's lock. A stop() that fails to pause the
    // consumer in the wrapped model adds that failure to failedToPause too.
    private @Nullable RuntimeException applyLifecycle(String subscriptionId, Lifecycle applied, List<String> paused, List<RuntimeException> failedToPause, @Nullable RuntimeException firstFailure) {
        @Nullable CompetingConsumer cc = findFirstCompetingConsumerMatching(c -> c.hasSubscriptionId(subscriptionId)).orElse(null);
        if (applied.started()) {
            lifecycleAppliedOnThisThread.set(applied);
            try {
                if (nonCompetingConsumersSubscriptions.contains(subscriptionId)) {
                    // Started for each such subscription too, under its lock. The subscription still gets its turn when the
                    // start fails.
                    try {
                        runInTheWrappedModel(null, true, () -> null);
                    } catch (StoppedMeanwhile e) {
                        logDebug("Not starting the wrapped model, since this model was stopped meanwhile (subscriptionId={})", subscriptionId);
                        return firstFailure;
                    } catch (RuntimeException e) {
                        firstFailure = withSuppressed(firstFailure, e);
                    }
                    if (!resumes(applied, subscriptionId)) {
                        // Kept paused, whether the user or stop() paused it, as a competing one is
                        return firstFailure;
                    }
                    try {
                        // Only the paused ones are resumed. Starting a model that is already started arrives here too,
                        // and the delegate refuses to resume a subscription that is already running.
                        runInTheWrappedModel(null, false, () -> {
                            if (isPausedInTheWrappedModel(subscriptionId)) {
                                callingTheWrappedModel(() -> delegate.resumeSubscription(subscriptionId));
                            }
                            return null;
                        });
                        if (applied.resumeSubscriptionsAutomatically()) {
                            pausedByTheUserBeforeAStartReturned.remove(subscriptionId);
                        }
                    } catch (StoppedMeanwhile e) {
                        logDebug("Not resuming subscription, since this model was stopped meanwhile (subscriptionId={})", subscriptionId);
                    } catch (RuntimeException e) {
                        return withSuppressed(firstFailure, e);
                    }
                } else if (cc != null) {
                    startConsumer(cc, applied);
                }
            } finally {
                lifecycleAppliedOnThisThread.remove();
            }
            return firstFailure;
        }
        if (cc != null && !mayRunWhileStopped.contains(cc.subscriptionIdAndSubscriberId)) {
            try {
                stopConsumer(cc, paused, failedToPause, applied);
            } catch (RuntimeException e) {
                recordAsPausedUnlessItRuns(cc.subscriptionIdAndSubscriberId, true, e);
                reconcileLater(cc.subscriptionIdAndSubscriberId);
                firstFailure = withSuppressed(firstFailure, e);
            } catch (Error e) {
                recordAsPausedUnlessItRuns(cc.subscriptionIdAndSubscriberId, true, e);
                reconcileLater(cc.subscriptionIdAndSubscriberId);
                throw e;
            }
        }
        @Nullable BeingMade beingMade;
        synchronized (this) {
            beingMade = subscriptionsBeingMade.get(subscriptionId);
        }
        // A subscribe on another thread gives up its registration at its next step too, but that step can take as long
        // as the wrapped model takes to make the subscription. One the wrapped model already runs, or cannot say
        // whether it does, is left to that step, which pauses it first and keeps the lease when it cannot.
        if (beingMade != null) {
            try {
                if (beingMade.registrationReturned && !isRunningInTheWrappedModel(subscriptionId)) {
                    // Forgotten too, so its next step registers again once this model has been started
                    giveUpTheRegistration(beingMade);
                }
            } catch (RuntimeException e) {
                firstFailure = withSuppressed(firstFailure, e);
            }
        }
        return firstFailure;
    }

    // A consumer that a call coming before this stop() meant to run, and did not, because this stop() refused it or the
    // call failed, is paused as by the user too, as it would have been had that call run it and this stop() paused it
    // after
    private void stopConsumer(CompetingConsumer cc, List<String> paused, List<RuntimeException> failedToPause, Lifecycle stop) {
        String subscriptionId = cc.getSubscriptionId();
        try {
            if (cc.isRunning() && isRunningInTheWrappedModel(subscriptionId)) {
                pauseInTheWrappedModel(subscriptionId);
                if (isRunningInTheWrappedModel(subscriptionId)) {
                    throw new IllegalStateException("Subscription " + subscriptionId + " still runs in the wrapped subscription model after it was paused there, so this node keeps its lease");
                }
            }
        } catch (RuntimeException e) {
            failedToPause.add(e);
            throw e;
        }
        unregisterCompetingConsumer(cc, c -> {
            logDebug("Stopped CompetingConsumer subscription (subscriberId={}, subscriptionId={})", c.getSubscriberId(), c.getSubscriptionId());
            if (c.isRunning()) {
                competingConsumers.put(c.subscriptionIdAndSubscriberId, c.registerPaused(true));
                paused.add(c.getSubscriptionId());
            } else if (c.isMeantToRunBefore(stop) && c.isWaiting()) {
                competingConsumers.put(c.subscriptionIdAndSubscriberId, c.registerPausedWhileWaiting());
            } else if (c.isMeantToRunBefore(stop)) {
                competingConsumers.put(c.subscriptionIdAndSubscriberId, c.registerPaused(true));
            }
        });
    }

    /**
     * Starts this model, and starts the wrapped model with {@code start(false)} when it is not running. A competing
     * subscription that waits for its lease competes for it again and runs once this node holds it. One whose lease this
     * node holds runs in the wrapped model once this starts it, also when that start took effect and then threw. With
     * either flag, this also runs one the wrapped model holds paused while it runs, such as one the user paused directly
     * on the wrapped model.
     * <p>
     * This returns without waiting for such a subscription to run. It hands the resume to the subscription's try, a
     * thread of its own that tries again until the subscription runs, loses its lease, or is paused or cancelled
     * through this model. A resume that fails is logged as a warning. When another call holds the lock of the
     * subscription as this gets to it, whichever call takes that lock next applies this {@code start(..)} to it. So a
     * resume of such a subscription that blocks in the wrapped model holds up neither this, {@code stop()} nor any
     * other subscription.
     * <p>
     * With {@code resumeSubscriptionsAutomatically} set, a subscription the user or {@code stop()} paused through
     * this model is resumed too. Without it, such a subscription stays paused until it is resumed. Until a
     * {@code start(..)} has returned without throwing, a model built over a wrapped model that was not running resumes
     * every paused subscription either way, also one {@code stop()} paused, except one the user paused and has not
     * resumed since, which {@code start(false)} keeps paused. Once a {@code start(true)} has resumed such a subscription,
     * {@code start(false)} resumes it too, also when that {@code start(true)} threw. A {@code start(false)} hands the
     * resume of a competing subscription that {@code stop()} paused to its try too, and returns without waiting for it.
     * <p>
     * This still waits while it makes a competing subscription that waits for its lease compete, and while it resumes
     * one that lost its lease or, as {@code start(true)}, one the user or {@code stop()} paused through this model. A
     * {@code start(true)} in 0.33.0 waited for both too. A start or resume that blocks in the wrapped model there holds
     * up this {@code start(..)}, and a {@code stop()} that waits for it.
     * <p>
     * {@code stop()} waits for this to return, and {@code shutdown()} waits while this starts the wrapped model, for as
     * long as the wrapped model takes to start.
     * <p>
     * When starting the wrapped model or a subscription that does not compete fails, the failure is thrown once every
     * subscription has had its turn, and {@link #isRunning()} returns {@code false} until a later {@code start(..)}
     * returns without throwing.
     *
     * @see SubscriptionModelLifeCycle#start(boolean)
     */
    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
        synchronized (wrappedModelStart) {
            startsWaiting++;
            wrappedModelStart.notifyAll();
        }
        try {
            lifecycleLock.lock();
        } finally {
            synchronized (wrappedModelStart) {
                startsWaiting--;
            }
        }
        try {
            startHoldingTheLifecycleLock(resumeSubscriptionsAutomatically);
            returnedWithoutThrowing();
        } catch (Throwable e) {
            lastStartThrew = true;
            throw e;
        } finally {
            lifecycleLock.unlock();
        }
    }

    // Before the lifecycle lock is let go, so the next start(..) or stop() begins from it
    private synchronized void returnedWithoutThrowing() {
        lastStartThrew = false;
        noStartReturnedSinceBuilt = false;
    }

    private void startHoldingTheLifecycleLock(boolean resumeSubscriptionsAutomatically) {
        Lifecycle start;
        Set<String> subscriptionIds;
        synchronized (this) {
            logDebug("Starting CompetingConsumer subscription model");
            start = beginLifecycle(true, resumeSubscriptionsAutomatically);
            if (start.resumesWhatIsPaused()) {
                subscriptionsBeingMade.values().forEach(beingMade -> beingMade.resumedMeanwhile = true);
            }
            synchronized (wrappedModelStart) {
                stoppedByUser.set(false);
                mayRunWhileStopped.clear();
                stopInEffect = 0;
                wrappedModelStart.notifyAll();
            }
            subscriptionIds = subscriptionIdsTakenBy(start);
            handOverToEach(start, subscriptionIds);
        }
        letEveryWaitingEventThrough();
        // A subscription that fails to start must not keep the subscriptions after it from starting. A failure of one
        // that does not compete is thrown once every subscription has had its turn. A competing consumer that fails is
        // tried again on a thread of its own instead. It has tried to give its lease back by then, unless the wrapped
        // model runs it anyway or shutdown() has begun, and a lease it could not give back is tried again on that same
        // thread.
        @Nullable RuntimeException firstFailure;
        @Nullable Throwable wrappedModelFailure = null;
        Set<SubscriptionIdAndSubscriberId> pausedByAStop = new HashSet<>();
        try {
            onceAStartOrStopHasBegun.run();
            wrappedModelFailure = startTheWrappedModelFor(start, pausedByAStop);
            firstFailure = applyToEverySubscription(start, subscriptionIds, new ArrayList<>(), wrappedModelFailure instanceof RuntimeException failure ? failure : null);
        } catch (Error e) {
            throw wrappedModelFailure instanceof Error first ? first : e;
        } finally {
            doneWith(start, false);
            // Each one this start(..) did not hand to its try, such as one whose lease this node did not hold as the
            // start got to it. One handed over is left to the call that applies the start(..) to it.
            synchronized (this) {
                pausedByAStop.removeIf(key -> !pausedByAStopOfTheWrappedModel.contains(key) || handedOver.containsKey(key.subscriptionId()));
            }
            resumeOnceThisNodeHoldsTheirLease(pausedByAStop);
        }
        if (wrappedModelFailure instanceof Error error) {
            throw error;
        }
        if (firstFailure != null) {
            throw firstFailure;
        }
    }

    // Also with no subscription, or a subscription that does not compete made after this start(..) would be held paused
    // by a wrapped model nothing starts. What the wrapped model holds paused stays paused. A failure is returned, an
    // Error too, so the subscriptions still get their turn before start(..) throws it. The competing consumers that
    // starting it finds paused there by a stop of the wrapped model are added to pausedByAStop instead of being tried
    // on threads of their own here, since the start(..) hands each one it gets to under that consumer's lock to its try.
    // A try started here could take a lock before the start(..) gets to it, and the start(..) would then hand that
    // consumer over to a thread of its own too.
    private @Nullable Throwable startTheWrappedModelFor(Lifecycle start, Set<SubscriptionIdAndSubscriberId> pausedByAStop) {
        lifecycleAppliedOnThisThread.set(start);
        try {
            runInTheWrappedModel(null, true, false, null, pausedByAStop, () -> null);
            return null;
        } catch (StoppedMeanwhile | ShutDownMeanwhile e) {
            logDebug("Not starting the wrapped subscription model, since this model was stopped or shut down meanwhile");
            return null;
        } catch (RuntimeException | Error e) {
            return e;
        } finally {
            lifecycleAppliedOnThisThread.remove();
        }
    }

    // start(..) has tried to start the wrapped model before this. A consumer is subscribed or resumed there only once
    // this node holds its lease.
    private void startConsumer(CompetingConsumer cc, Lifecycle start) {
        try {
            if (cc.isRunning()) {
                if (aLifecycleCallWaitsForThisThread.get()) {
                    leaveItsResumeToItsTry(cc);
                } else {
                    resumeIfTheWrappedModelHoldsItPaused(cc);
                }
                return;
            }
            // A waiting consumer competes again whatever the flag says, since nothing paused it. That includes one made
            // while this model was stopped. So does one that lost its lease before the stop, since no user paused it
            // either. One the user or stop() paused is resumed only when asked to, or when no start(..) has returned
            // since this model was built stopped and the user did not pause it.
            if (cc.isWaiting()) {
                logDebug("Starting CompetingConsumer subscription (subscriberId={}, subscriptionId={}, state={})", cc.getSubscriberId(), cc.getSubscriptionId(), cc.state.getClass().getSimpleName());
                registerAndStartIfGranted(cc);
            } else if (cc.isPaused() && (resumes(start, cc.getSubscriptionId()) || cc.isPausedByTheLossOfItsLease())) {
                logDebug("Starting CompetingConsumer subscription (subscriberId={}, subscriptionId={}, state={})", cc.getSubscriberId(), cc.getSubscriptionId(), cc.state.getClass().getSimpleName());
                if (start.resumeSubscriptionsAutomatically()) {
                    // Undoes the user's pause, also when the resume below fails, since what fails is tried again
                    pausedByTheUserBeforeAStartReturned.remove(cc.getSubscriptionId());
                }
                // A start(true) and a consumer that lost its lease are resumed here, as they always were. A start(false)
                // resumes one that stop() paused only while no start(..) has returned since this model was built
                // stopped, and a thread that call waits for hands that resume to the consumer's try.
                if (aLifecycleCallWaitsForThisThread.get() && !start.resumeSubscriptionsAutomatically() && !cc.isPausedByTheLossOfItsLease()) {
                    leaveItsResumeFromAPauseToItsTry(cc);
                } else {
                    resume(cc.getSubscriptionId(), false);
                }
            }
        } catch (Throwable e) {
            // An Error too is tried again, and then thrown
            triedAgainAfter(cc.subscriptionIdAndSubscriberId, e);
            if (e instanceof Error error) {
                throw error;
            }
        }
    }

    // On a thread no start(..) or stop() waits for, such as one a start(..) was handed over to. A consumer recorded as
    // running here that the wrapped model holds paused would keep its lease with nothing delivered, whether a stop() of
    // the wrapped model or a pause called on it directly paused it there. Either flag resumes it, since only a pause
    // through this model keeps a consumer this node holds the lease for from running.
    // The lease is asked about first, since asking the wrapped model lets the held events through. One a resume is
    // registering is left to that resume, as on a grant. A wrapped model that is not running is started by the resume.
    private void resumeIfTheWrappedModelHoldsItPaused(CompetingConsumer cc) {
        SubscriptionIdAndSubscriberId key = cc.subscriptionIdAndSubscriberId;
        if (resumedOnceRegistered.contains(key) || !hasLock(key.subscriptionId(), key.subscriberId())) {
            return;
        }
        if (isRunningInTheWrappedModel(key.subscriptionId()) || !isPausedInTheWrappedModel(key.subscriptionId())) {
            pausedByAStopOfTheWrappedModel.remove(key);
            return;
        }
        logDebug("Resuming CompetingConsumer that holds its lease and that the wrapped model holds paused (subscriberId={}, subscriptionId={})", key.subscriberId(), key.subscriptionId());
        giveTheLeaseBackIfItThrows(key, cc.state, () -> resumeInTheWrappedModel(key));
    }

    // On a thread that a start(..) or stop() waits for, which no call into the wrapped model for one consumer may hold
    // up. The wrapped model can block in a resume for as long as it likes, so a consumer this node holds the lease for is
    // handed to its try, which asks the wrapped model whether it runs the consumer, resumes it there when it does not, and
    // tries again until the consumer runs, loses its lease, or is paused or cancelled through this model.
    private void leaveItsResumeToItsTry(CompetingConsumer cc) {
        SubscriptionIdAndSubscriberId key = cc.subscriptionIdAndSubscriberId;
        if (resumedOnceRegistered.contains(key) || !hasLock(key.subscriptionId(), key.subscriberId())) {
            return;
        }
        reconcileLater(key, true);
    }

    // On a thread that a start(false) waits for, for a consumer that stop() paused. Recorded as paused by nothing but
    // the loss of its lease, or as waiting again when it never ran, the consumer competes again. Its try registers it,
    // runs it in the wrapped model once this node holds its lease, and tries again as for any consumer that lost its
    // lease, so a resume that blocks in the wrapped model holds up only that try.
    // A stop() that comes after the start(false), until the register of the try returns without the lease, pauses it as
    // by the user, as it would have had the start(false) run it before returning. Recorded as lost to the lease alone,
    // the next start(false) would resume it.
    private void leaveItsResumeFromAPauseToItsTry(CompetingConsumer cc) {
        SubscriptionIdAndSubscriberId key = cc.subscriptionIdAndSubscriberId;
        LaterStops laterStops = stopsAfterThisCall(key);
        competingConsumers.put(key, new CompetingConsumer(key, cc.state instanceof CompetingConsumerState.PausedWhileWaiting paused
                ? paused.waiting.meantToRunBefore(laterStops)
                : new CompetingConsumerState.Paused(false, laterStops)));
        reconcileLater(key, true);
    }

    // Called on finding the wrapped model not running, which holds paused every competing consumer recorded as running
    // here, except the one the call is for, which that call runs
    private Set<SubscriptionIdAndSubscriberId> pausedByTheStoppedWrappedModel(@Nullable SubscriptionIdAndSubscriberId callFor) {
        Set<SubscriptionIdAndSubscriberId> paused = new HashSet<>();
        competingConsumers.forEach((key, cc) -> {
            if (cc.isRunning() && !key.equals(callFor)) {
                paused.add(key);
            }
        });
        pausedByAStopOfTheWrappedModel.addAll(paused);
        return paused;
    }

    // Once the wrapped model may run again. Each is tried on a thread of its own, since its lock can be held by the
    // thread that started the wrapped model, and the try resumes it there while this node holds its lease.
    private void resumeOnceThisNodeHoldsTheirLease(Set<SubscriptionIdAndSubscriberId> paused) {
        paused.forEach(key -> reconcileLater(key, true));
    }

    // The first failure, with every later one attached to it as suppressed
    private static RuntimeException withSuppressed(@Nullable RuntimeException firstFailure, RuntimeException failure) {
        if (firstFailure == null) {
            return failure;
        }
        firstFailure.addSuppressed(failure);
        return firstFailure;
    }

    /**
     * Whether this model is started, which is what {@link #start(boolean)} and {@link #stop()} change. A new model is
     * started when the wrapped model runs as it is built, and stopped otherwise, such as over one built with
     * {@code autoStartup(false)}. This returns {@code false} once a {@code stop()} is past any {@code start(..)} or
     * {@code stop()} it waits for, until a later {@code start(..)} gets that far, also while a subscription runs on this
     * node meanwhile, such as one resumed after that {@code stop()}. It also returns {@code false} after a {@code
     * start(..)} that threw, until a later one returns without throwing. Once {@link #shutdown()} has begun it returns
     * {@code false} for good.
     * <p>
     * It asks the wrapped model only as this model is built, and says nothing about whether this node delivers
     * anything. A started node that holds no lease returns {@code true}. So does a started model whose wrapped model
     * was stopped by a call to {@code stop()} on the wrapped model itself. Call {@link #isRunning(String)} to find out whether a subscription runs on this node.
     *
     * @see SubscriptionModelLifeCycle#isRunning()
     */
    @Override
    public boolean isRunning() {
        return !shutDown && !stoppedByUser.get() && !lastStartThrew;
    }

    /**
     * @see SubscriptionModelLifeCycle#isRunning(String)
     */
    @Override
    public boolean isRunning(String subscriptionId) {
        return getWrappedSubscriptionModel().isRunning(subscriptionId);
    }

    // Reports its own consumers as well as the delegate's, because the delegate knows a consumer that has not won the
    // lock yet only when it has made the subscription
    @Override
    public Set<String> subscriptionIds() {
        Set<String> ids = competingConsumers.keySet().stream()
                .map(SubscriptionIdAndSubscriberId::subscriptionId)
                .collect(Collectors.toCollection(HashSet::new));
        IntrospectableSubscriptions.findIn(getWrappedSubscriptionModel())
                .map(IntrospectableSubscriptions::subscriptionIds)
                .ifPresent(ids::addAll);
        return Set.copyOf(ids);
    }

    /**
     * @see SubscriptionModelLifeCycle#isPaused(String)
     */
    @Override
    public boolean isPaused(String subscriptionId) {
        // A consumer paused before the wrapped model made its subscription is known only here. The wrapped model reports
        // one it holds paused, which is one made while this model was stopped and one whose lease went to another node
        // while the wrapped model made it. subscriptionIds() merges both sources for the same reason.
        return isPausedHere(subscriptionId) || delegate.isPaused(subscriptionId);
    }

    private boolean isPausedHere(String subscriptionId) {
        return findFirstCompetingConsumerMatching(cc -> cc.hasSubscriptionId(subscriptionId) && cc.isPaused()).isPresent();
    }

    /**
     * @see SubscriptionModelLifeCycle#resumeSubscription(String)
     */
    @Override
    public Subscription resumeSubscription(String subscriptionId) {
        return actOn(subscriptionId, () -> resume(subscriptionId, true));
    }

    // Only a resume the user asks for lets a subscription run while this model is stopped, and only one that comes after
    // that stop(), which is one that began after it, see actOn. One that comes before it is the call that runs first,
    // as it would have under the stop() that came after it, which then pauses the subscription. A start(..) applied to
    // a subscription handed over can come after a later stop() began, which must still pause it after that resume.
    private Subscription resume(String subscriptionId, boolean askedForByTheUser) {
        logDebug("Trying to resume CompetingConsumer subscription (subscriptionId={})", subscriptionId);
        long stopSeen;
        @Nullable SubscriptionLock held = subscriptionLocks.get(subscriptionId);
        synchronized (wrappedModelStart) {
            boolean stopCameFirst = held == null || !held.lock.isHeldByCurrentThread() || stopInEffect <= held.lastStopBegunWhenTaken;
            stopSeen = stoppedByUser.get() && stopCameFirst ? stopInEffect : 0;
        }
        requireKnown(subscriptionId);
        boolean running;
        @Nullable Throwable askingFailed = null;
        try {
            running = isRunningInTheWrappedModel(subscriptionId);
        } catch (Throwable e) {
            if (nonCompetingConsumersSubscriptions.contains(subscriptionId)) {
                throw e;
            }
            // A competing consumer is resumed as asked, and the try that follows decides from what the wrapped model
            // does once it can say
            running = false;
            askingFailed = e;
        }
        if (running && isPausedHere(subscriptionId)) {
            // A start() called on the wrapped model itself runs again what this model holds paused, with or without
            // its lease, so it is paused there first and then resumed as any other paused consumer is. One the wrapped
            // model reports paused as well has a pause there that has not taken effect yet, as a catch-up's does until
            // its replay has ended, and is refused as running. A failure is tried again, as one asking above is.
            try {
                if (isPausedInTheWrappedModel(subscriptionId)) {
                    throw new SubscriptionAlreadyRunningException(subscriptionId);
                }
                logDebug("Pausing a paused CompetingConsumer that the wrapped model runs before resuming it (subscriptionId={})", subscriptionId);
                pauseInTheWrappedModel(subscriptionId);
            } catch (SubscriptionAlreadyRunningException e) {
                throw e;
            } catch (Throwable e) {
                askingFailed = e;
            }
        } else if (running) {
            logDebug("Subscription already is running, cannot resume (subscriptionId={}, delegate={})", subscriptionId, delegate.toString());
            throw new SubscriptionAlreadyRunningException(subscriptionId);
        }
        @Nullable Throwable failureToTryAgain = askingFailed;

        if (nonCompetingConsumersSubscriptions.contains(subscriptionId)) {
            logDebug("Subscription was a non-competing consumer subscription, will delegate to {} (subscriptionId={})", delegate.getClass().getName(), subscriptionId);
            // A stop() that begins while this runs in the wrapped model waits for it before it stops the wrapped model.
            // A wrapped model can start itself to resume it, so what it held paused while it was not running is resumed
            // as after any other start of the wrapped model.
            Subscription resumedSubscription = runInTheWrappedModelAlsoWhileStopped(() -> {
                Set<SubscriptionIdAndSubscriberId> paused = wrappedModelSaysItRuns() ? Set.of() : pausedByTheStoppedWrappedModel(null);
                try {
                    return delegate.resumeSubscription(subscriptionId);
                } finally {
                    resumeOnceThisNodeHoldsTheirLease(paused);
                }
            });
            pausedByTheUserBeforeAStartReturned.remove(subscriptionId);
            return resumedSubscription;
        }

        logDebug("Finding first competing consumer that matches the subscription (subscriptionId={})", subscriptionId);
        return findFirstCompetingConsumerMatching(competingConsumer -> competingConsumer.hasSubscriptionId(subscriptionId))
                .map(competingConsumer -> {
                    if (askedForByTheUser) {
                        pausedByTheUserBeforeAStartReturned.remove(subscriptionId);
                    }
                    if (askedForByTheUser && stopSeen != 0) {
                        mayRunUnderTheSameStop(competingConsumer.subscriptionIdAndSubscriberId, stopSeen);
                    }
                    if (competingConsumer.isPausedWhileWaiting()) {
                        // Restore the Waiting this consumer was paused from before anything else runs, so every
                        // branch below treats it exactly like a plain resume of a waiting consumer. The write has
                        // to land before any strategy call, because registering can grant the lock and call
                        // onConsumeGranted synchronously on this thread, and that callback only starts a consumer
                        // it finds Waiting in the map.
                        competingConsumer = competingConsumer.restoreWaiting();
                        competingConsumers.put(competingConsumer.subscriptionIdAndSubscriberId, competingConsumer);
                    }
                    String subscriberId = competingConsumer.getSubscriberId();
                    SubscriptionIdAndSubscriberId resumed = competingConsumer.subscriptionIdAndSubscriberId;
                    try {
                        if (failureToTryAgain != null) {
                            throw thrownAsItIs(failureToTryAgain);
                        }
                        boolean hasLock = hasLock(subscriptionId, subscriberId);
                        logDebug("Resuming CompetingConsumer (subscriberId={}, subscriptionId={}, state={}, hasLock={})", subscriberId, subscriptionId, competingConsumer.state.getClass().getSimpleName(), hasLock);
                        CompetingConsumer paused = competingConsumer;
                        if (hasLock) {
                            if (competingConsumer.isWaiting()) {
                                return startWaitingConsumer(competingConsumer);
                            }
                            competingConsumers.put(competingConsumer.subscriptionIdAndSubscriberId, competingConsumer.registerRunning());
                            // Paused here, or recorded as running here although the wrapped model does not run it,
                            // since the isRunning check above asks the wrapped model
                            return giveTheLeaseBackIfItThrows(resumed, paused.state, () -> resumeInTheWrappedModel(resumed));
                        } else if (competingConsumer.isWaiting()) {
                            return registerWaitingAndStartIfGranted(competingConsumer);
                        } else if (registerAsRunning(competingConsumer)) {
                            // Paused here, or recorded as running here although the wrapped model does not run it
                            return giveTheLeaseBackIfItThrows(resumed, paused.state, () -> resumeInTheWrappedModel(resumed));
                        }
                        // Not allowed to resume without the lock
                        return new CompetingConsumerSubscription(subscriptionId, subscriberId);
                    } catch (ShutDownMeanwhile e) {
                        // Nothing to try again once this model is shut down
                        throw e;
                    } catch (Throwable e) {
                        // Recorded as competing, so what failed is tried again rather than asked for again, an Error
                        // too, which is then thrown
                        // A stop() that comes after this call pauses it as by the user, as it would have had this call run it
                        CompetingConsumer current = competingConsumers.get(resumed);
                        if (current != null && current.state instanceof CompetingConsumerState.Paused p && (p.pausedByUser || p.laterStops == null)) {
                            competingConsumers.put(resumed, new CompetingConsumer(resumed, new CompetingConsumerState.Paused(false, stopsAfter(resumed, e))));
                        } else if (current != null && current.state instanceof CompetingConsumerState.Waiting waiting && waiting.laterStops == null) {
                            competingConsumers.put(resumed, new CompetingConsumer(resumed, waiting.meantToRunBefore(stopsAfter(resumed, e))));
                        }
                        triedAgainAfter(resumed, e);
                        if (e instanceof Error error) {
                            throw error;
                        }
                        return new CompetingConsumerSubscription(subscriptionId, subscriberId);
                    }
                })
                .orElseThrow(() -> new IllegalStateException("Cannot resume subscription " + subscriptionId + " since another consumer currently subscribes to it."));
    }

    /**
     * @see SubscriptionModelLifeCycle#pauseSubscription(String)
     */
    @Override
    public void pauseSubscription(String subscriptionId) {
        actOn(subscriptionId, () -> {
            try {
                pauseSubscription(subscriptionId, true);
            } catch (Throwable e) {
                // A pause can take effect before it throws, and is then the user's as much as one that returned
                if (pausedByTheUserAfterAFailure(subscriptionId, e)) {
                    pausedBeforeAStartReturned(subscriptionId);
                }
                throw e;
            }
            pausedBeforeAStartReturned(subscriptionId);
            return null;
        });
    }

    private synchronized void pausedBeforeAStartReturned(String subscriptionId) {
        if (noStartReturnedSinceBuilt) {
            pausedByTheUserBeforeAStartReturned.add(subscriptionId);
        }
    }

    // False also when the wrapped model cannot say, with its failure added to the one being handled
    private boolean pausedByTheUserAfterAFailure(String subscriptionId, Throwable failure) {
        if (nonCompetingConsumersSubscriptions.contains(subscriptionId)) {
            try {
                return isPausedInTheWrappedModel(subscriptionId);
            } catch (Throwable e) {
                failure.addSuppressed(e);
                return false;
            }
        }
        return findFirstCompetingConsumerMatching(cc -> cc.hasSubscriptionId(subscriptionId)
                && (cc.isPausedWhileWaiting() || cc.state instanceof CompetingConsumerState.Paused paused && paused.pausedByUser)).isPresent();
    }

    /**
     * Runs a pause, resume or cancel for one subscription under that subscription's lock, so it waits for a try or
     * another call for the same subscription, and holds up nothing else while it waits. The monitor is taken only to
     * read and write what several subscriptions share, never while calling the lease strategy or the wrapped model.
     * <p>
     * The call comes after each {@code start(..)} and {@code stop()} that began before it, and before each one that
     * began after it, also when that one began while this call waited for the lock and was handed over meanwhile. So
     * it decides from where those calls stood when it began, see {@link #begins(String)}. While it waits for the lock,
     * any other thread that holds the lock applies none of the ones that began after it, see
     * {@link #firstCallWaiting(SubscriptionLock)}. Once it has the lock, each {@code start(..)} and {@code stop()} that
     * began before it and is not applied to the subscription yet is applied first, also one whose own thread has not
     * got to the subscription, see {@link #applyWhatWasHandedOverFirst(String, SubscriptionLock, long, boolean)}. The
     * ones that began after it are left to their own threads, or to the thread they were handed to, which apply them
     * once this call has returned. The pause, resume and cancel calls of the same subscription that wait for the lock
     * go on in the order they began, see {@link #waitForItsTurn(SubscriptionLock, CallWaiting)}. One made on a thread that
     * already holds the lock, from inside a call this model makes to the lease strategy or the wrapped model, waits for
     * nothing and applies nothing first. It comes before each {@code start(..)} and {@code stop()} not yet applied to
     * the subscription, also one that began before it.
     * <p>
     * One of those applied first that fails is given up instead of tried again, and the ones after it are still applied
     * in turn. The {@code start(..)} or {@code stop()} given up does not throw that failure, also when its own thread
     * had not got to the subscription. This call is then made all the same, so what it throws is what fails in this
     * call, or the first {@link Error} that one given up threw, which is thrown once this call has been made. Giving up
     * a {@code start(..)} gives up only what it does for this subscription. Its start of the wrapped model is left to a
     * thread of its own, see {@link #keepStartingTheWrappedModel(Lifecycle)}.
     */
    private <T> T actOn(String subscriptionId, Supplier<T> call) {
        SubscriptionLock lock = useSubscriptionLock(subscriptionId);
        boolean waits = !lock.lock.isHeldByCurrentThread();
        // Taken once this call uses the lock, so a start(..) or stop() applied to the subscription before this call
        // has the lock is applied while holding the same lock, which records it
        Began began;
        @Nullable CallWaiting waiting = null;
        synchronized (this) {
            began = begins(subscriptionId);
            if (waits) {
                waiting = new CallWaiting(++callsWaited, began.lifecycleCalls());
                lock.callsWaiting.addLast(waiting);
            }
        }
        if (waiting != null) {
            beforeACallWaitsForTheLock.run();
        }
        lock.lock.lock();
        taken(lock);
        if (waiting != null) {
            waitForItsTurn(lock, waiting);
            synchronized (wrappedModelStart) {
                lock.lastStopBegunWhenTaken = began.lastStopBegun();
                lock.mayRunWhenTaken = began.mayRun();
            }
        }
        long begunBefore = began.lifecycleCalls();
        @Nullable Error givenUpError = null;
        try {
            while (true) {
                try {
                    applyWhatWasHandedOverFirst(subscriptionId, lock, begunBefore, false);
                    break;
                } catch (Throwable e) {
                    if (!giveUpTheFirstHandedOver(subscriptionId, begunBefore, e)) {
                        break;
                    }
                    if (e instanceof Error error && givenUpError == null) {
                        givenUpError = error;
                    }
                }
            }
            T made;
            try {
                made = call.get();
            } catch (Throwable e) {
                if (givenUpError != null) {
                    // A model that throws one Error instance for every failure throws the one given up here too
                    if (e != givenUpError) {
                        givenUpError.addSuppressed(e);
                    }
                    throw givenUpError;
                }
                throw e;
            }
            if (givenUpError != null) {
                throw givenUpError;
            }
            return made;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Where the {@code start(..)} and {@code stop()} calls stand at one point in the order they began, taken under the
     * monitor and {@code wrappedModelStart} together, since each of them updates both in one step. It records how many
     * have begun, the last {@code stop()} among them, and whether the subscription may run.
     */
    private Began begins(String subscriptionId) {
        synchronized (this) {
            synchronized (wrappedModelStart) {
                return new Began(lifecycleCalls, lastStopBegun, mayRun(subscriptionId));
            }
        }
    }

    private record Began(long lifecycleCalls, long lastStopBegun, boolean mayRun) {
    }

    // Called under wrappedModelStart
    private boolean mayRun(String subscriptionId) {
        return !stoppedByUser.get() || mayRunWhileStopped.stream().anyMatch(key -> key.subscriptionId().equals(subscriptionId));
    }

    // Called under the subscription's lock once the oldest start(..) or stop() handed over for it has failed. Gives it
    // up when it began before begunBefore, so the ones after it are still applied in turn. One that began later is left
    // to the thread it was handed to. Answers whether it was given up. A start(..) given up for a subscription that does
    // not compete still has the wrapped model started, since that start is one for the whole model.
    private boolean giveUpTheFirstHandedOver(String subscriptionId, long begunBefore, Throwable failure) {
        @Nullable Lifecycle givenUp;
        boolean startsTheWrappedModel;
        synchronized (this) {
            @Nullable HandedOver handed = handedOver.get(subscriptionId);
            if (handed == null) {
                return false;
            }
            givenUp = handed.notApplied.peekFirst();
            if (givenUp == null || givenUp.id() > begunBefore) {
                return false;
            }
            handed.applied(givenUp);
            startsTheWrappedModel = givenUp.started() && nonCompetingConsumersSubscriptions.contains(subscriptionId);
        }
        log.warn("A start(..) or stop() handed over for subscription {} failed, and a pause, resume or cancel of that subscription came after it, so it is not tried again for that subscription ({})",
                subscriptionId, givenUp, failure);
        if (startsTheWrappedModel) {
            keepStartingTheWrappedModel(givenUp);
        }
        return true;
    }

    /**
     * Starts the wrapped model for a {@code start(..)} that a pause, resume or cancel gave up for one subscription, on a
     * thread of its own, with the backoff a try uses, until the wrapped model runs, or a {@code stop()} that began after
     * that {@code start(..)} or {@code shutdown()} refuses it. One thread does it for every such {@code start(..)}, the
     * latest one given up.
     */
    private void keepStartingTheWrappedModel(Lifecycle start) {
        synchronized (this) {
            @Nullable Lifecycle alreadyStarting = wrappedModelStartGivenUp;
            if (shutDown || (alreadyStarting != null && alreadyStarting.id() >= start.id())) {
                return;
            }
            wrappedModelStartGivenUp = start;
            if (alreadyStarting != null) {
                return;
            }
        }
        Lifecycle tried = start;
        @Nullable Throwable failed = null;
        while (true) {
            try {
                startStartingTheWrappedModel();
                return;
            } catch (Throwable e) {
                if (failed != null) {
                    e.addSuppressed(failed);
                }
                failed = e;
            }
            synchronized (this) {
                @Nullable Lifecycle latest = wrappedModelStartGivenUp;
                if (shutDown || latest == null || latest == tried) {
                    // No thread starts it then, so the next start(..) given up starts one anew
                    wrappedModelStartGivenUp = null;
                    throw thrownAsItIs(failed);
                }
                // A later one was given up meanwhile and returned, counting on this thread
                tried = latest;
            }
        }
    }

    private void startStartingTheWrappedModel() {
        Thread.ofPlatform().daemon().name(WRAPPED_MODEL_START_THREAD).start(() -> {
            Duration backoff = RECONCILE_FIRST_BACKOFF;
            int failures = 0;
            while (true) {
                Lifecycle starting;
                synchronized (this) {
                    starting = requireNonNull(wrappedModelStartGivenUp);
                }
                boolean done = true;
                lifecycleAppliedOnThisThread.set(starting);
                try {
                    runInTheWrappedModel(null, true, () -> null);
                } catch (StoppedMeanwhile | ShutDownMeanwhile e) {
                    logDebug("Not starting the wrapped model for a start(..) given up for a subscription, since this model was stopped or shut down meanwhile");
                } catch (Throwable e) {
                    done = false;
                    if (failures++ % RECONCILE_TRIES_BETWEEN_WARNINGS == 0) {
                        log.warn("Could not start the wrapped subscription model for a start(..) given up for a subscription, so it is tried again", e);
                    }
                } finally {
                    lifecycleAppliedOnThisThread.remove();
                }
                synchronized (this) {
                    if (shutDown || (done && wrappedModelStartGivenUp == starting)) {
                        wrappedModelStartGivenUp = null;
                        return;
                    }
                }
                if (done) {
                    // A later start(..) was given up meanwhile, and is applied at once
                    continue;
                }
                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException e) {
                    synchronized (this) {
                        wrappedModelStartGivenUp = null;
                    }
                    return;
                }
                Duration doubled = backoff.multipliedBy(2);
                backoff = doubled.compareTo(RECONCILE_MAX_BACKOFF) > 0 ? RECONCILE_MAX_BACKOFF : doubled;
            }
        });
    }

    private SubscriptionLock lockSubscription(String subscriptionId) {
        SubscriptionLock lock = useSubscriptionLock(subscriptionId);
        lock.lock.lock();
        taken(lock);
        try {
            applyWhatWasHandedOverFirst(subscriptionId, lock);
        } catch (RuntimeException e) {
            lock.unlock();
            throw new IllegalStateException("A start(..) or stop() that came before this call could not be applied to subscription " + subscriptionId
                    + " yet, so this call was not made. The thread of that start(..) or stop(), or the thread it was handed to, tries it again.", e);
        } catch (Throwable e) {
            lock.unlock();
            throw e;
        }
        return lock;
    }

    /**
     * Applies each {@code start(..)} and {@code stop()} not applied to the subscription yet, oldest first, for a call
     * that has just taken the subscription's lock, also one whose own thread has not got to the subscription. That call
     * then comes after each of them, as it would have with the lock free, instead of after only the ones whose thread
     * got the lock first. Does nothing on a lock this thread held already, since the call holding it is still under
     * way. Throws what failed for a subscription no try covers, and the thread of that {@code start(..)} or
     * {@code stop()}, or the thread it was handed to, applies that one and every one after it.
     */
    private void applyWhatWasHandedOverFirst(String subscriptionId, SubscriptionLock lock) {
        applyWhatWasHandedOverFirst(subscriptionId, lock, Long.MAX_VALUE, true);
    }

    // As applyWhatWasHandedOverFirst(String, SubscriptionLock), but only for the start(..) and stop() calls numbered up
    // to begunBefore, and with leftToCallsWaiting only for those that no pause, resume or cancel waiting for the lock
    // comes before. The ones after them are left to the thread they were handed to.
    private void applyWhatWasHandedOverFirst(String subscriptionId, SubscriptionLock lock, long begunBefore, boolean leftToCallsWaiting) {
        if (lock.lock.getHoldCount() > 1) {
            return;
        }
        while (true) {
            @Nullable HandedOver handed;
            @Nullable Lifecycle next;
            long upTo;
            synchronized (this) {
                handed = handedOver.get(subscriptionId);
                next = handed == null ? null : handed.notApplied.peekFirst();
                upTo = leftToCallsWaiting ? Math.min(begunBefore, firstCallWaiting(lock)) : begunBefore;
                if (leftToCallsWaiting && next != null && mayBeTakenBack(next)) {
                    return;
                }
            }
            if (handed == null || next == null || next.id() > upTo) {
                return;
            }
            applyHandedOver(subscriptionId, handed, next);
        }
    }

    // Called under the monitor
    private boolean mayBeTakenBack(Lifecycle lifecycle) {
        return lifecycle.id() == stopThatMayBeTakenBack;
    }

    /**
     * The number of {@code start(..)} and {@code stop()} calls begun before the first pause, resume or cancel that waits
     * for the lock, or {@link Long#MAX_VALUE} when none does. A thread holding the lock applies none numbered above it
     * to the subscription, so that call comes before them, also when that thread took the lock first. Called under the
     * monitor.
     */
    private long firstCallWaiting(SubscriptionLock lock) {
        @Nullable CallWaiting first = lock.callsWaiting.peekFirst();
        return first == null ? Long.MAX_VALUE : first.lifecycleCallsBegun();
    }

    // Whether a pause, resume or cancel numbered callsWaitedBefore or lower waits for the lock
    private synchronized boolean aCallWaitsFor(SubscriptionLock lock, long callsWaitedBefore) {
        @Nullable CallWaiting first = lock.callsWaiting.peekFirst();
        return first != null && first.number <= callsWaitedBefore;
    }

    private synchronized long callsWaitedSoFar() {
        return callsWaited;
    }

    /**
     * Whether the pause, resume or cancel holding the lock, which waited for it, goes on now. It goes on once every
     * other one that began before it and waits for the lock has gone on, since a lock does not promise to let the
     * threads waiting for it go in the order they began, and once a try has gone ahead with each lease callback that
     * came before it began and was handed to that try. Called under the lock.
     */
    private void waitForItsTurn(SubscriptionLock lock, CallWaiting waiting) {
        boolean interrupted = false;
        Turn turn;
        while ((turn = turnOf(lock, waiting)) != Turn.GOES_ON) {
            if (turn == Turn.AFTER_A_CALL) {
                lock.callBeforeGone.awaitUninterruptibly();
                continue;
            }
            // Timed, since a try that ends without going ahead, on shutdown or an interrupt, signals nothing
            try {
                lock.callBeforeGone.await(LEFT_TO_A_CALL_WAITING.toNanos(), TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private enum Turn {GOES_ON, AFTER_A_CALL, AFTER_A_LEASE_CALLBACK}

    private synchronized Turn turnOf(SubscriptionLock lock, CallWaiting waiting) {
        if (lock.callsWaiting.peekFirst() != waiting) {
            return Turn.AFTER_A_CALL;
        } else if (aLeaseCallbackComesBefore(lock.subscriptionId, waiting.number)) {
            return Turn.AFTER_A_LEASE_CALLBACK;
        }
        lock.callsWaiting.removeFirst();
        lock.callBeforeGone.signalAll();
        return Turn.GOES_ON;
    }

    // A pause, resume or cancel waiting for the lock of its subscription, numbered among every one that has waited,
    // with the number of start(..) and stop() calls begun before it
    private static final class CallWaiting {
        private final long number;
        private final long lifecycleCallsBegun;

        private CallWaiting(long number, long lifecycleCallsBegun) {
            this.number = number;
            this.lifecycleCallsBegun = lifecycleCallsBegun;
        }

        private long lifecycleCallsBegun() {
            return lifecycleCallsBegun;
        }
    }

    // Null when another thread holds the lock
    private @Nullable SubscriptionLock tryLockSubscription(String subscriptionId) {
        SubscriptionLock lock = useSubscriptionLock(subscriptionId);
        if (lock.lock.tryLock()) {
            return taken(lock);
        }
        lock.stopUsing();
        return null;
    }

    // Null once this model is shut down, so a thread of this model's own waiting for a call stuck in the database ends
    // within a check interval of shutdown() instead of outliving it
    private @Nullable SubscriptionLock lockSubscriptionUnlessShutDown(String subscriptionId) throws InterruptedException {
        SubscriptionLock lock = useSubscriptionLock(subscriptionId);
        try {
            while (!shutDown) {
                if (lock.lock.tryLock(SHUTDOWN_CHECK_INTERVAL.toMillis(), TimeUnit.MILLISECONDS)) {
                    return taken(lock);
                }
            }
        } catch (InterruptedException e) {
            lock.stopUsing();
            throw e;
        }
        lock.stopUsing();
        return null;
    }

    // A call that takes the lock comes before every stop() that begins while it holds it
    private SubscriptionLock taken(SubscriptionLock lock) {
        if (lock.lock.getHoldCount() == 1) {
            synchronized (wrappedModelStart) {
                decidesFromNow(lock);
            }
        }
        return lock;
    }

    // What the call holding the lock decides from, the stop() calls that had begun and whether the consumer may run.
    // Called under wrappedModelStart.
    private void decidesFromNow(SubscriptionLock lock) {
        lock.lastStopBegunWhenTaken = lastStopBegun;
        lock.mayRunWhenTaken = mayRun(lock.subscriptionId);
    }

    // The lock of the subscription, made when no thread holds or waits for one, and counted as used until the caller
    // unlocks it or gives up waiting for it
    private SubscriptionLock useSubscriptionLock(String subscriptionId) {
        return subscriptionLocks.compute(subscriptionId, (__, current) -> {
            SubscriptionLock lock = current == null ? new SubscriptionLock(subscriptionId) : current;
            lock.users++;
            return lock;
        });
    }

    // A subscription's lock, with the number of threads that hold or wait for it, which changes only inside
    // subscriptionLocks.compute. The entry is removed when that number reaches zero, and a thread that comes after
    // makes a new one, which no other thread can hold then.
    private final class SubscriptionLock {
        private final String subscriptionId;
        private final ReentrantLock lock = new ReentrantLock();
        private int users;
        // The last stop() that had begun when the thread holding the lock took it, or when the pause, resume or cancel
        // holding it began, read and written by that thread only
        private long lastStopBegunWhenTaken;
        // Whether this model was started then, or let the consumer run while stopped, read and written by that thread
        // only. A grant decides from it when a stop() began since.
        private boolean mayRunWhenTaken;
        // Each pause, resume or cancel waiting for this lock, in the order they began, read and written under the
        // monitor only
        private final ArrayDeque<CallWaiting> callsWaiting = new ArrayDeque<>();
        // Signalled when a pause, resume or cancel stops waiting for this lock, so one that let it go first takes it again
        private final Condition callBeforeGone = lock.newCondition();
        // The failure the call holding the lock last logged a warning with, and how many failures were suppressed in
        // it then, read and written by that thread only. Forgotten once that call lets go of the lock.
        private @Nullable Throwable warnedAbout;
        private int suppressedWhenWarned;

        private SubscriptionLock(String subscriptionId) {
            this.subscriptionId = subscriptionId;
        }

        private void unlock() {
            if (lock.getHoldCount() == 1) {
                warnedAbout = null;
            }
            lock.unlock();
            stopUsing();
        }

        private void stopUsing() {
            subscriptionLocks.compute(subscriptionId, (__, current) -> --users == 0 ? null : current);
        }
    }

    /**
     * Starts the wrapped model, without resuming what it holds paused, before a consumer whose lease this node holds is
     * subscribed or resumed there. A stopped model holds a new subscription paused, so a consumer subscribed there
     * without it would be recorded as running here and delivered nothing. Most subscriptions paused there are ones this
     * node holds no lease for, or ones that resume on a grant of their own, so none of them is resumed.
     */
    private void startTheWrappedModelIfStopped(SubscriptionIdAndSubscriberId key) {
        runInTheWrappedModel(key, true, () -> null);
    }

    private Subscription resumeInTheWrappedModel(SubscriptionIdAndSubscriberId key) {
        Subscription resumed = runWithoutHoldingUpAStop(key, () -> callingTheWrappedModel(() -> delegate.resumeSubscription(key.subscriptionId())));
        pausedByAStopOfTheWrappedModel.remove(key);
        return resumed;
    }

    /**
     * Resumes or subscribes a competing consumer in the wrapped model once {@link #runInTheWrappedModel} lets the call
     * in, which starts the wrapped model first when it is not running. Only that start is counted, so neither
     * {@code stop()} nor {@code shutdown()} waits for a call that waits inside the wrapped model, which over a durable
     * model reads the stored position for as long as the database cannot be reached. Once a {@code stop()} has
     * overtaken the call, {@link #awaitTheLease} holds every event of the subscription until this model next calls the
     * wrapped model for it, as the pause below does.
     * <p>
     * A {@code stop()} or {@code shutdown()} that began once the call was let in comes after it in the order the calls
     * began, and can stop the wrapped model, or shut it down, before the call returns. What the call ran there is then
     * stopped here, as that {@code stop()} or {@code shutdown()} would have stopped it had the call returned first. A
     * wrapped model that started itself on the call is stopped again, see
     * {@link #stopTheWrappedModelAgainIfItStartedItself(long)}, and the subscription is paused there unless the wrapped
     * model holds it paused already or runs again. The consumer gives up its lease once that {@code stop()} is applied
     * to it, which happens once its lock is free. Once {@code shutdown()} has begun this throws as for a call that was
     * refused.
     */
    private <T> T runWithoutHoldingUpAStop(SubscriptionIdAndSubscriberId key, Supplier<T> call) {
        long[] stopsWhenLetIn = new long[1];
        runInTheWrappedModel(key, true, false, stopsWhenLetIn, null, () -> null);
        @Nullable Delivery delivery = deliveries.get(key.subscriptionId());
        if (delivery != null) {
            delivery.callLetInAfterStop = stopsWhenLetIn[0];
        }
        T result;
        try {
            result = call.get();
        } finally {
            try {
                stopWhatALateCallRan(key, stopsWhenLetIn[0]);
            } finally {
                if (delivery != null) {
                    delivery.callLetInAfterStop = NO_CALL_LET_IN;
                }
            }
        }
        synchronized (wrappedModelStart) {
            if (shutDown) {
                throw new ShutDownMeanwhile();
            }
        }
        return result;
    }

    private void stopWhatALateCallRan(SubscriptionIdAndSubscriberId key, long stopsWhenLetIn) {
        boolean shutDownMeanwhile;
        boolean stoppedMeanwhile;
        synchronized (wrappedModelStart) {
            shutDownMeanwhile = shutDown;
            stoppedMeanwhile = stoppedSince(key, stopsWhenLetIn);
        }
        if (!shutDownMeanwhile && !stoppedMeanwhile) {
            return;
        }
        String subscriptionId = key.subscriptionId();
        stopTheWrappedModelAgainIfItStartedItself(stopsWhenLetIn);
        try {
            // A wrapped model that runs again was started after the stop(), by a start(..) or a call allowed while stopped
            if (shutDownMeanwhile || !wrappedModelIsRunning()) {
                if (!isPausedInTheWrappedModel(subscriptionId)) {
                    pauseInTheWrappedModel(subscriptionId);
                }
            }
        } catch (RuntimeException e) {
            log.warn("Could not pause a subscription resumed in the wrapped subscription model after a stop() or shutdown() that did not wait for the resume. Unless this model is shut down, its events wait for the lease or for the next call this model makes to the wrapped model for the subscription (subscriptionId={})", subscriptionId, e);
        }
    }

    // Read under wrappedModelStart only. A consumer the user resumed after that stop() began runs while stopped.
    private boolean stoppedSince(SubscriptionIdAndSubscriberId key, long stopsWhenLetIn) {
        return stoppedByUser.get() && lastStopBegun > stopsWhenLetIn && !mayRunWhileStopped.contains(key);
    }

    private void pauseInTheWrappedModel(String subscriptionId) {
        inTheWrappedModelFor(subscriptionId, () -> {
            delegate.pauseSubscription(subscriptionId);
            return null;
        });
    }

    private void cancelInTheWrappedModel(String subscriptionId) {
        inTheWrappedModelFor(subscriptionId, () -> {
            delegate.cancelSubscription(subscriptionId);
            return null;
        });
    }

    private void stopTheWrappedModel() {
        inTheWrappedModelForEvery(() -> {
            delegate.stop();
            return null;
        });
    }

    private void startTheWrappedModel() {
        inTheWrappedModelForEvery(() -> {
            delegate.start(false);
            return null;
        });
    }

    private void shutDownTheWrappedModel() {
        inTheWrappedModelForEvery(() -> {
            delegate.shutdown();
            return null;
        });
    }

    private boolean wrappedModelIsRunning() {
        return inTheWrappedModelForEvery(delegate::isRunning);
    }

    // Lets no held event through, unlike wrappedModelIsRunning(), for a call that is for no competing subscription. A
    // wrapped model that cannot say counts as not running, so a competing consumer it may hold paused is tried.
    private boolean wrappedModelSaysItRuns() {
        try {
            return callingTheWrappedModel(delegate::isRunning);
        } catch (RuntimeException e) {
            return false;
        }
    }

    private boolean isRunningInTheWrappedModel(String subscriptionId) {
        return inTheWrappedModelFor(subscriptionId, () -> delegate.isRunning(subscriptionId));
    }

    private boolean isPausedInTheWrappedModel(String subscriptionId) {
        return inTheWrappedModelFor(subscriptionId, () -> delegate.isPaused(subscriptionId));
    }

    /**
     * Runs a call this model makes into the wrapped model for a step of its own on the subscription, such as a pause on
     * a loss of the lease, or a look at whether the wrapped model runs the subscription before it acts. Every event of
     * the subscription that waits in {@link #awaitTheLease} when the call begins, or that comes while the call runs, is
     * delivered as it would be without the lease strategy. A wrapped model can wait in the call for the thread that
     * delivers such an event, as one does that waits for a running action in {@code pauseSubscription}, or that delivers
     * while holding a lock the call takes.
     */
    private <T> T inTheWrappedModelFor(String subscriptionId, Supplier<T> call) {
        @Nullable Delivery delivery = deliveries.get(subscriptionId);
        return lettingThrough(delivery == null ? List.of() : List.of(delivery), call);
    }

    /**
     * As {@link #inTheWrappedModelFor}, for a call that stops, starts or shuts down the wrapped model, or asks whether
     * it runs. It lets the waiting events of every competing subscription through, not only those of the subscription
     * the caller acts for. Every call that may start the wrapped model asks whether it runs first, so a grant, resume or
     * subscribe that runs one competing subscription in the wrapped model lets the events of all the others through.
     */
    private <T> T inTheWrappedModelForEvery(Supplier<T> call) {
        return lettingThrough(List.copyOf(deliveries.values()), call);
    }

    // Delivers every event that waits in awaitTheLease without the lease, as a call for every subscription does, for a
    // start(..) that may make no call into the wrapped model for a subscription the wrapped model runs anyway
    private void letEveryWaitingEventThrough() {
        lettingThrough(List.copyOf(deliveries.values()), () -> null);
    }

    // A wrapped model can still hand over an event it read before a cancel, and no later call of this model would let
    // it through, so it is delivered as it would be without the lease strategy
    private void forgetTheDeliveriesOf(String subscriptionId) {
        @Nullable Delivery delivery = deliveries.remove(subscriptionId);
        if (delivery != null) {
            delivery.forget();
        }
    }

    /**
     * Runs a call into the wrapped model. Each event of the subscriptions in {@code waiting} that waits in
     * {@link #awaitTheLease} when the call begins, or that comes while it runs, goes to the action without the lease.
     * The node that holds the lease can deliver such an event too, so it can arrive twice, but it is never dropped.
     */
    private <T> T lettingThrough(List<Delivery> waiting, Supplier<T> call) {
        waiting.forEach(Delivery::callBegins);
        try {
            return callingTheWrappedModel(call);
        } finally {
            waiting.forEach(Delivery::callReturned);
        }
    }

    // Counted for every call this model makes into the wrapped model, so an event the wrapped model delivers on the
    // thread that makes the call is not held by the call it waits inside, see awaitTheLease
    private <T> T callingTheWrappedModel(Supplier<T> call) {
        int[] calls = callsIntoTheWrappedModelOnThisThread.get();
        calls[0]++;
        try {
            return call.get();
        } finally {
            calls[0]--;
        }
    }

    /**
     * Runs {@code action} for an event once this node may deliver it for the subscription, see
     * {@link #awaitTheLease}. When an interrupt ended the wait and the interrupt flag was clear as the event came, the
     * action runs with the flag clear, and the flag is set again once the action returns or throws. Otherwise the flag is not
     * changed here, so an interrupt that comes after the wait, or while the lease strategy is asked and it then reports
     * the lease held, can be set while the action runs.
     */
    private Consumer<CloudEvent> deliveredUnderTheLease(SubscriptionIdAndSubscriberId key, Delivery delivery, Consumer<CloudEvent> action) {
        return event -> {
            boolean interruptedWhileWaiting = awaitTheLease(key, delivery);
            try {
                action.accept(event);
            } finally {
                if (interruptedWhileWaiting) {
                    Thread.currentThread().interrupt();
                }
            }
        };
    }

    /**
     * Returns once this node may deliver an event for the subscription. It may while the lease strategy reports the
     * lease held and no {@code stop()} has begun since a resume or subscribe of the subscription that it does not wait
     * for. Until then the event waits here, on the thread the wrapped model delivers it on, and is neither run nor
     * skipped. A lease strategy that throws counts as reporting the lease not held.
     * <p>
     * It also returns without the lease strategy reporting the lease held, and the event is then delivered as it would
     * be without the lease strategy. It is never dropped, but the node that holds the lease can deliver it too, so it
     * can arrive twice. These are the causes:
     * <ul>
     * <li>A call this model makes into the wrapped model, see {@link #lettingThrough}. One for this subscription, to
     * pause or cancel it or to ask whether the wrapped model runs it or holds it paused, lets its events through. One
     * for the wrapped model as a whole, to start, stop or shut it down or to ask whether it runs, lets the events of
     * every subscription through, see {@link #inTheWrappedModelForEvery}. This model asks whether the wrapped model
     * runs before every call that may start it, so a grant, resume or subscribe that runs another competing
     * subscription in the wrapped model lets this event through, and {@code stop()} asks the same before it stops the
     * wrapped model. A {@code start(..)} lets every waiting event through also when it makes no such call. The event
     * goes once such a call has begun since it started waiting, or while one is under way.</li>
     * <li>A forgotten subscription. Once a cancel has forgotten the subscription, or a subscribe of it has thrown, no
     * later call of this model is for it, so an event the wrapped model still hands over goes at once.</li>
     * <li>The delivering thread itself. An event the wrapped model delivers on a thread that is inside any call this
     * model makes into it, such as a subscribe or a resume, goes at once, since that call can't return while the event
     * waits.</li>
     * <li>An interrupt. The thread is interrupted during a wait between two looks at the lease, or has its interrupt
     * flag set as such a wait begins. A lease strategy whose {@code hasLock} clears the interrupt flag can take an
     * interrupt, one that comes while it is asked or one already set when the event came, and the event then goes on
     * waiting.</li>
     * <li>{@code shutdown()}, once it has begun.</li>
     * </ul>
     * Nothing else ends the wait, so an event a wrapped model delivers while this model is stopped waits for the lease
     * or for one of these causes.
     * <p>
     * Returns {@code true} when an interrupt ended the wait and the interrupt flag was clear as the event came, and the
     * flag is then clear. When an interrupt ended the wait and the flag was set as the event came, the flag is set again
     * and this returns {@code false}. This changes the flag in no other case, though a lease strategy whose
     * {@code hasLock} clears it can.
     */
    private boolean awaitTheLease(SubscriptionIdAndSubscriberId key, Delivery delivery) {
        boolean interruptedBefore = Thread.currentThread().isInterrupted();
        long callsSeen = delivery.callsBegun();
        long waitMillis = DELIVERY_FIRST_WAIT_MILLIS;
        boolean waited = false;
        while (!shutDown && !mayDeliver(key, delivery)) {
            if (!waited) {
                logDebug("Holding an event until this node may deliver it (subscriberId={}, subscriptionId={})", key.subscriberId(), key.subscriptionId());
                // A wrapped model started directly delivers here, and nothing registers the subscription for its lease
                if (notStartedSinceBuilt && delivery.warnedOfNoStart.compareAndSet(false, true)) {
                    log.warn("Holding the events of subscription {} until this node holds its lease, which it doesn't compete for until start() is called on the {}. Call start() on the {}, not on the subscription model it wraps (subscriberId={}, subscriptionId={})",
                            key.subscriptionId(), CompetingConsumerSubscriptionModel.class.getSimpleName(), CompetingConsumerSubscriptionModel.class.getSimpleName(), key.subscriberId(), key.subscriptionId());
                }
                waited = true;
            }
            boolean called;
            try {
                called = callsIntoTheWrappedModelOnThisThread.get()[0] > 0 || delivery.calledSince(callsSeen, waitMillis);
            } catch (InterruptedException e) {
                logDebug("Delivering an event without the lease, since the thread was interrupted (subscriberId={}, subscriptionId={})", key.subscriberId(), key.subscriptionId());
                if (interruptedBefore) {
                    Thread.currentThread().interrupt();
                    return false;
                }
                return true;
            }
            if (called) {
                logDebug("Delivering an event without the lease, since this model calls the wrapped model for the subscription or has forgotten it (subscriberId={}, subscriptionId={})", key.subscriberId(), key.subscriptionId());
                return false;
            }
            waitMillis = Math.min(waitMillis * 2, DELIVERY_MAX_WAIT_MILLIS);
        }
        return false;
    }

    private boolean mayDeliver(SubscriptionIdAndSubscriberId key, Delivery delivery) {
        long letIn = delivery.callLetInAfterStop;
        if (letIn != NO_CALL_LET_IN) {
            synchronized (wrappedModelStart) {
                if (stoppedSince(key, letIn)) {
                    return false;
                }
            }
        }
        try {
            return hasLock(key.subscriptionId(), key.subscriberId());
        } catch (RuntimeException e) {
            logDebug("Could not find out whether this node holds the lease, so the event waits (subscriberId={}, subscriptionId={})", key.subscriberId(), key.subscriptionId(), e);
            return false;
        }
    }

    private static final long NO_CALL_LET_IN = -1;

    // What an event of a competing subscription that waits for the lease looks at, see awaitTheLease. A ReentrantLock
    // rather than the monitor, so a virtual thread that waits here gives up its carrier thread on JDK 21 to 23 too.
    private static final class Delivery {
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition callBegun = lock.newCondition();
        // Calls this model made into the wrapped model for its own steps on the subscription, begun and under way
        private long callsBegun;
        private int callsUnderWay;
        // The last stop() begun when a resume or subscribe that stop() does not wait for was let in, while it runs
        private volatile long callLetInAfterStop = NO_CALL_LET_IN;
        // Set once this model has forgotten the subscription, after which no call of this model lets an event through
        private boolean forgotten;
        // Set once a held event has logged that this model was never started
        private final AtomicBoolean warnedOfNoStart = new AtomicBoolean();

        private long callsBegun() {
            lock.lock();
            try {
                return callsBegun;
            } finally {
                lock.unlock();
            }
        }

        private void callBegins() {
            lock.lock();
            try {
                callsBegun++;
                callsUnderWay++;
                callBegun.signalAll();
            } finally {
                lock.unlock();
            }
        }

        private void callReturned() {
            lock.lock();
            try {
                callsUnderWay--;
            } finally {
                lock.unlock();
            }
        }

        private void forget() {
            lock.lock();
            try {
                forgotten = true;
                callBegun.signalAll();
            } finally {
                lock.unlock();
            }
        }

        // Whether a call has begun since callsSeen or is under way, or the subscription is forgotten, waiting at most
        // waitMillis for one of them
        private boolean calledSince(long callsSeen, long waitMillis) throws InterruptedException {
            lock.lock();
            try {
                if (!forgotten && callsUnderWay == 0 && callsBegun == callsSeen) {
                    callBegun.await(waitMillis, TimeUnit.MILLISECONDS);
                }
                return forgotten || callsUnderWay > 0 || callsBegun != callsSeen;
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * Runs a call that starts the wrapped model or runs a subscription there, first starting the wrapped model when
     * {@code startIt} is set and it is not running. Throws {@link StoppedMeanwhile} instead while this model is
     * stopped, unless {@code key} names a consumer that may run then. {@code key} is null for a subscription that does
     * not compete. A {@code start(..)} applied after a later {@code stop()} began is refused the same way, also once a
     * {@code start(..)} after that {@code stop()} has begun.
     * <p>
     * A caller decides to run a subscription from what holds before it gets here, without the monitor, and a
     * {@code stop()} can overtake it in between. So the check happens here, under the lock {@code stop()} holds while it
     * records that this model is stopped, and {@code stop()} waits for every call let through before that to return
     * before it stops the wrapped model. A {@code stop()} that has returned is then never followed by a start of the
     * wrapped model, or a subscription run there, that it overtook. A call that {@code stop()} refuses is refused at
     * once, also while {@code stop()} waits for those. One for a consumer that may run while this model is stopped
     * waits for the wrapped model to be stopped first, unless its own thread runs one of them.
     * <p>
     * Once {@code shutdown()} has begun every call is refused with {@link ShutDownMeanwhile}, and {@code shutdown()}
     * waits for each one let through before that to return before it shuts the wrapped model down.
     */
    private <T> T runInTheWrappedModel(@Nullable SubscriptionIdAndSubscriberId key, boolean startIt, Supplier<T> call) {
        return runInTheWrappedModel(key, startIt, false, null, null, call);
    }

    /**
     * Runs a call the user makes for a subscription that does not compete, which runs while this model is stopped too.
     * {@code stop()} still waits for it when it is under way, and it waits for a {@code stop()} that is stopping the
     * wrapped model, so it comes either before that {@code stop()} or after it.
     */
    private <T> T runInTheWrappedModelAlsoWhileStopped(Supplier<T> call) {
        return runInTheWrappedModel(null, false, true, null, null, call);
    }

    // stopsWhenLetIn receives the last stop() that had begun when the call was let in. The competing consumers that
    // starting the wrapped model finds paused there by a stop of it are added to pausedByAStop when it is given, and
    // tried on threads of their own otherwise.
    private <T> T runInTheWrappedModel(@Nullable SubscriptionIdAndSubscriberId key, boolean startIt, boolean alsoWhileStopped, long @Nullable [] stopsWhenLetIn, @Nullable Set<SubscriptionIdAndSubscriberId> pausedByAStop, Supplier<T> call) {
        int[] runsOnThisThread = runsInTheWrappedModelOnThisThread.get();
        @Nullable Lifecycle start = lifecycleAppliedOnThisThread.get();
        @Nullable SubscriptionLock held = key == null ? null : subscriptionLocks.get(key.subscriptionId());
        @Nullable SubscriptionLock heldHere = held != null && held.lock.isHeldByCurrentThread() ? held : null;
        synchronized (wrappedModelStart) {
            while (true) {
                // shutdown() waits for every call let through before it began, and lets none through after
                if (shutDown) {
                    throw new ShutDownMeanwhile();
                }
                // A start(..) applied after a later stop() began, or a call that took the lock or began waiting for it
                // before that stop(), comes before
                // that stop() in the order the calls began. In that order it would have run here before the stop()
                // stopped it again, which a start(..) after the stop() cannot undo.
                if (!alsoWhileStopped && start != null && lastStopBegun > start.id()) {
                    throw new StoppedMeanwhile(start.id(), lastStopBegun);
                }
                if (!alsoWhileStopped && start == null && heldHere != null && lastStopBegun > heldHere.lastStopBegunWhenTaken) {
                    long after = heldHere.lastStopBegunWhenTaken;
                    // What the call decides next on the same hold of the lock comes after the stop()
                    decidesFromNow(heldHere);
                    throw new StoppedMeanwhile(after, lastStopBegun);
                }
                if (!alsoWhileStopped && stoppedByUser.get() && (key == null || !mayRunWhileStopped.contains(key))) {
                    // Any other call comes after that stop(), so the stop() pauses nothing it refused
                    throw new StoppedMeanwhile(stopInEffect, stopInEffect);
                }
                if (stoppingTheWrappedModel == 0 || runsOnThisThread[0] > 0) {
                    break;
                }
                try {
                    wrappedModelStart.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while waiting for the wrapped subscription model to be stopped", e);
                }
            }
            runsInTheWrappedModel++;
            if (stopsWhenLetIn != null) {
                stopsWhenLetIn[0] = lastStopBegun;
            }
            if (stoppedByUser.get()) {
                stopThatLetACallRun = stopInEffect;
            }
        }
        runsOnThisThread[0]++;
        try {
            if (startIt && !wrappedModelIsRunning()) {
                Set<SubscriptionIdAndSubscriberId> paused = pausedByTheStoppedWrappedModel(key);
                try {
                    startTheWrappedModel();
                } finally {
                    // Also when the start took effect and then threw
                    if (pausedByAStop == null) {
                        resumeOnceThisNodeHoldsTheirLease(paused);
                    } else {
                        pausedByAStop.addAll(paused);
                    }
                }
            }
            return call.get();
        } finally {
            runsOnThisThread[0]--;
            synchronized (wrappedModelStart) {
                runsInTheWrappedModel--;
                wrappedModelStart.notifyAll();
            }
        }
    }

    // Lets the consumer run while this model is stopped, and false instead when stopSeen is no longer the stop() in effect
    private boolean mayRunUnderTheSameStop(SubscriptionIdAndSubscriberId key, long stopSeen) {
        @Nullable SubscriptionLock held = subscriptionLocks.get(key.subscriptionId());
        synchronized (wrappedModelStart) {
            if (!stoppedByUser.get() || stopInEffect != stopSeen) {
                return false;
            }
            mayRunWhileStopped.add(key);
            if (held != null && held.lock.isHeldByCurrentThread() && held.lastStopBegunWhenTaken == lastStopBegun) {
                held.mayRunWhenTaken = true;
            }
            return true;
        }
    }

    /**
     * Whether a grant may run the consumer. A grant holding the consumer's lock comes before every {@code stop()} that
     * began since its thread took the lock, so it decides from what held then. Running the consumer in the wrapped
     * model is then refused for those {@code stop()} calls, which pause it as by the user once they are applied to it.
     */
    private boolean mayRunOnAGrant(SubscriptionIdAndSubscriberId key) {
        @Nullable SubscriptionLock held = subscriptionLocks.get(key.subscriptionId());
        synchronized (wrappedModelStart) {
            if (held != null && held.lock.isHeldByCurrentThread() && lastStopBegun > held.lastStopBegunWhenTaken) {
                return held.mayRunWhenTaken;
            }
            return !stoppedByUser.get() || mayRunWhileStopped.contains(key);
        }
    }

    // Thrown instead of starting the wrapped model, or running a subscription there, while this model is stopped, or
    // for a start(..) applied after a later stop() began. Refused by the stop() calls numbered above after and up to upTo.
    private static final class StoppedMeanwhile extends IllegalStateException {
        private final long after;
        private final long upTo;

        private StoppedMeanwhile(long after, long upTo) {
            super(CompetingConsumerSubscriptionModel.class.getSimpleName() + " was stopped, so the wrapped subscription model is not started");
            this.after = after;
            this.upTo = upTo;
        }

        private LaterStops laterStops() {
            return new LaterStops(after, upTo);
        }
    }

    /**
     * The {@code stop()} calls numbered above {@code after} and up to {@code upTo}, for a consumer a call meant to run
     * before them and did not, since one of them refused it or the call failed. Each of them pauses the consumer as by
     * the user when it finds it paused or waiting, as it would have had the call run it first. Kept in the state that
     * call recorded, so whatever records the consumer anew afterwards drops it.
     */
    private record LaterStops(long after, long upTo) {
        private boolean include(Lifecycle stop) {
            return stop.id() > after && stop.id() <= upTo;
        }

        private boolean includeAny() {
            return upTo > after;
        }

        // The ones numbered at most stop
        private LaterStops upToStop(long stop) {
            return new LaterStops(after, Math.min(upTo, stop));
        }
    }

    // Every stop() that comes after the call under way on this thread for the consumer, which is every one that began
    // after the start(..) applied on this thread, or else after this thread took the consumer's lock
    private LaterStops stopsAfterThisCall(SubscriptionIdAndSubscriberId key) {
        @Nullable Lifecycle start = lifecycleAppliedOnThisThread.get();
        if (start != null) {
            return new LaterStops(start.id(), Long.MAX_VALUE);
        }
        @Nullable SubscriptionLock held = subscriptionLocks.get(key.subscriptionId());
        if (held != null && held.lock.isHeldByCurrentThread()) {
            return new LaterStops(held.lastStopBegunWhenTaken, Long.MAX_VALUE);
        }
        synchronized (wrappedModelStart) {
            return new LaterStops(lastStopBegun, Long.MAX_VALUE);
        }
    }

    private LaterStops stopsAfter(SubscriptionIdAndSubscriberId key, Throwable failure) {
        return failure instanceof StoppedMeanwhile refused ? refused.laterStops() : stopsAfterThisCall(key);
    }

    // The stop() calls that have begun since the call under way on this thread, or null when none has. Unlike
    // stopsAfterThisCall, for a call that returns, so a stop() that begins once it has does not count.
    private @Nullable LaterStops stopsSoFarAfterThisCall(SubscriptionIdAndSubscriberId key) {
        LaterStops after = stopsAfterThisCall(key);
        synchronized (wrappedModelStart) {
            return lastStopBegun > after.after() ? new LaterStops(after.after(), lastStopBegun) : null;
        }
    }

    // The stop() calls that began while a register made by the call under way on this thread was under way, or null
    // when none did
    private @Nullable LaterStops stopsWhileRegistering(SubscriptionIdAndSubscriberId key, long stopsBeforeTheRegister) {
        @Nullable LaterStops sinceThisCall = stopsSoFarAfterThisCall(key);
        if (sinceThisCall == null || sinceThisCall.upTo() <= stopsBeforeTheRegister) {
            return null;
        }
        return new LaterStops(Math.max(sinceThisCall.after(), stopsBeforeTheRegister), sinceThisCall.upTo());
    }

    // Thrown instead of starting the wrapped model, or running a subscription there, once shutdown() has begun
    private static final class ShutDownMeanwhile extends IllegalStateException {
        private ShutDownMeanwhile() {
            super(CompetingConsumerSubscriptionModel.class.getSimpleName() + " is shut down, so the wrapped subscription model is not started");
        }
    }

    private CompetingConsumerSubscription makeCompetingConsumerSubscription(BeingMade beingMade, SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        SubscriptionIdAndSubscriberId key = beingMade.key;
        String subscriptionId = key.subscriptionId();
        logDebug("Starting CompetingConsumer subscription (subscriberId={}, subscriptionId={})", key.subscriberId(), subscriptionId);
        Delivery delivery = new Delivery();
        Consumer<CloudEvent> delivered = deliveredUnderTheLease(key, delivery, action);
        beingMade.delivery = delivery;
        deliveries.put(subscriptionId, delivery);
        AtomicBoolean madeInTheWrappedModel = beingMade.madeInTheWrappedModel;
        beingMade.waiting = new CompetingConsumerState.Waiting(() -> {
            logDebug("Starting delegated CompetingConsumer subscription after waiting (subscriberId={}, subscriptionId={})", key.subscriberId(), subscriptionId);
            if (isPausedInTheWrappedModel(subscriptionId)) {
                return resumeInTheWrappedModel(key);
            }
            // A start() called on the wrapped model itself resumes what it holds paused, this subscription too. One
            // running there under this id that this model never made is someone else's, and subscribing refuses it.
            return runWithoutHoldingUpAStop(key, () -> {
                if (madeInTheWrappedModel.get() && isRunningInTheWrappedModel(subscriptionId)) {
                    return new CompetingConsumerSubscription(subscriptionId, key.subscriberId());
                }
                Subscription made = callingTheWrappedModel(() -> delegate.subscribe(subscriptionId, filter, startAt, delivered));
                madeInTheWrappedModel.set(true);
                return made;
            });
        }, () -> {
            Subscription made = callingTheWrappedModel(() -> delegate.subscribePaused(subscriptionId, filter, startAt, delivered));
            madeInTheWrappedModel.set(true);
            return made;
        }, madeInTheWrappedModel);
        try {
            return takeTheStepsOfASubscribe(beingMade, filter, startAt, delivered);
        } catch (Throwable e) {
            if (deliveries.remove(subscriptionId, delivery)) {
                delivery.forget();
            }
            throw e;
        }
    }

    private CompetingConsumerSubscription takeTheStepsOfASubscribe(BeingMade beingMade, SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> delivered) {
        SubscriptionIdAndSubscriberId key = beingMade.key;
        String subscriptionId = key.subscriptionId();
        while (true) {
            Step step;
            SubscriptionLock stepLock = lockSubscription(subscriptionId);
            try {
                step = nextStep(beingMade);
            } finally {
                stepLock.unlock();
            }
            try {
                switch (step) {
                    case DONE -> {
                        return new CompetingConsumerSubscription(subscriptionId, key.subscriberId(), beingMade.subscription);
                    }
                    case REGISTER -> {
                        beingMade.registered = true;
                        registerCompetingConsumer(subscriptionId, key.subscriberId());
                        beingMade.registrationReturned = true;
                    }
                    case SUBSCRIBE_PAUSED -> {
                        try {
                            beingMade.subscription = callingTheWrappedModel(() -> delegate.subscribePaused(subscriptionId, filter, startAt, delivered));
                            beingMade.madeInTheWrappedModel.set(true);
                        } catch (UnsupportedOperationException e) {
                            logDebug("Wrapped model cannot hold the subscription paused, competing for the lease straight away (subscriberId={}, subscriptionId={})", key.subscriberId(), subscriptionId);
                            beingMade.refusesSubscribePaused = true;
                        }
                    }
                    case SUBSCRIBE -> {
                        // Neither stop() nor shutdown() waits for this, since over a durable model it reads the stored
                        // position, which retries for as long as the database cannot be reached. The next step decides
                        // from what holds once it returns, as after any other step.
                        beingMade.subscription = callingTheWrappedModel(() -> delegate.subscribe(subscriptionId, filter, startAt, delivered));
                        beingMade.madeInTheWrappedModel.set(true);
                    }
                }
            } catch (Throwable e) {
                SubscriptionLock lock = lockSubscription(subscriptionId);
                try {
                    if (recordOrTakeBackAfterAFailedStep(beingMade, e)) {
                        return new CompetingConsumerSubscription(subscriptionId, key.subscriberId(), beingMade.subscription);
                    }
                } finally {
                    lock.unlock();
                }
                throw e;
            }
        }
    }

    // What one subscribe has made so far. Only the subscribing thread writes it, except for cancelled,
    // resumedMeanwhile and what is tried again once it is made, which other calls set under the monitor. stop() reads
    // registrationReturned, set once the registration has returned, to give up a lease it won, and then clears it and
    // registered under the subscription's lock, while the subscribing thread is in a step that touches neither.
    private static final class BeingMade {
        private final SubscriptionIdAndSubscriberId key;
        private CompetingConsumerState.@Nullable Waiting waiting;
        private boolean registered;
        private volatile boolean registrationReturned;
        private boolean refusesSubscribePaused;
        // The last stop() that had begun when the step making the subscription in the wrapped model was decided
        private long stopsBeforeTheWrappedModelMadeIt;
        private @Nullable Subscription subscription;
        // Set once the wrapped model has made the subscription for this model, and kept for as long as it is recorded
        private final AtomicBoolean madeInTheWrappedModel = new AtomicBoolean();
        private volatile boolean cancelled;
        // Set for a competing subscription before the wrapped model gets its action, so a cancel can forget it
        private volatile @Nullable Delivery delivery;
        // A start(true) since the delegate got it and no stop() after, which resumed only what the delegate knew
        private volatile boolean resumedMeanwhile;
        // A call failed for the subscription while it was being made, or a lease callback found its lock taken, and
        // what failed is tried again once it is. At once when a lease callback was among them, since nothing failed.
        private volatile boolean triedAgainOnceMade;
        private volatile boolean triedAgainAtOnce;
        // As for the try, see reconcileLater(SubscriptionIdAndSubscriberId, boolean, long), read and written under the
        // monitor only
        private long callsWaitedBeforeALeaseCallback = NO_LEASE_CALLBACK;

        private BeingMade(SubscriptionIdAndSubscriberId key) {
            this.key = key;
        }
    }

    // A step subscribe takes without the subscription's lock, or DONE once the subscription is recorded
    private enum Step {
        REGISTER, SUBSCRIBE_PAUSED, SUBSCRIBE, DONE
    }

    /**
     * Decides the next step of a subscribe from what holds now, and records the subscription once nothing is left to
     * do. A lease callback for it before then finds nothing recorded and does nothing, so the lease is asked for here.
     * <p>
     * The wrapped model gets the subscription through {@code subscribePaused}, and it is resumed there only here, once
     * this node holds its lease. While this model is stopped that happens without competing, since a lease won while
     * stopped would lock every other node out of a subscription this node does not serve. A wrapped model that refuses
     * {@code subscribePaused} gets it through {@code subscribe} once this node wins the lease, and one it runs while
     * stopped may keep running.
     * <p>
     * Nothing here cancels a subscription in the wrapped model unless the user cancelled it, since cancelling can delete
     * the position a durable subscription has stored. Anything made for a state that no longer holds, or that a step
     * here fails to resume, is paused in the wrapped model and waits for a grant. A failure before the wrapped model has
     * made anything makes the subscribe throw. Once it has, the subscription is recorded, and what failed is tried again
     * once the subscribe returns.
     */
    private Step nextStep(BeingMade beingMade) {
        SubscriptionIdAndSubscriberId key = beingMade.key;
        String subscriptionId = key.subscriptionId();
        if (shutDown || beingMade.cancelled) {
            throw notMade(beingMade);
        }
        boolean stopped;
        long stopSeen;
        long stopsSeen;
        synchronized (wrappedModelStart) {
            stopped = stoppedByUser.get();
            stopSeen = stopInEffect;
            stopsSeen = lastStopBegun;
        }
        if (beingMade.subscription == null) {
            beingMade.stopsBeforeTheWrappedModelMadeIt = stopsSeen;
            try {
                if (stopped && !beingMade.refusesSubscribePaused) {
                    // A registration that a stop() overtook may have won a lease this node must not hold while stopped
                    giveUpTheRegistration(beingMade);
                    return Step.SUBSCRIBE_PAUSED;
                } else if (!beingMade.registered) {
                    return Step.REGISTER;
                } else if (!hasLock(subscriptionId, key.subscriberId())) {
                    if (stopped) {
                        giveUpTheRegistration(beingMade);
                    }
                    return recordWaiting(beingMade);
                } else if (stopped) {
                    if (!mayRunUnderTheSameStop(key, stopSeen)) {
                        // A start(..) or another stop() came in between, so the step is decided again from there
                        return nextStep(beingMade);
                    }
                    return Step.SUBSCRIBE;
                }
                startTheWrappedModelIfStopped(key);
                return beingMade.refusesSubscribePaused ? Step.SUBSCRIBE : Step.SUBSCRIBE_PAUSED;
            } catch (StoppedMeanwhile e) {
                // A stop() overtook this step, so the step is decided again from there
                return nextStep(beingMade);
            } catch (RuntimeException e) {
                // Nothing is made yet, so giving up the registration frees the id
                takeBack(beingMade, e);
                throw e;
            }
        }
        boolean waits;
        try {
            if (stopped) {
                // Only a subscription the wrapped model runs may run while stopped, so one it holds paused gives up that
                if (!mayRunWhileStopped.contains(key) || !isRunningInTheWrappedModel(subscriptionId)) {
                    mayRunWhileStopped.remove(key);
                    waits = true;
                } else {
                    waits = !hasLock(subscriptionId, key.subscriberId());
                }
            } else if (!beingMade.registered) {
                return Step.REGISTER;
            } else if (!hasLock(subscriptionId, key.subscriberId())) {
                waits = true;
            } else {
                if (isPausedInTheWrappedModel(subscriptionId)) {
                    resumeInTheWrappedModel(key);
                } else {
                    // A stop() and a start(..) can both come while the wrapped model makes the subscription, and the
                    // wrapped model is then stopped
                    startTheWrappedModelIfStopped(key);
                }
                waits = false;
            }
        } catch (StoppedMeanwhile e) {
            // A stop() overtook this step, and the subscription waits for start(), as one made while stopped does
            waits = true;
        } catch (RuntimeException e) {
            log.warn("Could not run CompetingConsumer in the wrapped subscription model, so it waits for a grant of its lease, which tries it again (subscriberId={}, subscriptionId={})",
                    key.subscriberId(), subscriptionId, e);
            waits = true;
        }
        Step next = waits ? waitForAGrant(beingMade) : recordRunning(key);
        if (stopped) {
            stopTheWrappedModelAgainIfItStartedItself(beingMade.stopsBeforeTheWrappedModelMadeIt);
        }
        return next;
    }

    /**
     * A {@code stop()} that began while the wrapped model made a subscription does not wait for it, and can stop the
     * wrapped model before it is made. A wrapped model that starts itself on a subscribe then runs again, where the
     * {@code stop()} would have left it stopped had the subscribe come first. It is stopped again here, the way
     * {@code stop()} stops it, so a call that would start it meanwhile waits for this. Nothing is stopped once a
     * {@code start(..)} has begun or is waiting, or once a call allowed while stopped has run in the wrapped model since
     * the last {@code stop()}, since that call may have started it.
     */
    private void stopTheWrappedModelAgainIfItStartedItself(long stopsBefore) {
        synchronized (wrappedModelStart) {
            while (stoppingTheWrappedModel != 0) {
                try {
                    wrappedModelStart.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            if (shutDown || !stoppedByUser.get() || lastStopBegun <= stopsBefore || startsWaiting > 0 || runsInTheWrappedModel > 0
                    || !mayRunWhileStopped.isEmpty() || stopThatLetACallRun == stopInEffect) {
                return;
            }
            stoppingTheWrappedModel = stopInEffect;
        }
        try {
            if (wrappedModelIsRunning()) {
                stopTheWrappedModel();
            }
        } catch (RuntimeException e) {
            log.warn("Could not stop the wrapped subscription model again after it started itself to make a subscription while this model was stopped", e);
        } finally {
            synchronized (wrappedModelStart) {
                stoppingTheWrappedModel = 0;
                wrappedModelStart.notifyAll();
            }
        }
    }

    private Step recordRunning(SubscriptionIdAndSubscriberId key) {
        competingConsumers.put(key, new CompetingConsumer(key, new CompetingConsumerState.Running()));
        return Step.DONE;
    }

    /**
     * Pauses what the subscribe made in the wrapped model, and records it as waiting for a grant of its lease, which
     * resumes it. It keeps its registration while it may compete, and gives up a lease it holds, so another node or a
     * later grant here runs it. While this model is stopped it gives up the registration instead, and {@code start()}
     * registers it again.
     * <p>
     * One the wrapped model still runs after the pause is recorded as running and keeps its registration, since it still
     * delivers. It keeps a lease it holds, as one that {@code stop()} cannot pause does, and competes for one it lost,
     * as one whose lease-loss callback could not pause it does. One whose registration a {@code stop()} gave up before
     * the wrapped model ran it registers again first.
     * <p>
     * A pause here that throws, and giving up the lease or the registration when that throws, are tried again once the
     * subscribe returns. Any other failure records the subscription as what the wrapped model holds, and is tried
     * again the same way.
     */
    private Step waitForAGrant(BeingMade beingMade) {
        SubscriptionIdAndSubscriberId key = beingMade.key;
        String subscriptionId = key.subscriptionId();
        try {
            if (isRunningInTheWrappedModel(subscriptionId)) {
                boolean pauseFailed = false;
                try {
                    pauseInTheWrappedModel(subscriptionId);
                } catch (RuntimeException e) {
                    log.warn("Could not pause CompetingConsumer in the wrapped subscription model, so the pause is tried again (subscriberId={}, subscriptionId={})", key.subscriberId(), subscriptionId, e);
                    pauseFailed = true;
                }
                if (isRunningInTheWrappedModel(subscriptionId)) {
                    logDebug("Wrapped model still runs the CompetingConsumer after it was paused there, so it stays registered (subscriberId={}, subscriptionId={})", key.subscriberId(), subscriptionId);
                    if (!beingMade.registered) {
                        return Step.REGISTER;
                    }
                    if (pauseFailed) {
                        beingMade.triedAgainOnceMade = true;
                    }
                    return recordRunning(key);
                }
            }
            recordWaiting(beingMade);
            try {
                if (stoppedByUser.get() && !mayRunWhileStopped.contains(key)) {
                    giveUpTheRegistration(beingMade);
                } else if (beingMade.registered && hasLock(subscriptionId, key.subscriberId())) {
                    competingConsumerStrategy.releaseCompetingConsumer(subscriptionId, key.subscriberId());
                }
            } catch (RuntimeException e) {
                log.warn("Could not give up the lease or the registration of CompetingConsumer, so it is tried again (subscriberId={}, subscriptionId={})", key.subscriberId(), subscriptionId, e);
                beingMade.triedAgainOnceMade = true;
            }
            return Step.DONE;
        } catch (RuntimeException e) {
            return recordAfterAFailure(beingMade, e);
        }
    }

    // Once the wrapped model has made the subscription, a failure records it as what the wrapped model holds, and what
    // failed is tried again once the subscribe returns
    private Step recordAfterAFailure(BeingMade beingMade, Throwable failure) {
        SubscriptionIdAndSubscriberId key = beingMade.key;
        log.warn("Could not make CompetingConsumer compete for its lease, so it is recorded and what failed is tried again (subscriberId={}, subscriptionId={})", key.subscriberId(), key.subscriptionId(), failure);
        boolean runs = false;
        try {
            runs = isRunningInTheWrappedModel(key.subscriptionId());
        } catch (RuntimeException e) {
            failure.addSuppressed(e);
        }
        beingMade.triedAgainOnceMade = true;
        return runs ? recordRunning(key) : recordWaiting(beingMade);
    }

    // Registered while this model runs, so the grant starts it. Unregistered while stopped, so start() registers it.
    private Step recordWaiting(BeingMade beingMade) {
        competingConsumers.put(beingMade.key, new CompetingConsumer(beingMade.key, requireNonNull(beingMade.waiting)));
        return Step.DONE;
    }

    // Forgotten here also when the unregister throws, which is tried again, so the next step registers again
    private void giveUpTheRegistration(BeingMade beingMade) {
        if (beingMade.registered) {
            beingMade.registered = false;
            beingMade.registrationReturned = false;
            unregisterCompetingConsumer(beingMade.key.subscriptionId(), beingMade.key.subscriberId());
        }
    }

    // Takes back what the subscribe made and gives up its registration first
    private IllegalStateException notMade(BeingMade beingMade) {
        IllegalStateException failure = new IllegalStateException("Subscription " + beingMade.key.subscriptionId() + " was not made, since " + CompetingConsumerSubscriptionModel.class.getSimpleName()
                + " was shut down or had the subscription cancelled while it was being made");
        takeBack(beingMade, failure);
        return failure;
    }

    /**
     * Takes back what a subscribe has made when it throws, and gives up its registration. Only a subscription the user
     * cancelled is cancelled in the wrapped model, since cancelling can delete the position a durable subscription has
     * stored. Anything else it made is paused there, and one made before a shutdown is left for the wrapped model's own
     * {@code shutdown()}. Every failure on the way is added to {@code failure} as suppressed.
     */
    private void takeBack(BeingMade beingMade, Throwable failure) {
        SubscriptionIdAndSubscriberId key = beingMade.key;
        String subscriptionId = key.subscriptionId();
        mayRunWhileStopped.remove(key);
        if (beingMade.subscription != null) {
            try {
                if (beingMade.cancelled) {
                    cancelInTheWrappedModel(subscriptionId);
                } else if (isRunningInTheWrappedModel(subscriptionId)) {
                    pauseInTheWrappedModel(subscriptionId);
                }
            } catch (Throwable e) {
                failure.addSuppressed(e);
            }
        }
        try {
            giveUpTheRegistration(beingMade);
        } catch (Throwable e) {
            failure.addSuppressed(e);
        }
    }

    // A step without the subscription's lock failed. Answers whether the subscribe returns all the same, which it does
    // once the wrapped model has made the subscription, unless the model was shut down or the id cancelled.
    private boolean recordOrTakeBackAfterAFailedStep(BeingMade beingMade, Throwable failure) {
        if (shutDown || beingMade.cancelled || beingMade.subscription == null) {
            takeBack(beingMade, failure);
            return false;
        }
        recordAfterAFailure(beingMade, failure);
        return true;
    }

    private void pauseSubscription(String subscriptionId, boolean pausedByUser) {
        logDebug("Trying to pause CompetingConsumer subscription (subscriptionId={}, pausedByUser={})", subscriptionId, pausedByUser);
        requireKnown(subscriptionId);
        CompetingConsumer competingConsumer = findFirstCompetingConsumerMatching(cc -> cc.hasSubscriptionId(subscriptionId)).orElse(null);
        if (pausedByUser && competingConsumer != null && competingConsumer.state instanceof CompetingConsumerState.Paused paused && !paused.pausedByUser) {
            logDebug("CompetingConsumer paused by the system, recording it as paused by user and unregistering it from the strategy (subscriptionId={}, subscriberId={})", subscriptionId, competingConsumer.getSubscriberId());
            // A consumer resumed without winning the lease waits for its grant, and the grant resumes it unless the
            // user paused it. Recorded first, so a grant out of the unregister below finds it paused by the user.
            competingConsumers.put(competingConsumer.subscriptionIdAndSubscriberId, competingConsumer.registerPaused(true));
            // A start() called on the wrapped model itself runs it there again, so it is paused there too. When that
            // pause fails, a try pauses it there and unregisters it, and the failure is thrown.
            try {
                if (isRunningInTheWrappedModel(subscriptionId)) {
                    pauseInTheWrappedModel(subscriptionId);
                }
            } catch (Throwable e) {
                reconcileLater(competingConsumer.subscriptionIdAndSubscriberId);
                throw e;
            }
            unregisterCompetingConsumer(competingConsumer.getSubscriptionId(), competingConsumer.getSubscriberId());
            return;
        }
        // The state recorded here, not the delegate's, since a consumer that won its lease before the wrapped model
        // was started is running here and paused there, and pausing it still has to give up the lease
        if (competingConsumer == null ? isPaused(subscriptionId) : competingConsumer.isPaused()) {
            throw new SubscriptionNotRunningException(subscriptionId, "Subscription " + subscriptionId + " is already paused.");
        }

        if (nonCompetingConsumersSubscriptions.contains(subscriptionId)) {
            delegate.pauseSubscription(subscriptionId);
        } else {
            if (competingConsumer == null) {
                logDebug("Failed to find CompetingConsumer for subscription (subscriptionId={}, pausedByUser={})", subscriptionId, pausedByUser);
                // The delegate refuses this as well, but never sees it: an id with no competing consumer here stops at
                // this branch, so returning quietly was the wrapper answering for the delegate, and answering wrongly.
                throw new SubscriptionNotRunningException(subscriptionId);
            } else if (competingConsumer.isWaiting()) {
                logDebug("CompetingConsumer in waiting state, pausing and unregistering from the strategy so the lock passes to another consumer (subscriptionId={}, subscriberId={}, pausedByUser={})", subscriptionId, competingConsumer.getSubscriberId(), pausedByUser);
                // A start() called on the wrapped model itself runs a subscription this model made there held paused,
                // also while it waits here for its lease, so it is paused there first. When that pause fails, the
                // consumer keeps waiting and the failure is thrown.
                if (((CompetingConsumerState.Waiting) competingConsumer.state).madeInTheWrappedModel() && isRunningInTheWrappedModel(subscriptionId)) {
                    pauseInTheWrappedModel(subscriptionId);
                }
                // Only pausedByUser=true reaches a waiting consumer here. The other caller, onConsumeProhibited,
                // only pauses a consumer it already found running, and a waiting one never is. Recorded first, so
                // a synchronous onConsumeProhibited out of the unregister below finds this consumer already
                // paused rather than still waiting.
                competingConsumers.put(competingConsumer.subscriptionIdAndSubscriberId, competingConsumer.registerPausedWhileWaiting());
                // Staying registered would mean competing for the lock while paused. The strategy's own refresh
                // re-registers every consumer that lacks the lock, so a registered-but-paused consumer would win
                // it back and sit on it, and every other node would stay locked out until this one is resumed.
                unregisterCompetingConsumer(competingConsumer.getSubscriptionId(), competingConsumer.getSubscriberId());
            } else {
                try {
                    if (!isPausedInTheWrappedModel(subscriptionId)) {
                        pauseInTheWrappedModel(subscriptionId);
                    }
                } catch (SubscriptionRefusedException e) {
                    // The delegate no longer knows this id, most likely a catch-up subscription whose replay had
                    // already failed before this call. That leaves nothing to pause downstream, but the lease still
                    // needs releasing below, otherwise this node reports itself Running while holding a lease no
                    // delegate is actually serving.
                    logDebug("Delegate refused to pause subscription, continuing to release the lease (subscriptionId={}, subscriberId={})", subscriptionId, competingConsumer.getSubscriberId(), e);
                } catch (Throwable e) {
                    // The pause can take effect before it throws, and the consumer is then paused here too
                    recordAsPausedUnlessItRuns(competingConsumer.subscriptionIdAndSubscriberId, pausedByUser, e);
                    reconcileLater(competingConsumer.subscriptionIdAndSubscriberId);
                    throw e;
                }
                pauseConsumer(competingConsumer, pausedByUser);
                if (pausedByUser) {
                    logDebug("Will unregister competing consumer because subscription was paused explicitly by user (subscriptionId={}, subscriberId={})", subscriptionId, competingConsumer.getSubscriberId());
                    // A user-paused subscription needs an explicit resume to restart, so unregister the competing
                    // consumer: it cannot become leader again until the subscription is explicitly resumed.
                    unregisterCompetingConsumer(competingConsumer.getSubscriptionId(), competingConsumer.getSubscriberId());
                } else {
                    logDebug("Will release competing consumer because subscription was paused by system (subscriptionId={}, subscriberId={})", subscriptionId, competingConsumer.getSubscriberId());
                    // Not paused by the user, so just release the competing consumer so it can re-gain leader
                    // status later without an explicit resume.
                    competingConsumerStrategy.releaseCompetingConsumer(competingConsumer.getSubscriptionId(), competingConsumer.getSubscriberId());
                }
            }
        }
    }

    /**
     * @see SubscriptionModelWrapper#getWrappedSubscriptionModel()
     */
    @Override
    public SubscriptionModel getWrappedSubscriptionModel() {
        return delegate;
    }

    /**
     * This model resolves the start position to find out whether to compete for the subscription. The model it wraps
     * receives the caller's own {@link StartAt} either way and resolves it under its own class, so where the
     * subscription starts is settled there rather than here.
     *
     * @return {@code false}
     * @see SubscriptionModelWrapper#decidesWhereTheSubscriptionStarts()
     */
    @Override
    public boolean decidesWhereTheSubscriptionStarts() {
        return false;
    }

    /**
     * Waits while a {@code start(..)} starts the wrapped model, for as long as the wrapped model takes to start. It does
     * not wait for a resume of a competing subscription that a {@code start(..)} handed to a try.
     *
     * @see SubscriptionModelLifeCycle#shutdown()
     */
    @PreDestroy
    @Override
    public void shutdown() {
        logDebug("Trying to shutdown CompetingConsumer subscription model");
        waitForEveryCallLetIntoTheWrappedModel();
        synchronized (this) {
            tries.values().forEach(Try::wake);
        }
        // Without the subscription locks, which a call waiting for the lease strategy through an outage can hold.
        // Shutting the strategy down first ends a registration waiting between two attempts, and makes each later
        // unregister one attempt.
        competingConsumerStrategy.shutdown();
        competingConsumerStrategy.removeListener(this);
        Set<SubscriptionIdAndSubscriberId> leased = new HashSet<>(registrations.keySet());
        leased.addAll(competingConsumers.keySet());
        nonCompetingConsumersSubscriptions.clear();
        try {
            // A wrapped model that throws may still deliver, so its leases are left to expire rather than given up
            shutDownTheWrappedModel();
            giveUpEveryLeaseOnce(leased);
        } finally {
            competingConsumers.clear();
            registrations.clear();
        }
    }

    /**
     * Lets no further call start the wrapped model or run a subscription there, and waits for each one let through
     * before that to return, so none of them starts the wrapped model or runs a subscription there once it is shut
     * down. A lease callback or a try already under way is refused at its next such call. Like {@code stop()}, this
     * waits for as long as such a call waits inside the wrapped model.
     */
    private void waitForEveryCallLetIntoTheWrappedModel() {
        int runsOnThisThread = runsInTheWrappedModelOnThisThread.get()[0];
        boolean interrupted = false;
        synchronized (wrappedModelStart) {
            shutDown = true;
            wrappedModelStart.notifyAll();
            while (runsInTheWrappedModel > runsOnThisThread) {
                try {
                    wrappedModelStart.wait();
                } catch (InterruptedException e) {
                    // Keeps waiting, since shutting the wrapped model down now would let a call still under way start
                    // it or run a subscription there alongside the shutdown. The interrupt is restored before returning.
                    interrupted = true;
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    // One attempt per lease, each on a thread of its own, since one hanging or throwing must not keep the rest. A lease
    // not given up by the deadline expires on its own after the lease time.
    private void giveUpEveryLeaseOnce(Set<SubscriptionIdAndSubscriberId> leased) {
        List<Thread> releases = new ArrayList<>();
        for (SubscriptionIdAndSubscriberId key : leased) {
            releases.add(Thread.ofPlatform().daemon().name("occurrent-lease-release-on-shutdown-" + key.subscriptionId()).start(() -> {
                try {
                    competingConsumerStrategy.unregisterCompetingConsumer(key.subscriptionId(), key.subscriberId());
                } catch (Throwable e) {
                    log.warn("Could not give up the lease of CompetingConsumer on shutdown, so it expires after the lease time (subscriberId={}, subscriptionId={})", key.subscriberId(), key.subscriptionId(), e);
                }
            }));
        }
        long deadline = System.nanoTime() + LEASE_RELEASE_TIMEOUT_ON_SHUTDOWN.toNanos();
        try {
            for (Thread release : releases) {
                long left = deadline - System.nanoTime();
                if (left > 0) {
                    release.join(Duration.ofNanos(left));
                }
                if (release.isAlive()) {
                    log.warn("Giving up a lease on shutdown took longer than {}, so it expires after the lease time ({})", LEASE_RELEASE_TIMEOUT_ON_SHUTDOWN, release.getName());
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void onConsumeGranted(String subscriptionId, String subscriberId) {
        logDebug("Consumption granted to CompetingConsumer (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
        onLeaseCallback(SubscriptionIdAndSubscriberId.from(subscriptionId, subscriberId), () -> granted(subscriptionId, subscriberId));
    }

    private void granted(String subscriptionId, String subscriberId) {
        CompetingConsumer competingConsumer = competingConsumers.get(SubscriptionIdAndSubscriberId.from(subscriptionId, subscriberId));
        if (competingConsumer == null) {
            logDebug("Failed to find CompetingConsumer, returning (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
            return;
        }
        // The strategy may have decided the grant before this callback got the lock, and a stop(), a pause or a
        // refresh may have given the lease up since. Acting on it would start a subscription without its lease.
        SubscriptionIdAndSubscriberId key = competingConsumer.subscriptionIdAndSubscriberId;
        boolean holdsTheLease;
        try {
            holdsTheLease = hasLock(subscriptionId, subscriberId);
        } catch (Throwable e) {
            log.warn("Could not find out whether CompetingConsumer still holds the lease it was granted, so it is tried again (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId, e);
            reconcileLater(key);
            if (e instanceof Error error) {
                throw error;
            }
            return;
        }
        if (!holdsTheLease) {
            logDebug("CompetingConsumer no longer holds the lease it was granted, ignoring the grant (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
            return;
        }

        // A subscription the user resumed since stop() runs once it wins the lease, on a later grant as much as on the
        // resume itself. Nothing else runs while this model is stopped.
        boolean mayRun = mayRunOnAGrant(key);
        switch (competingConsumer.state) {
            case CompetingConsumerState.Waiting waiting -> {
                if (mayRun) {
                    startWaitingConsumer(competingConsumer);
                } else {
                    logDebug("Won't start waiting consumer because subscription model was explicitly stopped by user (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
                    handBackGrantedLock(competingConsumer);
                }
            }
            case CompetingConsumerState.Paused paused -> {
                if (paused.pausedByUser) {
                    logDebug("Won't resume CompetingConsumer, because it was paused by user (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
                    handBackGrantedLock(competingConsumer);
                } else if (!mayRun) {
                    logDebug("Won't resume system-paused CompetingConsumer because subscription model was explicitly stopped by user (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
                    handBackGrantedLock(competingConsumer);
                } else {
                    // Not as asked for by the user, so a stop() that began since the lock was taken is not undone
                    try {
                        resume(subscriptionId, false);
                    } catch (SubscriptionAlreadyRunningException e) {
                        // The wrapped model runs it until the pause it was asked for takes effect, as a catch-up does
                        // until its replay has ended, so a try resumes it once it is paused there
                        logDebug("CompetingConsumer still runs in the wrapped model after its pause, so it is resumed once it is paused there (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
                        reconcileLater(key);
                    }
                }
            }
            case CompetingConsumerState.PausedWhileWaiting pausedWhileWaiting -> {
                logDebug("Won't start CompetingConsumer, because it was paused while waiting for the lock (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
                handBackGrantedLock(competingConsumer);
            }
            case CompetingConsumerState.Running running -> {
                // One the wrapped model runs already has what this callback would give it, and so does one a resume is
                // registering, which that resume runs once the register returns. One it does not run otherwise, which a
                // failed call or a pause called on the wrapped model itself can cause, is resumed, since no other grant
                // comes for it.
                if (resumedOnceRegistered.contains(key)) {
                    return;
                }
                boolean runs;
                try {
                    runs = isRunningInTheWrappedModel(subscriptionId);
                } catch (Throwable e) {
                    triedAgainAfter(key, e);
                    if (e instanceof Error error) {
                        throw error;
                    }
                    return;
                }
                if (!runs) {
                    if (mayRun) {
                        giveTheLeaseBackIfItThrows(key, running, () -> resumeInTheWrappedModel(key));
                    } else {
                        competingConsumers.put(key, competingConsumer.registerPaused(true));
                        handBackGrantedLock(competingConsumer);
                    }
                }
            }
        }
    }

    /**
     * Unregisters a consumer the strategy just granted the lock to but that the model will not let consume right
     * now, so the lock passes on rather than being held by a consumer that will never act on it.
     */
    private void handBackGrantedLock(CompetingConsumer cc) {
        logDebug("Handing the granted lock back because CompetingConsumer is not allowed to consume right now (subscriberId={}, subscriptionId={})", cc.getSubscriberId(), cc.getSubscriptionId());
        try {
            unregisterCompetingConsumer(cc.getSubscriptionId(), cc.getSubscriberId());
        } catch (Throwable e) {
            triedAgainAfter(cc.subscriptionIdAndSubscriberId, e);
            if (e instanceof Error error) {
                throw error;
            }
        }
    }

    @Override
    public void onConsumeProhibited(String subscriptionId, String subscriberId) {
        logDebug("Consumption prohibited for CompetingConsumer (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
        onLeaseCallback(SubscriptionIdAndSubscriberId.from(subscriptionId, subscriberId), () -> prohibited(subscriptionId, subscriberId));
    }

    private void prohibited(String subscriptionId, String subscriberId) {
        SubscriptionIdAndSubscriberId subscriptionIdAndSubscriberId = SubscriptionIdAndSubscriberId.from(subscriptionId, subscriberId);
        CompetingConsumer competingConsumer = competingConsumers.get(subscriptionIdAndSubscriberId);
        if (competingConsumer == null) {
            logDebug("CompetingConsumer couldn't be found when calling onConsumeProhibited (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
            return;
        }

        if (competingConsumer.isRunning()) {
            logDebug("CompetingConsumer is running, will pause subscription and consumers (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
            // Pausing (not just stopping delivery) is what lets resume later use the checkpoint. Without it:
            // 1. Subscriber 1 loses lock
            // 2. An event is published (A)
            // 3. Subscriber 2 doesn't have lock yet
            // 4. No one has the lock is detected, Subscriber 2 is resumed, but A was already missed.
            // Also only one subscription can exist per id per CatchupSubscriptionModel instance.
            try {
                pauseSubscription(subscriptionId, false);
            } catch (Throwable e) {
                // An Error too, which is then thrown, and the strategy tells its other listeners first
                log.warn("Could not pause CompetingConsumer after this node lost its lease, so the pause is tried again (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId, e);
                reconcileLater(subscriptionIdAndSubscriberId);
                if (e instanceof Error error) {
                    throw error;
                }
            }
        } else if (competingConsumer.isPaused()) {
            logDebug("CompetingConsumer is already paused, won't do anything (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
        } else {
            logDebug("CompetingConsumer is neither running nor paused, won't do anything (subscriberId={}, subscriptionId={}, state={})", subscriberId, subscriptionId, competingConsumer.state.getClass().getSimpleName());
        }
    }

    // A lease callback that found the lock taken, for a consumer that is recorded or that a subscribe is making. The
    // try decides from what holds once it has the lock, and for a consumer being made it starts once the subscribe
    // has returned. A callback for any other consumer is not this model's, since the lease strategy tells every
    // listener about every consumer.
    // callsWaitedBefore is as for the try, see reconcileLater(SubscriptionIdAndSubscriberId, boolean, long)
    private synchronized void reconcileLaterIfKnown(SubscriptionIdAndSubscriberId key, long callsWaitedBefore) {
        if (isKnown(key)) {
            reconcileLater(key, true, callsWaitedBefore);
        }
    }

    // Recorded, or being made by a subscribe
    private synchronized boolean isKnown(SubscriptionIdAndSubscriberId key) {
        BeingMade beingMade = subscriptionsBeingMade.get(key.subscriptionId());
        return competingConsumers.containsKey(key) || (beingMade != null && beingMade.key.equals(key));
    }

    /**
     * Acts on a lease callback under the consumer's lock. A callback out of a try's own call to the strategy is left to
     * that try, which decides again once the call returns. One for a consumer whose lock another thread holds is left
     * to a try on a thread of its own, also while a subscribe is making the consumer, since waiting for the lock would
     * hold up the strategy's notifier for as long as the call holding the lock waits for the database through an
     * outage. The try decides from what holds once the call holding the lock has returned, so no callback is lost. A
     * callback that finds a pause, resume or cancel waiting for the lock that began before the callback came is left to
     * a try the same way, and comes after that call. A pause, resume or cancel that began after the callback came waits
     * for it instead, also when the callback was left to a try, which then goes ahead of that call however the two
     * reach the lock. While a subscribe is still making the consumer, a call made then can come before a callback that
     * came earlier, since the try starts only once the subscribe has returned.
     * <p>
     * A callback on the strategy's own thread that a {@code stop()} overtook is left to a try too, since nothing failed
     * that the strategy could report. Any other failure reaches the strategy. One out of a call of this model's own,
     * which holds the lock already, throws into that call, which decides what to do about it.
     * <p>
     * A callback that has the lock applies each {@code start(..)} and {@code stop()} handed over for the subscription
     * first. One that comes once {@code shutdown()} has begun does nothing. A grant under way then is refused when it
     * starts or resumes the subscription in the wrapped model, puts the consumer back as it was, and returns without
     * reporting a failure.
     */
    private void onLeaseCallback(SubscriptionIdAndSubscriberId key, Runnable callback) {
        if (shutDown || key.equals(triedOnThisThread.get())) {
            return;
        }
        if (!isKnown(key)) {
            return;
        }
        long callsWaitedBefore = callsWaitedSoFar();
        @Nullable SubscriptionLock held = subscriptionLocks.get(key.subscriptionId());
        // An entry this thread holds stays while it does, so this finds it
        boolean outOfACallOfThisModel = held != null && held.lock.isHeldByCurrentThread();
        @Nullable SubscriptionLock lock = tryLockSubscription(key.subscriptionId());
        if (lock != null && !outOfACallOfThisModel && aCallWaitsFor(lock, callsWaitedBefore)) {
            lock.unlock();
            lock = null;
        }
        if (lock == null) {
            reconcileLaterIfKnown(key, callsWaitedBefore);
            return;
        }
        try {
            applyWhatWasHandedOverFirst(key.subscriptionId(), lock);
            callback.run();
        } catch (ShutDownMeanwhile e) {
            if (outOfACallOfThisModel) {
                throw e;
            }
            logDebug("A lease callback for CompetingConsumer came while this model was being shut down, so it does nothing (subscriberId={}, subscriptionId={})", key.subscriberId(), key.subscriptionId());
        } catch (StoppedMeanwhile e) {
            if (outOfACallOfThisModel) {
                throw e;
            }
            triedAgainAfter(key, e);
        } finally {
            lock.unlock();
        }
    }

    // A call to the wrapped model threw, and may have taken effect first. A consumer recorded as running is recorded as
    // paused unless the wrapped model says it still runs it. One the wrapped model cannot answer for is recorded as
    // paused too, with that failure added to failure, so a later try pauses it again rather than resuming it.
    private void recordAsPausedUnlessItRuns(SubscriptionIdAndSubscriberId key, boolean pausedByUser, Throwable failure) {
        CompetingConsumer current = competingConsumers.get(key);
        if (current != null && current.isRunning() && !saysItRunsInTheWrappedModel(key, failure)) {
            competingConsumers.put(key, current.registerPaused(pausedByUser));
        }
    }

    // False also when the wrapped model cannot answer, with its failure added to the one being handled
    private boolean saysItRunsInTheWrappedModel(SubscriptionIdAndSubscriberId key, Throwable failure) {
        try {
            return isRunningInTheWrappedModel(key.subscriptionId());
        } catch (Throwable e) {
            failure.addSuppressed(e);
            return false;
        }
    }

    // False when the wrapped model cannot answer with a RuntimeException, so a call that throws afterwards is judged by
    // what it runs then. An Error is thrown.
    private boolean runsInTheWrappedModelBefore(SubscriptionIdAndSubscriberId key) {
        try {
            return isRunningInTheWrappedModel(key.subscriptionId());
        } catch (RuntimeException e) {
            return false;
        }
    }

    // True also when the wrapped model cannot answer, with its failure added to the one being handled
    private boolean runsInTheWrappedModel(SubscriptionIdAndSubscriberId key, Throwable failure) {
        try {
            return isRunningInTheWrappedModel(key.subscriptionId());
        } catch (Throwable e) {
            failure.addSuppressed(e);
            return true;
        }
    }

    // A failure of the wrapped model or the lease strategy, thrown as it is
    private static RuntimeException thrownAsItIs(Throwable failure) {
        if (failure instanceof Error error) {
            throw error;
        } else if (failure instanceof RuntimeException e) {
            throw e;
        }
        throw new IllegalStateException(failure);
    }

    // Logs the failure as a warning, and brings the consumer to where it belongs on a thread of its own
    private void triedAgainAfter(SubscriptionIdAndSubscriberId key, Throwable failure) {
        if (failure instanceof StoppedMeanwhile) {
            logDebug("A stop() overtook a call for CompetingConsumer, so it is brought to where it belongs (subscriberId={}, subscriptionId={})", key.subscriberId(), key.subscriptionId());
            reconcileLater(key, true);
            return;
        }
        if (warnedAboutInThisCall(key, failure)) {
            logDebug("A call for CompetingConsumer failed, and what failed was logged with a warning already, so it is tried again (subscriberId={}, subscriptionId={})", key.subscriberId(), key.subscriptionId());
        } else {
            log.warn("A call for CompetingConsumer failed, so it is tried again (subscriberId={}, subscriptionId={})", key.subscriberId(), key.subscriptionId(), failure);
            warnedInThisCall(key, failure);
        }
        reconcileLater(key);
    }

    // Recorded on the lock of the call under way on this thread for the consumer, which forgets it once that call lets
    // go of the lock. Nothing is recorded on a thread that doesn't hold that lock.
    private void warnedInThisCall(SubscriptionIdAndSubscriberId key, Throwable failure) {
        @Nullable SubscriptionLock held = subscriptionLocks.get(key.subscriptionId());
        if (held != null && held.lock.isHeldByCurrentThread()) {
            held.warnedAbout = failure;
            held.suppressedWhenWarned = failure.getSuppressed().length;
        }
    }

    // Whether the call under way on this thread for the consumer logged a warning with failure, and nothing was added
    // to it since, such as the failure of another listener of the lease strategy
    private boolean warnedAboutInThisCall(SubscriptionIdAndSubscriberId key, Throwable failure) {
        @Nullable SubscriptionLock held = subscriptionLocks.get(key.subscriptionId());
        return held != null && held.lock.isHeldByCurrentThread() && held.warnedAbout == failure && held.suppressedWhenWarned == failure.getSuppressed().length;
    }

    // A call for the consumer failed, so its try waits for the backoff first
    private void reconcileLater(SubscriptionIdAndSubscriberId key) {
        reconcileLater(key, false);
    }

    /**
     * Brings a consumer to where it belongs, on a thread of its own, and tries again with a backoff until it is there.
     * While this model is started, or the user resumed the consumer since {@code stop()}, and the consumer is neither
     * cancelled nor paused by the user or by {@code stop()}, that is registered with the lease strategy, and running in
     * the wrapped model only while this node holds its lease. Otherwise it is unregistered, and paused in the wrapped
     * model when that model runs it. Each try decides from what the wrapped model does, not from what this model
     * recorded, since a call that threw may have taken effect first. Every fifth try that fails is logged as a warning.
     * A consumer that a subscribe is making is tried once that subscribe returns or throws.
     * <p>
     * With {@code atOnce} nothing failed, as for a lease callback that found the consumer's lock taken, and the try acts
     * as soon as it has the lock, also when it is waiting for the backoff after an earlier failure. Otherwise the first
     * try waits for the backoff too, so what failed a moment ago is not asked again straight away, and a call the
     * caller makes for the subscription right after gets its lock first.
     */
    private void reconcileLater(SubscriptionIdAndSubscriberId key, boolean atOnce) {
        reconcileLater(key, atOnce, NO_LEASE_CALLBACK);
    }

    // For a lease callback, callsWaitedBefore is the number of pause, resume and cancel calls that had waited for a lock
    // when it came, and otherwise NO_LEASE_CALLBACK. Until the try next goes ahead with the lock, it lets each pause,
    // resume or cancel numbered at most the earliest of those callbacks' numbers go first, and each one numbered above
    // waits for it, so the earliest callback comes after the calls that began before it and before the ones that began
    // after it. A try with no callback handed to it lets every one waiting go first. Merging takes the earliest
    // callback, so a failure handed to the try changes nothing.
    private synchronized void reconcileLater(SubscriptionIdAndSubscriberId key, boolean atOnce, long callsWaitedBefore) {
        if (shutDown) {
            return;
        }
        BeingMade beingMade = subscriptionsBeingMade.get(key.subscriptionId());
        if (beingMade != null && beingMade.key.equals(key)) {
            beingMade.triedAgainOnceMade = true;
            beingMade.callsWaitedBeforeALeaseCallback = Math.min(beingMade.callsWaitedBeforeALeaseCallback, callsWaitedBefore);
            if (atOnce) {
                beingMade.triedAgainAtOnce = true;
            }
            return;
        }
        @Nullable Try underWay = tries.get(key);
        if (underWay != null) {
            // The try under way may have asked the lease strategy and the wrapped model before this, so it asks again
            underWay.decideAgain = true;
            underWay.callsWaitedBefore = Math.min(underWay.callsWaitedBefore, callsWaitedBefore);
            if (atOnce) {
                underWay.actAtOnce();
            }
            return;
        }
        Try tried = new Try(atOnce, callsWaitedBefore);
        tries.put(key, tried);
        try {
            Thread.ofPlatform().daemon().name("occurrent-competing-consumer-reconcile-" + key.subscriptionId()).start(() -> keepTrying(key, tried));
        } catch (Throwable e) {
            // No thread tries it then, so the next failure for the consumer starts a try anew
            tries.remove(key, tried);
            throw e;
        }
    }

    // Ends once the consumer is where it belongs, this model is shut down, or the thread is interrupted, and removes
    // the try on each of those paths, so the next failure for the consumer starts a new one
    private void keepTrying(SubscriptionIdAndSubscriberId key, Try tried) {
        try {
            Duration backoff = RECONCILE_FIRST_BACKOFF;
            int failures = 0;
            while (tried.waitForTheBackoff(backoff)) {
                try {
                    reconcile(key, tried);
                    return;
                } catch (InterruptedException e) {
                    logDebug("Stopped trying CompetingConsumer again, since its thread was interrupted (subscriberId={}, subscriptionId={})", key.subscriberId(), key.subscriptionId());
                    return;
                } catch (Throwable e) {
                    failures++;
                    // One logged with a warning already counts, but is not warned about again
                    boolean warned = tried.lastFailureWarnedAbout;
                    tried.lastFailureWarnedAbout = false;
                    if (!warned && failures % RECONCILE_TRIES_BETWEEN_WARNINGS == 0) {
                        log.warn("Still could not bring CompetingConsumer to where it belongs after {} tries, so it is tried again (subscriberId={}, subscriptionId={})",
                                failures, key.subscriberId(), key.subscriptionId(), e);
                    }
                    if (failures > 1) {
                        Duration doubled = backoff.multipliedBy(2);
                        backoff = doubled.compareTo(RECONCILE_MAX_BACKOFF) > 0 ? RECONCILE_MAX_BACKOFF : doubled;
                    }
                }
            }
            logDebug("Stopped trying CompetingConsumer again, since the model is shut down or the thread was interrupted (subscriberId={}, subscriptionId={})", key.subscriberId(), key.subscriptionId());
        } finally {
            synchronized (this) {
                tries.remove(key, tried);
            }
        }
    }

    /**
     * The try under way for a consumer. {@code decideAgain} and {@code callsWaitedBefore} are read and written under
     * the monitor only, and {@code callsWaitedBefore} is {@link #NO_LEASE_CALLBACK} unless a lease callback was handed
     * to the try since it last went ahead with the lock. The try waits for its backoff on its own monitor, never on
     * this model's, and is woken there when it is to act at once or this model is shut down.
     */
    private final class Try {
        private boolean decideAgain;
        private long callsWaitedBefore;
        private volatile boolean atOnce;
        // Whether the last try that failed logged a warning with what failed, read and written by the try's thread only
        private boolean lastFailureWarnedAbout;

        private Try(boolean atOnce, long callsWaitedBefore) {
            this.atOnce = atOnce;
            this.callsWaitedBefore = callsWaitedBefore;
        }

        private void actAtOnce() {
            atOnce = true;
            wake();
        }

        private void wake() {
            synchronized (this) {
                notifyAll();
            }
        }

        // False once this model is shut down, or the thread is interrupted while it waits
        private boolean waitForTheBackoff(Duration backoff) {
            if (!atOnce) {
                beforeATryWaitsForItsBackoff.run();
            }
            long deadline = System.nanoTime() + backoff.toNanos();
            synchronized (this) {
                while (!atOnce && !shutDown) {
                    long left = deadline - System.nanoTime();
                    if (left <= 0) {
                        break;
                    }
                    try {
                        TimeUnit.NANOSECONDS.timedWait(this, left);
                    } catch (InterruptedException e) {
                        return false;
                    }
                }
                atOnce = false;
            }
            return !shutDown;
        }
    }

    // One try, which holds the consumer's lock and not the monitor while it calls the lease strategy or the wrapped
    // model. It asks them what holds before every decision, and decides under the monitor from those answers, so a
    // decision calls neither. Deciding that nothing is left removes the try there, unless a failure or a lease callback
    // was handed to this try since it last asked, and the try then asks again. So one reported during a call is seen by
    // the next decision or starts a new thread.
    private void reconcile(SubscriptionIdAndSubscriberId key, Try tried) throws InterruptedException {
        @Nullable SubscriptionLock lock = lockSubscriptionForATry(key.subscriptionId(), tried);
        if (lock == null) {
            return;
        }
        try {
            applyWhatWasHandedOverFirst(key.subscriptionId(), lock);
        } catch (Throwable e) {
            lock.unlock();
            throw e;
        }
        triedOnThisThread.set(key);
        try {
            boolean stillRunsAfterItsPause = false;
            for (int calls = 0; calls < CALLS_PER_RECONCILE_TRY; calls++) {
                boolean known = competingConsumers.containsKey(key);
                boolean runs = known && isRunningInTheWrappedModel(key.subscriptionId());
                boolean holdsLease = known && Boolean.TRUE.equals(registrations.get(key)) && hasLock(key.subscriptionId(), key.subscriberId());
                // A wrapped model that reports the subscription running and paused has not applied the pause yet, as a
                // catch-up does not until its replay has ended. Resumed now, it would be paused once the pause takes
                // effect, so the try waits for the backoff and asks again.
                if (runs && holdsLease && pausedByTheLossOfItsLease(key) && isPausedInTheWrappedModel(key.subscriptionId())) {
                    throw new IllegalStateException("CompetingConsumer (subscriberId=" + key.subscriberId() + ", subscriptionId=" + key.subscriptionId() + ") still runs in the wrapped subscription model until the pause it was asked for there takes effect, so it is resumed once it is paused there");
                }
                @Nullable BooleanSupplier call = nextCall(key, runs, holdsLease, stillRunsAfterItsPause);
                if (call == null) {
                    return;
                } else if (call != DECIDE_AGAIN) {
                    stillRunsAfterItsPause = call.getAsBoolean();
                }
            }
            throw new IllegalStateException("CompetingConsumer (subscriberId=" + key.subscriberId() + ", subscriptionId=" + key.subscriptionId() + ") was not where it belongs after " + CALLS_PER_RECONCILE_TRY + " calls");
        } catch (Throwable e) {
            // Asked before the lock is let go of, which forgets it
            tried.lastFailureWarnedAbout = warnedAboutInThisCall(key, e);
            throw e;
        } finally {
            triedOnThisThread.remove();
            lock.unlock();
        }
    }

    private boolean pausedByTheLossOfItsLease(SubscriptionIdAndSubscriberId key) {
        @Nullable CompetingConsumer competingConsumer = competingConsumers.get(key);
        return competingConsumer != null && competingConsumer.isPausedByTheLossOfItsLease();
    }

    // Null once this model is shut down. Waits for each pause, resume or cancel that the try lets go first, see
    // reconcileLater(SubscriptionIdAndSubscriberId, boolean, long).
    private @Nullable SubscriptionLock lockSubscriptionForATry(String subscriptionId, Try tried) throws InterruptedException {
        while (true) {
            @Nullable SubscriptionLock lock = lockSubscriptionUnlessShutDown(subscriptionId);
            if (lock == null || goesAhead(lock, tried)) {
                return lock;
            }
            lock.unlock();
            Thread.sleep(LEFT_TO_A_CALL_WAITING);
        }
    }

    // Whether no pause, resume or cancel the try lets go first waits for the lock. The try then acts on each lease
    // callback handed to it so far, so it lets every call waiting go first from then on, and the calls that waited for
    // those callbacks go on once it lets go of the lock. Called under the lock.
    private synchronized boolean goesAhead(SubscriptionLock lock, Try tried) {
        if (aCallWaitsFor(lock, tried.callsWaitedBefore)) {
            return false;
        }
        if (tried.callsWaitedBefore != NO_LEASE_CALLBACK) {
            tried.callsWaitedBefore = NO_LEASE_CALLBACK;
            lock.callBeforeGone.signalAll();
        }
        return true;
    }

    // Whether a lease callback that came before the pause, resume or cancel numbered number was handed to a try of the
    // subscription that has not gone ahead with the lock since
    private synchronized boolean aLeaseCallbackComesBefore(String subscriptionId, long number) {
        for (Map.Entry<SubscriptionIdAndSubscriberId, Try> underWay : tries.entrySet()) {
            if (underWay.getKey().subscriptionId().equals(subscriptionId) && underWay.getValue().callsWaitedBefore < number) {
                return true;
            }
        }
        return false;
    }

    // What nextCall answers when nothing is left to call, but something was handed to the try since it last asked
    private static final BooleanSupplier DECIDE_AGAIN = () -> false;

    // Null once nothing is left to do. The call returns whether the wrapped model still runs the consumer after it paused
    // it there for a consumer that no longer competes.
    private synchronized @Nullable BooleanSupplier nextCall(SubscriptionIdAndSubscriberId key, boolean runs, boolean holdsLease, boolean stillRunsAfterItsPause) {
        @Nullable Try underWay = tries.get(key);
        boolean handedOverMeanwhile = underWay != null && underWay.decideAgain;
        if (underWay != null) {
            underWay.decideAgain = false;
        }
        @Nullable BooleanSupplier call = null;
        BeingMade beingMade = subscriptionsBeingMade.get(key.subscriptionId());
        if (!shutDown && beingMade != null && beingMade.key.equals(key)) {
            beingMade.triedAgainOnceMade = true;
        } else if (!shutDown) {
            CompetingConsumer competingConsumer = competingConsumers.get(key);
            call = competingConsumer != null && competes(competingConsumer)
                    ? nextCallToCompete(competingConsumer, runs, holdsLease)
                    : nextCallToStopCompeting(key, competingConsumer, runs, stillRunsAfterItsPause);
            if (call == null && handedOverMeanwhile) {
                return DECIDE_AGAIN;
            }
        }
        if (call == null) {
            tries.remove(key);
        }
        return call;
    }

    private boolean competes(CompetingConsumer cc) {
        boolean paused = cc.isPausedWhileWaiting() || (cc.state instanceof CompetingConsumerState.Paused p && p.pausedByUser);
        return !paused && (!stoppedByUser.get() || mayRunWhileStopped.contains(cc.subscriptionIdAndSubscriberId));
    }

    // Registers the consumer, and then runs it in the wrapped model if this node holds its lease, or pauses it there if
    // not
    private @Nullable BooleanSupplier nextCallToCompete(CompetingConsumer cc, boolean runs, boolean holdsLease) {
        SubscriptionIdAndSubscriberId key = cc.subscriptionIdAndSubscriberId;
        String subscriptionId = key.subscriptionId();
        if (!Boolean.TRUE.equals(registrations.get(key))) {
            return () -> {
                if (!registerCompetingConsumer(subscriptionId, key.subscriberId())) {
                    laterStopsEndWithTheRegister(key);
                }
                return false;
            };
        }
        if (holdsLease) {
            CompetingConsumerState previous = cc.state;
            if (previous instanceof CompetingConsumerState.Waiting waiting) {
                logDebug("Start CompetingConsumer that has previously been waiting (subscriberId={}, subscriptionId={})", key.subscriberId(), subscriptionId);
                competingConsumers.put(key, cc.registerRunning());
                return () -> {
                    giveTheLeaseBackIfItThrows(key, previous, waiting::startSubscription);
                    return false;
                };
            } else if (cc.isPausedByTheLossOfItsLease() || cc.isRunning()) {
                // The try resumes it whatever paused it there, so it needs no resume on behalf of a stop of the wrapped
                // model after this
                pausedByAStopOfTheWrappedModel.remove(key);
                competingConsumers.put(key, cc.registerRunning());
                if (!runs) {
                    return () -> {
                        giveTheLeaseBackIfItThrows(key, previous, () -> resumeInTheWrappedModel(key));
                        return false;
                    };
                }
            }
            return null;
        }
        if (runs) {
            return () -> {
                try {
                    pauseInTheWrappedModel(subscriptionId);
                } catch (Throwable e) {
                    recordAsPausedUnlessItRuns(key, false, e);
                    throw e;
                }
                if (isRunningInTheWrappedModel(subscriptionId)) {
                    throw new IllegalStateException("Subscription " + subscriptionId + " still runs in the wrapped subscription model after it was paused there, while this node does not hold its lease");
                }
                // Paused for the lease it lost. Recorded before the next decision, which records a consumer still
                // recorded as running as paused by the user once a stop() has begun.
                CompetingConsumer current = competingConsumers.get(key);
                if (current != null && current.isRunning()) {
                    competingConsumers.put(key, current.registerPaused(false));
                }
                return false;
            };
        }
        // The wrapped model does not run it, whatever this model recorded
        if (cc.isRunning()) {
            competingConsumers.put(key, cc.registerPaused(false));
        }
        return null;
    }

    // Once a register of a try returned without the lease. A stop() that begins after it finds the consumer waiting for
    // its lease, as after a register on the thread of the call meant to run the consumer, so only one that began before
    // pauses it as by the user.
    private void laterStopsEndWithTheRegister(SubscriptionIdAndSubscriberId key) {
        long stopsSoFar;
        synchronized (wrappedModelStart) {
            stopsSoFar = lastStopBegun;
        }
        competingConsumers.computeIfPresent(key, (__, cc) -> cc.meantToRunBeforeStopsUpTo(stopsSoFar));
    }

    // Pauses the consumer in the wrapped model if that model runs it, as stop() does, and unregisters it. One the wrapped
    // model still runs after the pause keeps its registration and its lease, as with stop().
    private @Nullable BooleanSupplier nextCallToStopCompeting(SubscriptionIdAndSubscriberId key, @Nullable CompetingConsumer cc, boolean runs, boolean stillRunsAfterItsPause) {
        String subscriptionId = key.subscriptionId();
        if (cc != null && runs) {
            if (stillRunsAfterItsPause) {
                return null;
            }
            return () -> {
                try {
                    pauseInTheWrappedModel(subscriptionId);
                } catch (Throwable e) {
                    recordAsPausedUnlessItRuns(key, true, e);
                    throw e;
                }
                return isRunningInTheWrappedModel(subscriptionId);
            };
        }
        // The wrapped model does not run it, whatever this model recorded
        if (cc != null && cc.isRunning()) {
            competingConsumers.put(key, cc.registerPaused(true));
        }
        if (!registrations.containsKey(key)) {
            return null;
        }
        return () -> {
            unregisterOrAtLeastGiveUpTheLease(key);
            return false;
        };
    }

    // When the unregister throws, an Error included, this tries at least to give the lease back, if the lease strategy
    // still reports it held
    private void unregisterOrAtLeastGiveUpTheLease(SubscriptionIdAndSubscriberId key) {
        try {
            unregisterCompetingConsumer(key.subscriptionId(), key.subscriberId());
        } catch (Throwable e) {
            try {
                if (hasLock(key.subscriptionId(), key.subscriberId())) {
                    competingConsumerStrategy.releaseCompetingConsumer(key.subscriptionId(), key.subscriberId());
                }
            } catch (Throwable releaseFailure) {
                e.addSuppressed(releaseFailure);
            }
            throw e;
        }
    }

    private Subscription startWaitingConsumer(CompetingConsumer cc) {
        logDebug("Start CompetingConsumer that has previously been waiting (subscriberId={}, subscriptionId={})", cc.getSubscriberId(), cc.getSubscriptionId());
        String subscriptionId = cc.getSubscriptionId();
        pausedByAStopOfTheWrappedModel.remove(cc.subscriptionIdAndSubscriberId);
        competingConsumers.put(SubscriptionIdAndSubscriberId.from(subscriptionId, cc.getSubscriberId()), cc.registerRunning());
        return giveTheLeaseBackIfItThrows(cc.subscriptionIdAndSubscriberId, cc.state, ((CompetingConsumerState.Waiting) cc.state)::startSubscription);
    }

    /**
     * Starts or resumes a consumer through the wrapped model while this node holds its lease, and gives the lease back
     * if the wrapped model throws. Holding on to it would leave the subscription with a lease nobody on this node
     * serves, and every other node locked out of it for as long as this node keeps refreshing.
     * <p>
     * The call can take effect before it throws. A consumer the wrapped model runs afterwards, and did not run before,
     * stays recorded as running and keeps the lease, since after giving the lease back it would go on delivering while
     * another node takes it. What the wrapped model ran before the call is not what the call started, such as a
     * subscription made there directly under the same id.
     * <p>
     * Otherwise {@code previous}, the state the consumer had before it was recorded as running, is put back first, so
     * a synchronous {@code onConsumeProhibited} out of giving the lease back finds nothing running to pause. The lease
     * is released, so the consumer stays a candidate and a later grant tries it again, also on a node with no other
     * node to take the subscription over. A waiting consumer is put back as waiting, which a grant starts. Any other is
     * put back as paused by the system, which a grant resumes, whoever paused it, since starting it again is what was
     * asked for. Put back as paused by the user, it would never compete for the
     * lease again. Put back as running, which is what a consumer the wrapped model does not run can be recorded as, a
     * grant would find nothing to do. Either way nothing would retry it.
     * <p>
     * A {@code stop()} that comes after the call, one that refused it included, pauses the consumer as by the user once
     * it is applied to it, as it would have paused it had the call run first.
     * <p>
     * The lease is given back before the warning is logged, so a single warning is logged with what the wrapped model
     * threw and any failure to give the lease back. A lease that could not be given back is warned about also when a
     * {@code stop()} overtook the call, and so is a failure to make the subscription paused in the wrapped model then.
     * What the wrapped model threw is thrown as it is. The call or try holding the consumer's lock doesn't warn about
     * it again, unless something was added to it since, such as the failure of another listener of the lease strategy.
     * A try still counts it as a failed try.
     */
    private Subscription giveTheLeaseBackIfItThrows(SubscriptionIdAndSubscriberId key, CompetingConsumerState previous, Supplier<Subscription> start) {
        // Asked inside the try, so an Error asking puts the consumer back and tries to give the lease back too
        boolean ranBefore = false;
        try {
            ranBefore = runsInTheWrappedModelBefore(key);
            return start.get();
        } catch (Throwable e) {
            // Nothing ran, so the consumer is put back as it was, unless shutdown() has forgotten it, and shutdown()
            // gives up the lease
            if (e instanceof ShutDownMeanwhile) {
                competingConsumers.computeIfPresent(key, (__, current) -> new CompetingConsumer(key, previous));
                throw e;
            }
            int suppressedBefore = e.getSuppressed().length;
            if (!ranBefore && runsInTheWrappedModel(key, e)) {
                log.warn("The wrapped subscription model threw while it started or resumed a subscription this node holds the lease for, but runs it, so the lease is kept (subscriberId={}, subscriptionId={})",
                        key.subscriberId(), key.subscriptionId(), e);
                warnedInThisCall(key, e);
                competingConsumers.put(key, new CompetingConsumer(key, new CompetingConsumerState.Running()));
                // Also tried again, for a wrapped model that could not say whether it runs the subscription
                reconcileLater(key);
                throw e;
            }
            LaterStops laterStops = stopsAfter(key, e);
            if (previous instanceof CompetingConsumerState.Waiting waiting && laterStops.includeAny() && e instanceof StoppedMeanwhile && heldPausedInTheWrappedModel(key, waiting, e)) {
                competingConsumers.put(key, new CompetingConsumer(key, new CompetingConsumerState.Paused(false, laterStops)));
            } else if (previous instanceof CompetingConsumerState.Waiting waiting) {
                competingConsumers.put(key, new CompetingConsumer(key, waiting.meantToRunBefore(laterStops)));
            } else {
                competingConsumers.put(key, new CompetingConsumer(key, new CompetingConsumerState.Paused(false, laterStops)));
            }
            boolean givenBack = false;
            try {
                competingConsumerStrategy.releaseCompetingConsumer(key.subscriptionId(), key.subscriberId());
                givenBack = true;
            } catch (Throwable givingBackFailed) {
                e.addSuppressed(givingBackFailed);
                // A lease still held would never be granted again, so the consumer is tried again from here
                reconcileLater(key);
            }
            if (!(e instanceof StoppedMeanwhile)) {
                log.warn(givenBack
                                ? "The wrapped subscription model failed to start itself, or to start or resume a subscription this node holds the lease for, so the lease is given back (subscriberId={}, subscriptionId={})"
                                : "The wrapped subscription model failed to start itself, or to start or resume a subscription this node holds the lease for, and the lease could not be given back, so it is tried again (subscriberId={}, subscriptionId={})",
                        key.subscriberId(), key.subscriptionId(), e);
                warnedInThisCall(key, e);
            } else if (!givenBack) {
                log.warn("A stop() overtook the start or resume of a subscription this node holds the lease for, and the lease could not be given back, so it is tried again (subscriberId={}, subscriptionId={})",
                        key.subscriberId(), key.subscriptionId(), e);
            } else if (e.getSuppressed().length > suppressedBefore) {
                log.warn("A stop() overtook the start of a subscription this node holds the lease for, and the wrapped subscription model could not make it paused there, so the lease is given back (subscriberId={}, subscriptionId={})",
                        key.subscriberId(), key.subscriptionId(), e);
            } else {
                logDebug("A stop() overtook the start or resume of a subscription this node holds the lease for, so the lease is given back (subscriberId={}, subscriptionId={})", key.subscriberId(), key.subscriptionId());
            }
            throw e;
        }
    }

    /**
     * Whether the wrapped model holds a waiting consumer paused, once a {@code stop()} refused to start it for a call
     * that comes before that {@code stop()}. In that order the call would have made the subscription in the wrapped
     * model, and the {@code stop()} paused it there, so it is made there paused, as a subscribe while this model is
     * stopped makes it. False when the wrapped model cannot hold it paused, or fails to, and the consumer then stays
     * waiting, which the next grant makes it from.
     */
    private boolean heldPausedInTheWrappedModel(SubscriptionIdAndSubscriberId key, CompetingConsumerState.Waiting waiting, Throwable refused) {
        synchronized (wrappedModelStart) {
            if (shutDown) {
                return false;
            }
        }
        try {
            if (!isPausedInTheWrappedModel(key.subscriptionId())) {
                waiting.holdPaused();
            }
            return true;
        } catch (UnsupportedOperationException e) {
            logDebug("Wrapped model cannot hold the subscription paused, so it stays waiting (subscriberId={}, subscriptionId={})", key.subscriberId(), key.subscriptionId());
            return false;
        } catch (Throwable e) {
            refused.addSuppressed(e);
            return false;
        }
    }

    /**
     * Registers a waiting consumer with the strategy and starts it if that grants the lock there and then.
     * <p>
     * Registering can win the lock synchronously, and {@link #onConsumeGranted(String, String)} then starts
     * the consumer itself, but only on a change of status. A register that finds the lock already held gets no
     * callback, so the answer is read from the return value, and the state is re-read afterwards to see
     * whether the callback already acted on it.
     */
    private Subscription registerAndStartIfGranted(CompetingConsumer cc) {
        String subscriptionId = cc.getSubscriptionId();
        String subscriberId = cc.getSubscriberId();
        boolean acquiredLock = registerCompetingConsumer(subscriptionId, subscriberId);
        CompetingConsumer current = competingConsumers.get(cc.subscriptionIdAndSubscriberId);
        // hasLock as well, since a callback that failed to start the consumer left it waiting and gave the lease back
        if (acquiredLock && current != null && current.isWaiting() && hasLock(subscriptionId, subscriberId)) {
            return startWaitingConsumer(current);
        }
        return new CompetingConsumerSubscription(subscriptionId, subscriberId);
    }

    // For a resume of a waiting consumer. A stop() that began while the register was under way comes after the resume,
    // so it pauses the consumer as by the user once the register returns without the lease, as in registerAsRunning.
    private Subscription registerWaitingAndStartIfGranted(CompetingConsumer cc) {
        SubscriptionIdAndSubscriberId key = cc.subscriptionIdAndSubscriberId;
        long stopsBeforeTheRegister;
        synchronized (wrappedModelStart) {
            stopsBeforeTheRegister = lastStopBegun;
        }
        Subscription subscription = registerAndStartIfGranted(cc);
        @Nullable LaterStops whileRegistering = stopsWhileRegistering(key, stopsBeforeTheRegister);
        if (whileRegistering != null) {
            competingConsumers.computeIfPresent(key, (__, current) -> current.state instanceof CompetingConsumerState.Waiting waiting
                    ? new CompetingConsumer(key, waiting.meantToRunBefore(whileRegistering))
                    : current);
        }
        return subscription;
    }

    void pauseConsumer(CompetingConsumer cc, boolean pausedByUser) {
        logDebug("Pausing CompetingConsumer (subscriberId={}, subscriptionId={}, pausedByUser={})", cc.getSubscriberId(), cc.getSubscriptionId(), pausedByUser);
        SubscriptionIdAndSubscriberId subscriptionIdAndSubscriberId = SubscriptionIdAndSubscriberId.from(cc);
        pausedByAStopOfTheWrappedModel.remove(subscriptionIdAndSubscriberId);
        competingConsumers.put(subscriptionIdAndSubscriberId, cc.registerPaused(pausedByUser));
    }

    private record SubscriptionIdAndSubscriberId(String subscriptionId, String subscriberId) {

        private static SubscriptionIdAndSubscriberId from(String subscriptionId, String subscriberId) {
            return new SubscriptionIdAndSubscriberId(subscriptionId, subscriberId);
        }

        private static SubscriptionIdAndSubscriberId from(CompetingConsumer cc) {
            return from(cc.getSubscriptionId(), cc.getSubscriberId());
        }
    }


    private record CompetingConsumer(SubscriptionIdAndSubscriberId subscriptionIdAndSubscriberId, CompetingConsumerState state) {

        boolean hasId(String subscriptionId, String subscriberId) {
            return hasSubscriptionId(subscriptionId) && Objects.equals(getSubscriberId(), subscriberId);
        }

        boolean hasSubscriptionId(String subscriptionId) {
            return Objects.equals(getSubscriptionId(), subscriptionId);
        }

        boolean isPaused() {
            return state instanceof CompetingConsumerState.Paused || state instanceof CompetingConsumerState.PausedWhileWaiting;
        }

        boolean isRunning() {
            return state instanceof CompetingConsumerState.Running;
        }

        boolean isWaiting() {
            return state instanceof CompetingConsumerState.Waiting;
        }

        boolean isPausedWhileWaiting() {
            return state instanceof CompetingConsumerState.PausedWhileWaiting;
        }

        boolean isPausedByTheLossOfItsLease() {
            return state instanceof CompetingConsumerState.Paused paused && !paused.pausedByUser;
        }

        // A call that comes before the stop() meant to run this consumer, and did not
        boolean isMeantToRunBefore(Lifecycle stop) {
            @Nullable LaterStops laterStops = null;
            if (state instanceof CompetingConsumerState.Paused paused) {
                laterStops = paused.laterStops;
            } else if (state instanceof CompetingConsumerState.Waiting waiting) {
                laterStops = waiting.laterStops;
            }
            return laterStops != null && laterStops.include(stop);
        }

        // The same consumer, which a stop() numbered above stop no longer pauses as by the user
        CompetingConsumer meantToRunBeforeStopsUpTo(long stop) {
            if (state instanceof CompetingConsumerState.Paused paused && paused.laterStops != null) {
                return new CompetingConsumer(subscriptionIdAndSubscriberId, new CompetingConsumerState.Paused(paused.pausedByUser, paused.laterStops.upToStop(stop)));
            } else if (state instanceof CompetingConsumerState.Waiting waiting && waiting.laterStops != null) {
                return new CompetingConsumer(subscriptionIdAndSubscriberId, waiting.meantToRunBefore(waiting.laterStops.upToStop(stop)));
            }
            return this;
        }

        boolean isPausedFor(String subscriptionId) {
            return isPaused() && hasSubscriptionId(subscriptionId);
        }

        String getSubscriptionId() {
            return subscriptionIdAndSubscriberId.subscriptionId;
        }

        String getSubscriberId() {
            return subscriptionIdAndSubscriberId.subscriberId;
        }

        CompetingConsumer registerRunning() {
            return new CompetingConsumer(subscriptionIdAndSubscriberId, new CompetingConsumerState.Running());
        }

        CompetingConsumer registerPaused(boolean pausedByUser) {
            return new CompetingConsumer(subscriptionIdAndSubscriberId, new CompetingConsumerState.Paused(pausedByUser));
        }

        // Only for a currently-Waiting consumer. The Waiting is kept rather than discarded, because it holds
        // the start supplier that is the only way to bring this consumer up. It never subscribed, so there is
        // nothing for delegate.resumeSubscription to resume.
        CompetingConsumer registerPausedWhileWaiting() {
            return new CompetingConsumer(subscriptionIdAndSubscriberId, new CompetingConsumerState.PausedWhileWaiting((CompetingConsumerState.Waiting) state));
        }

        // Only for a currently-PausedWhileWaiting consumer. Restores the Waiting it was paused from, supplier
        // intact. A resume records the consumer anew, so the stop() calls that an earlier failed call left on it are
        // dropped.
        CompetingConsumer restoreWaiting() {
            CompetingConsumerState.Waiting waiting = ((CompetingConsumerState.PausedWhileWaiting) state).waiting;
            return new CompetingConsumer(subscriptionIdAndSubscriberId, new CompetingConsumerState.Waiting(waiting.supplier, waiting.pausedSupplier, waiting.madeInTheWrappedModel));
        }
    }

    sealed interface CompetingConsumerState {

        final class Running implements CompetingConsumerState {
        }

        final class Waiting implements CompetingConsumerState {
            private final Supplier<Subscription> supplier;
            private final Supplier<Subscription> pausedSupplier;
            // Set once the wrapped model has made the subscription for this consumer
            private final AtomicBoolean madeInTheWrappedModel;
            private final @Nullable LaterStops laterStops;

            Waiting(Supplier<Subscription> supplier, Supplier<Subscription> pausedSupplier, AtomicBoolean madeInTheWrappedModel) {
                this(supplier, pausedSupplier, madeInTheWrappedModel, null);
            }

            private Waiting(Supplier<Subscription> supplier, Supplier<Subscription> pausedSupplier, AtomicBoolean madeInTheWrappedModel, @Nullable LaterStops laterStops) {
                this.supplier = supplier;
                this.pausedSupplier = pausedSupplier;
                this.madeInTheWrappedModel = madeInTheWrappedModel;
                this.laterStops = laterStops;
            }

            // The same consumer, which those stop() calls pause as by the user
            private Waiting meantToRunBefore(LaterStops laterStops) {
                return new Waiting(supplier, pausedSupplier, madeInTheWrappedModel, laterStops);
            }

            private boolean madeInTheWrappedModel() {
                return madeInTheWrappedModel.get();
            }

            private Subscription startSubscription() {
                return supplier.get();
            }

            // Makes the subscription in the wrapped model held paused there
            private void holdPaused() {
                pausedSupplier.get();
            }
        }

        final class Paused implements CompetingConsumerState {
            private final boolean pausedByUser;
            private final @Nullable LaterStops laterStops;

            Paused(boolean pausedByUser) {
                this(pausedByUser, null);
            }

            private Paused(boolean pausedByUser, @Nullable LaterStops laterStops) {
                this.pausedByUser = pausedByUser;
                this.laterStops = laterStops;
            }
        }

        final class PausedWhileWaiting implements CompetingConsumerState {
            private final Waiting waiting;

            PausedWhileWaiting(Waiting waiting) {
                this.waiting = waiting;
            }
        }
    }

    private void unregisterCompetingConsumer(CompetingConsumer cc, Consumer<CompetingConsumer> and) {
        logDebug("Unregistering CompetingConsumer (subscriberId={}, subscriptionId={})", cc.getSubscriberId(), cc.getSubscriptionId());
        and.accept(cc);
        unregisterCompetingConsumer(cc.getSubscriptionId(), cc.getSubscriberId());
    }

    // Every register and unregister goes through these two, so the registrations recorded here follow the strategy.
    // After one that throws the registration is unknown, and the consumer is tried again.
    private boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
        logDebug("Registering CompetingConsumer (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
        SubscriptionIdAndSubscriberId key = SubscriptionIdAndSubscriberId.from(subscriptionId, subscriberId);
        registrations.put(key, false);
        final boolean acquired;
        try {
            acquired = competingConsumerStrategy.registerCompetingConsumer(subscriptionId, subscriberId);
        } catch (Throwable e) {
            reconcileLater(key);
            throw e;
        }
        if (shutDown) {
            // shutdown() does not wait for a register, and may have given up the leases before this register returned
            giveUpALeaseTakenAfterShutdownBegan(key);
            return false;
        }
        registrations.put(key, true);
        return acquired;
    }

    // One attempt, as shutdown() makes for every other lease. One not given up expires after the lease time. An Error
    // is thrown once the registration is forgotten.
    private void giveUpALeaseTakenAfterShutdownBegan(SubscriptionIdAndSubscriberId key) {
        @Nullable Error error = null;
        try {
            competingConsumerStrategy.unregisterCompetingConsumer(key.subscriptionId(), key.subscriberId());
        } catch (Throwable e) {
            log.warn("Could not give up the lease of CompetingConsumer registered while this model was shut down, so it expires after the lease time (subscriberId={}, subscriptionId={})", key.subscriberId(), key.subscriptionId(), e);
            if (e instanceof Error thrown) {
                error = thrown;
            }
        } finally {
            registrations.remove(key);
        }
        if (error != null) {
            throw error;
        }
    }

    private void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
        SubscriptionIdAndSubscriberId key = SubscriptionIdAndSubscriberId.from(subscriptionId, subscriberId);
        registrations.put(key, false);
        try {
            competingConsumerStrategy.unregisterCompetingConsumer(subscriptionId, subscriberId);
        } catch (Throwable e) {
            reconcileLater(key);
            throw e;
        }
        registrations.remove(key);
    }

    /**
     * Records the consumer as running, registers it, and records it as paused by the system if registering did not win
     * the lock. The consumer stays registered, so the grant that comes once the lease is free resumes it. Recorded as
     * paused by the user instead, as a consumer {@link #stop()} paused is, that grant would hand the lease back and the
     * consumer would never compete for it again.
     * <p>
     * The order matters. Registering can be granted the lock there and then, and {@link #onConsumeGranted(String, String)}
     * resumes a paused consumer itself, so a consumer still recorded as paused when registration is granted is resumed
     * once by that callback and once by the caller here, and the second resume finds the delegate already running.
     * Recording it as running first leaves the callback nothing to do.
     * <p>
     * Only for a paused consumer. A waiting one has to stay waiting across the call, because that is what makes
     * {@code onConsumeGranted} subscribe it.
     * <p>
     * A register that throws, an {@link Error} included, records the consumer as paused by the system, which competes,
     * so the registration is tried again. Left recorded as running, no grant would ever come for a consumer the
     * strategy never registered, and a start(..) tried again would find it running and do nothing.
     */
    private boolean registerAsRunning(CompetingConsumer competingConsumer) {
        SubscriptionIdAndSubscriberId key = competingConsumer.subscriptionIdAndSubscriberId;
        competingConsumers.put(key, competingConsumer.registerRunning());
        final boolean acquired;
        final long stopsBeforeTheRegister;
        synchronized (wrappedModelStart) {
            stopsBeforeTheRegister = lastStopBegun;
        }
        resumedOnceRegistered.add(key);
        try {
            acquired = registerCompetingConsumer(key.subscriptionId(), key.subscriberId());
        } catch (Throwable e) {
            competingConsumers.put(key, new CompetingConsumer(key, new CompetingConsumerState.Paused(false, stopsAfter(key, e))));
            throw e;
        } finally {
            resumedOnceRegistered.remove(key);
        }
        if (!acquired) {
            // A stop() that began while the register was under way comes after it, as it would have had the register
            // won the lock before another node took it, so it pauses the consumer as by the user. One that began
            // before the register is one the strategy answered after, so it finds the consumer paused by the system.
            competingConsumers.put(key, new CompetingConsumer(key, new CompetingConsumerState.Paused(false, stopsWhileRegistering(key, stopsBeforeTheRegister))));
        }
        return acquired;
    }

    private boolean hasLock(String subscriptionId, String subscriberId) {
        return competingConsumerStrategy.hasLock(subscriptionId, subscriberId);
    }

    /**
     * Uniqueness is scoped to this instance, and only to this instance. Several instances subscribing to one
     * subscription id is the competing consumer pattern itself, and the strategy is what coordinates them. Several
     * subscriptions for one id <i>inside</i> one instance is a different thing, and nothing here can express it:
     * {@link #cancelSubscription(String)}, {@link #pauseSubscription(String)} and {@link #resumeSubscription(String)}
     * all resolve by subscription id alone, so the second one would be unreachable through every one of them, and both
     * would be sharing the single delegate that refuses a duplicate id in its own right.
     * <p>
     * Both collections count, because a subscription whose start position opted out of competing consumption occupies
     * the id just as much as a competing one does.
     */
    // A subscription id is unique per model instance, so an id neither collection here holds is unknown to this
    // model, whatever the delegate may separately know about it.
    private void requireKnown(String subscriptionId) {
        if (!isSubscriptionIdInUse(subscriptionId)) {
            throw new UnknownSubscriptionException(subscriptionId);
        }
    }

    private boolean isSubscriptionIdInUse(String subscriptionId) {
        return nonCompetingConsumersSubscriptions.contains(subscriptionId)
                || findFirstCompetingConsumerMatching(cc -> cc.hasSubscriptionId(subscriptionId)).isPresent();
    }

    private Optional<CompetingConsumer> findFirstCompetingConsumerMatching(Predicate<CompetingConsumer> predicate) {
        return findCompetingConsumersMatching(predicate).findFirst();
    }

    private Stream<CompetingConsumer> findCompetingConsumersMatching(Predicate<CompetingConsumer> predicate) {
        return competingConsumers.values().stream().filter(predicate);
    }

    private static void logDebug(String message, Object... params) {
        if (log.isDebugEnabled()) {
            log.debug(message, params);
        }
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", CompetingConsumerSubscriptionModel.class.getSimpleName() + "[", "]")
                .add("delegate=" + delegate)
                .add("competingConsumerStrategy=" + competingConsumerStrategy)
                .add("competingConsumers=" + competingConsumers)
                .add("nonCompetingConsumersSubscriptions=" + nonCompetingConsumersSubscriptions)
                .toString();
    }
}