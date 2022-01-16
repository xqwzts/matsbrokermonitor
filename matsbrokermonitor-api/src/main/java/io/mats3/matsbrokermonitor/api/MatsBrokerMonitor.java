package io.mats3.matsbrokermonitor.api;

import java.io.Closeable;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.function.Consumer;

/**
 * Provides a way to get data from the broker which is not possible to glean from the Mats system itself, nor from
 * standard ways by the JMS API (or any other known messaging protocol).
 * <p />
 * These data are:
 * <ul>
 * <li>Which destinations exist, and whether they are queues and topics (however, the total number of Mats endpoints can
 * been found by asking all MatsFactories).</li>
 * <li>Dead Letter Queues, both properly configured (on the broker) Individual Dead Letter Queues, and any unfortunate
 * (typically default) global DLQ.</li>
 * <li>Biggest point: <b>The number of messages on all the destinations and Dead Letter Queues.</b></li>
 * <li>Bonus point: The age of any head (first) message on the queue.</li>
 * </ul>
 *
 * Note: "Fully Qualified Destination Name" means that the name fully specifies the queue or topic, e.g. for ActiveMQ
 * this includes a schema-like notation "queue://" or "topic://" as prefix. This to handle a queue having the same name
 * as a topic - even though the Mats API forbids this: An "endpointId" fully qualifies a Mats endpoint seen from the
 * MatsFactory's side, no matter if it is e.g. a "terminator" (queue-based) or "subscriptionTerminator" (topic-based).
 *
 * @author Endre Stølsvik 2021-12-16 23:10 - http://stolsvik.com/, endre@stolsvik.com
 */
public interface MatsBrokerMonitor extends Closeable {

    void start();

    void close();

    /**
     * @return a Map[FullyQualifiedDestinationName, {@link MatsBrokerDestination}] for currently known Mats-relevant
     *         destinations.
     */
    ConcurrentNavigableMap<String, MatsBrokerDestination> getMatsDestinations();

    Optional<BrokerInfo> getBrokerInfo();

    void registerListener(Consumer<DestinationUpdateEvent> listener);

    void forceUpdate();

    interface BrokerInfo {
        /**
         * @return currently supported <code>"ActiveMQ"</code>.
         */
        String getBrokerType();

        /**
         * @return the name of the broker.
         */
        String getBrokerName();

        /**
         * @return any broker-specific information, in JSON format - the {@link #getBrokerType()} should be taken into
         *         account.
         */
        String getBrokerJson();
    }

    interface DestinationUpdateEvent {
        /**
         * A full update might be sent periodically.
         *
         * @return whether this is a full update (<code>true</code>), in which case the receiver should consider the
         *         {@link #getNewOrUpdatedDestinations()} as authoritative information about all currently known
         *         destinations on the broker, thus overwriting any local view kept by incremental updates.
         */
        boolean isFullUpdate();

        /**
         * @return a Map[FullyQualifiedDestinationName, {@link MatsBrokerDestination}] for any new or updated
         *         Mats-relevant destinations.
         */
        NavigableMap<String, MatsBrokerDestination> getNewOrUpdatedDestinations();
    }

    interface MatsBrokerDestination {
        /**
         * @return the millis-since-Epoch when this was last updated, using the time of this receiving computer. Compare
         *         to {@link #getLastUpdateBrokerMillis()}.
         */
        long getLastUpdateLocalMillis();

        /**
         * @return the millis-since-epoch <i>on the broker side</i> when this update was produced. Compare to
         *         {@link #getLastUpdateLocalMillis()}.
         */
        OptionalLong getLastUpdateBrokerMillis();

        /**
         * @return the fully qualified destination name, which uniquely specifies the destination (natively) for the
         *         broker, including whether it is a queue or a topic. For ActiveMq, this looks like
         *         <code>"queue://mats.ServiceName.serviceMethodName"</code>.
         */
        String getFqDestinationName();

        /**
         * @return the raw destination name (called "physical name" in ActiveMQ code lingo), which isn't "fully
         *         qualified", i.e. "mats.ServiceName.serviceMethodName" or "ActiveMQ.DLQ" - but not including any
         *         scheme prefix like "queue://" or "topic://". To get whether it is a queue or topic, use
         *         {@link #getDestinationType()}.
         * @see #getMatsStageId()
         */
        String getDestinationName();

        /**
         * @return whether this is a Queue (<code>true</code>) or a Topic (<code>false</code>).
         */
        DestinationType getDestinationType();

        /**
         * @return whether this is a Dead Letter Queue (<code>true</code>) or not (<code>false</code>).
         */
        boolean isDlq();

        /**
         * If the broker isn't properly configured with a broker-specific <i>Individual Dead Letter Queue policy</i>
         * where each queue gets its own DLQ (being the original queue name prefixed with "DLQ."), then there will be a
         * global DLQ. This is not very good - but it will nevertheless be reported. This method will then return
         * {@link Optional#empty()}, while get {@link #getDestinationName()} will be the actual DLQ name (e.g. for
         * ActiveMQ, it is <code>"ActiveMQ.DLQ"</code>, while for Artemis it is <code>"DLQ"</code>).
         *
         * @return whether this is the global DLQ for the broker ({@link #isDlq()} will then also return
         *         <code>true</code>).
         */
        boolean isGlobalDlq();

        /**
         * If this {@link #getDestinationName()} represent a Mats Stage (both a normal Queue or Topic for a Mats Stage,
         * or an individual DLQ for a Mats Stage), then this will be the StageId - i.e. the destination name with the
         * MatsDestinationPrefix (default "mats."), or and any "DLQ." prefix (thus standard/default "DLQ.mats.") cropped
         * off.
         * <p/>
         * If the broker isn't properly configured with a broker-specific <i>Individual Dead Letter Queue policy</i>
         * where each queue gets its own DLQ (being the original queue name prefixed with "DLQ."), then there will be a
         * global DLQ. This is not very good - but it will nevertheless be reported. This method will then return
         * {@link Optional#empty()}, while get {@link #getDestinationName()} will be the actual DLQ name (e.g. for
         * ActiveMQ, it is <code>"ActiveMQ.DLQ"</code>, while for Artemis it is <code>"DLQ"</code>).
         * <p/>
         * <b>Note: It is highly recommended to configure the broker with an individual DLQ policy!</b>
         *
         * @return the Mats StageId if the {@link #getDestinationName()} represent a Mats Stage, or DLQ for such.
         * @see #getDestinationName()
         */
        Optional<String> getMatsStageId();

        /**
         * @return the number of messages on this destination <i>when this update was produced</i> (i.e. from the
         *         {@link #getLastUpdateBrokerMillis()}). Will include <i>in-flight messages</i> if that number is set,
         *         i.e. messages that are sent to a consumer, but not yet acknowledged/consumed - see
         *         {@link #getNumberOfInflightMessages()}.
         */
        long getNumberOfQueuedMessages();

        /**
         * @return the number of messages "in flight", i.e. <i>being delivered</i> to consumers, <i>when this update was
         *         produced</i> (E.g. "in flight count" in ActiveMQ nomenclature, while "delivering count" for Artemis).
         *         Note the situation described in {@link #getHeadMessageAgeMillis()}, whereby a consumer might have
         *         been given 1000 messages to consume, but the actual consume loop is stuck.
         */
        OptionalLong getNumberOfInflightMessages();

        /**
         * @return the age of the message at the head of the queue <i>when this update was produced</i> (i.e. from the
         *         {@link #getLastUpdateBrokerMillis()}) - do note that this might refer to a message that is dispatched
         *         (sent to a consumer), but where the consumer has not acknowledged it yet (i.e. via ack or commit of
         *         the transaction where it was received). Note: There is a possibility that a message A has been
         *         dispatched to a (slow) consumer 1, while there's also a later message B that was dispatched to, and
         *         already acknowledged by, a consumer 2. The consumer 1 might even be stuck - so you'll see a very old
         *         message as head (and there might be several of such old messages due to batch-sending from MQ to
         *         consumers), while the queue actually still progresses since consumer 2 is still chugging along.
         */
        OptionalLong getHeadMessageAgeMillis();
    }

}
