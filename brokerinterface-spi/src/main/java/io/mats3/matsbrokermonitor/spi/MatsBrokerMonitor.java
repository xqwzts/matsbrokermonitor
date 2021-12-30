package io.mats3.matsbrokermonitor.spi;

import java.io.Closeable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Provides a way to get data from the broker which is not possible to glean from the Mats system itself, nor from
 * standard ways over the JMS API (or any other known messaging protocol).
 * <p />
 * These data are:
 * <ul>
 * <li>Which destinations exist, and whether they are queues and topics (however, the total number of Mats endpoints can
 * been found by asking all MatsFactories)</li>
 * <li>Dead Letter Queues - both properly configured (on the broker) Individual Dead Letter Queues, and any unfortunate
 * (typically default) global DLQs</li>
 * <li>Biggest point: <b>The number of messages on the destination or Dead Letter Queue</b></li>
 * </ul>
 *
 * Note: "Fully Qualified Destination Name" means that the name fully specifies the queue or topic, e.g. for ActiveMQ
 * this includes a schema-like notation "queue://" or "topic://" as prefix. This to handle a queue having the same name
 * as a topic - even though the Mats API forbids this: An "endpointId" shall fully qualify a Mats endpoint, no matter if
 * it is e.g. a "terminator" (queue-based) or "subscriptionTerminator" (topic-based).
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
    Map<String, MatsBrokerDestination> getMatsDestinations();

    void registerListener(Consumer<DestinationUpdateEvent> listener);

    interface DestinationUpdateEvent {
        /**
         * A full update might be sent periodically.
         *
         * @return whether this is a full update, in which case the receiver should consider the
         *         {@link #getNewOrUpdatedDestinations()} as authoritative information about all existing known
         *         destinations on the broker, thus overwriting any local view kept by incrementally updating, and that
         *         the {@link #getRemovedDestinations()} won't contain anything (anything not in this update doesn't
         *         exist).
         */
        boolean isFullUpdate();

        /**
         * @return a Map[FullyQualifiedDestinationName, {@link MatsBrokerDestination}] for any new or updated
         *         Mats-relevant destinations.
         */
        Map<String, MatsBrokerDestination> getNewOrUpdatedDestinations();

        /**
         * @return the set of fully qualified destination names (queues or topics) that are new since last update.
         */
        Set<String> getNewDestinations();

        /**
         * @return the set of fully qualified destination names (queues or topics) that have disappeared since last
         *         update (computed by "scavenging", i.e. not seen for a while) - notice that this might happen with
         *         existing Mats endpoints if the broker removes the queue or topic e.g. due to inactivity or boot. Such
         *         a situation should just be interpreted as that stageId not having any messages in queue.
         */
        Set<String> getRemovedDestinations();
    }

    interface MatsBrokerDestination {
        /**
         * @return the millis-from-Epoch when this was last updated.
         */
        long getLastUpdateMillis();

        /**
         * @return the raw destination name, but not "fully qualified", i.e. "mats.ServiceName.serviceMethodName" or
         *         "ActiveMQ.DLQ" - but not including any scheme prefix like "queue://" or "topic://" (they aren't
         *         standard). To get whether it is a queue or topic, use {@link #isQueue()}.
         * @see #getMatsStageId()
         */
        String getDestinationName();

        /**
         * @return whether this is a Queue (<code>true</code>) or a Topic (<code>false</code>).
         */
        boolean isQueue();

        /**
         * @return whether this is a Dead Letter Queue (<code>true</code>) or not (<code>false</code>).
         */
        boolean isDlq();

        /**
         * If this {@link #getDestinationName()} represent a Mats StageId (both a normal Queue or Topic for a Stage, or
         * an individual DLQ for a Mats Stage), then this will be the name of it - i.e. the destination name with the
         * MatsDestinationPrefix (default "mats."), or and any "DLQ." prefix (thus standard/default "DLQ.mats.") cropped
         * off.
         * <p />
         * If the broker isn't properly configured with a broker-specific <i>Individual Dead Letter Queue policy</i>
         * where each queue gets its own DLQ (being the original queue name prefixed with "DLQ."), then there will be a
         * global DLQ. This is not very good - but it will nevertheless be reported. This method will then return
         * {@link Optional#empty()}, while get {@link #getDestinationName()} will be the actual DLQ name (e.g. for
         * ActiveMQ, it is <code>"ActiveMQ.DLQ"</code>, while for Artemis it is <code>"DLQ"</code>).
         * <p/>
         * <b>It is highly recommended to use individual DLQ policies.</b>
         *
         * @return the Mats StageId if the {@link #getDestinationName()} represent a Mats Stage.
         * @see #getDestinationName()
         */
        Optional<String> getMatsStageId();

        /**
         * @return the number of messages on this destination. Might include "in-flight" messages, i.e. messages that
         *         are sent to a consumer, but not yet consumed.
         */
        long getNumberOfQueuedMessages();
    }
}
