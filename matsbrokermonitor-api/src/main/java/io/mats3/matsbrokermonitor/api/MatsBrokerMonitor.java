package io.mats3.matsbrokermonitor.api;

import java.io.Closeable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.function.Consumer;

/**
 * Provides a way to get data from the broker which is not possible to glean from the Mats3 system itself, nor from
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
 * <p/>
 * Note: "Fully Qualified Destination Name" means that the name fully specifies the queue or topic, e.g. for ActiveMQ
 * this includes a schema-like notation "queue://" or "topic://" as prefix. This to handle a queue having the same name
 * as a topic - even though the Mats API forbids this: An "endpointId" fully qualifies a Mats endpoint seen from the
 * MatsFactory's side, no matter if it is e.g. a "terminator" (queue-based) or "subscriptionTerminator" (topic-based).
 * <p/>
 * Note: This is the "monitor the queues" part of the MatsBrokerMonitor. The "browse the queues and actions on messages"
 * part is found in {@link MatsBrokerBrowseAndActions}. The reason for this separation of the API is that the pieces
 * defined in this piece are not part of a standard JMS API and must be implemented specifically for each broker, while
 * the "control" part can be implemented using standard JMS API.
 *
 * @see UpdateEvent for more information about how the information is presented and updated.
 * @see MatsBrokerBrowseAndActions for the API to browse and act upon the messages on the destinations.
 *
 * @author Endre Stølsvik 2021-12-16 23:10 - http://stolsvik.com/, endre@stolsvik.com
 */
public interface MatsBrokerMonitor extends Closeable {

    void start();

    void close();

    Optional<BrokerSnapshot> getSnapshot();

    void registerListener(Consumer<UpdateEvent> listener);

    void removeListener(Consumer<UpdateEvent> listener);

    void forceUpdate(String correlationId, boolean full);

    interface BrokerSnapshot {
        /**
         * @return the millis-since-Epoch when this was last updated, using the time of this receiving computer. Compare
         *         to {@link #getLastUpdateBrokerMillis()}.
         */
        long getLastUpdateLocalMillis();

        /**
         * @return the millis-since-epoch <i>on the broker side</i> when this was last updated. Compare to
         *         {@link #getLastUpdateLocalMillis()}.
         */
        OptionalLong getLastUpdateBrokerMillis();

        /**
         * @return the number of milliseconds it took to gather these statistics, e.g. between the statistics request(s)
         *         were started, to the (last) response was received (Only available on the node which performed the
         *         job).
         */
        OptionalDouble getStatisticsUpdateMillis();

        /**
         * @return a
         *         <code>Map[FullyQualifiedDestinationName, {@link MatsBrokerDestination MatsBrokerDestination}]</code> for
         *         currently known Mats-relevant destinations. (FullyQualifiedDestinationName ==
         *         {@link MatsBrokerDestination#getFqDestinationName() destination.getFqDestinationName()}).
         */
        NavigableMap<String, MatsBrokerDestination> getMatsDestinations();

        /**
         * @return a {@link BrokerInfo} instance.
         */
        Optional<BrokerInfo> getBrokerInfo();
    }

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
        Optional<String> getBrokerJson();
    }

    /**
     * MatsBrokerMonitor will send an {@link UpdateEvent} to all registered listeners when it has received an update
     * from the broker. This will happen periodically, and when forced by invoking
     * {@link #forceUpdate(String, boolean)}, and might also be sent when destinations disappear. It is assumed that the
     * receiver of this event will keep a local copy of the data, and update it with the information in the event. If an
     * update comes with {@link #isFullUpdate()} <code>false</code>, the receiver should assume that any destinations
     * not mentioned have zero messages pending. If {@link #isFullUpdate()} is <code>true</code>, the receiver should
     * consider the {@link #getEventDestinations()} as authoritative information about all currently known
     * Mats3-relevant destinations on the broker, thus overwriting any local view kept by incremental updates.
     */
    interface UpdateEvent {
        /**
         * @return when the MatsBrokerMonitor node created this {@link UpdateEvent}, in millis-since-Epoch.
         */
        long getStatisticsUpdateMillis();

        /**
         * @return the correlationId if this update event is a reply to an invocation of
         *         {@link #forceUpdate(String, boolean)}, otherwise {@link Optional#empty()}.
         */
        Optional<String> getCorrelationId();

        /**
         * A full update will be sent when {@link #forceUpdate(String, boolean)} was invoked with the 'full' parameter
         * set to <code>true</code>, and will be sent periodically, and might be sent when destinations disappear.
         * <p/>
         * When this is <code>true</code>, the receiver should consider the {@link #getEventDestinations()} as
         * authoritative information about all currently known Mats3-relevant destinations on the broker, thus
         * overwriting any local view kept by incremental updates. {@link MatsBrokerMonitor#getSnapshot()
         * BrokerSnapshots} taken after such an event will also reflect the new situation.
         * <p/>
         * When this is <code>false</code>, the receiver shall assume that any destinations not mentioned have zero
         * messages pending.
         *
         * @return whether this is a full update (<code>true</code>), in which case the receiver should consider the
         *         {@link #getEventDestinations()} as authoritative information about all currently known destinations
         *         on the broker, thus overwriting any local view kept by incremental updates.
         */
        boolean isFullUpdate();

        /**
         * Relevant for the local listeners on the {@link MatsBrokerMonitor} nodes. If this is <code>true</code>, the
         * {@link MatsBrokerMonitor} node which produced this event is the one which did the actual statistics request
         * to the broker - there will only be one node with <code>true</code>, all others will have <code>false</code>.
         * If you are to forward or record the statistics, the node having <code>true</code> here would be the correct
         * node to do it on.
         * <p/>
         * Note: If you are getting this update by the 'matsbrokermonitor-broadcastreceiver' module (via the
         * 'matsbrokermonitor-broadcastandcontrol' module), this will always return <code>false</code>, as this is then
         * a "broadcast copy" of the UpdateEvent produced by the node which did the actual statistics request to the
         * broker (then broadcast by the node that had <code>true</code> here).
         *
         * @return <code>true</code> if this UpdateEvent originated on this node - if you are to forward or record the
         *         statistics, the node having <code>true</code> here would be the correct node to do it from, as there
         *         will only be one node that has <code>true</code>, all others have <code>false</code>.
         */
        boolean isUpdateEventOriginatedOnThisNode();

        /**
         * @return a {@link BrokerInfo} instance if available.
         */
        Optional<BrokerInfo> getBrokerInfo();

        /**
         * @return a Map[FullyQualifiedDestinationName, {@link MatsBrokerDestination}] for either the non-zero
         *         destinations, or if {@link #isFullUpdate()} is true, all destinations.
         */
        NavigableMap<String, MatsBrokerDestination> getEventDestinations();
    }

    interface MatsBrokerDestination {

        String STAGE_ATTRIBUTE_DESTINATION = "mats.mbm.Queue";
        String STAGE_ATTRIBUTE_DLQ = "mats.mbm.DLQ";

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
         *         <code>"queue://mats.ServiceName.someServiceMethodName"</code> or
         *         <code>"topic://mats.ServiceName.someSubscriptionTerminator"</code>.
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
         * @return what type of destination this is: QUEUE or TOPIC.
         */
        DestinationType getDestinationType();

        /**
         * @return whether this is a Dead Letter Queue (<code>true</code>) or not (<code>false</code>).
         */
        boolean isDlq();

        /**
         * If the broker isn't properly configured with a broker-specific <i>Individual Dead Letter Queue policy</i>
         * where each queue gets its own DLQ (being the original queue name prefixed with "DLQ."), then there will be a
         * global DLQ. This is not very good - but it will nevertheless be reported. The method
         * {@link #getMatsStageId()} will then return {@link Optional#empty()}, while get {@link #getDestinationName()}
         * will be the actual DLQ name (e.g. for ActiveMQ, it is <code>"ActiveMQ.DLQ"</code>, while for Artemis it is
         * <code>"DLQ"</code>).
         * <p/>
         * <b>Note: It is highly recommended to configure the broker with an individual DLQ policy!</b>
         *
         * @return whether this is the global DLQ for the broker ({@link #isDlq()} will then also return
         *         <code>true</code>).
         */
        boolean isDefaultGlobalDlq();

        /**
         * If this {@link #getDestinationName()} represent a Mats Stage (both a normal Queue or Topic for a Mats Stage,
         * or an individual DLQ for a Mats Stage), then this will be the StageId - i.e. the destination name with the
         * MatsDestinationPrefix (default "mats."), and any "DLQ." prefix (thus standard/default "DLQ.mats.") cropped
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

    /**
     * Queue or Topic.
     */
    enum DestinationType {
        QUEUE, TOPIC;
    }

    /**
     * A DTO-implementation of {@link BrokerInfo}, which can be sent over Mats3 (field serialized). Used by the
     * Broadcaster and BroadcasterReceiver.
     */
    class BrokerInfoDto implements BrokerInfo {
        private String bt;
        private String bn;
        private Optional<String> bj;

        public static BrokerInfoDto of(BrokerInfo brokerInfo) {
            return new BrokerInfoDto(brokerInfo);
        }

        private BrokerInfoDto() {
            /* need no-args constructor for deserializing with Jackson */
        }

        private BrokerInfoDto(BrokerInfo brokerInfo) {
            bt = brokerInfo.getBrokerType();
            bn = brokerInfo.getBrokerName();
            bj = brokerInfo.getBrokerJson();
        }

        @Override
        public String getBrokerType() {
            return bt;
        }

        @Override
        public String getBrokerName() {
            return bn;
        }

        @Override
        public Optional<String> getBrokerJson() {
            return bj;
        }

        @Override
        public String toString() {
            return "BrokerInfoDto{" +
                    "brokerType='" + bt + '\'' +
                    ", brokerName='" + bn + '\'' +
                    ", brokerJson=" + bj.orElse("").length() + " chars}";
        }
    }

    /**
     * A DTO-implementation of {@link MatsBrokerDestination}, which can be sent over Mats3 (field serialized). Used by
     * the Broadcaster and BroadcasterReceiver.
     */
    class MatsBrokerDestinationDto implements MatsBrokerDestination {
        private long lulm;
        private OptionalLong lubm;
        private String fqdn;
        private String dn;
        private DestinationType dt;
        private boolean dlq;
        private boolean gdlq;
        private Optional<String> msid;
        private long noqm;
        private OptionalLong noifm;
        private OptionalLong age;

        public static MatsBrokerDestinationDto of(MatsBrokerDestination matsBrokerDestination) {
            return new MatsBrokerDestinationDto(matsBrokerDestination);
        }

        private MatsBrokerDestinationDto() {
            /* need no-args constructor for deserializing with Jackson */
        }

        private MatsBrokerDestinationDto(MatsBrokerDestination matsBrokerDestination) {
            lulm = matsBrokerDestination.getLastUpdateLocalMillis();
            lubm = matsBrokerDestination.getLastUpdateBrokerMillis();
            fqdn = matsBrokerDestination.getFqDestinationName();
            dn = matsBrokerDestination.getDestinationName();
            dt = matsBrokerDestination.getDestinationType();
            dlq = matsBrokerDestination.isDlq();
            gdlq = matsBrokerDestination.isDefaultGlobalDlq();
            msid = matsBrokerDestination.getMatsStageId();
            noqm = matsBrokerDestination.getNumberOfQueuedMessages();
            noifm = matsBrokerDestination.getNumberOfInflightMessages();
            age = matsBrokerDestination.getHeadMessageAgeMillis();
        }

        @Override
        public long getLastUpdateLocalMillis() {
            return lulm;
        }

        @Override
        public OptionalLong getLastUpdateBrokerMillis() {
            return lubm;
        }

        @Override
        public String getFqDestinationName() {
            return fqdn;
        }

        @Override
        public String getDestinationName() {
            return dn;
        }

        @Override
        public DestinationType getDestinationType() {
            return dt;
        }

        @Override
        public boolean isDlq() {
            return dlq;
        }

        @Override
        public boolean isDefaultGlobalDlq() {
            return gdlq;
        }

        @Override
        public Optional<String> getMatsStageId() {
            return msid;
        }

        @Override
        public long getNumberOfQueuedMessages() {
            return noqm;
        }

        @Override
        public OptionalLong getNumberOfInflightMessages() {
            return noifm;
        }

        @Override
        public OptionalLong getHeadMessageAgeMillis() {
            return age;
        }

        @Override
        public String toString() {
            return "MatsBrokerDestinationImpl{" +
                    "lastUpdateMillis=" + LocalDateTime.ofInstant(Instant.ofEpochMilli(lulm), ZoneId.systemDefault()) +
                    ", lastUpdateBrokerMillis=" + lubm +
                    ", fqDestinationName='" + fqdn + '\'' +
                    ", destinationName='" + dn + '\'' +
                    ", matsStageId='" + msid + '\'' +
                    ", destinationType=" + dt +
                    ", isDlq=" + dlq +
                    ", isGlobalDlq=" + gdlq +
                    ", numberOfQueuedMessages=" + noqm +
                    ", numberOfInFlightMessages=" + noifm +
                    ", headMessageAgeMillis=" + age +
                    '}';
        }
    }
}
