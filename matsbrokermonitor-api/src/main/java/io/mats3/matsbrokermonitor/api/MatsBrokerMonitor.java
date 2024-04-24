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
 * <p>
 * Note: "Fully Qualified Destination Name" means that the name fully specifies the queue or topic, e.g. for ActiveMQ
 * this includes a schema-like notation "queue://" or "topic://" as prefix. This to handle a queue having the same name
 * as a topic - even though the Mats API forbids this: An "endpointId" fully qualifies a Mats endpoint seen from the
 * MatsFactory's side, no matter if it is e.g. a "terminator" (queue-based) or "subscriptionTerminator" (topic-based).
 * <p>
 * Note: This is the "monitor the queues" part of the MatsBrokerMonitor. The "browse the queues and actions on messages"
 * part is found in {@link MatsBrokerBrowseAndActions}. The reason for this separation of the API is that the
 * functionality defined in this part must be implemented specifically for each broker (the JMS API does not have
 * functions for it), while the "browse and control" part can be implemented using the standard JMS API.
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
         * @return the millis-since-epoch <i>on the broker side</i> when this was last updated, if available. Compare to
         *         {@link #getLastUpdateLocalMillis()}.
         */
        OptionalLong getLastUpdateBrokerMillis();

        /**
         * @return the number of milliseconds it took to gather these statistics, e.g. between the statistics request(s)
         *         were started, to the (last) response was received (Only available on the node which performed the
         *         job).
         */
        OptionalDouble getStatisticsRequestReplyLatencyMillis();

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
         * <p>
         * When this is <code>true</code>, the receiver should consider the {@link #getEventDestinations()} as
         * authoritative information about all currently known Mats3-relevant destinations on the broker, thus
         * overwriting any local view kept by incremental updates. {@link MatsBrokerMonitor#getSnapshot()
         * BrokerSnapshots} taken after such an event will also reflect the new situation.
         * <p>
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
         * <p>
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
         * Queue or Topic.
         */
        enum DestinationType {
            QUEUE, TOPIC;
        }

        /**
         * Mats-specific Stage Destination Type of this destination, which is a categorization of the different
         * destinations a Mats Stage can have
         */
        enum StageDestinationType {

            // THE ORDERING OF THE ENUM VALUES IS IMPORTANT!! They are checked in order of '.values()' return.
            // The reason is that we first need to check those that actually have a midfix, and then those without,
            // otherwise all will match the ones without midfix!

            /**
             * <i>Non-Persistent Interactive</i> Mats destination, e.g.
             * "[matsQueuePrefix]matssys.NPIA.ExampleService.someMethod.stage2". This is a special destination for
             * messages that are marked as non-persistent, and are interactive (i.e. request-reply where a human is
             * waiting). This queue is consumed by a special non-transactional consumer to process these messages as
             * fast as possible. The guaranteed delivery aspect is thus out the window, but since the messages are
             * marked as non-persistent anyway, this is not a problem.
             */
            NON_PERSISTENT_INTERACTIVE(false, "matssys.NPIA."),

            /**
             * <i>Dead Letter Queue for a Non-Persistent Interactive</i> Mats destination, e.g.
             * "DLQ.[matsQueuePrefix]matssys.NPIA.ExampleService.someMethod.stage2".
             * <p>
             * <b>Note: This is not in use by 'Mats Managed DLQ Divert', as the DLQ for NPIA will be the same as for the
             * standard queue.</b> However, if the broker ends up DLQing the NPIA messages, it will employ its standard
             * DLQ handling, which typically is to prefix the existing queue name by "DLQ.". Thus, we will have to
             * handle this DLQ too.
             */
            DEAD_LETTER_QUEUE_NON_PERSISTENT_INTERACTIVE(true, "matssys.NPIA."),

            /**
             * <i>"Muted" Dead Letter Queue</i> for a standard Mats destination, e.g.
             * "DLQ.[matsQueuePrefix]matssys.MUTED_DLQ.ExampleService.someMethod.stage2". This is used when we have a
             * DLQ message that we do not want to have warnings on anymore, because we're already aware of the problem,
             * and are working on it.
             * <p>
             * <b>Note: There is only one "Muted" DLQ, even though there could potentially be two DLQs
             * ({@link #DEAD_LETTER_QUEUE} and {@link #DEAD_LETTER_QUEUE_NON_PERSISTENT_INTERACTIVE}).
             */
            DEAD_LETTER_QUEUE_MUTED(true, "matssys.MUTED_DLQ."),

            /**
             * <i>Wiretap</i> Mats destination, e.g.
             * "[matsQueuePrefix]matssys.WIRETAP.ExampleService.someMethod.stage2". This is a feature whereby you can
             * configure a MatsFactory to produce a copy of all messages sent to a specific Mats Stage and put this copy
             * on the corresponding Wiretap destination. This is useful for debugging.
             */
            WIRETAP(false, "matssys.WIRETAP."),

            /**
             * <i>Standard</i> Mats destination, e.g. "[matsQueuePrefix]ExampleService.someMethod.stage2" - this is
             * where the ordinary Mats Stage processors are consuming from.
             */
            STANDARD(false, ""),

            /**
             * <i>Dead Letter Queue</i> for a {@link #STANDARD standard Mats destination}, e.g.
             * "DLQ.[matsQueuePrefix]ExampleService.someMethod.stage2".
             */
            DEAD_LETTER_QUEUE(true, ""),

            /**
             * Of unknown type - this will never be utilized from the MatsBrokerMonitor. This is to protect against
             * serialization errors on the receiving side if the enum is later extended and the client isn't updated
             * when receiving an update event - we use String as the serialization format.
             */
            UNKNOWN(false, "");

            private final boolean _dlq;
            private final String _midfix;

            StageDestinationType(boolean dlq, String midfix) {
                _dlq = dlq;
                _midfix = midfix;
            }

            public boolean isDlq() {
                return _dlq;
            }

            public String getMidfix() {
                return _midfix;
            }
        }

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
         * Returns whether this is the <i>Broker-specific Global DLQ</i>. If the broker isn't properly configured with a
         * broker-specific <i>Individual Dead Letter Queue policy</i> where each queue gets its own DLQ (being the
         * original queue name prefixed with "DLQ."), then there will be a Global DLQ. This is an unfortunate setup when
         * using Mats3, since it will lump all processing errors for all stages, important or not, into a big heap, and
         * this MatsBrokerMonitor cannot then nicely show where each problem is. If this destination represents the
         * Global DLQ, then this method will return <code>true</code>. The method {@link #getMatsStageId()} will then
         * return {@link Optional#empty()}, while {@link #getDestinationName()} will be the actual DLQ name (e.g. for
         * ActiveMQ, it is <code>"ActiveMQ.DLQ"</code>, while for Artemis it is <code>"DLQ"</code>) - and the
         * {@link #getFqDestinationName()} will be the fully qualified name, e.g. <code>"queue://ActiveMQ.DLQ"</code>.
         * <p>
         * <b>Note: It is highly recommended to configure the broker with an individual DLQ policy!</b>
         *
         * @return whether this is the global DLQ for the broker ({@link #isDlq()} will then also return
         *         <code>true</code>).
         */
        boolean isBrokerDefaultGlobalDlq();

        /**
         * If this {@link #getDestinationName()} represent a Mats Stage (e.g. a Queue or Topic for a Mats Stage, or an
         * individual DLQ for a Mats Stage, or any other of the {@link StageDestinationType}), then this will be the
         * StageId - i.e. the destination name where the mats-specifics are cropped off, e.g. MatsDestinationPrefix
         * (default "mats."), and any "DLQ." prefix (thus standard/default "DLQ.mats.") and any other prefixes
         * representing the different QueueTypes (e.g. "mats.WIRETAP." for Wiretap). Thus, if this is a DLQ, then this
         * will be the Mats StageId for which it is the DLQ.
         * <p>
         * <b>Please read the information about Global DLQ here: {@link #isBrokerDefaultGlobalDlq()}</b>
         *
         * @return the Mats StageId if the {@link #getDestinationName()} represent a Mats Stage, or DLQ for such.
         * @see #getStageDestinationType() for the categorization of the type of stage destination this is.
         * @see #getDestinationName() for the raw destination name.
         * @see #getFqDestinationName() for the fully qualified destination name.
         */
        Optional<String> getMatsStageId();

        /**
         * @return the Mats-specific {@link StageDestinationType} of this destination, which is a categorization of the
         *         different destinations a Mats Stage can have (standard, DLQ, Muted DLQ, Wiretap etc). If this
         *         destination does not represent a Mats Stage, it will return {@link Optional#empty()}.
         * @see #getMatsStageId() for the StageId.
         */
        Optional<StageDestinationType> getStageDestinationType();

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
        private Optional<String> sdt; // Use String for serialization, as we might have an older client missing enums.
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
            gdlq = matsBrokerDestination.isBrokerDefaultGlobalDlq();
            msid = matsBrokerDestination.getMatsStageId();
            sdt = matsBrokerDestination.getStageDestinationType().map(Enum::name);
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
        public boolean isBrokerDefaultGlobalDlq() {
            return gdlq;
        }

        @Override
        public Optional<String> getMatsStageId() {
            return msid;
        }

        @Override
        public Optional<StageDestinationType> getStageDestinationType() {
            // ?: Is it empty?
            if (sdt.isEmpty()) {
                // -> Yes, it is empty, return empty.
                return Optional.empty();
            }
            // Handle unknown types if this is deserialized from a DTO and the client is not updated.
            try {
                return Optional.of(StageDestinationType.valueOf(sdt.get()));
            }
            catch (IllegalArgumentException e) {
                // If the enum is unknown, we return the UNKNOWN type.
                return Optional.of(StageDestinationType.UNKNOWN);
            }
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
                    ", stageDestinationType=" + sdt +
                    ", isDlq=" + dlq +
                    ", isGlobalDlq=" + gdlq +
                    ", numberOfQueuedMessages=" + noqm +
                    ", numberOfInFlightMessages=" + noifm +
                    ", headMessageAgeMillis=" + age +
                    '}';
        }
    }
}
