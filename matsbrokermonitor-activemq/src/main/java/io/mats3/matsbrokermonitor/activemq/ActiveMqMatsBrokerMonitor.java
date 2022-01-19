package io.mats3.matsbrokermonitor.activemq;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import javax.jms.ConnectionFactory;

import io.mats3.matsbrokermonitor.api.DestinationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mats3.matsbrokermonitor.activemq.ActiveMqBrokerStatsQuerier.ActiveMqBrokerStatsEvent;
import io.mats3.matsbrokermonitor.activemq.ActiveMqBrokerStatsQuerier.DestinationStatsDto;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor;

/**
 * @author Endre Stølsvik 2021-12-27 14:40 - http://stolsvik.com/, endre@stolsvik.com
 */
public class ActiveMqMatsBrokerMonitor implements MatsBrokerMonitor, Statics {
    private static final Logger log = LoggerFactory.getLogger(ActiveMqMatsBrokerMonitor.class);

    static ActiveMqMatsBrokerMonitor create(ActiveMqBrokerStatsQuerierImpl querier, String matsDestinationPrefix) {
        return new ActiveMqMatsBrokerMonitor(querier, matsDestinationPrefix);
    }

    private final ActiveMqBrokerStatsQuerier _querier;

    private final String _matsDestinationPrefix;
    private final String _matsDestinationIndividualDlqPrefix;

    /**
     * Creates an instance, supplying both the querier and the MatsDestinationPrefix.
     *
     * @param querier
     *            the querier doing the actual talking with ActiveMQ.
     * @param matsDestinationPrefix
     *            the destination prefix that the MatsFactory is configured with.
     * @return the newly created instance.
     */
    public static ActiveMqMatsBrokerMonitor create(ActiveMqBrokerStatsQuerier querier, String matsDestinationPrefix) {
        return new ActiveMqMatsBrokerMonitor(querier, matsDestinationPrefix);
    }

    /**
     * Convenience method where the {@link ActiveMqBrokerStatsQuerierImpl} is instantiated locally based on the supplied
     * JMS {@link ConnectionFactory} and MatsDestinationPrefix.
     *
     * @param jmsConnectionFactory
     *            the ConnectionFactory to the backing ActiveMQ.
     * @return the newly created instance.
     */
    public static ActiveMqMatsBrokerMonitor create(ConnectionFactory jmsConnectionFactory,
            String matsDestinationPrefix) {
        ActiveMqBrokerStatsQuerierImpl querier = ActiveMqBrokerStatsQuerierImpl.create(jmsConnectionFactory);
        return create(querier, matsDestinationPrefix);
    }

    /**
     * Convenience method where the {@link ActiveMqBrokerStatsQuerierImpl} is instantiated locally based on the supplied
     * JMS {@link ConnectionFactory}, and where the MatsFactory is assumed to be set up with the default "mats."
     * MatsDestinationPrefix.
     *
     * @param jmsConnectionFactory
     *            the ConnectionFactory to the backing ActiveMQ.
     * @return the newly created instance.
     */
    public static ActiveMqMatsBrokerMonitor create(ConnectionFactory jmsConnectionFactory) {
        return create(jmsConnectionFactory, "mats.");
    }

    private ActiveMqMatsBrokerMonitor(ActiveMqBrokerStatsQuerier querier, String matsDestinationPrefix) {
        if (querier == null) {
            throw new NullPointerException("querier");
        }
        if (matsDestinationPrefix == null) {
            throw new NullPointerException("matsDestinationPrefix");
        }
        _querier = querier;
        // Set(/override) the MatsDestinationPrefix (they must naturally be set the same here and there).
        _querier.setMatsDestinationPrefix(matsDestinationPrefix);
        _querier.registerListener(this::eventFromQuerier);

        _matsDestinationPrefix = matsDestinationPrefix;
        _matsDestinationIndividualDlqPrefix = INDIVIDUAL_DLQ_PREFIX + matsDestinationPrefix;
    }

    private final CopyOnWriteArrayList<Consumer<DestinationUpdateEvent>> _listeners = new CopyOnWriteArrayList<>();

    private volatile BrokerInfo _brokerInfo;

    @Override
    public void registerListener(Consumer<DestinationUpdateEvent> listener) {
        _listeners.add(listener);
    }

    @Override
    public void forceUpdate() {
        _querier.forceUpdate();
    }

    @Override
    public void start() {
        _querier.start();
    }

    @Override
    public void close() {
        _querier.close();
    }

    @Override
    public ConcurrentNavigableMap<String, MatsBrokerDestination> getMatsDestinations() {
        return _currentDestinationsMap;
    }

    @Override
    public Optional<BrokerInfo> getBrokerInfo() {
        return Optional.ofNullable(_brokerInfo);
    }

    // ===== IMPLEMENTATION

    private static class MatsBrokerDestinationImpl implements MatsBrokerDestination {
        private final long _lastUpdateMillis;
        private final long _lastUpdateBrokerMillis;
        private final String _fqDestinationName;
        private final String _destinationName;
        private final String _matsStageId;
        private final DestinationType _destinationType;
        private final boolean _isDlq;
        private final boolean _isGlobalDlq;
        private final long _numberOfQueuedMessages;
        private final long _numberOfInFlightMessages;
        private final long _headMessageAgeMillis;

        public MatsBrokerDestinationImpl(long lastUpdateMillis, long lastUpdateBrokerMillis, String fqDestinationName,
                String destinationName, String matsStageId, DestinationType destinationType, boolean isDlq,
                boolean isGlobalDlq, long numberOfQueuedMessages, long numberOfInFlightMessages,
                long headMessageAgeMillis) {
            _lastUpdateMillis = lastUpdateMillis;
            _lastUpdateBrokerMillis = lastUpdateBrokerMillis;
            _fqDestinationName = fqDestinationName;
            _destinationName = destinationName;
            _matsStageId = matsStageId;
            _destinationType = destinationType;
            _isDlq = isDlq;
            _isGlobalDlq = isGlobalDlq;
            _numberOfQueuedMessages = numberOfQueuedMessages;
            _numberOfInFlightMessages = numberOfInFlightMessages;
            _headMessageAgeMillis = headMessageAgeMillis;
        }

        @Override
        public long getLastUpdateLocalMillis() {
            return _lastUpdateMillis;
        }

        @Override
        public OptionalLong getLastUpdateBrokerMillis() {
            if (_lastUpdateBrokerMillis == 0) {
                return OptionalLong.empty();
            }
            return OptionalLong.of(_lastUpdateBrokerMillis);
        }

        @Override
        public String getFqDestinationName() {
            return _fqDestinationName;
        }

        @Override
        public String getDestinationName() {
            return _destinationName;
        }

        @Override
        public DestinationType getDestinationType() {
            return _destinationType;
        }

        @Override
        public boolean isDlq() {
            return _isDlq;
        }

        @Override
        public boolean isGlobalDlq() {
            return _isGlobalDlq;
        }

        @Override
        public Optional<String> getMatsStageId() {
            return Optional.ofNullable(_matsStageId);
        }

        @Override
        public long getNumberOfQueuedMessages() {
            return _numberOfQueuedMessages;
        }

        @Override
        public OptionalLong getNumberOfInflightMessages() {
            return OptionalLong.of(_numberOfInFlightMessages);
        }

        @Override
        public OptionalLong getHeadMessageAgeMillis() {
            if (_headMessageAgeMillis == 0) {
                return OptionalLong.empty();
            }
            return OptionalLong.of(_headMessageAgeMillis);
        }

        @Override
        public String toString() {
            return "MatsBrokerDestinationImpl{" +
                    "_lastUpdateMillis=" + LocalDateTime.ofInstant(Instant.ofEpochMilli(_lastUpdateMillis),
                            ZoneId.systemDefault()) +
                    ", _lastUpdateBrokerMillis=" + (_lastUpdateBrokerMillis == 0 ? "{not present}"
                            : LocalDateTime.ofInstant(Instant.ofEpochMilli(_lastUpdateBrokerMillis),
                                    ZoneId.systemDefault())) +
                    ", _destinationName='" + _destinationName + '\'' +
                    ", _matsStageId='" + _matsStageId + '\'' +
                    ", _destinationType=" + _destinationType +
                    ", _isDlq=" + _isDlq +
                    ", _isGlobalDlq=" + _isGlobalDlq +
                    ", _numberOfQueuedMessages=" + _numberOfQueuedMessages +
                    ", _numberOfInFlightMessages=" + (_numberOfInFlightMessages == 0
                            ? "{not present}"
                            : _numberOfInFlightMessages) +
                    ", _headMessageAgeMillis=" + (_headMessageAgeMillis == 0
                            ? "{not present}"
                            : _headMessageAgeMillis) +
                    '}';
        }
    }

    private static class BrokerInfoImpl implements BrokerInfo {
        private final String _brokerType;
        private final String _brokerName;
        private final String _brokerJson;

        public BrokerInfoImpl(String brokerType, String brokerName, String brokerJson) {
            _brokerType = brokerType;
            _brokerName = brokerName;
            _brokerJson = brokerJson;
        }

        @Override
        public String getBrokerType() {
            return _brokerType;
        }

        @Override
        public String getBrokerName() {
            return _brokerName;
        }

        @Override
        public String getBrokerJson() {
            return _brokerJson;
        }
    }

    private static class DestinationUpdateEventImpl implements DestinationUpdateEvent {

        private final boolean _isFullUpdate;
        private final NavigableMap<String, MatsBrokerDestination> _newOrUpdatedDestinations;

        public DestinationUpdateEventImpl(boolean isFullUpdate,
                NavigableMap<String, MatsBrokerDestination> newOrUpdatedDestinations) {
            _isFullUpdate = isFullUpdate;
            _newOrUpdatedDestinations = newOrUpdatedDestinations;
        }

        @Override
        public boolean isFullUpdate() {
            return _isFullUpdate;
        }

        @Override
        public NavigableMap<String, MatsBrokerDestination> getNewOrUpdatedDestinations() {
            return _newOrUpdatedDestinations;
        }
    }

    private final ConcurrentNavigableMap<String, MatsBrokerDestination> _currentDestinationsMap = new ConcurrentSkipListMap<>();

    private void eventFromQuerier(ActiveMqBrokerStatsEvent event) {
        ConcurrentNavigableMap<String, DestinationStatsDto> destStatsDtos = _querier
                .getCurrentDestinationStatsDtos();

        List<String> fqDestinationsNewOrUpdated = new ArrayList<>();

        int matsDestinationCount = 0;

        for (Entry<String, DestinationStatsDto> entry : destStatsDtos.entrySet()) {
            String fqDestinationName = entry.getKey();
            // DestinationName: remove both "queue://" and "topic://", both are 8 length.
            String destinationName = fqDestinationName.substring(8);

            // Whether this is a queue/topic for a MatsStage
            boolean isMatsStageDestination = destinationName.startsWith(_matsDestinationPrefix);

            // Whether this is an individual DLQ for a MatsStage
            boolean isMatsStageDlq = destinationName.startsWith(_matsDestinationIndividualDlqPrefix);

            // Whether this is the global DLQ
            boolean isGlobalDlq = ACTIVE_MQ_GLOBAL_DLQ_NAME.equals(destinationName);

            // Whether we care about this destination: Either Mats stage or individual DLQ for such, or global DLQ
            boolean matsRelevant = isMatsStageDestination || isMatsStageDlq || isGlobalDlq;

            // ?: Do we care?
            if (!matsRelevant) {
                // -> No, so go to next.
                continue;
            }

            // ----- This is a Mats relevant destination!

            matsDestinationCount++;

            // Is this a queue?
            DestinationType destinationType = fqDestinationName.startsWith("queue://")
                    ? DestinationType.QUEUE
                    : DestinationType.TOPIC;

            // Whether this is a DLQ: Individual DLQ or global DLQ.
            boolean isDlq = isMatsStageDlq || isGlobalDlq;

            // :: Find the MatsStageId for this destination, chop off "mats." or "DLQ.mats."
            String matsStageId;
            if (isMatsStageDestination) {
                matsStageId = destinationName.substring(_matsDestinationPrefix.length());
            }
            else if (isMatsStageDlq) {
                matsStageId = destinationName.substring(_matsDestinationIndividualDlqPrefix.length());
            }
            else {
                matsStageId = null;
            }

            DestinationStatsDto stats = entry.getValue();

            long lastUpdateMillis = stats.statsReceived.toEpochMilli();

            long numberOfQueuedMessages = stats.size;

            long numberOfInflightMessages = stats.inflightCount;

            long lastUpdateBrokerMillis = stats.brokerTime.map(Instant::toEpochMilli).orElse(0L);

            // If present: Calculate age: If lastUpdateBrokerMillis present, use this, otherwise lastUpdateMillis
            long headMessageAgeMillis = stats.headMessageBrokerInTime.map(instant -> (lastUpdateBrokerMillis != 0
                    ? lastUpdateBrokerMillis
                    : lastUpdateMillis) - instant.toEpochMilli())
                    .orElse(0L);

            if (log.isDebugEnabled()) log.debug("FQ Name: " + fqDestinationName + ", destinationName:["
                    + destinationName
                    + "], matsStageId:[" + matsStageId + "], destinationType:[" + destinationType + "], isDlq:[" + isDlq
                    + "], queuedMessages:[" + numberOfQueuedMessages + "]");

            // Create the representation
            MatsBrokerDestinationImpl matsBrokerDestination = new MatsBrokerDestinationImpl(lastUpdateMillis,
                    lastUpdateBrokerMillis, fqDestinationName, destinationName, matsStageId, destinationType, isDlq,
                    isGlobalDlq, numberOfQueuedMessages, numberOfInflightMessages, headMessageAgeMillis);
            // Put it in the map.
            MatsBrokerDestination existingInfo = _currentDestinationsMap.put(fqDestinationName, matsBrokerDestination);
            // ?: Was this new, or there an update in number of queued messages?
            if ((existingInfo == null) || existingInfo.getNumberOfQueuedMessages() != numberOfQueuedMessages) {
                // -> Yes, new or updated - so then it is new-or-changed!
                fqDestinationsNewOrUpdated.add(fqDestinationName);
            }
        }

        // ----- We've parsed the update from the Querier.

        // TODO: At most log.debug
        log.info("Got event for [" + destStatsDtos.size() + "] destinations, [" + matsDestinationCount + "] of them was"
                + " Mats-relevant, [" + fqDestinationsNewOrUpdated.size() + "] was new/updated. Notifying listeners.");

        // :: Scavenge old destinations no longer getting updates (i.e. not existing anymore).
        Iterator<MatsBrokerDestination> currentDestinationsIterator = _currentDestinationsMap.values().iterator();
        long longAgo = System.currentTimeMillis() - SCAVENGE_OLD_DESTINATIONS_SECONDS * 1000;
        int removedMatsDestinations = 0;
        while (currentDestinationsIterator.hasNext()) {
            MatsBrokerDestination next = currentDestinationsIterator.next();
            if (next.getLastUpdateLocalMillis() < longAgo) {
                log.info("Removing destination [" + next.getDestinationName() + "]");
                currentDestinationsIterator.remove();
                removedMatsDestinations++;
            }
        }
        boolean removedDestinations_ForceFullUpdate = removedMatsDestinations > 0;
        // :: Construct the update-map
        NavigableMap<String, MatsBrokerDestination> newOrUpdatedDestinations;

        // ?: Was any Mats-destinations removed by scavenge above?
        if (removedDestinations_ForceFullUpdate) {
            // -> Yes, Mats-destinations removed, so create event "update map" consisting of all known destinations.
            newOrUpdatedDestinations = new TreeMap<>();
            newOrUpdatedDestinations.putAll(_currentDestinationsMap);
        }
        // ?: Was there any new or updated mats destinations?
        else if (!fqDestinationsNewOrUpdated.isEmpty()) {
            // -> Yes, new or updated, so construct the event "update map" with new or updated.
            newOrUpdatedDestinations = new TreeMap<>();
            for (String fqDestination : fqDestinationsNewOrUpdated) {
                newOrUpdatedDestinations.put(fqDestination, _currentDestinationsMap.get(fqDestination));
            }
        }
        else {
            // -> No new or changed, so use an empty event "update map".
            newOrUpdatedDestinations = Collections.emptyNavigableMap();
        }

        // :: Create the BrokerInfo object, if we have info
        _brokerInfo = _querier.getCurrentBrokerStatsDto()
                .map(brokerStatsDto -> new BrokerInfoImpl(BROKER_TYPE,
                        brokerStatsDto.brokerName,
                        brokerStatsDto.toJson()))
                .orElse(null);

        // :: Construct and send the update event to listeners.
        DestinationUpdateEventImpl update = new DestinationUpdateEventImpl(false, newOrUpdatedDestinations);
        for (Consumer<DestinationUpdateEvent> listener : _listeners) {
            try {
                listener.accept(update);
            }
            catch (Throwable t) {
                log.error("The listener [" + listener.getClass().getName() + "] threw when being invoked."
                        + " Ignoring.", t);
            }
        }
    }
}
