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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mats3.matsbrokermonitor.activemq.ActiveMqBrokerStatsQuerier.ActiveMqBrokerStatsEvent;
import io.mats3.matsbrokermonitor.activemq.ActiveMqBrokerStatsQuerierImpl.DestinationStatsDto;
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
     * Convenience method if the MatsFactory is set up with the default "mats." MatsDestinationPrefix.
     *
     * @param querier
     *            the querier doing the actual talking with ActiveMQ.
     * @return the newly created instance.
     */
    public static ActiveMqMatsBrokerMonitor create(ActiveMqBrokerStatsQuerier querier) {
        return create(querier, "mats.");
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

    // ===== IMPLEMENTATION

    private static class MatsBrokerDestinationImpl implements MatsBrokerDestination {
        private final long _lastUpdateMillis;
        private final long _lastUpdateBrokerMillis;
        private final String _destinationName;
        private final String _matsStageId;
        private final boolean _isQueue;
        private final boolean _isDlq;
        private final long _numberOfQueuedMessages;
        private final long _numberOfInFlightMessages;
        private final long _headMessageAgeMillis;

        public MatsBrokerDestinationImpl(long lastUpdateMillis, long lastUpdateBrokerMillis, String destinationName,
                String matsStageId, boolean isQueue, boolean isDlq, long numberOfQueuedMessages,
                long numberOfInFlightMessages, long headMessageAgeMillis) {
            _lastUpdateMillis = lastUpdateMillis;
            _lastUpdateBrokerMillis = lastUpdateBrokerMillis;
            _destinationName = destinationName;
            _matsStageId = matsStageId;
            _isQueue = isQueue;
            _isDlq = isDlq;
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
        public String getDestinationName() {
            return _destinationName;
        }

        @Override
        public boolean isQueue() {
            return _isQueue;
        }

        @Override
        public boolean isDlq() {
            return _isDlq;
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
            if (_numberOfInFlightMessages == 0) {
                return OptionalLong.empty();
            }
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
                    ", _isQueue=" + _isQueue +
                    ", _isDlq=" + _isDlq +
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
            String fqName = entry.getKey();
            // DestinationName: remove both "queue://" and "topic://", both are 8 length.
            String destinationName = fqName.substring(8);

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
            boolean isQueue = fqName.startsWith("queue://");

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

            if (log.isDebugEnabled()) log.debug("FQ Name: " + fqName + ", destinationName:[" + destinationName
                    + "], matsStageId:[" + matsStageId + "], isQueue:[" + isQueue + "], isDlq:[" + isDlq
                    + "], queuedMessages:[" + numberOfQueuedMessages + "]");

            // Create the representation
            MatsBrokerDestinationImpl matsBrokerDestination = new MatsBrokerDestinationImpl(lastUpdateMillis,
                    lastUpdateBrokerMillis, destinationName, matsStageId, isQueue, isDlq, numberOfQueuedMessages,
                    numberOfInflightMessages, headMessageAgeMillis);
            // Put it in the map.
            MatsBrokerDestination existingInfo = _currentDestinationsMap.put(fqName, matsBrokerDestination);
            // ?: Was this new, or there an update in number of queued messages?
            if ((existingInfo == null) || existingInfo.getNumberOfQueuedMessages() != numberOfQueuedMessages) {
                // -> Yes, new or updated - so then it is new-or-changed!
                fqDestinationsNewOrUpdated.add(fqName);
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
