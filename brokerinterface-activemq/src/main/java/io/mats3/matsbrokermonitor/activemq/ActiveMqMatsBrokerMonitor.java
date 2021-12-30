package io.mats3.matsbrokermonitor.activemq;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mats3.matsbrokermonitor.activemq.ActiveMqBrokerStatsQuerier.ActiveMqBrokerStatsEvent;
import io.mats3.matsbrokermonitor.activemq.ActiveMqBrokerStatsQuerierImpl.DestinationStatsDto;
import io.mats3.matsbrokermonitor.spi.MatsBrokerMonitor;

/**
 * @author Endre Stølsvik 2021-12-27 14:40 - http://stolsvik.com/, endre@stolsvik.com
 */
public class ActiveMqMatsBrokerMonitor implements MatsBrokerMonitor {
    private static final Logger log = LoggerFactory.getLogger(ActiveMqMatsBrokerMonitor.class);

    static ActiveMqMatsBrokerMonitor create(ActiveMqBrokerStatsQuerierImpl querier, String matsDestinationPrefix) {
        return new ActiveMqMatsBrokerMonitor(querier, matsDestinationPrefix);
    }

    private final ActiveMqBrokerStatsQuerier _querier;

    private final String _matsDestinationPrefix;
    private final String _matsDestinationIndividualDlqPrefix;

    ActiveMqMatsBrokerMonitor(ActiveMqBrokerStatsQuerier querier, String matsDestinationPrefix) {
        if (querier == null) {
            throw new NullPointerException("querier");
        }
        if (matsDestinationPrefix == null) {
            throw new NullPointerException("matsDestinationPrefix");
        }
        _querier = querier;
        _querier.setMatsDestinationPrefix(matsDestinationPrefix);
        _querier.registerListener(this::eventFromQuerier);

        _matsDestinationPrefix = matsDestinationPrefix;
        _matsDestinationIndividualDlqPrefix = Statics.INDIVIDUAL_DLQ_PREFIX + matsDestinationPrefix;
    }

    private final CopyOnWriteArrayList<Consumer<DestinationUpdateEvent>> _listeners = new CopyOnWriteArrayList<>();

    @Override
    public void registerListener(Consumer<DestinationUpdateEvent> listener) {
        _listeners.add(listener);
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
    public Map<String, MatsBrokerDestination> getMatsDestinations() {
        return null;
    }

    // ===== IMPLEMENTATION

    private static class MatsBrokerDestinationImpl implements MatsBrokerDestination {
        private final long _lastUpdateMillis;
        private final String _destinationName;
        private final String _matsStageId;
        private final boolean _isQueue;
        private final boolean _isDlq;
        private final long _numberOfQueuedMessages;

        public MatsBrokerDestinationImpl(long lastUpdateMillis, String destinationName, String matsStageId,
                boolean isQueue, boolean isDlq, long numberOfQueuedMessages) {
            _lastUpdateMillis = lastUpdateMillis;
            _destinationName = destinationName;
            _matsStageId = matsStageId;
            _isQueue = isQueue;
            _isDlq = isDlq;
            _numberOfQueuedMessages = numberOfQueuedMessages;
        }

        @Override
        public long getLastUpdateMillis() {
            return _lastUpdateMillis;
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
    }

    private static class DestinationUpdateEventImpl implements DestinationUpdateEvent {

        @Override
        public boolean isFullUpdate() {
            return false;
        }

        @Override
        public Map<String, MatsBrokerDestination> getNewOrUpdatedDestinations() {
            return null;
        }

        @Override
        public Set<String> getNewDestinations() {
            return null;
        }

        @Override
        public Set<String> getRemovedDestinations() {
            return null;
        }
    }

    private final ConcurrentNavigableMap<String, MatsBrokerDestination> _destinationsMap = new ConcurrentSkipListMap<>();

    private void eventFromQuerier(ActiveMqBrokerStatsEvent event) {
        log.info("Got event [" + event + "].");
        ConcurrentNavigableMap<String, DestinationStatsDto> destStatsDtos = _querier
                .getCurrentDestinationStatsDtos();

        for (Entry<String, DestinationStatsDto> entry : destStatsDtos.entrySet()) {
            String fqName = entry.getKey();
            // DestinationName: remove both "queue://" and "topic://", both are 8 length.
            String destinationName = fqName.substring(8);

            // Whether this is a queue/topic for a MatsStage
            boolean isMatsStageDestination = destinationName.startsWith(_matsDestinationPrefix);

            // Whether this is an individual DLQ for a MatsStage
            boolean isMatsStageDlq = destinationName.startsWith(_matsDestinationIndividualDlqPrefix);

            // Whether this is the global DLQ
            boolean isGlobalDlq = Statics.ACTIVE_MQ_GLOBAL_DLQ_NAME.equals(destinationName);

            // Whether we care about this destination: Either Mats stage or individual DLQ for such, or global DLQ
            boolean relevant = isMatsStageDestination || isMatsStageDlq || isGlobalDlq;

            // ?: Do we care?
            if (!relevant) {
                // -> No, so go to next.
                continue;
            }

            // ----- This is a Mats relevant destination!

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

            log.info("FQ Name: " + fqName + ", destinationName:[" + destinationName + "], matsStageId:[" + matsStageId
                    + "], isQueue:[" + isQueue + "], isDlq:[" + isDlq +
                    "], queuedMessages:[" + numberOfQueuedMessages + "]");

            MatsBrokerDestinationImpl matsBrokerDestination = new MatsBrokerDestinationImpl(lastUpdateMillis,
                    destinationName, matsStageId, isQueue, isDlq, numberOfQueuedMessages);

            // TODO: Evaluate equal-ness before putting
            _destinationsMap.put(destinationName, matsBrokerDestination);
        }

        // We've parsed the update.

        DestinationUpdateEventImpl update = new DestinationUpdateEventImpl();
        for (Consumer<DestinationUpdateEvent> listener : _listeners) {
            try {
                listener.accept(update);
            }
            catch (Throwable t) {
                log.error("The listener [" + listener.getClass().getName() + "] threw when being invoked."
                        + " Ignoring.", t);
            }
        }

        // NOTE: We don't know whether this was all the destinations there are, due to the extreme asynchronous-ness of
        // these updates. So we can't know whether this was a 70% update, with the 30% rest coming in a second update.
        // Thus, we need to keep some kind of tally on what has updated. In particular with the "removed" part.

    }
}
